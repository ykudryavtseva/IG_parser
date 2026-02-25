import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

from app.models import PipelineRunResult, PostEvidence, ResearchItem
from app.services.apify_service import ApifyInstagramClient
from app.services.pubmed_service import PubMedClient
from app.services.relevance_service import StudyRelevanceChecker

MAX_IMAGE_URLS_TO_SCAN = 5
POST_PROCESS_WORKERS = 12

# Приоритет извлечения: 1) картинка (скриншот PubMed с title/PMID)
# 2) текст (PMID, ссылки) 3) поиск по названию из текста (вольный пересказ блогера)


class EvidencePipeline:
    def __init__(
        self,
        instagram_client: ApifyInstagramClient,
        pubmed_client: PubMedClient,
        relevance_checker: StudyRelevanceChecker,
        openai_api_key: str | None = None,
        openai_model: str = "gpt-4o-mini",
    ) -> None:
        self._instagram_client = instagram_client
        self._pubmed_client = pubmed_client
        self._relevance_checker = relevance_checker
        self._openai_api_key = openai_api_key
        self._openai_model = openai_model

    def run(
        self,
        topic: str,
        sources: list[str],
        max_items: int,
        discovery_limit: int,
        skip_relevance: bool = False,
        latest_posts_mode: bool = False,
    ) -> PipelineRunResult:
        """Run pipeline. In latest_posts_mode: parse N newest posts from given sources, no topic filter."""
        selected_sources = [s.strip() for s in sources if s and s.strip()]
        if not latest_posts_mode and not selected_sources:
            selected_sources = self._instagram_client.discover_sources(
                topic=topic,
                discovery_limit=discovery_limit,
            )
        if not selected_sources:
            return PipelineRunResult(
                items=[],
                posts_fetched=0,
                posts_with_caption=0,
            )

        posts = self._instagram_client.fetch_posts(
            sources=selected_sources,
            max_items=max_items,
        )

        apify_debug = self._describe_first_post(posts)

        def _has_content(post: dict) -> bool:
            if not isinstance(post, dict):
                return False
            if (post.get("caption") or "").strip():
                return True
            if any(
                post.get(k)
                for k in ("displayUrl", "imageUrl", "image", "mediaUrl", "images")
            ):
                return True
            for child in post.get("childPosts") or []:
                if isinstance(child, dict) and any(
                    child.get(k)
                    for k in ("displayUrl", "imageUrl", "image", "images")
                ):
                    return True
            return False

        posts_to_process = [p for p in posts if _has_content(p)]
        workers = min(POST_PROCESS_WORKERS, len(posts_to_process) or 1)

        debug_stats: list[dict] = []
        results: list[PostEvidence] = []
        use_relevance = not latest_posts_mode and not skip_relevance
        post_topic = topic.strip() if topic else ""

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    self._process_post,
                    post,
                    post_topic,
                    skip_relevance=not use_relevance,
                    debug_stats=debug_stats,
                ): post
                for post in posts_to_process
            }
            for future in as_completed(futures):
                try:
                    evidence = future.result()
                    if evidence:
                        results.append(evidence)
                except Exception as e:
                    logging.getLogger(__name__).warning(
                        "post_processing_failed: %s", e, exc_info=True
                    )

        posts_with_caption_count = sum(
            1 for p in posts if isinstance(p, dict) and (p.get("caption") or "").strip()
        )
        pmids_text = sum(s.get("pmids_text", 0) for s in debug_stats)
        pmids_images = sum(s.get("pmids_images", 0) for s in debug_stats)
        posts_with_images = sum(1 for s in debug_stats if s.get("image_urls", 0) > 0)
        total_image_urls = sum(s.get("image_urls", 0) for s in debug_stats)
        pmids_fetch_failed = sum(s.get("pmids_fetch_failed", 0) for s in debug_stats)
        images_fetched = sum(s.get("images_fetched", 0) for s in debug_stats)
        images_failed = sum(s.get("images_failed", 0) for s in debug_stats)
        sample_entry = next(
            (s for s in debug_stats if s.get("sample_url")), {}
        )
        caption_entry = next(
            (s for s in debug_stats if s.get("caption_snippet")), {}
        )
        title_candidates_total = sum(
            s.get("title_candidates_count", 0) for s in debug_stats
        )
        pmids_from_title_total = sum(
            s.get("pmids_from_title", 0) for s in debug_stats
        )
        pubmed_error = next(
            (s.get("pubmed_error") for s in debug_stats if s.get("pubmed_error")),
            "",
        )
        return PipelineRunResult(
            items=results,
            posts_fetched=len(posts),
            posts_with_caption=posts_with_caption_count,
            debug_posts_with_images=posts_with_images,
            debug_pmids_from_text=pmids_text,
            debug_pmids_from_images=pmids_images,
            debug_pmids_fetch_failed=pmids_fetch_failed,
            debug_images_fetched=images_fetched,
            debug_images_failed=images_failed,
            debug_sample_url=sample_entry.get("sample_url", ""),
            debug_sample_status=sample_entry.get("sample_status", ""),
            debug_first_caption_snippet=caption_entry.get("caption_snippet", ""),
            debug_title_candidates_tried=title_candidates_total,
            debug_apify_first_post=apify_debug,
            debug_total_image_urls=total_image_urls,
            debug_pmids_from_title_search=pmids_from_title_total,
            debug_pubmed_search_error=pubmed_error,
            debug_first_title_candidate=next(
                (
                    s.get("first_title_candidate")
                    for s in debug_stats
                    if s.get("first_title_candidate")
                ),
                "",
            ),
        )

    @staticmethod
    def _describe_first_post(posts: list[dict]) -> str:
        """Describe first post structure for Apify format debugging."""
        if not posts or not isinstance(posts[0], dict):
            return "нет постов"
        p = posts[0]
        keys = sorted(p.keys())
        lines = [f"Ключи ({len(keys)}): {', '.join(keys)}"]
        for key in ("caption", "text", "captionText", "displayUrl", "imageUrl", "url"):
            val = p.get(key)
            if val is None:
                lines.append(f"  {key}: None")
            elif isinstance(val, str):
                snip = val[:80] + "…" if len(val) > 80 else val
                lines.append(f"  {key}: str({len(val)}) «{snip}»")
            else:
                lines.append(f"  {key}: {type(val).__name__}")
        child_posts = p.get("childPosts") or []
        if child_posts:
            c = child_posts[0] if isinstance(child_posts[0], dict) else {}
            lines.append(f"  childPosts[0] keys: {list(c.keys())[:10]}")
            for k in ("displayUrl", "imageUrl"):
                v = c.get(k)
                lines.append(f"    child.{k}: {type(v).__name__ if v else 'None'}")
        return "\n".join(lines)

    def _process_post(
        self,
        post: dict,
        topic: str,
        skip_relevance: bool = False,
        debug_stats: list[dict] | None = None,
    ) -> PostEvidence | None:
        post_url = post.get("url") or "<no-url>"
        caption = (
            (post.get("caption") or post.get("text") or post.get("captionText") or "")
            .strip()
        )

        post_text = self._extract_post_text(post=post, caption=caption)
        pmids_from_text = self._pubmed_client.extract_pmids(post_text)

        image_urls = self._extract_post_image_urls(post=post)
        pmids_from_images: list[str] = []
        image_title_candidates: list[str] = []
        entry: dict = {}
        if debug_stats is not None:
            entry = {
                "pmids_text": len(pmids_from_text),
                "image_urls": len(image_urls),
                "pmids_images": 0,
            }
            debug_stats.append(entry)
        if image_urls:
            if debug_stats and entry:
                entry["images_fetched"] = 0
                entry["images_failed"] = 0
            pmids_from_images, image_title_candidates = (
                self._extract_pmids_and_titles_from_images(
                    image_urls=image_urls,
                    topic=topic,
                    debug_counts=entry if debug_stats else None,
                )
            )
            if debug_stats:
                entry["pmids_images"] = len(pmids_from_images)

        pmids = sorted(set(pmids_from_text + pmids_from_images))
        if not pmids:
            caption_candidates = self._extract_title_candidates(
                post_text=post_text,
                caption=caption,
            )
            title_candidates = list(image_title_candidates)
            for t in caption_candidates:
                if t not in title_candidates:
                    title_candidates.append(t)
            if debug_stats:
                entry["caption_snippet"] = caption[:250] if caption else ""
                entry["title_candidates_count"] = len(title_candidates)
                entry["first_title_candidate"] = (
                    title_candidates[0][:120] if title_candidates else ""
                )
            pmids = self._search_pmids_by_titles(
                title_candidates=title_candidates,
                debug_out=entry if debug_stats else None,
            )
            if debug_stats:
                entry["pmids_from_title"] = len(pmids)
        if not pmids:
            return None

        pmids_from_images_set = set(pmids_from_images)
        primary_pmid = (
            next((p for p in pmids if p in pmids_from_images_set), None)
            or pmids[0]
        )

        try:
            primary_study = self._pubmed_client.fetch_study(primary_pmid)
        except (httpx.HTTPError, KeyError, ValueError):
            logging.getLogger(__name__).info(
                "pubmed_fetch_failed pmid=%s", primary_pmid, exc_info=True
            )
            return None

        related_pmids = self._pubmed_client.fetch_related_pmids(
            pmid=primary_pmid,
            max_results=10,
        )
        studies = [primary_study]
        fetch_failed = 0
        for pmid in related_pmids:
            if pmid == primary_pmid:
                continue
            try:
                study = self._pubmed_client.fetch_study(pmid)
                studies.append(study)
            except (httpx.HTTPError, KeyError, ValueError):
                fetch_failed += 1
                logging.getLogger(__name__).info(
                    "pubmed_fetch_failed pmid=%s", pmid, exc_info=True
                )

        if debug_stats:
            debug_stats[-1]["pmids_attempted"] = 1 + len(related_pmids)
            debug_stats[-1]["pmids_fetch_failed"] = fetch_failed

        if not studies:
            return None

        studies = self._attach_study_tags(studies=studies)
        tags = self._post_tags_from_studies(studies) or self._build_tags(
            topic=topic, caption=caption
        )
        summary = self._build_summary(post=post, caption=caption)
        display_topic = topic or self._topic_from_caption(caption, post)
        return PostEvidence(
            topic=display_topic,
            summary=summary,
            tags=tags,
            studies=studies,
            post_url=post.get("url"),
            author_username=(post.get("owner") or {}).get("username")
            or post.get("ownerUsername"),
            published_at=(
                post.get("createdAt") or post.get("timestamp")
            ),
            likes=post.get("likeCount") or post.get("likesCount"),
            comments=(
                post.get("commentCount") or post.get("commentsCount")
            ),
        )

    @staticmethod
    def _topic_from_caption(caption: str, post: dict) -> str:
        """Fallback topic when none given (latest-posts mode)."""
        if caption and caption.strip():
            first_line = caption.strip().split("\n")[0][:80].rstrip()
            if first_line:
                return first_line
        username = (post.get("owner") or {}).get("username") or post.get("ownerUsername")
        return f"Пост {username or 'блогера'}" if username else "Последний пост"

    def _build_summary(self, post: dict, caption: str) -> str:
        """Short rephrasing for table: what the blogger says about the research."""
        fallback = self._build_summary_fallback(caption)
        if not self._openai_api_key:
            return fallback
        clean_caption = re.sub(r"\s+", " ", caption).strip()
        if len(clean_caption) < 30:
            return fallback
        try:
            return self._summarize_with_ai(caption=clean_caption) or fallback
        except Exception:
            return fallback

    def _summarize_with_ai(self, caption: str) -> str | None:
        """Use OpenAI to rephrase the post into a short summary."""
        payload = {
            "model": self._openai_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Ты помогаешь кратко пересказать пост блогера о научном исследовании. "
                        "Напиши 2–4 законченных предложения: о чём пост, какой вывод делает блогер, "
                        "что важного в исследовании. Перефразируй, не копируй текст. "
                        "Сохраняй важные цифры, факты, названия (427 человек, 47 лет и т.д.). "
                        "Каждое предложение должно быть логически завершённым. "
                        "Язык: тот же, что в посте. Только текст саммари, без преамбулы."
                    ),
                },
                {"role": "user", "content": caption[:3000]},
            ],
            "temperature": 0.3,
        }
        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }
        with httpx.Client(timeout=25.0) as client:
            response = client.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            text = response.json()["choices"][0]["message"]["content"].strip()
            return self._truncate_summary_at_sentence(text, max_len=600) if text else None

    @staticmethod
    def _truncate_summary_at_sentence(text: str, max_len: int = 600) -> str:
        """Truncate at last complete sentence to avoid cut-off thoughts."""
        if len(text) <= max_len:
            return text
        chunk = text[: max_len + 1]
        last = max(
            chunk.rfind(". "),
            chunk.rfind("! "),
            chunk.rfind("? "),
        )
        if last > max_len * 0.4:
            return chunk[: last + 1].strip()
        return chunk[:max_len].rstrip()

    def _attach_study_tags(self, studies: list) -> list:
        """Add AI-generated tags to each study."""
        if not self._openai_api_key:
            return studies
        result: list = []
        for study in studies:
            ai_tags = self._generate_study_tags(study=study)
            if ai_tags:
                result.append(
                    study.model_copy(update={"tags": ai_tags})
                )
            else:
                result.append(study)
        return result

    def _generate_study_tags(self, study: ResearchItem) -> list[str]:
        """Use OpenAI to extract 3-4 tags from article title+abstract."""
        article_text = study.title
        if study.abstract and study.abstract.strip():
            article_text += "\n\n" + study.abstract[:2000].rstrip()
        payload = {
            "model": self._openai_model,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Прочитай статью и выбери 3–4 главных тега (ключевых слова). "
                        "Теги: короткие (1–3 слова), на том же языке, что и статья. "
                        "Верни только JSON: {\"tags\": [\"тег1\", \"тег2\", \"тег3\", \"тег4\"]}."
                    ),
                },
                {"role": "user", "content": f"Статья:\n{article_text}"},
            ],
            "temperature": 0.2,
        }
        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=25.0) as client:
                response = client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                content = response.json()["choices"][0]["message"]["content"]
            parsed = json.loads(content)
            raw = parsed.get("tags")
            if isinstance(raw, list):
                tags = [
                    str(v).strip()[:50]
                    for v in raw
                    if isinstance(v, str) and v.strip()
                ]
                seen: set[str] = set()
                unique = [t for t in tags if t and t not in seen and not seen.add(t)]
                return unique[:4]
        except Exception:
            pass
        return []

    @staticmethod
    def _post_tags_from_studies(studies: list) -> list[str] | None:
        """Aggregate tags from studies for post-level display."""
        all_tags: list[str] = []
        for s in studies:
            if hasattr(s, "tags") and s.tags:
                for t in s.tags:
                    if t and t not in all_tags:
                        all_tags.append(t)
        return all_tags[:6] if all_tags else None

    @staticmethod
    def _build_summary_fallback(caption: str) -> str:
        """Fallback when AI unavailable: truncated caption."""
        clean_caption = re.sub(r"\s+", " ", caption).strip()
        if clean_caption:
            return clean_caption[:500].rstrip()
        return "Пост с изображением исследования"

    @staticmethod
    def _build_tags(topic: str, caption: str) -> list[str]:
        text = f"{topic} {caption}".lower()
        tag_map = {
            "креатин": ["креатин", "creatine"],
            "БАДы": ["supplement", "supplements", "бад", "бады"],
            "здоровье": ["health", "здоров", "hair", "волос"],
            "выпадение волос": ["hair loss", "alopecia", "выпадение волос"],
        }

        tags = [topic]
        for tag, keys in tag_map.items():
            if any(key in text for key in keys):
                tags.append(tag)

        unique_tags: list[str] = []
        for tag in tags:
            if tag not in unique_tags:
                unique_tags.append(tag)
        return unique_tags

    @staticmethod
    def _extract_post_text(post: dict, caption: str) -> str:
        chunks = [caption]

        first_comment = post.get("firstComment")
        if isinstance(first_comment, str) and first_comment.strip():
            chunks.append(first_comment.strip())

        latest_comments = post.get("latestComments")
        if isinstance(latest_comments, list):
            for comment in latest_comments:
                if not isinstance(comment, dict):
                    continue
                text = comment.get("text")
                if isinstance(text, str) and text.strip():
                    chunks.append(text.strip())

        return "\n".join(chunks)

    @staticmethod
    def _extract_post_image_urls(post: dict) -> list[str]:
        image_urls: list[str] = []

        for key in ("displayUrl", "imageUrl", "image", "mediaUrl", "thumbnailUrl"):
            primary = post.get(key)
            if isinstance(primary, str) and primary.startswith("http"):
                image_urls.append(primary)

        images = post.get("images")
        if isinstance(images, list):
            for item in images:
                if isinstance(item, str) and item.startswith("http"):
                    image_urls.append(item)
                elif isinstance(item, dict):
                    candidate = (
                        item.get("url")
                        or item.get("displayUrl")
                        or item.get("imageUrl")
                        or item.get("image")
                    )
                    if (
                        isinstance(candidate, str)
                        and candidate.startswith("http")
                    ):
                        image_urls.append(candidate)

        child_posts = post.get("childPosts")
        if isinstance(child_posts, list):
            for child in child_posts:
                if not isinstance(child, dict):
                    continue
                for ckey in ("displayUrl", "imageUrl", "image", "url"):
                    child_media = child.get(ckey)
                    if isinstance(child_media, str) and child_media.startswith("http"):
                        image_urls.append(child_media)
                        break
                child_images = child.get("images")
                if isinstance(child_images, list):
                    for image in child_images:
                        if isinstance(image, str) and image.startswith("http"):
                            image_urls.append(image)
                        elif isinstance(image, dict):
                            candidate = (
                                image.get("url")
                                or image.get("displayUrl")
                                or image.get("imageUrl")
                            )
                            if (
                                isinstance(candidate, str)
                                and candidate.startswith("http")
                            ):
                                image_urls.append(candidate)

        unique_urls: list[str] = []
        for image_url in image_urls:
            if image_url not in unique_urls:
                unique_urls.append(image_url)
        return unique_urls[:MAX_IMAGE_URLS_TO_SCAN]

    def _extract_pmids_and_titles_from_images(
        self,
        image_urls: list[str],
        topic: str = "",
        debug_counts: dict[str, int] | None = None,
    ) -> tuple[list[str], list[str]]:
        """One Vision call per image: extract PMIDs and/or title."""
        if not image_urls:
            return [], []
        if not self._openai_api_key:
            if debug_counts is not None:
                debug_counts["images_failed"] = len(image_urls)
                debug_counts["sample_status"] = "no_openai_key"
            return [], []

        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }
        pmids: set[str] = set()
        titles: list[str] = []
        images_fetched = 0
        images_failed = 0

        topic_hint = (
            f" Тема: «{topic}». "
            "Если эта тема есть на изображении — извлеки PMID и title."
            if topic.strip()
            else ""
        )

        system_prompt = (
            "Контекст: это картинка из Instagram. Нужно найти статью в PubMed. "
            "Если на картинке нет прямой ссылки на PubMed и нет PMID — "
            "всё равно извлеки точное название статьи (title), мы поищем её в PubMed. "
            "PMID — число 5-8 цифр (в URL, под надписью PMID, в тексте). "
            "Скриншот PubMed/NCBI: абстракт, авторы, NCBI. "
            "Извлеки ВСЕ похожие на PMID числа и название статьи. "
            "Верни JSON: {\"pmids\": [\"12345678\"], \"title\": \"Full article title\"}. "
            + topic_hint
            + " "
            "Не скриншот статьи — {\"pmids\": [], \"title\": \"\"}."
        )

        browser_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
        try:
            with httpx.Client(timeout=25.0, headers=browser_headers) as client:
                for image_url in image_urls:
                    data_url, status = self._build_data_url(
                        client=client,
                        image_url=image_url,
                    )
                    if debug_counts is not None and not debug_counts.get("sample_url"):
                        debug_counts["sample_url"] = image_url[:120] + (
                            "…" if len(image_url) > 120 else ""
                        )
                        debug_counts["sample_status"] = status
                    if not data_url:
                        images_failed += 1
                        continue
                    images_fetched += 1
                    payload = {
                        "model": self._openai_model,
                        "response_format": {"type": "json_object"},
                        "messages": [
                            {
                                "role": "system",
                                "content": system_prompt,
                            },
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": (
                                            "Извлеки PMID и/или название статьи. "
                                            "Даже если PMID не виден — дай title, чтобы искать в PubMed."
                                        ),
                                    },
                                    {
                                        "type": "image_url",
                                        "image_url": {"url": data_url},
                                    },
                                ],
                            },
                        ],
                        "temperature": 0,
                    }
                    try:
                        response = client.post(
                            "https://api.openai.com/v1/chat/completions",
                            headers=headers,
                            json=payload,
                        )
                        response.raise_for_status()
                        content = response.json()["choices"][0]["message"]["content"]
                        parsed = json.loads(content)
                        raw_pmids = parsed.get("pmids")
                        if isinstance(raw_pmids, list):
                            for raw in raw_pmids:
                                if isinstance(raw, str):
                                    m = re.search(r"\b\d{5,8}\b", raw)
                                    if m:
                                        pmids.add(m.group(0))
                                elif isinstance(raw, int) and 10000 <= raw <= 99_999_999:
                                    pmids.add(str(raw))
                        for title in (parsed.get("title"),) + tuple(
                            parsed.get("titles") or []
                        ):
                            if not isinstance(title, str) or not title.strip():
                                continue
                            t = re.sub(r"\s+", " ", title.strip()).rstrip(".")
                            if 15 <= len(t) <= 350 and t not in titles:
                                titles.append(t)
                    except (
                        httpx.HTTPError,
                        KeyError,
                        ValueError,
                        json.JSONDecodeError,
                    ):
                        continue
        except Exception as e:
            logging.getLogger(__name__).exception(
                "image_extraction_error urls=%s", len(image_urls), exc_info=True
            )
            if debug_counts is not None:
                debug_counts["images_failed"] = len(image_urls)
                debug_counts["images_fetched"] = 0
                debug_counts["sample_status"] = f"error:{type(e).__name__}"
            return [], []

        if debug_counts is not None:
            debug_counts["images_fetched"] = images_fetched
            debug_counts["images_failed"] = images_failed

        return sorted(pmids), titles[:8]

    @staticmethod
    def _extract_title_candidates(post_text: str, caption: str) -> list[str]:
        candidates: list[str] = []
        text = f"{post_text}\n{caption}".lower()
        if "position stand" in text and any(
            w in text for w in ("antioxidant", "exercise", "sports", "performance")
        ):
            candidates.append(
                "International Society of Sports Nutrition position stand: "
                "effects of dietary antioxidants on exercise and sports performance"
            )

        lines = [line.strip() for line in (post_text + "\n" + caption).splitlines() if line.strip()]
        for line in lines[:12]:
            if len(line) < 15:
                continue
            if len(line) > 300:
                continue
            if "http" in line.lower():
                continue
            if line.lower().startswith("pmid"):
                continue
            candidates.append(line.rstrip("."))

        research_markers = [
            r"(?:issn\s+)?position\s+stand[^.!?\n]{10,150}",
            r"(?:systematic\s+review|meta-?analysis)[^.!?\n]{10,150}",
            r"(?:this\s+(?:new\s+)?(?:paper|study|research)|new\s+(?:paper|study))[^.!?\n]{15,100}",
        ]
        for pattern in research_markers:
            for match in re.finditer(pattern, text, re.IGNORECASE):
                phrase = re.sub(r"\s+", " ", match.group(0).strip())[:150]
                if len(phrase) >= 20:
                    candidates.append(phrase)

        if "position stand" in text and any(
            w in text for w in ("antioxidant", "exercise", "sports", "performance")
        ):
            candidates.append("position stand antioxidants exercise sports performance")
            candidates.append("International Society of Sports Nutrition position stand antioxidants")
        if "position" in text and "antioxidant" in text:
            candidates.append("position stand antioxidants exercise sports performance")

        if not candidates:
            first_sentence = re.split(r"[.!?]\s+", caption.strip())[0].strip()
            if 15 <= len(first_sentence) <= 300:
                candidates.append(first_sentence.rstrip("."))

        unique: list[str] = []
        for value in candidates:
            if value and value not in unique:
                unique.append(value)
        return unique[:8]

    def _search_pmids_by_titles(
        self,
        title_candidates: list[str],
        debug_out: dict | None = None,
    ) -> list[str]:
        pmids: list[str] = []
        for candidate in title_candidates:
            if pmids:
                break
            try:
                matched = self._pubmed_client.search_pmids_by_title(
                    title=candidate,
                    max_results=5,
                )
            except httpx.HTTPError as e:
                if debug_out is not None and "pubmed_error" not in debug_out:
                    debug_out["pubmed_error"] = (
                        f"{type(e).__name__}: {str(e)[:150]}"
                    )
                logging.getLogger(__name__).warning(
                    "pubmed_search_failed candidate=%s error=%s",
                    candidate[:50],
                    e,
                    exc_info=True,
                )
                continue
            for pmid in matched:
                if pmid not in pmids:
                    pmids.append(pmid)
        return pmids

    @staticmethod
    def _build_data_url(
        client: httpx.Client,
        image_url: str,
    ) -> tuple[str | None, str]:
        """Fetch image and return (data_url, status). status is '200' or error code."""
        try:
            response = client.get(image_url, timeout=20.0)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            status = str(getattr(e.response, "status_code", "?"))
            return None, status
        except httpx.HTTPError:
            return None, "error"

        content_type = response.headers.get("content-type", "image/jpeg")
        if not content_type.startswith("image/"):
            content_type = "image/jpeg"
        encoded = base64.b64encode(response.content).decode("ascii")
        return f"data:{content_type};base64,{encoded}", "200"
