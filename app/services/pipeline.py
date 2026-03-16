import base64
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import httpx

from app.models import PipelineRunResult, PostEvidence, ResearchItem


def _parse_post_date(value: str | None) -> datetime | None:
    """Parse ISO or similar date string for sorting. Returns None if unparseable."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")[:26])
    except (ValueError, TypeError):
        return None


def _sort_by_date_oldest_first(items: list[PostEvidence]) -> list[PostEvidence]:
    """Sort PostEvidence by published_at ascending (oldest first, newest last)."""

    def _key(item: PostEvidence) -> tuple[int, float]:
        dt = _parse_post_date(item.published_at)
        return (1 if dt is None else 0, dt.timestamp() if dt else 0.0)

    return sorted(items, key=_key)


from app.services.apify_service import ApifyInstagramClient
from app.services.pubmed_service import PubMedClient
from app.services.relevance_service import StudyRelevanceChecker
from app.services.transcription_service import TranscriptionProvider

MAX_IMAGE_URLS_TO_SCAN = 8
POST_PROCESS_WORKERS = 6  # fewer workers to avoid NCBI rate-limit / hangs

# Приоритет извлечения: 1) картинка (скриншот PubMed с title/PMID)
# 2) текст (PMID, ссылки) 3) поиск по названию из текста (вольный пересказ блогера)

CITATION_PATTERN = re.compile(
    r"([A-Za-z][\w-]*)\s+et\s+al\.?,?\s*,\s*(.+?)\s+(\d{4})\b",
    re.IGNORECASE,
)
# "Author et al., Journal, Year" — comma before year (e.g. Li et al., Nat Med, 2026)
CITATION_PATTERN_COMMA_YEAR = re.compile(
    r"([A-Za-z][\w-]*)\s+et\s+al\.?,?\s*,\s*([^,]+),\s*(\d{4})\b",
    re.IGNORECASE,
)
CITATION_PATTERN_NO_JOURNAL = re.compile(
    r"([A-Za-z][\w-]*)\s+et\s+al\.?,?\s*,\s*(\d{4})\b(?:\s*\(([^)]+)\))?",
    re.IGNORECASE,
)
CITATIONS_HEADER_PATTERN = re.compile(
    r"^\s*(?:citations|references)\s*:\s*",
    re.IGNORECASE | re.MULTILINE,
)
# Strip bullets, dashes, numbered items before citation matching
CITATION_LINE_PREFIX = re.compile(r"^[\s\u2022\u2023\u25E6\-\*\d.]+\s*", re.UNICODE)

AD_MARKERS = (
    "#реклама",
    "#ad",
    "#ads",
    "#партнёрство",
    "#partnership",
    "sponsored",
    "paid partnership",
)

TRIVIAL_PHRASES = (
    "thanks for watching",
    "subscribe",
    "follow for more",
    "like and share",
    "comment below",
    "you know it's spring when",
    "you know it's summer when",
)

EVIDENCE_TERMS = (
    "pmid",
    "pubmed",
    "position stand",
    "issn",
    "research",
    "study",
    "meta-analysis",
    "систематический обзор",
    "исслед",
    "научн",
)

SUBSTANCE_TERMS = (
    *EVIDENCE_TERMS,
    "evidence",
    "data show",
    "data suggests",
    "finding",
    "данные",
    "вывод",
    "результат",
    "рассужд",
    "effect",
    "affect",
    "performance",
    "shows",
    "shown",
    "demonstrates",
    "suggests",
    "suggested",
    "training",
    "athletes",
    "according to",
    "found that",
    "colleagues",
)


class EvidencePipeline:
    def __init__(
        self,
        instagram_client: ApifyInstagramClient,
        pubmed_client: PubMedClient,
        relevance_checker: StudyRelevanceChecker,
        openai_api_key: str | None = None,
        openai_model: str = "gpt-4o-mini",
        transcription_service: TranscriptionProvider | None = None,
    ) -> None:
        self._instagram_client = instagram_client
        self._pubmed_client = pubmed_client
        self._relevance_checker = relevance_checker
        self._openai_api_key = openai_api_key
        self._openai_model = openai_model
        self._transcription_service = transcription_service

    def run(
        self,
        topic: str,
        sources: list[str],
        max_items: int,
        discovery_limit: int,
        skip_relevance: bool = False,
        latest_posts_mode: bool = False,
        only_posts_newer_than: str | None = None,
        processed_post_ids: set[str] | None = None,
        skip_scientific_filter: bool = False,
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

        posts, apify_error = self._instagram_client.fetch_posts(
            sources=selected_sources,
            max_items=max_items,
            only_posts_newer_than=only_posts_newer_than,
        )

        apify_debug = self._describe_first_post(posts)

        def _has_content(post: dict) -> bool:
            if not isinstance(post, dict):
                return False
            if (post.get("caption") or "").strip():
                return True
            if any(
                post.get(k)
                for k in (
                    "displayUrl",
                    "imageUrl",
                    "image",
                    "mediaUrl",
                    "images",
                    "videoUrl",
                )
            ):
                return True
            for child in post.get("childPosts") or []:
                if not isinstance(child, dict):
                    continue
                if any(
                    child.get(k)
                    for k in (
                        "displayUrl",
                        "imageUrl",
                        "image",
                        "images",
                        "videoUrl",
                    )
                ):
                    return True
            return False

        processed_set = processed_post_ids or set()
        filtered = []
        for p in posts:
            if not _has_content(p):
                continue
            post_id = p.get("id") or p.get("url") or ""
            if post_id and post_id in processed_set:
                continue
            if self._is_non_research_post(p):
                continue
            filtered.append(p)

        posts_to_process = filtered
        workers = (
            1
            if latest_posts_mode
            else min(POST_PROCESS_WORKERS, len(posts_to_process) or 1)
        )

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
                    skip_scientific_filter=skip_scientific_filter,
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

        results = _sort_by_date_oldest_first(results)

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
        sample_entry = next((s for s in debug_stats if s.get("sample_url")), {})
        caption_entry = next((s for s in debug_stats if s.get("caption_snippet")), {})
        title_candidates_total = sum(
            s.get("title_candidates_count", 0) for s in debug_stats
        )
        pmids_from_title_total = sum(s.get("pmids_from_title", 0) for s in debug_stats)
        pubmed_error = next(
            (s.get("pubmed_error") for s in debug_stats if s.get("pubmed_error")),
            "",
        )
        transcript_reason = next(
            (
                s.get("transcript_reason", "")
                for s in debug_stats
                if s.get("transcript_reason")
                and s.get("transcript_reason") not in ("ok", "unknown")
            ),
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
            debug_apify_error=apify_error or "",
            debug_transcript_reason=transcript_reason,
        )

    @staticmethod
    def _is_trivial_raw_post(caption: str, transcript: str | None) -> bool:
        """Skip raw posts with no research/evidence cues (pmid, study, research, etc.)."""
        combined = f"{(caption or '').strip()} {(transcript or '').strip()}".strip()
        lower = combined.lower()
        if any(p in lower for p in TRIVIAL_PHRASES) and len(combined) < 150:
            return True
        if not any(term in lower for term in EVIDENCE_TERMS):
            return True
        return False

    def _is_scientific_post_content(
        self, caption: str, transcript: str | None
    ) -> bool:
        """
        LLM classifier: does the post contain scientific reasoning/conclusion?
        Excludes: event coverage, race results, congratulations, lifestyle fluff.
        """
        combined = f"{(caption or '').strip()} {(transcript or '').strip()}".strip()
        if not combined or self._is_trivial_raw_post(caption, transcript):
            return False
        if not self._openai_api_key:
            return not self._is_trivial_raw_post(caption, transcript)

        prompt = (
            "Фильтр: полезный ли это экспертный контент? Отвечай JSON: {\"scientific\": true|false}.\n\n"
            "Включить (true): разбор исследований, механизмов, данных; цитаты/обсуждение литературы; "
            "экспертные рассуждения на тему спорта/питания/здоровья — даже без прямых ссылок; "
            "опыт спикера с обоснованием «почему так»; рассуждения, похожие на исследовательский подход; "
            "полезная спортивная информация с аргументацией. Не обязательно PMID или «study shows».\n\n"
            "Исключить (false): мотивационный флер без содержания («люби цели», «расти»); "
            "поздравления; отчёты о соревнованиях; реклама коучинга; "
            "жизненные советы без рассуждений. Goals/training/growth без аргументации — false."
        )
        payload = {
            "model": self._openai_model,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": combined[:4000]},
            ],
            "temperature": 0,
        }
        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=15.0) as client:
                r = client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                )
                r.raise_for_status()
                content = r.json()["choices"][0]["message"]["content"]
                parsed = json.loads(content)
                return bool(parsed.get("scientific"))
        except Exception:
            return not self._is_trivial_raw_post(caption, transcript)

    def _is_non_research_post(self, post: dict) -> bool:
        """Skip ads, sponsored posts, pure marketing."""
        caption = (
            post.get("caption") or post.get("text") or post.get("captionText") or ""
        ).lower()
        for marker in AD_MARKERS:
            if marker.lower() in caption:
                return True
        return False

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
        skip_scientific_filter: bool = False,
    ) -> PostEvidence | None:
        post_url = post.get("url") or "<no-url>"
        caption = (
            post.get("caption") or post.get("text") or post.get("captionText") or ""
        ).strip()

        post_text = self._extract_post_text(post=post, caption=caption)
        transcript: str | None = None
        pmids_from_transcript: list[str] = []
        video_url = self._extract_video_url(post=post)
        transcript_reason = "no_video_url" if not video_url else ""
        if (
            video_url
            and self._transcription_service
            and self._detect_content_type(post=post) == "reel"
        ):
            svc = self._transcription_service
            if hasattr(svc, "transcribe_with_reason"):
                transcript, transcript_reason = svc.transcribe_with_reason(video_url)
            else:
                transcript = svc.transcribe(video_url)
                transcript_reason = "ok" if transcript else "unknown"
        elif video_url and not self._transcription_service:
            transcript_reason = "no_transcription_service"
            if transcript:
                post_text = f"{post_text}\n\n{transcript}".strip()
                pmids_from_transcript = self._pubmed_client.extract_pmids(transcript)
        pmids_from_text = self._pubmed_client.extract_pmids(post_text)

        content_type = self._detect_content_type(post=post)
        image_urls = self._extract_post_image_urls(post=post)
        pmids_from_images: list[str] = []
        image_title_candidates: list[str] = []
        first_infographic_url: str | None = None
        entry: dict = {}
        if debug_stats is not None:
            entry = {
                "pmids_text": len(pmids_from_text),
                "image_urls": len(image_urls),
                "pmids_images": 0,
                "transcript_reason": transcript_reason,
            }
            debug_stats.append(entry)
        if image_urls and content_type != "reel":
            if debug_stats and entry:
                entry["images_fetched"] = 0
                entry["images_failed"] = 0
            pmids_from_images, image_title_candidates, first_infographic_url = (
                self._extract_pmids_and_titles_from_images(
                    image_urls=image_urls,
                    topic=topic,
                    debug_counts=entry if debug_stats else None,
                )
            )
            if debug_stats:
                entry["pmids_images"] = len(pmids_from_images)
            if first_infographic_url:
                entry["first_infographic_url"] = first_infographic_url

        pmids = sorted(set(pmids_from_text + pmids_from_transcript + pmids_from_images))
        # PMID только из картинки — проверяем, что контент научный (иначе false positive Vision)
        if (
            pmids
            and not pmids_from_text
            and not pmids_from_transcript
            and not skip_scientific_filter
            and not self._is_scientific_post_content(caption, transcript)
        ):
            return None
        if not pmids:
            author_text = self._extract_author_text_only(post=post, caption=caption)
            citation_queries, context_queries = self._parse_citation_lines(author_text)
            high_conf_candidates = self._extract_high_confidence_title_candidates(
                post_text=post_text,
                caption=caption,
            )
            title_candidates = list(image_title_candidates)
            for q in citation_queries + context_queries:
                if q not in title_candidates:
                    title_candidates.insert(0, q)
            for c in high_conf_candidates:
                if c not in title_candidates:
                    title_candidates.append(c)
            # Images in post/carousel = likely PubMed screenshots — add full caption
            # search (dangarnernutrition-style accounts)
            if image_urls and content_type != "reel":
                for c in self._extract_title_candidates(post_text, caption):
                    if c not in title_candidates:
                        title_candidates.append(c)
            if title_candidates:
                if debug_stats and entry:
                    entry["title_candidates_count"] = len(title_candidates)
                    entry["first_title_candidate"] = (
                        title_candidates[0][:120] if title_candidates else ""
                    )
                pmids = self._search_pmids_by_titles(
                    title_candidates=title_candidates,
                    citation_queries=citation_queries,
                    debug_out=entry if debug_stats else None,
                )
                if debug_stats and entry:
                    entry["pmids_from_title"] = len(pmids)
                # PMID только из поиска по подписи — контент должен быть научным
                if (
                    pmids
                    and not skip_scientific_filter
                    and not self._is_scientific_post_content(caption, transcript)
                ):
                    return None
        if debug_stats and not pmids:
            entry["caption_snippet"] = caption[:250] if caption else ""
        first_infographic = (
            entry.get("first_infographic_url")
            if (debug_stats and entry)
            else first_infographic_url
        )
        image_url = first_infographic or self._get_first_image_url(post=post) or (
            image_urls[0] if image_urls else None
        )
        if content_type == "reel":
            image_url = None  # reels: only transcript/description, no image
        has_media = bool(
            post.get("displayUrl") or post.get("imageUrl") or post.get("childPosts")
        )
        raw_content = bool(caption or has_media or transcript)
        if not pmids:
            if raw_content and (
                skip_scientific_filter
                or self._is_scientific_post_content(caption, transcript)
            ):
                tags_raw = self._build_tags(topic=topic, caption=caption)
                summary_raw = self._build_summary(
                    post=post, caption=caption, transcript=transcript
                )
                return PostEvidence(
                    topic=self._topic_from_caption(caption, post),
                    summary=summary_raw,
                    tags=tags_raw,
                    studies=[],
                    post_url=post.get("url"),
                    author_username=(post.get("owner") or {}).get("username")
                    or post.get("ownerUsername"),
                    published_at=post.get("createdAt") or post.get("timestamp"),
                    likes=post.get("likeCount") or post.get("likesCount"),
                    comments=post.get("commentCount") or post.get("commentsCount"),
                    content_type=content_type,
                    caption=caption or "",
                    image_url=image_url,
                    transcript=transcript,
                )
            return None

        pmids_from_images_set = set(pmids_from_images)
        ordered_pmids = list(dict.fromkeys(pmids))
        primary_pmid = (
            next((p for p in ordered_pmids if p in pmids_from_images_set), None)
            or ordered_pmids[0]
        )

        def _citation_source(pmid: str) -> str:
            if pmid in pmids_from_images:
                return "картинка"
            if pmid in pmids_from_transcript:
                return "транскрипт"
            return "описание"

        studies: list = []
        fetch_failed = 0
        for pmid in ordered_pmids[:12]:
            try:
                study = self._pubmed_client.fetch_study(pmid)
                study = study.model_copy(
                    update={"citation_source": _citation_source(pmid)}
                )
                studies.append(study)
            except (httpx.HTTPError, KeyError, ValueError):
                fetch_failed += 1
                logging.getLogger(__name__).info(
                    "pubmed_fetch_failed pmid=%s", pmid, exc_info=True
                )

        if not studies:
            return None

        if debug_stats:
            debug_stats[-1]["pmids_attempted"] = len(ordered_pmids)
            debug_stats[-1]["pmids_fetch_failed"] = fetch_failed

        existing_pmids = {s.pmid for s in studies}
        if len(studies) < 5:
            related_pmids = self._pubmed_client.fetch_related_pmids(
                primary_pmid, max_results=3
            )
            for r_pmid in related_pmids:
                if r_pmid in existing_pmids:
                    continue
                try:
                    related_study = self._pubmed_client.fetch_study(r_pmid)
                    related_study = related_study.model_copy(
                        update={"citation_source": "похожее"}
                    )
                    studies.append(related_study)
                    existing_pmids.add(r_pmid)
                except (httpx.HTTPError, KeyError, ValueError):
                    pass

        studies = self._attach_study_tags(studies=studies)
        if len(studies) > 1:
            primary_idx = next(
                (i for i, s in enumerate(studies) if s.pmid == primary_pmid),
                0,
            )
            primary = studies[primary_idx]
            others = [s for i, s in enumerate(studies) if i != primary_idx]
            explicit = [s for s in others if s.citation_source != "похожее"]
            similar = [s for s in others if s.citation_source == "похожее"]
            key_year = lambda s: s.year if s.year is not None else -1
            explicit_sorted = sorted(explicit, key=key_year, reverse=True)
            similar_sorted = sorted(similar, key=key_year, reverse=True)
            studies = [primary] + explicit_sorted + similar_sorted

        tags = self._post_tags_from_studies(studies) or self._build_tags(
            topic=topic, caption=caption
        )
        summary = self._build_summary(post=post, caption=caption, transcript=transcript)
        display_topic = topic or self._topic_from_caption(caption, post)
        return PostEvidence(
            topic=display_topic,
            summary=summary,
            tags=tags,
            studies=studies,
            post_url=post.get("url"),
            author_username=(post.get("owner") or {}).get("username")
            or post.get("ownerUsername"),
            published_at=(post.get("createdAt") or post.get("timestamp")),
            likes=post.get("likeCount") or post.get("likesCount"),
            comments=(post.get("commentCount") or post.get("commentsCount")),
            content_type=content_type,
            caption=caption or "",
            image_url=image_url,
            transcript=transcript,
        )

    @staticmethod
    def _detect_content_type(post: dict) -> str:
        """Detect post type: reel, carousel, or post."""
        if (
            post.get("videoUrl")
            or post.get("isVideo")
            or post.get("mediaType") == "Video"
        ):
            return "reel"
        child_posts = post.get("childPosts") or []
        if len(child_posts) > 1:
            return "carousel"
        return "post"

    @staticmethod
    def _extract_video_url(post: dict) -> str | None:
        """Return video URL for Reels. Apify actors use different field names."""
        for key in (
            "videoUrl",
            "mediaUrl",
            "video",
            "playbackUrl",
            "link",
            "displayUrl",
        ):
            url = post.get(key)
            if isinstance(url, str) and url.startswith("http"):
                if key in ("videoUrl", "mediaUrl", "video", "playbackUrl"):
                    return url
                if post.get("isVideo") or post.get("mediaType") == "Video":
                    return url
        for child in post.get("childPosts") or []:
            if not isinstance(child, dict):
                continue
            for ckey in (
                "videoUrl",
                "mediaUrl",
                "video",
                "playbackUrl",
                "url",
                "link",
            ):
                url = child.get(ckey)
                if isinstance(url, str) and url.startswith("http"):
                    return url
        return None

    @staticmethod
    def _get_first_image_url(post: dict) -> str | None:
        """Return first image URL from post or carousel."""
        for key in ("displayUrl", "imageUrl", "image", "mediaUrl"):
            url = post.get(key)
            if isinstance(url, str) and url.startswith("http"):
                return url
        for child in post.get("childPosts") or []:
            if not isinstance(child, dict):
                continue
            for ckey in ("displayUrl", "imageUrl", "image", "url"):
                url = child.get(ckey)
                if isinstance(url, str) and url.startswith("http"):
                    return url
            break
        return None

    @staticmethod
    def _topic_from_caption(caption: str, post: dict) -> str:
        """Fallback topic when none given (latest-posts mode)."""
        if caption and caption.strip():
            first_line = caption.strip().split("\n")[0][:80].rstrip()
            if first_line:
                return first_line
        username = (post.get("owner") or {}).get("username") or post.get(
            "ownerUsername"
        )
        return f"Пост {username or 'блогера'}" if username else "Последний пост"

    def _build_summary(
        self,
        post: dict,
        caption: str,
        transcript: str | None = None,
    ) -> str:
        """Short rephrasing for table: what the blogger says about the research."""
        fallback = self._build_summary_fallback(caption or (transcript or ""))
        if not self._openai_api_key:
            return fallback
        content = caption or ""
        if transcript:
            content = f"{content}\n\n{transcript}".strip()
        clean = re.sub(r"\s+", " ", content).strip()
        if len(clean) < 30:
            return fallback
        try:
            return self._summarize_with_ai(caption=clean[:3000]) or fallback
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
            return (
                self._truncate_summary_at_sentence(text, max_len=600) if text else None
            )

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
                result.append(study.model_copy(update={"tags": ai_tags}))
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
                        'Верни только JSON: {"tags": ["тег1", "тег2", "тег3", "тег4"]}.'
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
                    str(v).strip()[:50] for v in raw if isinstance(v, str) and v.strip()
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
    def _parse_citation_lines(text: str) -> tuple[list[str], list[str]]:
        """
        Extract citation lines. Returns (citation_queries, context_queries).
        citation_queries: "Author Journal Year" for search_pmids_by_citation.
        context_queries: "Author context Year" (e.g. Hill tart cherry meta-analysis 2021)
        for search_pmids_by_title.
        """
        if not text or not text.strip():
            return [], []
        citations: list[str] = []
        context_queries: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or CITATIONS_HEADER_PATTERN.match(stripped):
                continue
            rest = CITATIONS_HEADER_PATTERN.sub("", stripped).strip() or stripped
            rest = CITATION_LINE_PREFIX.sub("", rest).strip() or rest
            for match in CITATION_PATTERN.finditer(rest):
                journal = match.group(2).strip().rstrip(",")
                if journal and journal != match.group(3):
                    q = f"{match.group(1)} {journal} {match.group(3)}"
                    if q not in citations:
                        citations.append(q)
            for match in CITATION_PATTERN_COMMA_YEAR.finditer(rest):
                journal = match.group(2).strip().rstrip(",")
                if journal:
                    q = f"{match.group(1)} {journal} {match.group(3)}"
                    if q not in citations:
                        citations.append(q)
            for match in CITATION_PATTERN_NO_JOURNAL.finditer(rest):
                author, year, ctx = match.group(1), match.group(2), match.group(3)
                q = (
                    f"{author} {ctx or ''} {year}".strip()
                    if ctx
                    else f"{author} {year}"
                )
                if q not in context_queries:
                    context_queries.append(q)
        return (citations, context_queries)

    @staticmethod
    def _extract_author_text_only(post: dict, caption: str) -> str:
        """
        Extract text only from post owner: caption + comments where username matches.
        """
        owner_username = (
            (post.get("owner") or {}).get("username") or post.get("ownerUsername") or ""
        )
        owner_lower = owner_username.lower() if owner_username else ""

        def _commenter_username(comment: dict) -> str:
            owner = comment.get("owner")
            if isinstance(owner, dict):
                return (owner.get("username") or "").lower()
            return (
                comment.get("ownerUsername") or comment.get("username") or ""
            ).lower()

        def _is_author_comment(comment: dict) -> bool:
            if not owner_lower:
                return False
            return _commenter_username(comment) == owner_lower

        chunks = [caption] if caption else []

        for child in post.get("childPosts") or []:
            if not isinstance(child, dict):
                continue
            for key in ("caption", "captionText", "text"):
                val = child.get(key)
                if isinstance(val, str) and val.strip():
                    chunks.append(val.strip())
                    break

        def _collect_from_comment(c: dict) -> None:
            if _is_author_comment(c):
                text = c.get("text")
                if isinstance(text, str) and text.strip():
                    chunks.append(text.strip())
            for reply in c.get("replies") or []:
                if isinstance(reply, dict):
                    _collect_from_comment(reply)

        first_comment = post.get("firstComment")
        if isinstance(first_comment, dict):
            _collect_from_comment(first_comment)

        latest_comments = post.get("latestComments")
        if isinstance(latest_comments, list):
            for comment in latest_comments:
                if isinstance(comment, dict):
                    _collect_from_comment(comment)

        return "\n".join(chunks)

    @staticmethod
    def _extract_post_text(post: dict, caption: str) -> str:
        chunks = [caption] if caption else []

        for child in post.get("childPosts") or []:
            if not isinstance(child, dict):
                continue
            for key in ("caption", "captionText", "text"):
                val = child.get(key)
                if isinstance(val, str) and val.strip():
                    chunks.append(val.strip())
                    break

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
                    if isinstance(candidate, str) and candidate.startswith("http"):
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
                            if isinstance(candidate, str) and candidate.startswith(
                                "http"
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
    ) -> tuple[list[str], list[str], str | None]:
        """One Vision call per image: extract PMIDs/title. Prefer infographics."""
        if not image_urls:
            return [], [], None
        if not self._openai_api_key:
            if debug_counts is not None:
                debug_counts["images_failed"] = len(image_urls)
                debug_counts["sample_status"] = "no_openai_key"
            return [], [], None

        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }
        pmids: set[str] = set()
        titles: list[str] = []
        first_infographic_url: str | None = None
        images_fetched = 0
        images_failed = 0

        topic_hint = (
            f" Тема: «{topic}». "
            "Если эта тема есть на изображении — извлеки PMID и title."
            if topic.strip()
            else ""
        )

        system_prompt = (
            "Контекст: это картинка из Instagram. "
            "СНАЧАЛА определи: это инфографика (график, диаграмма, текст-слайд с данными) "
            "или обычное фото (селфи, еда, зал)? Верни is_infographic: true/false. "
            "Если is_infographic: false — верни только {\"is_infographic\": false, \"pmids\": [], \"title\": \"\"}. "
            "Если is_infographic: true — извлеки статью в PubMed. "
            "PMID — число 5-8 цифр. "
            "Если видишь список цитат — извлеки КАЖДУЮ в titles. "
            'Верни JSON: {"is_infographic": true, "pmids": [], "title": "...", "titles": []}. '
            + topic_hint
        )

        browser_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "Referer": "https://www.instagram.com/",
            "Origin": "https://www.instagram.com",
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
                        if parsed.get("is_infographic") is False:
                            continue
                        if first_infographic_url is None and parsed.get("is_infographic") is True:
                            first_infographic_url = image_url
                        raw_pmids = parsed.get("pmids")
                        if isinstance(raw_pmids, list):
                            for raw in raw_pmids:
                                if isinstance(raw, str):
                                    m = re.search(r"\b\d{5,8}\b", raw)
                                    if m:
                                        pmids.add(m.group(0))
                                elif (
                                    isinstance(raw, int) and 10000 <= raw <= 99_999_999
                                ):
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
            return [], [], None

        if debug_counts is not None:
            debug_counts["images_fetched"] = images_fetched
            debug_counts["images_failed"] = images_failed

        return sorted(pmids), titles[:8], first_infographic_url

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

        lines = [
            line.strip()
            for line in (post_text + "\n" + caption).splitlines()
            if line.strip()
        ]
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
            candidates.append(
                "International Society of Sports Nutrition position stand antioxidants"
            )
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

    @staticmethod
    def _extract_high_confidence_title_candidates(post_text: str, caption: str) -> list[str]:
        """
        High-confidence only: position stand, ISSN, systematic review, this paper/study.
        Excludes generic caption lines and first_sentence (to avoid false positives like
        unrelated PubMed matches from vague phrases).
        """
        candidates: list[str] = []
        text = f"{post_text}\n{caption}".lower()
        full_text = post_text + "\n" + caption

        if "position stand" in text and any(
            w in text for w in ("antioxidant", "exercise", "sports", "performance")
        ):
            candidates.append(
                "International Society of Sports Nutrition position stand: "
                "effects of dietary antioxidants on exercise and sports performance"
            )
            candidates.append("position stand antioxidants exercise sports performance")
            candidates.append(
                "International Society of Sports Nutrition position stand antioxidants"
            )
        if "position" in text and "antioxidant" in text:
            candidates.append("position stand antioxidants exercise sports performance")

        research_markers = [
            r"(?:issn\s+)?position\s+stand[^.!?\n]{10,150}",
            r"(?:systematic\s+review|meta-?analysis)[^.!?\n]{10,150}",
            r"(?:this\s+(?:new\s+)?(?:paper|study|research)|new\s+(?:paper|study))[^.!?\n]{15,100}",
        ]
        for pattern in research_markers:
            for match in re.finditer(pattern, full_text, re.IGNORECASE):
                phrase = re.sub(r"\s+", " ", match.group(0).strip())[:150]
                if len(phrase) >= 20:
                    candidates.append(phrase)

        unique: list[str] = []
        for value in candidates:
            if value and value not in unique:
                unique.append(value)
        return unique[:8]

    def _search_pmids_by_titles(
        self,
        title_candidates: list[str],
        citation_queries: list[str] | None = None,
        debug_out: dict | None = None,
    ) -> list[str]:
        pmids: list[str] = []
        citation_set = set(citation_queries or [])

        for candidate in title_candidates:
            try:
                if candidate in citation_set:
                    matched = self._pubmed_client.search_pmids_by_citation(
                        citation_query=candidate,
                        max_results=3,
                    )
                    if not matched:
                        matched = self._pubmed_client.search_pmids_by_title(
                            title=candidate,
                            max_results=3,
                        )
                else:
                    if pmids:
                        break
                    matched = self._pubmed_client.search_pmids_by_title(
                        title=candidate,
                        max_results=5,
                    )
            except httpx.HTTPError as e:
                if debug_out is not None and "pubmed_error" not in debug_out:
                    debug_out["pubmed_error"] = f"{type(e).__name__}: {str(e)[:150]}"
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
            if candidate not in citation_set and pmids:
                break
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
