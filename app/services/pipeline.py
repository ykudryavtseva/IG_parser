import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx

from app.models import PostEvidence
from app.services.apify_service import ApifyInstagramClient
from app.services.pubmed_service import PubMedClient
from app.services.relevance_service import StudyRelevanceChecker

MAX_IMAGE_URLS_TO_SCAN = 5
POST_PROCESS_WORKERS = 12


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
    ) -> list[PostEvidence]:
        selected_sources = sources
        if not selected_sources:
            selected_sources = self._instagram_client.discover_sources(
                topic=topic,
                discovery_limit=discovery_limit,
            )
        if not selected_sources:
            return []

        posts = self._instagram_client.fetch_posts(
            sources=selected_sources,
            max_items=max_items,
        )

        posts_with_caption = [
            p for p in posts
            if isinstance(p, dict) and (p.get("caption") or "").strip()
        ]
        workers = min(POST_PROCESS_WORKERS, len(posts_with_caption) or 1)

        results: list[PostEvidence] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(self._process_post, post, topic): post
                for post in posts_with_caption
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

        return results

    def _process_post(self, post: dict, topic: str) -> PostEvidence | None:
        post_url = post.get("url") or "<no-url>"
        caption = (post.get("caption") or "").strip()
        if not caption:
            return None

        post_text = self._extract_post_text(post=post, caption=caption)
        pmids = self._pubmed_client.extract_pmids(post_text)
        extraction_source = "text"
        image_urls: list[str] = []
        image_title_candidates: list[str] = []
        title_candidates: list[str] = []
        if not pmids:
            image_urls = self._extract_post_image_urls(post=post)
            pmids, image_title_candidates = (
                self._extract_pmids_and_titles_from_images(
                    image_urls=image_urls,
                )
            )
            extraction_source = "image"
        if not pmids:
            title_candidates = self._extract_title_candidates(
                post_text=post_text,
                caption=caption,
            )
            for title in image_title_candidates:
                if title not in title_candidates:
                    title_candidates.append(title)
            pmids = self._search_pmids_by_titles(title_candidates=title_candidates)
            extraction_source = "title"
        if not pmids:
            return None

        studies = []
        for pmid in pmids:
            try:
                study = self._pubmed_client.fetch_study(pmid)
            except (httpx.HTTPError, KeyError, ValueError):
                continue
            if not self._relevance_checker.is_relevant(
                topic=topic,
                study_title=study.title,
            ):
                continue
            studies.append(study)

        if not studies:
            return None

        tags = self._build_tags(topic=topic, caption=caption)
        summary = self._build_summary(post=post, caption=caption)
        return PostEvidence(
            topic=topic,
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
    def _build_summary(post: dict, caption: str) -> str:
        likes = post.get("likeCount") or post.get("likesCount")
        comments = post.get("commentCount") or post.get("commentsCount")
        views = (
            post.get("videoViewCount")
            or (post.get("video") or {}).get("playCount")
            or post.get("videoPlayCount")
        )

        numbers = []
        if likes is not None:
            numbers.append(f"likes={likes}")
        if comments is not None:
            numbers.append(f"comments={comments}")
        if views is not None:
            numbers.append(f"views={views}")
        metrics = ", ".join(numbers) if numbers else "metrics unavailable"

        clean_caption = re.sub(r"\s+", " ", caption).strip()
        truncated = clean_caption[:1800]
        return f"Post evidence snapshot ({metrics}). Text: {truncated}"

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

        primary = post.get("displayUrl")
        if isinstance(primary, str) and primary.startswith("http"):
            image_urls.append(primary)

        images = post.get("images")
        if isinstance(images, list):
            for item in images:
                if isinstance(item, str) and item.startswith("http"):
                    image_urls.append(item)
                elif isinstance(item, dict):
                    candidate = item.get("url") or item.get("displayUrl")
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
                child_display = child.get("displayUrl")
                if isinstance(child_display, str) and child_display.startswith("http"):
                    image_urls.append(child_display)
                child_images = child.get("images")
                if isinstance(child_images, list):
                    for image in child_images:
                        if isinstance(image, str) and image.startswith("http"):
                            image_urls.append(image)
                        elif isinstance(image, dict):
                            candidate = image.get("url") or image.get("displayUrl")
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
    ) -> tuple[list[str], list[str]]:
        """One Vision call per image: extract PMIDs and/or title."""
        if not self._openai_api_key or not image_urls:
            return [], []

        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }
        pmids: set[str] = set()
        titles: list[str] = []

        with httpx.Client(timeout=25.0) as client:
            for image_url in image_urls:
                data_url = self._build_data_url(
                    client=client,
                    image_url=image_url,
                )
                if not data_url:
                    continue
                payload = {
                    "model": self._openai_model,
                    "response_format": {"type": "json_object"},
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "Если на изображении скриншот PubMed, верни JSON: "
                                "{\"pmids\": [\"12345678\"], \"title\": \"Full study title\"}. "
                                "Извлеки PMID если виден, и точное название статьи. "
                                "Если не скриншот PubMed — {\"pmids\": [], \"title\": \"\"}."
                            ),
                        },
                        {
                            "role": "user",
                            "content": [
                                {"type": "text", "text": "PMID и название статьи."},
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
                        if 20 <= len(t) <= 250 and t not in titles:
                            titles.append(t)
                except (
                    httpx.HTTPError,
                    KeyError,
                    ValueError,
                    json.JSONDecodeError,
                ):
                    continue

        return sorted(pmids), titles[:5]

    @staticmethod
    def _extract_title_candidates(post_text: str, caption: str) -> list[str]:
        candidates: list[str] = []

        lines = [line.strip() for line in post_text.splitlines() if line.strip()]
        for line in lines[:8]:
            if len(line) < 20:
                continue
            if len(line) > 220:
                continue
            if "http" in line.lower():
                continue
            if line.lower().startswith("pmid"):
                continue
            candidates.append(line.rstrip("."))

        if not candidates:
            first_sentence = re.split(r"[.!?]\s+", caption.strip())[0].strip()
            if 20 <= len(first_sentence) <= 220:
                candidates.append(first_sentence.rstrip("."))

        unique: list[str] = []
        for value in candidates:
            if value not in unique:
                unique.append(value)
        return unique[:5]

    def _search_pmids_by_titles(self, title_candidates: list[str]) -> list[str]:
        pmids: list[str] = []
        for candidate in title_candidates:
            try:
                matched = self._pubmed_client.search_pmids_by_title(
                    title=candidate,
                    max_results=3,
                )
            except httpx.HTTPError:
                continue
            for pmid in matched:
                if pmid not in pmids:
                    pmids.append(pmid)
        return pmids

    @staticmethod
    def _build_data_url(client: httpx.Client, image_url: str) -> str | None:
        try:
            response = client.get(image_url, timeout=20.0)
            response.raise_for_status()
        except httpx.HTTPError:
            return None

        content_type = response.headers.get("content-type", "image/jpeg")
        if not content_type.startswith("image/"):
            content_type = "image/jpeg"
        encoded = base64.b64encode(response.content).decode("ascii")
        return f"data:{content_type};base64,{encoded}"
