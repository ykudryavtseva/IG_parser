"""
Twitter pipeline: fetch tweets, merge threads, extract evidence → PostEvidence.

Тред из нескольких твитов считается одним постом.
"""

import json
import logging
import re
from datetime import datetime
import httpx

from app.models import PipelineRunResult, PostEvidence, ResearchItem

from app.services.pubmed_service import PubMedClient
from app.services.relevance_service import StudyRelevanceChecker
from app.services.twitter_apify_client import ApifyTwitterClient

EVIDENCE_TERMS = (
    "pmid",
    "pubmed",
    "research",
    "study",
    "meta-analysis",
    "исслед",
    "научн",
)

AD_MARKERS = ("#реклама", "#ad", "#ads", "sponsored", "paid partnership")


def _parse_twitter_date(value: str | None) -> datetime | None:
    """Parse Twitter date: 'Fri Nov 24 17:49:36 +0000 2023' or ISO."""
    if not value:
        return None
    try:
        return datetime.strptime(
            str(value).strip()[:30], "%a %b %d %H:%M:%S %z %Y"
        )
    except (ValueError, TypeError):
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")[:26])
        except (ValueError, TypeError):
            return None


def _get_tweet_author(tweet: dict) -> str:
    """Extract author username from tweet."""
    author = tweet.get("author")
    if isinstance(author, dict):
        return author.get("userName") or author.get("username") or ""
    return tweet.get("username") or tweet.get("userName") or ""


def _get_conversation_id(tweet: dict) -> str:
    """Get conversation/thread id. Root tweet: id = conversation_id."""
    for key in ("conversationId", "conversation_id"):
        val = tweet.get(key)
        if val:
            return str(val)
    if not tweet.get("isReply"):
        return str(tweet.get("id") or "")
    parent = tweet.get("inReplyToStatusId") or tweet.get("replyTo") or tweet.get(
        "replyToStatusId"
    )
    return str(parent or tweet.get("id") or "")


def _merge_threads(tweets: list[dict]) -> list[dict]:
    """
    Group tweets into threads. Thread = root + replies from same author.
    Returns list of merged "posts" (dict with merged text, url, etc.).
    """
    by_conv: dict[str, list[dict]] = {}
    for t in tweets:
        if not isinstance(t, dict):
            continue
        cid = _get_conversation_id(t)
        if not cid:
            cid = str(t.get("id") or id(t))
        if cid not in by_conv:
            by_conv[cid] = []
        by_conv[cid].append(t)

    result: list[dict] = []
    for cid, group in by_conv.items():
        root = next((t for t in group if not t.get("isReply")), group[0])
        root_author = _get_tweet_author(root)
        same_author = [t for t in group if _get_tweet_author(t) == root_author]
        same_author.sort(
            key=lambda t: _parse_twitter_date(t.get("createdAt")) or datetime.min
        )
        texts = [str(t.get("text") or "").strip() for t in same_author if t.get("text")]
        merged_text = "\n\n".join(t for t in texts if t)
        first = same_author[0]
        last = same_author[-1]
        like_sum = sum(
            t.get("likeCount") or t.get("favoriteCount") or 0 for t in same_author
        )
        reply_sum = sum(t.get("replyCount") or 0 for t in same_author)
        image_url = _extract_first_image(first)
        for t in same_author:
            img = _extract_first_image(t)
            if img:
                image_url = img
                break
        result.append(
            {
                "id": first.get("id"),
                "url": first.get("url") or first.get("twitterUrl"),
                "text": merged_text,
                "caption": merged_text,
                "createdAt": first.get("createdAt"),
                "author": root_author,
                "likeCount": like_sum or first.get("likeCount"),
                "replyCount": reply_sum or first.get("replyCount"),
                "image_url": image_url,
                "tweets_in_thread": len(same_author),
            }
        )
    return result


def _extract_first_image(tweet: dict) -> str | None:
    """Extract first image URL from tweet media."""
    media = tweet.get("media") or tweet.get("photos") or []
    if isinstance(media, list):
        for m in media:
            if isinstance(m, dict):
                url = m.get("url") or m.get("mediaUrl") or m.get("link")
                if isinstance(url, str) and url.startswith("http"):
                    return url
            elif isinstance(m, str) and m.startswith("http"):
                return m
    return None


def _is_non_research_tweet(text: str) -> bool:
    """Skip ads, sponsored."""
    lower = (text or "").lower()
    for m in AD_MARKERS:
        if m.lower() in lower:
            return True
    return False


def _has_evidence_terms(text: str) -> bool:
    """Check if text mentions research/evidence."""
    lower = (text or "").lower()
    return any(term in lower for term in EVIDENCE_TERMS)


class TwitterPipeline:
    """Process Twitter posts into PostEvidence."""

    def __init__(
        self,
        twitter_client: ApifyTwitterClient,
        pubmed_client: PubMedClient,
        relevance_checker: StudyRelevanceChecker,
        openai_api_key: str | None = None,
        openai_model: str = "gpt-4o-mini",
    ) -> None:
        self._twitter_client = twitter_client
        self._pubmed_client = pubmed_client
        self._relevance_checker = relevance_checker
        self._openai_api_key = openai_api_key
        self._openai_model = openai_model

    def run(
        self,
        handles: list[str],
        max_items: int = 50,
        only_newer_than: str | None = None,
        processed_tweet_ids: set[str] | None = None,
        skip_scientific_filter: bool = False,
    ) -> PipelineRunResult:
        """Fetch tweets, merge threads, extract evidence."""
        clean = [h.strip().lstrip("@") for h in handles if h and str(h).strip()]
        if not clean:
            return PipelineRunResult(items=[], posts_fetched=0, posts_with_caption=0)

        tweets, apify_error = self._twitter_client.fetch_tweets(
            handles=clean,
            max_items=max_items,
            start_date=only_newer_than,
        )

        merged = _merge_threads(tweets)
        processed = processed_tweet_ids or set()
        filtered: list[dict] = []
        for m in merged:
            post_id = m.get("url") or m.get("id") or ""
            if post_id and post_id in processed:
                continue
            if _is_non_research_tweet(m.get("text") or ""):
                continue
            if not (m.get("text") or "").strip() and not m.get("image_url"):
                continue
            filtered.append(m)

        results: list[PostEvidence] = []
        for post in filtered:
            try:
                evidence = self._process_tweet(
                    post=post,
                    skip_scientific_filter=skip_scientific_filter,
                )
                if evidence:
                    results.append(evidence)
            except Exception as e:
                logging.getLogger(__name__).warning(
                    "twitter_process_failed: %s", e, exc_info=True
                )

        def _sort_key(item: PostEvidence) -> tuple[int, float]:
            dt = _parse_twitter_date(item.published_at)
            return (1 if dt is None else 0, dt.timestamp() if dt else 0.0)

        results.sort(key=_sort_key)

        return PipelineRunResult(
            items=results,
            posts_fetched=len(tweets),
            posts_with_caption=sum(1 for m in merged if (m.get("text") or "").strip()),
            debug_apify_error=apify_error or "",
        )

    def _process_tweet(
        self,
        post: dict,
        skip_scientific_filter: bool = False,
    ) -> PostEvidence | None:
        """Convert merged tweet/thread to PostEvidence."""
        text = (post.get("text") or post.get("caption") or "").strip()
        if not text and not post.get("image_url"):
            return None

        pmids = list(self._pubmed_client.extract_pmids(text))
        if not pmids:
            title_candidates = self._extract_title_candidates(text)
            seen: set[str] = set()
            for candidate in title_candidates:
                for pid in self._pubmed_client.search_pmids_by_title(
                    candidate, max_results=3
                ):
                    if pid not in seen:
                        seen.add(pid)
                        pmids.append(pid)
        if not pmids:
            if not skip_scientific_filter and not self._is_scientific(text):
                return None
            return self._build_raw_post(post=post, text=text)

        studies = []
        for pmid in pmids[:10]:
            try:
                study = self._pubmed_client.fetch_study(pmid)
                study = study.model_copy(update={"citation_source": "текст"})
                studies.append(study)
            except (httpx.HTTPError, KeyError, ValueError):
                pass

        if not studies:
            if skip_scientific_filter or self._is_scientific(text):
                return self._build_raw_post(post=post, text=text)
            return None

        tags = self._build_tags(text)
        summary = self._build_summary(text)
        topic = text.split("\n")[0][:80].rstrip() if text else "Твит"

        return PostEvidence(
            topic=topic,
            summary=summary,
            tags=tags,
            studies=studies,
            post_url=post.get("url"),
            author_username=post.get("author"),
            published_at=post.get("createdAt"),
            likes=post.get("likeCount"),
            comments=post.get("replyCount"),
            content_type="thread" if post.get("tweets_in_thread", 1) > 1 else "tweet",
            caption=text,
            image_url=post.get("image_url"),
            transcript=None,
        )

    def _build_raw_post(self, post: dict, text: str) -> PostEvidence:
        """Build PostEvidence for post without PMIDs."""
        tags = self._build_tags(text)
        summary = self._build_summary(text)
        topic = text.split("\n")[0][:80].rstrip() if text else "Твит"
        return PostEvidence(
            topic=topic,
            summary=summary,
            tags=tags,
            studies=[],
            post_url=post.get("url"),
            author_username=post.get("author"),
            published_at=post.get("createdAt"),
            likes=post.get("likeCount"),
            comments=post.get("replyCount"),
            content_type="thread" if post.get("tweets_in_thread", 1) > 1 else "tweet",
            caption=text,
            image_url=post.get("image_url"),
            transcript=None,
        )

    def _extract_title_candidates(self, text: str) -> list[str]:
        """Extract potential study titles from text."""
        candidates: list[str] = []
        for line in text.split("\n"):
            line = line.strip()
            if len(line) < 15 or len(line) > 300:
                continue
            if re.search(r"\d{4}\b", line):
                candidates.append(line)
        return candidates[:5]

    def _is_scientific(self, text: str) -> bool:
        """LLM classifier: scientific content?"""
        if not text or len(text) < 30:
            return False
        if not _has_evidence_terms(text):
            return False
        if not self._openai_api_key:
            return True
        payload = {
            "model": self._openai_model,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Фильтр: полезный ли это экспертный контент? "
                        'Отвечай JSON: {"scientific": true|false}. '
                        "Включить: разбор исследований, цитаты, экспертные рассуждения. "
                        "Исключить: мотивация без содержания, поздравления, реклама."
                    ),
                },
                {"role": "user", "content": text[:3000]},
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
                parsed = json.loads(
                    r.json()["choices"][0]["message"]["content"]
                )
                return bool(parsed.get("scientific"))
        except Exception:
            return _has_evidence_terms(text)

    def _build_tags(self, text: str) -> list[str]:
        """Keyword-based tags."""
        lower = (text or "").lower()
        tag_map = {
            "креатин": ["creatine", "креатин"],
            "БАДы": ["supplement", "бад"],
            "здоровье": ["health", "здоров"],
            "выпадение волос": ["hair loss", "alopecia"],
        }
        tags: list[str] = []
        for tag, keys in tag_map.items():
            if any(k in lower for k in keys):
                tags.append(tag)
        return tags[:4] if tags else ["Twitter"]

    def _build_summary(self, text: str) -> str:
        """Short summary. Use AI if key available."""
        clean = re.sub(r"\s+", " ", (text or "").strip())
        if len(clean) < 50:
            return clean[:300] if clean else "Твит"
        if not self._openai_api_key:
            return clean[:500].rstrip()
        payload = {
            "model": self._openai_model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Кратко перескажи твит/тред (2–4 предложения). "
                        "О чём пост, какой вывод. Язык: тот же, что в тексте."
                    ),
                },
                {"role": "user", "content": text[:3000]},
            ],
            "temperature": 0.3,
        }
        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }
        try:
            with httpx.Client(timeout=20.0) as client:
                r = client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                )
                r.raise_for_status()
                out = r.json()["choices"][0]["message"]["content"].strip()
                return out[:600] if out else clean[:500]
        except Exception:
            return clean[:500].rstrip()
