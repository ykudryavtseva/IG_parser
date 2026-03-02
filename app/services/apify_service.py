from apify_client import ApifyClient

STOP_WORDS = {
    "вызывает",
    "правда",
    "может",
    "можно",
    "нужно",
    "если",
    "или",
    "ли",
    "это",
    "как",
    "что",
    "the",
    "does",
    "is",
    "are",
    "can",
    "could",
    "would",
    "should",
    "what",
    "how",
    "может",
    "случиться",
    "случится",
}

EVIDENCE_TERMS = (
    "pmid",
    "pubmed",
    "evidence",
    "research",
    "study",
    "meta-analysis",
    "systematic review",
    "научн",
    "исслед",
    "доказ",
)

RU_PHRASE_MAP = {
    "выпадение волос": "hair loss",
    "витамин д": "vitamin d",
    "переизбыток витамина д": "too much vitamin d",
    "передозировка витамина д": "vitamin d overdose",
    "дефицит витамина д": "vitamin d deficiency",
}

RU_WORD_MAP = {
    "креатин": "creatine",
    "выпадение": "hair loss",
    "волос": "hair",
    "волосы": "hair",
    "облысение": "alopecia",
    "здоровье": "health",
    "бад": "supplement",
    "бады": "supplements",
    "исследование": "research",
    "исследования": "research",
    "научный": "scientific",
    "научные": "scientific",
    "витамин": "vitamin",
    "витамина": "vitamin",
    "витаминов": "vitamins",
    "переизбыток": "overdose",
    "избыток": "excess",
    "передозировка": "overdose",
    "дефицит": "deficiency",
    "токсичность": "toxicity",
    "д3": "vitamin d3",
    "d3": "vitamin d3",
}

RU_TYPO_MAP = {
    "сулчиться": "случиться",
    "случиьтся": "случится",
    "случитсья": "случится",
}


class ApifyInstagramClient:
    def __init__(
        self,
        token: str,
        posts_actor_id: str,
        search_actor_id: str,
    ) -> None:
        self._client = ApifyClient(token)
        self._posts_actor_id = posts_actor_id
        self._search_actor_id = search_actor_id

    def fetch_posts(
        self,
        sources: list[str],
        max_items: int,
        only_posts_newer_than: str | None = None,
    ) -> list[dict]:
        run_input = self._build_posts_input(
            sources=sources,
            max_items=max_items,
            only_posts_newer_than=only_posts_newer_than,
        )
        run = self._client.actor(self._posts_actor_id).call(run_input=run_input)
        items = self._client.dataset(run["defaultDatasetId"]).list_items().items
        return [item for item in items if isinstance(item, dict)]

    def discover_sources(self, topic: str, discovery_limit: int) -> list[str]:
        query = self._build_search_query(topic=topic)
        run_input = {
            "search": query,
            "searchType": "user",
            "searchLimit": max(5, discovery_limit),
            "enhanceUserSearchWithFacebookPage": False,
        }
        run = self._client.actor(self._search_actor_id).call(run_input=run_input)
        items = self._client.dataset(run["defaultDatasetId"]).list_items().items

        topic_terms = self._topic_terms(topic=topic)
        candidates: list[tuple[int, str]] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("private") is True:
                continue
            username = item.get("username")
            if not isinstance(username, str):
                continue

            score = self._source_score(item=item, topic_terms=topic_terms)
            candidates.append((score, username))

        candidates.sort(key=lambda pair: pair[0], reverse=True)

        usernames: list[str] = []
        for _, username in candidates:
            if username not in usernames:
                usernames.append(username)
            if len(usernames) >= discovery_limit:
                break
        return usernames

    @staticmethod
    def _build_search_query(topic: str) -> str:
        normalized_topic = ApifyInstagramClient._normalize_topic_text(topic=topic)
        terms = ApifyInstagramClient._topic_terms(topic=normalized_topic)
        translated_terms = ApifyInstagramClient._english_discovery_terms(
            topic=normalized_topic
        )

        merged_terms: list[str] = []
        for term in translated_terms + terms:
            if term not in merged_terms:
                merged_terms.append(term)

        top_words = merged_terms[:5]
        if not top_words:
            return topic
        return ", ".join(top_words)

    @staticmethod
    def _topic_terms(topic: str) -> list[str]:
        words = [
            word.strip("?!.:,;()[]{}\"'").lower()
            for word in topic.split()
        ]
        terms: list[str] = []
        for word in words:
            if len(word) < 4:
                continue
            if word in STOP_WORDS:
                continue
            if word not in terms:
                terms.append(word)
            translated = RU_WORD_MAP.get(word)
            if translated and translated not in terms:
                terms.append(translated)
        return terms

    @staticmethod
    def _english_discovery_terms(topic: str) -> list[str]:
        lowered = topic.lower()
        terms: list[str] = []

        for phrase, translated in RU_PHRASE_MAP.items():
            if phrase in lowered and translated not in terms:
                terms.append(translated)

        for raw_word in lowered.split():
            word = raw_word.strip("?!.:,;()[]{}\"'")
            translated = RU_WORD_MAP.get(word)
            if translated and translated not in terms:
                terms.append(translated)

        if "витамин" in lowered and (" д" in lowered or " d" in lowered):
            if "vitamin d" not in terms:
                terms.append("vitamin d")
        return terms

    @staticmethod
    def _normalize_topic_text(topic: str) -> str:
        normalized = topic.lower()
        for typo, fixed in RU_TYPO_MAP.items():
            normalized = normalized.replace(typo, fixed)
        return normalized

    @staticmethod
    def _source_score(item: dict, topic_terms: list[str]) -> int:
        searchable_fields = [
            item.get("username") or "",
            item.get("fullName") or "",
            item.get("biography") or "",
            item.get("businessCategoryName") or "",
            item.get("externalUrl") or "",
        ]
        text = " ".join(field for field in searchable_fields if isinstance(field, str))
        normalized = text.lower()

        score = 0
        for term in topic_terms:
            if term in normalized:
                score += 2
        for marker in EVIDENCE_TERMS:
            if marker in normalized:
                score += 5
        if item.get("verified") is True:
            score += 1
        return score

    def _build_posts_input(
        self,
        sources: list[str],
        max_items: int,
        only_posts_newer_than: str | None = None,
    ) -> dict:
        actor_id = self._posts_actor_id.lower()
        if "instagram-scraper-api" in actor_id:
            start_urls = [self._normalize_to_url(source) for source in sources]
            out = {
                "startUrls": start_urls,
                "maxItems": max_items,
            }
            if only_posts_newer_than:
                out["onlyPostsNewerThan"] = only_posts_newer_than
            return out

        out: dict = {
            "username": sources,
            "resultsLimit": max_items,
            "skipPinnedPosts": True,
        }
        if only_posts_newer_than:
            out["onlyPostsNewerThan"] = only_posts_newer_than
        return out

    @staticmethod
    def _normalize_to_url(source: str) -> str:
        if source.startswith("http://") or source.startswith("https://"):
            return source
        return f"https://www.instagram.com/{source.strip('/')}/"
