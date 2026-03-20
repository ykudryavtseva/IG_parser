"""Apify client for Twitter (X) — apidojo/twitter-scraper-lite."""

from apify_client import ApifyClient

DEFAULT_TWITTER_ACTOR = "apidojo/twitter-scraper-lite"


class ApifyTwitterClient:
    """Fetch tweets from Twitter via Apify actor."""

    def __init__(
        self,
        token: str,
        actor_id: str = DEFAULT_TWITTER_ACTOR,
    ) -> None:
        self._client = ApifyClient(token)
        self._actor_id = actor_id

    def fetch_tweets(
        self,
        handles: list[str],
        max_items: int = 100,
        start_date: str | None = None,
    ) -> tuple[list[dict], str | None]:
        """
        Fetch tweets from given Twitter handles.

        start_date: YYYY-MM-DD or ISO, tweets after this date (Apify 'start').
        Returns (tweets, apify_error). apify_error is set when items contain errors.
        """
        clean = [self._normalize_handle(h) for h in handles if h and str(h).strip()]
        if not clean:
            return ([], "Не заданы аккаунты Twitter для выгрузки.")

        date_suffix = f" since:{start_date[:10]}" if start_date else ""
        search_terms = [f"from:{h}{date_suffix}" for h in clean]
        run_input: dict = {
            "searchTerms": search_terms,
            "sort": "Latest",
            "maxItems": max_items,
        }

        try:
            run = self._client.actor(self._actor_id).call(run_input=run_input)
        except Exception as exc:
            return ([], str(exc))

        dataset = self._client.dataset(run["defaultDatasetId"])
        items = list(dataset.iterate_items())

        apify_error: str | None = None
        valid: list[dict] = []

        for item in items:
            if not isinstance(item, dict):
                continue
            if item.get("error") or item.get("errorDescription"):
                if apify_error is None:
                    err = item.get("errorDescription") or item.get("error")
                    apify_error = str(err) if err else "Apify вернул ошибку"
                continue
            valid.append(item)

        return (valid, apify_error)

    @staticmethod
    def _normalize_handle(handle: str) -> str:
        """Remove @ and whitespace from Twitter handle."""
        s = str(handle).strip().lstrip("@")
        return s
