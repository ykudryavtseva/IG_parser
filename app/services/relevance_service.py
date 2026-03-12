import json
import re

import httpx

TOPIC_SIGNAL_MAP: dict[str, tuple[str, ...]] = {
    "vitamin d": (
        "vitamin d",
        "cholecalciferol",
        "calcidiol",
        "calcitriol",
        "25(oh)d",
    ),
    "витамин д": (
        "vitamin d",
        "cholecalciferol",
        "calcidiol",
        "calcitriol",
        "25(oh)d",
    ),
    "creatine": ("creatine", "creatinine"),
    "креатин": ("creatine",),
    "hair loss": ("hair loss", "alopecia", "androgenetic"),
    "выпадение волос": ("hair loss", "alopecia", "androgenetic"),
    "omega-3": (
        "omega-3",
        "omega 3",
        "n-3",
        "epa",
        "dha",
        "fish oil",
        "docosahexaenoic",
        "eicosapentaenoic",
    ),
    "omega 3": (
        "omega-3",
        "omega 3",
        "n-3",
        "epa",
        "dha",
        "fish oil",
        "docosahexaenoic",
        "eicosapentaenoic",
    ),
    "омега-3": (
        "omega-3",
        "omega 3",
        "n-3",
        "epa",
        "dha",
        "fish oil",
        "docosahexaenoic",
        "eicosapentaenoic",
    ),
    "омега 3": (
        "omega-3",
        "omega 3",
        "n-3",
        "epa",
        "dha",
        "fish oil",
        "docosahexaenoic",
        "eicosapentaenoic",
    ),
    "dementia": (
        "dementia",
        "alzheimer",
        "alzheimers",
        "cognitive decline",
        "neurodegenerative",
    ),
    "деменц": (
        "dementia",
        "alzheimer",
        "alzheimers",
        "cognitive decline",
        "neurodegenerative",
    ),
    "testosterone": (
        "testosterone",
        "androgen",
        "bioavailable testosterone",
        "androgen receptor",
    ),
    "тестостерон": (
        "testosterone",
        "androgen",
        "bioavailable testosterone",
        "androgen receptor",
    ),
}

VITAMIN_D_SIGNALS = (
    "vitamin d",
    "cholecalciferol",
    "calcidiol",
    "calcitriol",
    "25(oh)d",
)

CREATINE_SIGNALS = ("creatine", "creatinine")
HAIR_LOSS_SIGNALS = ("hair loss", "alopecia", "androgenetic")
OMEGA_3_SIGNALS = (
    "omega-3",
    "omega 3",
    "n-3",
    "epa",
    "dha",
    "fish oil",
    "docosahexaenoic",
    "eicosapentaenoic",
)
DEMENTIA_SIGNALS = (
    "dementia",
    "alzheimer",
    "alzheimers",
    "cognitive decline",
    "neurodegenerative",
)


class StudyRelevanceChecker:
    def __init__(
        self,
        openai_api_key: str | None = None,
        openai_model: str = "gpt-4o-mini",
    ) -> None:
        self._openai_api_key = openai_api_key
        self._openai_model = openai_model
        self._cache: dict[str, bool] = {}

    def is_relevant(self, topic: str, study_title: str) -> bool:
        cache_key = f"{topic}|{study_title}"
        if cache_key in self._cache:
            return self._cache[cache_key]

        heuristic_result = self._check_with_rules(
            topic=topic,
            study_title=study_title,
        )
        ai_result = self._check_with_ai(topic=topic, study_title=study_title)

        if ai_result is not None:
            self._cache[cache_key] = ai_result
            return ai_result
        self._cache[cache_key] = heuristic_result
        return heuristic_result

    def _check_with_ai(self, topic: str, study_title: str) -> bool | None:
        if not self._openai_api_key:
            return None

        payload = {
            "model": self._openai_model,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Ты фильтр релевантности. Отвечай JSON: "
                        '{"relevant": true|false}. '
                        "relevant=true если исследование связано с темой: "
                        "напрямую, через родственные термины (андрогены↔тестостерон, "
                        "витамин D↔кальцидиол) или как главный объект изучения. "
                        "relevant=false только при явной нерелевантности (другая область, "
                        "тема лишь в списке ссылок). При разумной связи — true."
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Вопрос/тема: {topic}\n"
                        f"Название исследования: {study_title}\n"
                        "Это исследование напрямую о теме? Да или нет?"
                    ),
                },
            ],
            "temperature": 0,
        }
        headers = {
            "Authorization": f"Bearer {self._openai_api_key}",
            "Content-Type": "application/json",
        }

        try:
            with httpx.Client(timeout=20.0) as client:
                response = client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers=headers,
                    json=payload,
                )
                response.raise_for_status()
                content = response.json()["choices"][0]["message"]["content"]
                parsed = json.loads(content)
                value = parsed.get("relevant")
                if isinstance(value, bool):
                    return value
        except Exception:
            return None
        return None

    @staticmethod
    def _check_with_rules(topic: str, study_title: str) -> bool:
        topic_low = topic.lower()
        title_low = study_title.lower()

        matched_groups = [
            signals
            for trigger, signals in TOPIC_SIGNAL_MAP.items()
            if trigger in topic_low
        ]
        if re.search(r"витамин\w*\s*д\b|vitamin\s*d\b", topic_low):
            matched_groups.append(VITAMIN_D_SIGNALS)
        if re.search(r"креатин\w*|creatine", topic_low):
            matched_groups.append(CREATINE_SIGNALS)
        if re.search(
            r"выпадени\w*\s+волос\w*|hair\s*loss|alopecia",
            topic_low,
        ):
            matched_groups.append(HAIR_LOSS_SIGNALS)
        if re.search(
            r"омега[-\s]*3|omega[-\s]*3|(?:^|\W)n[-\s]*3(?:\W|$)",
            topic_low,
        ):
            matched_groups.append(OMEGA_3_SIGNALS)
        if re.search(r"деменц\w*|dementia|alzheimer", topic_low):
            matched_groups.append(DEMENTIA_SIGNALS)
        if re.search(r"тестостерон|testosterone|androgen", topic_low):
            matched_groups.append(
                ("testosterone", "androgen", "bioavailable testosterone")
            )

        if not matched_groups:
            return True

        group_hits = [
            any(signal in title_low for signal in signals) for signals in matched_groups
        ]
        unique_group_count = len({tuple(group) for group in matched_groups})
        if unique_group_count >= 2:
            return sum(group_hits) >= 2
        return any(group_hits)
