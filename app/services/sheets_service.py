import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from app.models import PostEvidence, ResearchItem

SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheetsExporter:
    def __init__(
        self,
        spreadsheet_id: str,
        worksheet_name: str,
        credentials_path: str | None = None,
        credentials_json: str | None = None,
        openai_api_key: str | None = None,
        openai_model: str = "gpt-4o-mini",
    ) -> None:
        if not credentials_path and not credentials_json:
            raise RuntimeError(
                "Provide GOOGLE_SHEETS_CREDENTIALS_PATH or "
                "GOOGLE_SHEETS_CREDENTIALS_JSON"
            )

        self._spreadsheet_id = spreadsheet_id
        self._worksheet_name = worksheet_name
        self._openai_api_key = openai_api_key
        self._openai_model = openai_model
        self._ai_cache: dict[str, list[str]] = {}
        self._service = self._build_service(
            credentials_path=credentials_path,
            credentials_json=credentials_json,
        )
        self._worksheet_name = self._resolve_worksheet_name(
            requested_name=worksheet_name,
        )

    def export(self, items: list[PostEvidence]) -> int:
        self._prefill_ai_cache_parallel(items=items)
        rows = self._build_rows(items=items)
        if len(rows) == 1:
            return 0

        self._ensure_header_row(header=rows[0])
        body = {"values": rows[1:]}
        self._service.spreadsheets().values().append(
            spreadsheetId=self._spreadsheet_id,
            range=self._worksheet_name,
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body=body,
        ).execute()
        return len(rows) - 1

    def _prefill_ai_cache_parallel(self, items: list[PostEvidence]) -> None:
        """Run AI tag classification in parallel to speed up export."""
        tasks: list[tuple[str, PostEvidence, ResearchItem]] = []
        for item in items:
            for study in item.studies:
                cache_key = f"{item.topic}|{study.title}"
                if cache_key not in self._ai_cache:
                    tasks.append((cache_key, item, study))

        if not tasks or not self._openai_api_key:
            return

        max_workers = min(8, len(tasks))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {
                executor.submit(
                    self._classify_tags_with_ai, item=item, study=study
                ): cache_key
                for cache_key, item, study in tasks
            }
            for future in as_completed(future_to_key):
                cache_key = future_to_key[future]
                try:
                    ai_tags = future.result()
                    if ai_tags:
                        self._ai_cache[cache_key] = ai_tags
                except Exception:
                    pass

    def _ensure_header_row(self, header: list[str]) -> None:
        response = self._service.spreadsheets().values().get(
            spreadsheetId=self._spreadsheet_id,
            range=f"{self._worksheet_name}!1:1",
        ).execute()
        values = response.get("values", [])
        if values:
            existing_header = values[0]
            if existing_header != header:
                # Header schema changed; reset worksheet so row mapping stays stable.
                self._service.spreadsheets().values().clear(
                    spreadsheetId=self._spreadsheet_id,
                    range=self._worksheet_name,
                    body={},
                ).execute()
            else:
                return

        self._service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range=f"{self._worksheet_name}!1:1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

    def _resolve_worksheet_name(self, requested_name: str) -> str:
        metadata = self._service.spreadsheets().get(
            spreadsheetId=self._spreadsheet_id,
            fields="sheets.properties.title",
        ).execute()
        titles = [
            sheet.get("properties", {}).get("title", "")
            for sheet in metadata.get("sheets", [])
        ]
        if requested_name in titles:
            return requested_name
        if titles:
            return titles[0]
        return requested_name

    def _build_rows(self, items: list[PostEvidence]) -> list[list[str]]:
        header = [
            "Вопрос",
            "Ссылка на пост в инстаграм",
            "Саммари",
            "Название исследования",
            "Автор",
            "Год исследования",
            "Ссылка на исследование PMID",
            "Ссылка на полный текст исследования",
            "Тег по смыслу исследования",
        ]
        rows: list[list[str]] = [header]

        for item in items:
            common = [
                item.topic,
                item.post_url or "",
                item.summary,
            ]

            if not item.studies:
                rows.append(common + ["", "", "", "", "", ""])
                continue

            for study in item.studies:
                rows.append(
                    common
                    + [
                        study.title,
                        self._primary_author(study=study),
                        str(study.year) if study.year is not None else "",
                        study.pmid_url,
                        study.full_text_url or "",
                        self._study_tag(item=item, study=study),
                    ]
                )

        return rows

    @staticmethod
    def _primary_author(study: ResearchItem) -> str:
        if not study.authors:
            return ""
        return study.authors[0]

    def _study_tag(self, item: PostEvidence, study: ResearchItem) -> str:
        if study.tags:
            return ", ".join(study.tags[:4])
        cache_key = f"{item.topic}|{study.title}"
        if cache_key in self._ai_cache:
            return ", ".join(self._ai_cache[cache_key])

        ai_tags = self._classify_tags_with_ai(item=item, study=study)
        if ai_tags:
            self._ai_cache[cache_key] = ai_tags
            return ", ".join(ai_tags)

        text = f"{item.topic} {study.title}".lower()
        keyword_tags = {
            "витамин D": ("vitamin d", "d3", "cholecalciferol"),
            "креатин": ("creatine", "креатин"),
            "выпадение волос": ("hair loss", "alopecia", "выпадение волос"),
            "бад": ("supplement", "nutrition", "бады", "бад"),
            "здоровье": ("health", "metabolic", "wellness", "здоров"),
            "нейронаука": ("brain", "alzheimer", "neuro", "cognitive"),
            "спорт": ("exercise", "performance", "muscle", "training"),
        }

        tags: list[str] = []
        for tag, keywords in keyword_tags.items():
            if any(keyword in text for keyword in keywords):
                tags.append(tag)

        for tag in item.tags:
            if tag != item.topic and tag not in tags:
                tags.append(tag)

        if not tags:
            tags = [item.topic]
        tags = tags[:3]
        self._ai_cache[cache_key] = tags
        return ", ".join(tags)

    def _classify_tags_with_ai(
        self,
        item: PostEvidence,
        study: ResearchItem,
    ) -> list[str] | None:
        if not self._openai_api_key:
            return None

        article_text = study.title
        if study.abstract and study.abstract.strip():
            article_text += "\n\n" + study.abstract[:2000].rstrip()

        prompt = (
            "Прочитай статью и выбери 3–4 главных тега (ключевых слова), "
            "которые описывают её содержание. Теги: короткие (1–3 слова), "
            "на том же языке, что и статья. "
            "Верни только JSON: {\"tags\": [\"тег1\", \"тег2\", \"тег3\", \"тег4\"]}.\n\n"
            f"Статья:\n{article_text}"
        )

        payload = {
            "model": self._openai_model,
            "response_format": {"type": "json_object"},
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "Ты помогаешь извлекать теги из научных статей. "
                        "Возвращай только валидный JSON с полем tags."
                    ),
                },
                {"role": "user", "content": prompt},
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
        except Exception:
            return None

        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return None

        raw_tags = parsed.get("tags")
        if not isinstance(raw_tags, list):
            return None

        tags: list[str] = []
        for value in raw_tags:
            if isinstance(value, str) and value.strip():
                t = value.strip()[:50]
                if t and t not in tags:
                    tags.append(t)

        return tags[:4] if tags else None

    @staticmethod
    def _build_service(
        credentials_path: str | None,
        credentials_json: str | None,
    ):
        if credentials_path:
            credentials = Credentials.from_service_account_file(
                filename=str(Path(credentials_path)),
                scopes=SHEETS_SCOPE,
            )
        else:
            service_info = json.loads(credentials_json or "{}")
            credentials = Credentials.from_service_account_info(
                info=service_info,
                scopes=SHEETS_SCOPE,
            )

        return build(
            "sheets",
            "v4",
            credentials=credentials,
            cache_discovery=False,
        )
