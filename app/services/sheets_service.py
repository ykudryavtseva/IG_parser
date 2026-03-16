import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import httpx
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from app.models import PostEvidence, ResearchItem

SHEETS_SCOPE = ["https://www.googleapis.com/auth/spreadsheets"]
MAX_CELL_LEN = 40_000  # Google Sheets ~50k limit per cell


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
        self._worksheet_name, self._worksheet_fallback = (
            self._resolve_worksheet_name(requested_name=worksheet_name)
        )
        self._last_exported_rows: list[list[str]] | None = None

    def get_last_exported_rows(self) -> list[list[str]] | None:
        """Rows from last export (header + data), for local storage sync."""
        return getattr(self, "_last_exported_rows", None)

    def _range(self, a1: str = "") -> str:
        """Build A1 range; quote sheet name if it contains spaces/special chars."""
        name = self._worksheet_name
        needs_quotes = " " in name or "'" in name or "!" in name
        quoted = f"'{name}'" if needs_quotes else name
        return f"{quoted}!{a1}" if a1 else quoted

    def export(self, items: list[PostEvidence]) -> int:
        self._prefill_ai_cache_parallel(items=items)
        rows = self._build_rows(items=items)
        self._last_exported_rows = rows
        if len(rows) == 1:
            return 0

        self._ensure_header_row(header=rows[0])
        body = {"values": rows[1:]}
        response = (
            self._service.spreadsheets()
            .values()
            .append(
                spreadsheetId=self._spreadsheet_id,
                range=self._range("A1"),
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body=body,
            )
            .execute()
        )
        updated_rows = (
            response.get("updates", {}).get("updatedRows") or len(rows) - 1
        )
        if updated_rows != len(rows) - 1:
            raise RuntimeError(
                f"Sheets API reported {updated_rows} rows updated, expected "
                f"{len(rows) - 1}"
            )
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
        response = (
            self._service.spreadsheets()
            .values()
            .get(
                spreadsheetId=self._spreadsheet_id,
                range=self._range("1:1"),
            )
            .execute()
        )
        values = response.get("values", [])
        if values:
            existing_header = values[0]
            if existing_header != header:
                # Header changed; reset worksheet for stable row mapping.
                self._service.spreadsheets().values().clear(
                    spreadsheetId=self._spreadsheet_id,
                    range=self._range(),
                    body={},
                ).execute()
            else:
                return

        self._service.spreadsheets().values().update(
            spreadsheetId=self._spreadsheet_id,
            range=self._range("1:1"),
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()

    def _resolve_worksheet_name(self, requested_name: str) -> tuple[str, bool]:
        """Return (resolved_name, used_fallback). Fallback=True if requested not found."""
        metadata = (
            self._service.spreadsheets()
            .get(
                spreadsheetId=self._spreadsheet_id,
                fields="sheets.properties.title",
            )
            .execute()
        )
        titles = [
            sheet.get("properties", {}).get("title", "")
            for sheet in metadata.get("sheets", [])
        ]
        if requested_name in titles:
            return requested_name, False
        if titles:
            return titles[0], True
        return requested_name, False

    def get_worksheet_info(self) -> tuple[str, bool]:
        """Return (resolved_worksheet_name, used_fallback)."""
        return self._worksheet_name, getattr(
            self, "_worksheet_fallback", False
        )

    def get_sheet_gid(self) -> int | None:
        """Return sheetId (gid) for the worksheet, for URL #gid=."""
        try:
            metadata = (
                self._service.spreadsheets()
                .get(
                    spreadsheetId=self._spreadsheet_id,
                    fields="sheets.properties(sheetId,title)",
                )
                .execute()
            )
            for sheet in metadata.get("sheets", []):
                props = sheet.get("properties", {})
                if props.get("title") == self._worksheet_name:
                    return props.get("sheetId")
        except Exception:
            pass
        return None

    @staticmethod
    def _cell(val: str, max_len: int = 40000) -> str:
        """Truncate cell value to avoid Sheets API limits (~50k chars/cell)."""
        s = val or ""
        return s[:max_len] + ("…" if len(s) > max_len else "")

    def _build_rows(self, items: list[PostEvidence]) -> list[list[str]]:
        header = [
            "Название поста",
            "Инстаграм",
            "Дата публикации",
            "Ссылка на пост в инстаграм",
            "Тип контента",
            "Описание",
            "Картинка",
            "Транскрипт",
            "Саммари",
            "Название исследования",
            "Автор",
            "Год исследования",
            "Ссылка на исследование PMID",
            "Ссылка на полный текст исследования",
            "Тег по смыслу исследования",
            "Источник цитаты",
        ]
        rows: list[list[str]] = [header]
        post_cols_count = 9
        study_cols_count = 7
        empty_post_block = [""] * post_cols_count

        for item in items:
            post_block = [
                self._cell(item.topic),
                item.author_username or "",
                item.published_at or "",
                item.post_url or "",
                item.content_type or "",
                self._cell(item.caption or ""),
                item.image_url or "",
                self._cell(item.transcript or ""),
                self._cell(item.summary),
            ]

            if not item.studies:
                rows.append(post_block + [""] * study_cols_count)
                continue

            for idx, study in enumerate(item.studies):
                study_data = [
                    self._cell(study.title),
                    self._primary_author(study=study),
                    str(study.year) if study.year is not None else "",
                    study.pmid_url or "",
                    study.full_text_url or "",
                    self._study_tag(item=item, study=study),
                    study.citation_source or "",
                ]
                if idx == 0:
                    rows.append(post_block + study_data)
                else:
                    rows.append(empty_post_block + study_data)

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
            'Верни только JSON: {"tags": ["тег1", "тег2", "тег3"]}.\n\n'
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
