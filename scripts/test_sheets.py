#!/usr/bin/env python3
"""
Проверка подключения к Google Sheets.
Запуск: poetry run python scripts/test_sheets.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()


def _get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def main() -> int:
    print("=== Проверка Google Sheets ===\n")

    spreadsheet_id = _get_env("GOOGLE_SHEETS_SPREADSHEET_ID")
    worksheet_name = _get_env("GOOGLE_SHEETS_WORKSHEET", "Sheet1")
    credentials_path = _get_env("GOOGLE_SHEETS_CREDENTIALS_PATH") or None
    credentials_json = _get_env("GOOGLE_SHEETS_CREDENTIALS_JSON")

    if not spreadsheet_id:
        print("❌ GOOGLE_SHEETS_SPREADSHEET_ID не задан в .env")
        return 1

    if not credentials_path and not credentials_json:
        print("❌ Нужен GOOGLE_SHEETS_CREDENTIALS_PATH или GOOGLE_SHEETS_CREDENTIALS_JSON")
        return 1

    print(f"Spreadsheet ID: …{spreadsheet_id[-12:]}")
    print(f"Лист: {worksheet_name}")
    print()

    try:
        from app.services.sheets_service import GoogleSheetsExporter

        exporter = GoogleSheetsExporter(
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            credentials_path=credentials_path,
            credentials_json=credentials_json,
            openai_api_key=None,
            openai_model="gpt-4o-mini",
        )
        resolved, fallback = exporter.get_worksheet_info()
        if fallback:
            print(f"⚠ Лист «{worksheet_name}» не найден, используется «{resolved}»")
        else:
            print(f"✓ Подключение к листу «{resolved}» OK")

        gid = exporter.get_sheet_gid()
        if gid is not None:
            print(f"✓ GID листа: {gid}")
            url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}#gid={gid}"
            print(f"  Ссылка: {url}")

        from app.models import PostEvidence, ResearchItem

        test_item = PostEvidence(
            topic="[ТЕСТ] Проверка подключения",
            summary="Удалите эту строку — тест записи.",
            tags=[],
            studies=[
                ResearchItem(
                    title="Test",
                    authors=[],
                    year=None,
                    pmid="0",
                    pmid_url="",
                    full_text_url=None,
                    abstract=None,
                    tags=[],
                    citation_source="test",
                )
            ],
            post_url=None,
            author_username="test",
            published_at="",
            content_type="",
            caption="",
            image_url="",
            transcript="",
        )

        count = exporter.export(items=[test_item])
        print(f"\n✓ Запись выполнена: добавлено {count} строк(и)")
        print("  Откройте таблицу и удалите тестовую строку с [ТЕСТ] в начале.")

    except Exception as exc:
        print(f"\n❌ Ошибка: {exc}")
        if "403" in str(exc) or "permission" in str(exc).lower():
            print(
                "\nПодсказка: Поделитесь таблицей с email сервисного аккаунта "
                "(из GOOGLE_SHEETS_CREDENTIALS_JSON, поле client_email) с правами «Редактор»."
            )
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
