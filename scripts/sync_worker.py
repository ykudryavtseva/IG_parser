#!/usr/bin/env python3
"""
Фоновый воркер для автоматической синхронизации Instagram → Google Sheets.

Читает аккаунты и состояние из data/sync_state.json, запускает пайплайн,
выгружает в Sheets, обновляет state. Запуск: cron в 8:00 МСК (5:00 UTC).
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from app.services.sync_state import (
    get_only_posts_newer_than,
    load_state,
    mark_run_complete,
)


def _get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def main() -> int:

    state = load_state()
    accounts = state.get("accounts") or []

    if not accounts:
        print("Нет аккаунтов для мониторинга. Добавьте их через Streamlit и сохраните.")
        return 0

    apify_token = _get_env("APIFY_TOKEN")
    if not apify_token:
        print("APIFY_TOKEN не задан.")
        return 1

    from app.services.apify_service import ApifyInstagramClient
    from app.services.pipeline import EvidencePipeline
    from app.services.pubmed_service import PubMedClient
    from app.services.relevance_service import StudyRelevanceChecker

    posts_actor_id = _get_env("APIFY_ACTOR_ID", "apify/instagram-post-scraper")
    search_actor_id = _get_env("APIFY_SEARCH_ACTOR_ID", "apify/instagram-search-scraper")
    ncbi_tool = _get_env("NCBI_TOOL", "ig-parser-mvp")
    ncbi_email = _get_env("NCBI_EMAIL")
    openai_api_key = _get_env("OPENAI_API_KEY")
    openai_model = _get_env("OPENAI_MODEL", "gpt-4o-mini")

    instagram_client = ApifyInstagramClient(
        token=apify_token,
        posts_actor_id=posts_actor_id,
        search_actor_id=search_actor_id,
    )
    pubmed_client = PubMedClient(tool=ncbi_tool, email=ncbi_email)
    relevance_checker = StudyRelevanceChecker(
        openai_api_key=openai_api_key,
        openai_model=openai_model,
    )
    pipeline = EvidencePipeline(
        instagram_client=instagram_client,
        pubmed_client=pubmed_client,
        relevance_checker=relevance_checker,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
    )

    only_newer = get_only_posts_newer_than(state)
    processed = set(state.get("processed_post_ids") or [])

    run_result = pipeline.run(
        topic="",
        sources=accounts,
        max_items=20,
        discovery_limit=1,
        skip_relevance=True,
        latest_posts_mode=True,
        only_posts_newer_than=only_newer,
        processed_post_ids=processed,
    )

    items = run_result.items
    new_post_ids = [item.post_url or "" for item in items if item.post_url]

    if items:
        spreadsheet_id = _get_env("GOOGLE_SHEETS_SPREADSHEET_ID")
        if spreadsheet_id:
            from app.services.sheets_service import GoogleSheetsExporter

            credentials_path = _get_env("GOOGLE_SHEETS_CREDENTIALS_PATH") or None
            credentials_json = _get_env("GOOGLE_SHEETS_CREDENTIALS_JSON") or None
            worksheet_name = _get_env("GOOGLE_SHEETS_WORKSHEET", "Sheet1")
            try:
                exporter = GoogleSheetsExporter(
                    spreadsheet_id=spreadsheet_id,
                    worksheet_name=worksheet_name,
                    credentials_path=credentials_path,
                    credentials_json=credentials_json,
                    openai_api_key=openai_api_key,
                    openai_model=openai_model,
                )
                exported = exporter.export(items=items)
                print(f"Выгружено в Sheets: {exported} строк.")
            except Exception as exc:
                print(f"Ошибка экспорта в Sheets: {exc}")
                return 1
        else:
            print("GOOGLE_SHEETS_SPREADSHEET_ID не задан. Пропуск экспорта.")
    else:
        print("Нет новых постов с исследованиями.")

    mark_run_complete(state, new_post_ids)
    print(f"Готово. Обработано постов: {len(items)}, last_run обновлён.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
