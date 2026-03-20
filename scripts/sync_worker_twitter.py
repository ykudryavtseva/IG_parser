#!/usr/bin/env python3
"""
Фоновый воркер для автоматической синхронизации Twitter → Google Sheets (Лист 2).

Читает twitter_accounts и состояние из data/sync_state.json, запускает Twitter-пайплайн,
выгружает в Лист 2 и twitter_table.csv. Запуск: cron в 8:00 МСК (отдельно от IG).
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

from app.services.sync_state import (
    get_only_twitter_newer_than,
    load_state,
    mark_twitter_run_complete,
)


def _get_env(key: str, default: str = "") -> str:
    return os.getenv(key, default)


def main() -> int:
    state = load_state()
    accounts = state.get("twitter_accounts") or []

    if not accounts:
        print("Нет Twitter-аккаунтов для мониторинга. Добавьте через Streamlit.")
        return 0

    apify_token = _get_env("APIFY_TOKEN")
    if not apify_token:
        print("APIFY_TOKEN не задан.")
        return 1

    from app.services.pubmed_service import PubMedClient
    from app.services.relevance_service import StudyRelevanceChecker
    from app.services.twitter_apify_client import ApifyTwitterClient
    from app.services.twitter_pipeline import TwitterPipeline

    twitter_actor_id = _get_env(
        "APIFY_TWITTER_ACTOR_ID", "apidojo/twitter-scraper-lite"
    )
    ncbi_tool = _get_env("NCBI_TOOL", "ig-parser-mvp")
    ncbi_email = _get_env("NCBI_EMAIL")
    openai_api_key = _get_env("OPENAI_API_KEY")
    openai_model = _get_env("OPENAI_MODEL", "gpt-4o-mini")

    twitter_client = ApifyTwitterClient(
        token=apify_token,
        actor_id=twitter_actor_id,
    )
    pubmed_client = PubMedClient(tool=ncbi_tool, email=ncbi_email)
    relevance_checker = StudyRelevanceChecker(
        openai_api_key=openai_api_key,
        openai_model=openai_model,
    )
    pipeline = TwitterPipeline(
        twitter_client=twitter_client,
        pubmed_client=pubmed_client,
        relevance_checker=relevance_checker,
        openai_api_key=openai_api_key,
        openai_model=openai_model,
    )

    only_newer = get_only_twitter_newer_than(state)
    processed = set(state.get("processed_twitter_ids") or [])

    run_result = pipeline.run(
        handles=accounts,
        max_items=50,
        only_newer_than=only_newer,
        processed_tweet_ids=processed,
        skip_scientific_filter=True,
    )

    items = run_result.items
    new_ids = [item.post_url or "" for item in items if item.post_url]

    if items:
        rows_for_local: list = []
        spreadsheet_id = _get_env("GOOGLE_SHEETS_SPREADSHEET_ID")
        if spreadsheet_id:
            from app.services.sheets_service import GoogleSheetsExporter

            credentials_path = _get_env("GOOGLE_SHEETS_CREDENTIALS_PATH") or None
            credentials_json = _get_env("GOOGLE_SHEETS_CREDENTIALS_JSON") or None
            try:
                exporter = GoogleSheetsExporter(
                    spreadsheet_id=spreadsheet_id,
                    worksheet_name="Лист2",
                    credentials_path=credentials_path,
                    credentials_json=credentials_json,
                    openai_api_key=openai_api_key,
                    openai_model=openai_model,
                    source="twitter",
                )
                exported = exporter.export(items=items)
                print(f"Выгружено в Sheets (Лист 2): {exported} строк.")
                export_rows = exporter.get_last_exported_rows()
                if export_rows and len(export_rows) > 1:
                    rows_for_local = export_rows[1:]
            except Exception as exc:
                print(f"Ошибка экспорта в Sheets: {exc}")
                rows_for_local = []
        else:
            print("GOOGLE_SHEETS_SPREADSHEET_ID не задан.")

        if not rows_for_local:
            from app.services.table_storage import build_twitter_rows_from_items

            rows_for_local = build_twitter_rows_from_items(items)
        if rows_for_local:
            from app.services.table_storage import append_twitter_rows_to_csv

            n = append_twitter_rows_to_csv(rows_for_local)
            print(f"Добавлено в twitter_table.csv: {n} строк.")
    else:
        print("Нет новых твитов с исследованиями.")

    mark_twitter_run_complete(state, new_ids)
    print(f"Готово. Обработано: {len(items)}, last_twitter_run_at обновлён.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
