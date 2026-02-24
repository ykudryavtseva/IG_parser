import argparse
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from app.models import PostEvidence
from app.services.apify_service import ApifyInstagramClient
from app.services.pipeline import EvidencePipeline
from app.services.pubmed_service import PubMedClient
from app.services.relevance_service import StudyRelevanceChecker

DEFAULT_SOURCES = ["dangarnernutrition"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Instagram + PubMed evidence MVP")
    parser.add_argument("--topic", required=False, help="Topic question to investigate")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        required=False,
        help=(
            "Instagram username, profile URL, or post URL. "
            "Default: dangarnernutrition"
        ),
    )
    parser.add_argument("--max-items", type=int, default=100)
    parser.add_argument("--discovery-limit", type=int, default=5)
    parser.add_argument("--out-file", default="output/mvp_result.json")
    return parser.parse_args()


def main() -> None:
    load_dotenv()
    args = parse_args()
    topic = (args.topic or "").strip()
    if not topic:
        topic = input("Введите тему запроса: ").strip()
    if not topic:
        raise RuntimeError("Topic is required")

    apify_token = os.getenv("APIFY_TOKEN")
    if not apify_token:
        raise RuntimeError("APIFY_TOKEN is required in environment variables")

    posts_actor_id = os.getenv("APIFY_ACTOR_ID", "apify/instagram-post-scraper")
    search_actor_id = os.getenv(
        "APIFY_SEARCH_ACTOR_ID",
        "apify/instagram-search-scraper",
    )
    ncbi_tool = os.getenv("NCBI_TOOL", "ig-parser-mvp")
    ncbi_email = os.getenv("NCBI_EMAIL")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

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
    sources = args.sources or DEFAULT_SOURCES

    results = pipeline.run(
        topic=topic,
        sources=sources,
        max_items=args.max_items,
        discovery_limit=args.discovery_limit,
    )

    output_path = Path(args.out_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = [result.model_dump(mode="json") for result in results]
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")
    _export_to_sheets_if_configured(items=results)

    print(f"Saved {len(payload)} records to {output_path}")


def _export_to_sheets_if_configured(items: list[PostEvidence]) -> None:
    spreadsheet_id = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        return

    from app.services.sheets_service import GoogleSheetsExporter

    worksheet_name = os.getenv("GOOGLE_SHEETS_WORKSHEET", "Sheet1")
    credentials_path = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")
    credentials_json = os.getenv("GOOGLE_SHEETS_CREDENTIALS_JSON")
    openai_api_key = os.getenv("OPENAI_API_KEY")
    openai_model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    try:
        exporter = GoogleSheetsExporter(
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            credentials_path=credentials_path,
            credentials_json=credentials_json,
            openai_api_key=openai_api_key,
            openai_model=openai_model,
        )
        appended_rows = exporter.export(items=items)
        print(
            f"Google Sheets updated: spreadsheet={spreadsheet_id}, "
            f"worksheet={worksheet_name}, rows={appended_rows}"
        )
    except Exception as exc:
        print(f"Google Sheets export skipped: {exc}")


if __name__ == "__main__":
    main()
