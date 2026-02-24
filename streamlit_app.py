"""
Streamlit web UI for IG Parser — запуск пайплайна по ссылке в браузере.
"""

import json
import os
from pathlib import Path

import streamlit as st

from dotenv import load_dotenv

load_dotenv()

APP_VERSION = "1.6"
DEFAULT_SOURCES = ["dangarnernutrition"]


def _get_secret(key: str, default: str = "") -> str:
    """Read from Streamlit secrets (Cloud) or .env (local)."""
    try:
        secrets = getattr(st, "secrets", None)
        if secrets and hasattr(secrets, "__getitem__"):
            val = secrets.get(key)
            if val:
                return str(val)
    except (KeyError, TypeError):
        pass
    return os.getenv(key, default)


def _build_pipeline():
    """Build pipeline from env/secrets."""
    apify_token = _get_secret("APIFY_TOKEN")
    if not apify_token:
        st.error(
            "APIFY_TOKEN не задан. Добавьте в .env (локально) или в Secrets."
        )
        return None

    from app.services.apify_service import ApifyInstagramClient
    from app.services.pipeline import EvidencePipeline
    from app.services.pubmed_service import PubMedClient
    from app.services.relevance_service import StudyRelevanceChecker

    posts_actor_id = _get_secret(
        "APIFY_ACTOR_ID", "apify/instagram-post-scraper"
    )
    search_actor_id = _get_secret(
        "APIFY_SEARCH_ACTOR_ID", "apify/instagram-search-scraper"
    )
    ncbi_tool = _get_secret("NCBI_TOOL", "ig-parser-mvp")
    ncbi_email = _get_secret("NCBI_EMAIL")
    openai_api_key = _get_secret("OPENAI_API_KEY")
    openai_model = _get_secret("OPENAI_MODEL", "gpt-4o-mini")

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
    return pipeline


def _export_to_sheets_if_configured(items: list) -> int:
    spreadsheet_id = _get_secret("GOOGLE_SHEETS_SPREADSHEET_ID")
    if not spreadsheet_id:
        return 0

    from app.services.sheets_service import GoogleSheetsExporter

    worksheet_name = _get_secret("GOOGLE_SHEETS_WORKSHEET", "Sheet1")
    credentials_path = _get_secret("GOOGLE_SHEETS_CREDENTIALS_PATH")
    credentials_json = _get_secret("GOOGLE_SHEETS_CREDENTIALS_JSON")
    openai_api_key = _get_secret("OPENAI_API_KEY")
    openai_model = _get_secret("OPENAI_MODEL", "gpt-4o-mini")

    try:
        exporter = GoogleSheetsExporter(
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            credentials_path=credentials_path or None,
            credentials_json=credentials_json or None,
            openai_api_key=openai_api_key,
            openai_model=openai_model,
        )
        return exporter.export(items=items)
    except Exception as exc:
        st.warning(f"Экспорт в Google Sheets пропущен: {exc}")
        return 0


def main() -> None:
    st.set_page_config(
        page_title="IG Parser",
        page_icon="🔬",
        layout="wide",
    )
    st.title("🔬 IG Parser — Instagram → PubMed")
    st.caption(f"Версия {APP_VERSION}")
    st.markdown(
        "Введите тему или вопрос — пайплайн соберёт посты, извлечёт "
        "исследования из PubMed и выгрузит в Google Sheets (если настроено)."
    )

    topic = st.text_input(
        "Тема запроса",
        placeholder="например: омега-3 и деменция",
        help="Вопрос или ключевые слова для поиска",
    )

    search_full_profile = st.checkbox(
        "Искать по всей странице (до 500 постов)",
        value=True,
        help="Иначе — только последние N постов. По умолчанию включено.",
    )
    skip_relevance = st.checkbox(
        "Пропустить фильтр релевантности (отладка)",
        value=False,
        help="Показывать все найденные исследования без проверки на тему.",
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        max_items_input = st.number_input(
            "Макс. постов (если не «вся страница»)",
            min_value=20,
            max_value=500,
            value=100,
            disabled=search_full_profile,
        )
    with col2:
        discovery_limit = st.number_input(
            "Лимит источников", min_value=1, max_value=20, value=5
        )
    with col3:
        sources_input = st.text_input(
            "Источники (через запятую, default: dangarnernutrition)",
            placeholder="dangarnernutrition",
        )

    max_items = 500 if search_full_profile else max_items_input

    if st.button("Запустить", type="primary", use_container_width=True):
        if not topic or not topic.strip():
            st.error("Введите тему запроса.")
            return

        pipeline = _build_pipeline()
        if not pipeline:
            return

        if sources_input and sources_input.strip():
            sources = [s.strip() for s in sources_input.split(",") if s.strip()]
        else:
            sources = DEFAULT_SOURCES

        has_sheets = bool(_get_secret("GOOGLE_SHEETS_SPREADSHEET_ID"))

        with st.status("Обработка…", expanded=True) as status:
            st.write("**1. Поиск постов и извлечение исследований**")
            try:
                run_result = pipeline.run(
                    topic=topic.strip(),
                    sources=sources,
                    max_items=max_items,
                    discovery_limit=discovery_limit,
                    skip_relevance=skip_relevance,
                )
            except Exception as exc:
                st.exception(exc)
                return

            st.write(
                f"Получено постов: {run_result.posts_fetched}, "
                f"с подписями: {run_result.posts_with_caption}"
            )
            st.write(
                f"Постов с картинками: {run_result.debug_posts_with_images}, "
                f"PMID из текста: {run_result.debug_pmids_from_text}, "
                f"PMID из картинок: {run_result.debug_pmids_from_images}"
            )
            st.write(f"✓ Найдено записей: {len(run_result.items)}")

            results = run_result.items
            if not results:
                status.update(label="Готово", state="complete")
                if run_result.posts_with_caption == 0 and run_result.posts_fetched > 0:
                    st.info(
                        f"Apify вернул {run_result.posts_fetched} постов, "
                        "но ни у одного нет подписи. Проверьте имя аккаунта."
                    )
                elif run_result.posts_fetched == 0:
                    st.info(
                        "Постов не получено. Проверьте имя аккаунта и APIFY_TOKEN."
                    )
                else:
                    st.info(
                        "Релевантных постов с исследованиями не найдено. "
                        "Возможно, фильтр релевантности слишком строгий или "
                        "PMID не обнаружен в постах."
                    )
                    st.markdown(
                        "**Отладка:** попробуйте включить "
                        "«Пропустить фильтр релевантности» — если появятся "
                        "результаты, проблема в фильтре."
                    )
                return

            appended_rows = 0
            if has_sheets:
                st.write("**2. Выгрузка в Google Sheets**")
                appended_rows = _export_to_sheets_if_configured(items=results)
                st.write(f"✓ Добавлено строк: {appended_rows}")

            status.update(label="Готово", state="complete")

        if has_sheets and appended_rows > 0:
            spreadsheet_id = _get_secret("GOOGLE_SHEETS_SPREADSHEET_ID")
            if spreadsheet_id:
                sheets_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                st.markdown(f"📊 **Выгрузка в Google Sheets:** [открыть таблицу]({sheets_url})")

        for item in results:
            label = f"📌 {item.author_username or '—'} | {len(item.studies)} исследований"
            with st.expander(label):
                st.markdown(f"**Тема:** {item.topic}")
                if item.post_url:
                    st.markdown(f"[Ссылка на пост]({item.post_url})")
                summary = (
                    f"{item.summary[:500]}…"
                    if len(item.summary) > 500
                    else item.summary
                )
                st.markdown(f"**Кратко:** {summary}")
                st.markdown(f"**Теги:** {', '.join(item.tags)}")
                for study in item.studies:
                    link = f"- [{study.title}]({study.pmid_url}) — PMID:{study.pmid}"
                    st.markdown(link)

        output_dir = Path("output")
        output_dir.mkdir(parents=True, exist_ok=True)
        out_file = output_dir / "streamlit_result.json"
        payload = [r.model_dump(mode="json") for r in results]
        out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), "utf-8")
        st.download_button(
            "Скачать JSON",
            out_file.read_text(encoding="utf-8"),
            file_name="ig_parser_result.json",
            mime="application/json",
            use_container_width=True,
        )


if __name__ == "__main__":
    main()
