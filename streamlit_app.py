"""
Streamlit web UI for IG Parser — запуск пайплайна по ссылке в браузере.
"""

import json
import os
from pathlib import Path

import streamlit as st
from streamlit.errors import StreamlitSecretNotFoundError

from dotenv import load_dotenv

load_dotenv()

APP_VERSION = "1.9"
DEFAULT_SOURCES = ["dangarnernutrition"]


def _get_secret(key: str, default: str = "") -> str:
    """Read from Streamlit secrets (Cloud) or .env (local)."""
    try:
        secrets = getattr(st, "secrets", None)
        if secrets and hasattr(secrets, "__getitem__"):
            val = secrets.get(key)
            if val:
                return str(val)
    except (KeyError, TypeError, StreamlitSecretNotFoundError):
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

    from app.services.sync_state import load_state, save_state

    sync_state = load_state()
    saved_accounts = sync_state.get("accounts") or DEFAULT_SOURCES
    if isinstance(saved_accounts, list) and saved_accounts:
        default_sources = ", ".join(saved_accounts)
    else:
        default_sources = "dangarnernutrition"

    sources_input = st.text_input(
        "Блогер(ы) (через запятую)",
        value=default_sources,
        placeholder="dangarnernutrition, account2",
        help="Instagram username без @. Используется для теста по кнопке и для авто-синхронизации (после «Сохранить»).",
    )

    col_save, _ = st.columns([1, 3])
    with col_save:
        if st.button("Сохранить аккаунты для мониторинга"):
            accounts = [s.strip() for s in sources_input.split(",") if s.strip()]
            if accounts:
                sync_state["accounts"] = accounts
                save_state(sync_state)
                st.success(
                    f"Сохранено: {len(accounts)} аккаунт(ов). "
                    "Авто-синхронизация (cron 8:00 МСК) будет использовать этот список."
                )
            else:
                st.warning("Укажите хотя бы один аккаунт.")

    spreadsheet_id = _get_secret("GOOGLE_SHEETS_SPREADSHEET_ID")
    sheets_link = ""
    if spreadsheet_id:
        sheets_link = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"

    st.markdown(
        "Автоматическая выгрузка новых постов происходит ежедневно в 8:00 по МСК. "
        "Все выгруженные данные хранятся в таблице"
        + (f": [открыть]({sheets_link})" if sheets_link else ".") + "."
    )

    if st.button("Выгрузить новые посты сейчас", type="primary", use_container_width=True):
        if not sources_input or not sources_input.strip():
            st.error("Укажите хотя бы один блогер.")
            return

        pipeline = _build_pipeline()
        if not pipeline:
            return

        sources = [s.strip() for s in sources_input.split(",") if s.strip()]
        if not sources:
            sources = DEFAULT_SOURCES

        has_sheets = bool(_get_secret("GOOGLE_SHEETS_SPREADSHEET_ID"))

        openai_ok = bool(_get_secret("OPENAI_API_KEY"))
        with st.status("Обработка…", expanded=True) as status:
            st.write(
                f"**1. Поиск постов и извлечение исследований** "
                f"(OPENAI: {'✓ ключ задан' if openai_ok else '⚠ ключ НЕ задан'})"
            )
            try:
                run_result = pipeline.run(
                    topic="",
                    sources=sources,
                    max_items=20,
                    discovery_limit=1,
                    skip_relevance=True,
                    latest_posts_mode=True,
                )
            except Exception as exc:
                st.exception(exc)
                return

            st.write(
                f"Получено постов: {run_result.posts_fetched}, "
                f"с подписями: {run_result.posts_with_caption}"
            )
            if run_result.debug_apify_first_post:
                with st.expander("Формат Apify (структура первого поста)", expanded=True):
                    st.code(run_result.debug_apify_first_post)
            st.write(
                f"URL картинок извлечено: {run_result.debug_total_image_urls}, "
                f"постов с картинками: {run_result.debug_posts_with_images}, "
                f"PMID из текста: {run_result.debug_pmids_from_text}, "
                f"PMID из картинок: {run_result.debug_pmids_from_images}"
            )
            st.write(
                f"Картинок загружено: {run_result.debug_images_fetched}, "
                f"не удалось загрузить: {run_result.debug_images_failed}"
            )
            if run_result.debug_sample_url:
                with st.expander("Отладка: первый URL и статус загрузки"):
                    st.code(run_result.debug_sample_url)
                    st.write(f"Статус: `{run_result.debug_sample_status}`")

            has_images = run_result.debug_posts_with_images > 0
            all_failed = run_result.debug_images_failed > 0 and run_result.debug_images_fetched == 0
            sample_status = run_result.debug_sample_status
            cdn_blocked = has_images and all_failed and sample_status in ("403", "429", "401")
            no_key_for_images = sample_status == "no_openai_key"
            no_attempt = has_images and run_result.debug_images_fetched == 0 and run_result.debug_images_failed == 0
            if no_key_for_images:
                st.warning(
                    "⚠ **Картинки не обрабатываются:** OPENAI_API_KEY не передан в пайплайн. "
                    "Проверьте Secrets — ключ должен быть доступен при сборке пайплайна."
                )
            elif cdn_blocked:
                st.error(
                    "🔒 **Instagram CDN блокирует запросы** (HTTP "
                    + sample_status
                    + "). Сервера хостинга отклоняются — картинки не загружаются. "
                    "Результаты только по тексту. Запустите локально для полной обработки."
                )
            elif no_attempt:
                st.warning(
                    "Картинки не загружались (0 попыток). Fallback по тексту должен сработать "
                    "для постов с упоминанием «position stand», «ISSN» и т.п."
                )
            if run_result.debug_pmids_fetch_failed > 0:
                st.warning(
                    f"Загрузка из PubMed не удалась для {run_result.debug_pmids_fetch_failed} "
                    "PMID. Возможно, неверный PMID (ложное срабатывание) или "
                    "статья не найдена в PubMed."
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
                        "В последних постах не обнаружено PMID или исследований. "
                        "Проверьте, что пост содержит скриншоты PubMed или ссылки."
                    )
                    with st.expander("Диагностика: caption и поиск по названию"):
                        if run_result.debug_first_caption_snippet:
                            snip = run_result.debug_first_caption_snippet
                            st.write("**Фрагмент подписи:**")
                            st.code(snip[:300] + ("…" if len(snip) > 300 else ""))
                        else:
                            st.write("**Фрагмент подписи:** (пусто)")
                        st.write(
                            f"**Поиск по названию:** "
                            f"{run_result.debug_title_candidates_tried} кандидатов → "
                            f"найдено PMID: {run_result.debug_pmids_from_title_search}"
                        )
                        if run_result.debug_first_title_candidate:
                            st.write(
                                "**Первый кандидат:** "
                                f"`{run_result.debug_first_title_candidate}`"
                            )
                        if run_result.debug_pubmed_search_error:
                            st.error(
                                f"**Ошибка PubMed:** {run_result.debug_pubmed_search_error}"
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
                st.markdown(f"**Теги:** {', '.join(item.tags) or '—'}")
                for study in item.studies:
                    link = f"- [{study.title}]({study.pmid_url}) — PMID:{study.pmid}"
                    if study.tags:
                        link += f" — {', '.join(study.tags)}"
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
