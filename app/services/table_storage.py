"""Local CSV storage for research table — autonomous from Google Sheets."""

from pathlib import Path

import pandas as pd

from app.models import PostEvidence, ResearchItem

TABLE_HEADER = [
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

TWITTER_TABLE_HEADER = [
    "Название поста",
    "Twitter",
    "Дата публикации",
    "Ссылка на пост",
    "Тип контента",
    "Текст",
    "Картинка",
    "Транскрипт",
    "Саммари",
    "Название исследования",
    "Автор",
    "Год исследования",
    "Ссылка на PMID",
    "Ссылка на полный текст",
    "Тег по смыслу",
    "Источник цитаты",
]

DELETE_COL = "Удалить"


def _keyword_study_tag(item: PostEvidence, study: ResearchItem) -> str:
    """Keyword-based study tag (no AI, for autonomous storage)."""
    if study.tags:
        return ", ".join(study.tags[:4])
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
        if any(kw in text for kw in keywords):
            tags.append(tag)
    for tag in item.tags:
        if tag != item.topic and tag not in tags:
            tags.append(tag)
    if not tags:
        tags = [item.topic]
    return ", ".join(tags[:3])


def build_rows_from_items(items: list[PostEvidence]) -> list[list[str]]:
    """Build table rows from PostEvidence items (same structure as Sheets)."""
    post_cols = 9
    study_cols = 7
    empty_post = [""] * post_cols
    rows: list[list[str]] = []

    for item in items:
        post_block = [
            item.topic,
            item.author_username or "",
            item.published_at or "",
            item.post_url or "",
            item.content_type or "",
            item.caption or "",
            item.image_url or "",
            item.transcript or "",
            item.summary,
        ]
        if not item.studies:
            rows.append(post_block + [""] * study_cols)
            continue
        for idx, study in enumerate(item.studies):
            study_data = [
                study.title,
                study.authors[0] if study.authors else "",
                str(study.year) if study.year is not None else "",
                study.pmid_url,
                study.full_text_url or "",
                _keyword_study_tag(item=item, study=study),
                study.citation_source or "",
            ]
            rows.append(post_block + study_data if idx == 0 else empty_post + study_data)
    return rows


def get_table_path() -> Path:
    """Path to local research table CSV."""
    out = Path("output")
    out.mkdir(parents=True, exist_ok=True)
    return out / "research_table.csv"


def get_twitter_table_path() -> Path:
    """Path to local Twitter table CSV."""
    out = Path("output")
    out.mkdir(parents=True, exist_ok=True)
    return out / "twitter_table.csv"


def append_rows_to_csv(rows: list[list[str]]) -> int:
    """
    Append data rows to local CSV. Creates file with header if needed.
    Returns number of rows appended.
    """
    if not rows:
        return 0
    path = get_table_path()
    write_header = not path.exists()
    df_new = pd.DataFrame(rows, columns=TABLE_HEADER)
    df_new.to_csv(
        path,
        mode="a" if not write_header else "w",
        index=False,
        header=write_header,
        encoding="utf-8",
    )
    return len(rows)


def build_twitter_rows_from_items(items: list[PostEvidence]) -> list[list[str]]:
    """Build table rows from PostEvidence (Twitter structure)."""
    post_cols = 9
    study_cols = 7
    empty_post = [""] * post_cols
    rows: list[list[str]] = []

    for item in items:
        post_block = [
            item.topic,
            item.author_username or "",
            item.published_at or "",
            item.post_url or "",
            item.content_type or "",
            item.caption or "",
            item.image_url or "",
            item.transcript or "",
            item.summary,
        ]
        if not item.studies:
            rows.append(post_block + [""] * study_cols)
            continue
        for idx, study in enumerate(item.studies):
            study_data = [
                study.title,
                study.authors[0] if study.authors else "",
                str(study.year) if study.year is not None else "",
                study.pmid_url,
                study.full_text_url or "",
                _keyword_study_tag(item=item, study=study),
                study.citation_source or "",
            ]
            rows.append(post_block + study_data if idx == 0 else empty_post + study_data)
    return rows


def append_twitter_rows_to_csv(rows: list[list[str]]) -> int:
    """Append Twitter rows to twitter_table.csv. Returns rows appended."""
    if not rows:
        return 0
    path = get_twitter_table_path()
    write_header = not path.exists()
    df_new = pd.DataFrame(rows, columns=TWITTER_TABLE_HEADER)
    df_new.to_csv(
        path,
        mode="a" if not write_header else "w",
        index=False,
        header=write_header,
        encoding="utf-8",
    )
    return len(rows)


def load_table_csv() -> pd.DataFrame:
    """Load local table as DataFrame. Adds Удалить column for editing."""
    path = get_table_path()
    if not path.exists():
        df = pd.DataFrame(columns=TABLE_HEADER)
    else:
        df = pd.read_csv(path, encoding="utf-8")
        df = df.fillna("")
    if DELETE_COL not in df.columns:
        df.insert(0, DELETE_COL, False)
    return df


def save_table_csv(df: pd.DataFrame) -> None:
    """Save DataFrame to local CSV, excluding rows marked for deletion."""
    path = get_table_path()
    out = df.copy()
    if DELETE_COL in out.columns:
        drop_mask = out[DELETE_COL].apply(
            lambda x: str(x).lower().strip() in ("true", "1", "да", "yes")
            if pd.notna(x)
            else False
        )
        out = out.loc[~drop_mask].drop(columns=[DELETE_COL])
    out.to_csv(path, index=False, encoding="utf-8")


def load_twitter_table_csv() -> pd.DataFrame:
    """Load Twitter table as DataFrame. Adds Удалить column for editing."""
    path = get_twitter_table_path()
    if not path.exists():
        df = pd.DataFrame(columns=TWITTER_TABLE_HEADER)
    else:
        df = pd.read_csv(path, encoding="utf-8")
        df = df.fillna("")
    if DELETE_COL not in df.columns:
        df.insert(0, DELETE_COL, False)
    return df


def save_twitter_table_csv(df: pd.DataFrame) -> None:
    """Save Twitter DataFrame to local CSV, excluding rows marked for deletion."""
    path = get_twitter_table_path()
    out = df.copy()
    if DELETE_COL in out.columns:
        drop_mask = out[DELETE_COL].apply(
            lambda x: str(x).lower().strip() in ("true", "1", "да", "yes")
            if pd.notna(x)
            else False
        )
        out = out.loc[~drop_mask].drop(columns=[DELETE_COL])
    out.to_csv(path, index=False, encoding="utf-8")
