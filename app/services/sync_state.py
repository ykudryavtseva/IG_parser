"""
Хранение состояния синхронизации для автоматического мониторинга Instagram.

Состояние: список аккаунтов, время последнего запуска, ID обработанных постов.
"""

import json
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_STATE_PATH = Path.home() / ".ig_parser_state.json"
MAX_PROCESSED_IDS = 10_000  # ограничить размер, старые удалять


def _default_state() -> dict:
    return {
        "last_run_at": None,
        "accounts": [],
        "processed_post_ids": [],
    }


def load_state(path: Path | None = None) -> dict:
    """Загрузить состояние из JSON-файла."""
    filepath = path or Path(
        __file__
    ).resolve().parent.parent.parent / "data" / "sync_state.json"
    if not filepath.exists():
        return _default_state()
    try:
        raw = filepath.read_text(encoding="utf-8")
        data = json.loads(raw)
        out = _default_state()
        out["last_run_at"] = data.get("last_run_at")
        out["accounts"] = list(data.get("accounts", [])) if data.get("accounts") else []
        out["processed_post_ids"] = list(
            data.get("processed_post_ids", []) or []
        )[-MAX_PROCESSED_IDS:]
        return out
    except (json.JSONDecodeError, OSError):
        return _default_state()


def save_state(
    state: dict,
    path: Path | None = None,
) -> None:
    """Сохранить состояние в JSON-файл."""
    filepath = path or Path(
        __file__
    ).resolve().parent.parent.parent / "data" / "sync_state.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "last_run_at": state.get("last_run_at"),
        "accounts": state.get("accounts", []),
        "processed_post_ids": state.get("processed_post_ids", [])[-MAX_PROCESSED_IDS:],
    }
    filepath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def get_only_posts_newer_than(state: dict) -> str | None:
    """
    Вернуть значение для onlyPostsNewerThan на основе last_run_at.

    Используем полную дату-время, чтобы не подтягивать посты из того же дня.
    Иначе — "1 day" для первого запуска.
    """
    last = state.get("last_run_at")
    if not last:
        return "1 day"
    try:
        dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except (ValueError, TypeError):
        return "1 day"


def mark_run_complete(
    state: dict,
    new_post_ids: list[str],
    path: Path | None = None,
) -> None:
    """Обновить last_run_at и добавить обработанные post_id."""
    now = datetime.now(timezone.utc).isoformat()
    state["last_run_at"] = now
    seen = set(state.get("processed_post_ids", []))
    for pid in new_post_ids:
        if pid and pid not in seen:
            seen.add(pid)
    state["processed_post_ids"] = list(seen)[-MAX_PROCESSED_IDS:]
    save_state(state, path)
