#!/usr/bin/env python3
"""
Локальная отладка: что Apify возвращает, загружаются ли картинки.
Запуск: poetry run python scripts/debug_apify_images.py
"""
import json
import os
import sys
from pathlib import Path

# проект в PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv

load_dotenv()

# Секреты из .env
APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR = os.getenv("APIFY_ACTOR_ID", "apify/instagram-post-scraper")
SOURCE = os.getenv("DEBUG_SOURCE", "dangarnernutrition")
MAX_POSTS = 5


def main() -> None:
    if not APIFY_TOKEN:
        print("Задай APIFY_TOKEN в .env")
        return

    from app.services.apify_service import ApifyInstagramClient
    from app.services.pipeline import EvidencePipeline

    client = ApifyInstagramClient(
        token=APIFY_TOKEN,
        posts_actor_id=APIFY_ACTOR,
        search_actor_id=os.getenv("APIFY_SEARCH_ACTOR_ID", "apify/instagram-search-scraper"),
    )

    print("1. Загрузка постов из Apify...")
    posts = client.fetch_posts(sources=[SOURCE], max_items=MAX_POSTS)
    print(f"   Получено постов: {len(posts)}")

    if not posts:
        print("   Постов нет. Проверь SOURCE и APIFY_TOKEN.")
        return

    # Сохраняем сырой вывод
    out_dir = Path("output/debug")
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / "apify_raw_posts.json"
    # Оставляем только ключи для анализа (без длинных вложений)
    sample = []
    for i, p in enumerate(posts[:3]):
        keys = list(p.keys()) if isinstance(p, dict) else []
        sample.append({
            "index": i,
            "keys": keys,
            "displayUrl": p.get("displayUrl") if isinstance(p, dict) else None,
            "imageUrl": p.get("imageUrl") if isinstance(p, dict) else None,
            "images_type": type(p.get("images")).__name__ if isinstance(p, dict) else None,
            "images_len": len(p.get("images") or []) if isinstance(p, dict) else 0,
            "childPosts_len": len(p.get("childPosts") or []) if isinstance(p, dict) else 0,
        })
        if p.get("childPosts"):
            first_child = p["childPosts"][0] if p["childPosts"] else {}
            sample[-1]["first_child_keys"] = list(first_child.keys()) if isinstance(first_child, dict) else []
            sample[-1]["first_child_displayUrl"] = first_child.get("displayUrl") if isinstance(first_child, dict) else None
    print(f"\n2. Структура постов (первые 3):")
    print(json.dumps(sample, indent=2, ensure_ascii=False))

    # Извлекаем URL через наш код
    from app.services.pipeline import EvidencePipeline

    total_urls = 0
    for i, post in enumerate(posts[:3]):
        urls = EvidencePipeline._extract_post_image_urls(post)
        total_urls += len(urls)
        print(f"\n   Пост {i}: извлечено {len(urls)} URL")
        for j, url in enumerate(urls[:2]):
            print(f"      [{j}] {url[:80]}...")

    print(f"\n   Всего URL из {min(3, len(posts))} постов: {total_urls}")

    # Пробуем загрузить один URL
    if total_urls > 0:
        test_url = EvidencePipeline._extract_post_image_urls(posts[0])[0]
        print(f"\n3. Загрузка тестовой картинки: {test_url[:60]}...")
        import httpx
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            "Accept": "image/*",
        }
        try:
            r = httpx.get(test_url, headers=headers, timeout=15.0)
            print(f"   HTTP {r.status_code}, size={len(r.content)} bytes")
            if r.status_code != 200:
                print(f"   Тело: {r.text[:200]}")
        except Exception as e:
            print(f"   Ошибка: {e}")

    print(f"\nСырые данные сохранены в {raw_path}")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(
            [{k: v for k, v in p.items() if k not in ("latestComments", "coauthors")} for p in posts[:2]],
            f,
            ensure_ascii=False,
            indent=2,
        )


if __name__ == "__main__":
    main()
