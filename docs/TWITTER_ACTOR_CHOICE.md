# Twitter в IG Parser

## Настройка Google Sheets

Создайте **Лист 2** в таблице (рядом с Лист1 для Instagram). Twitter-данные выгружаются туда.

---

# Выбор Apify-актора для Twitter

## Рекомендация: `apidojo/twitter-scraper-lite` (Twitter Scraper Unlimited)

**URL:** https://apify.com/apidojo/twitter-scraper-lite

### Почему этот актор

| Критерий | apidojo/twitter-scraper-lite | web.harvester/twitter-scraper | apidojo/tweet-scraper |
|----------|------------------------------|-------------------------------|------------------------|
| **Цена** | Event-based (~$0.016/запрос, первые 40 твитов бесплатно) | $30/мес + usage | $0.40/1000 твитов |
| **Минимум твитов** | Нет | Нет | 50 твитов на запрос |
| **Один твит / тред** | ✅ | ✅ (withReplies) | ❌ |
| **Профили (from:handle)** | ✅ | ✅ | ✅ |
| **Треды (conversation_id)** | ✅ | Частично (repliesDepth) | ❌ |
| **Рейтинг** | 4.11 | 4.92 | 4.30 |
| **Success rate** | 100% | 98.6% | 100% |

Для сценария «несколько аккаунтов, 8:00 по крону, ~20–50 твитов на аккаунт»:
- **apidojo/tweet-scraper** не подходит: минимум 50 твитов на запрос.
- **web.harvester** — фикс $30/мес, избыточно при малом объёме.
- **apidojo/twitter-scraper-lite** — платим только за использование, без минимумов.

### Примерная стоимость (twitter-scraper-lite)

- 10 аккаунтов × $0.016 ≈ **$0.16** за запуск
- Первые ~40 твитов на аккаунт включены в стоимость запроса
- Дополнительные твиты: ~$0.0004–0.0008 за твит (Tier 1–2)
- При 10 аккаунтах × 30 запусков/мес: **~$5/мес** (vs $30 у web.harvester)

---

## Входные параметры (для нашего пайплайна)

```json
{
  "searchTerms": ["from:handle1", "from:handle2", "from:handle3"],
  "sort": "Latest",
  "maxItems": 100
}
```

Или через `twitterHandles`:

```json
{
  "twitterHandles": ["handle1", "handle2", "handle3"],
  "sort": "Latest",
  "maxItems": 100
}
```

---

## Выходной формат (пример)

```json
{
  "type": "tweet",
  "id": "1728108619189874825",
  "url": "https://x.com/elonmusk/status/1728108619189874825",
  "text": "More than 10 per human on average",
  "retweetCount": 11311,
  "replyCount": 6526,
  "likeCount": 104121,
  "createdAt": "Fri Nov 24 17:49:36 +0000 2023",
  "isReply": false,
  "author": {
    "userName": "elonmusk",
    "name": "Elon Musk",
    "id": "44196397"
  },
  "isRetweet": false,
  "isQuote": true
}
```

---

## Логика «тред = один пост»

1. **Профиль:** `searchTerms: ["from:handle"]` — получаем все твиты аккаунта (включая ответы).
2. **Группировка:** по `conversation_id` (если есть в ответе) или по цепочке `inReplyTo` / `replyTo`.
3. **Объединение:** сортировка по `createdAt`, склейка `text` через `\n\n`.
4. **Результат:** один пост на тред.

Если `conversation_id` нет в ответе актора — группируем по цепочке: корневой твит (`isReply: false`) + все ответы с `inReplyToStatusId` / `replyTo` на твиты того же автора.

---

## Альтернатива: получение тредов отдельно

Для твитов с `replyCount > 0` можно дополнительно запрашивать тред:

```json
{
  "searchTerms": ["conversation_id:1728108619189874825"],
  "sort": "Latest"
}
```

Это увеличит число запросов и стоимость. Оптимальнее сначала использовать `from:handle` — в результатах обычно есть и корневые твиты, и ответы автора (свои треды).
