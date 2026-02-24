# IG Parser MVP

MVP pipeline for collecting Instagram posts, extracting PubMed references, and
producing structured evidence output.

## Environment

Create `.env` with:

```env
APIFY_TOKEN=your_apify_token
APIFY_ACTOR_ID=apify/instagram-post-scraper
APIFY_SEARCH_ACTOR_ID=apify/instagram-search-scraper
NCBI_TOOL=ig-parser-mvp
NCBI_EMAIL=you@example.com
GOOGLE_SHEETS_SPREADSHEET_ID=your_spreadsheet_id
GOOGLE_SHEETS_WORKSHEET=Sheet1
# use one of these:
GOOGLE_SHEETS_CREDENTIALS_PATH=secrets/google-service-account.json
# or
GOOGLE_SHEETS_CREDENTIALS_JSON={"type":"service_account","project_id":"..."}
```

## Install

```bash
poetry install
```

## Run

### Web (браузер)

```powershell
poetry run streamlit run streamlit_app.py
```

Откройте ссылку (обычно http://localhost:8501). Введите тему, настройте параметры и нажмите «Запустить».

### CLI

```powershell
poetry run python -m app.main --topic "правда ли креатин вызывает выпадение волос" --max-items 50 --discovery-limit 5 --out-file output/mvp_result.json
```

Interactive topic mode:

```powershell
poetry run python -m app.main --max-items 50 --discovery-limit 5 --out-file output/mvp_result.json
```

Optional manual source override:

```powershell
poetry run python -m app.main --topic "правда ли креатин вызывает выпадение волос" --source "hubermanlab" --source "drandygalpin" --max-items 50 --out-file output/mvp_result.json
```

Google Sheets output is enabled automatically when
`GOOGLE_SHEETS_SPREADSHEET_ID` is set.

## Деплой (Streamlit Cloud)

1. Залейте репозиторий на GitHub.
2. Зарегистрируйтесь на [share.streamlit.io](https://share.streamlit.io).
3. New app → укажите репозиторий, ветку `main`, команду: `streamlit run streamlit_app.py`.
4. В Settings → Secrets добавьте переменные из `.env` (TOML-формат):

```toml
APIFY_TOKEN = "your_token"
OPENAI_API_KEY = "sk-..."
NCBI_EMAIL = "you@example.com"
GOOGLE_SHEETS_SPREADSHEET_ID = "your_spreadsheet_id"
GOOGLE_SHEETS_WORKSHEET = "Sheet1"
GOOGLE_SHEETS_CREDENTIALS_JSON = '{"type":"service_account","project_id":"..."}'
```

После деплоя приложение будет доступно по публичной ссылке.
