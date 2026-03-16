# Деплой на Streamlit Cloud — пошагово

## Шаг 1. GitHub-репозиторий

1. Убедитесь, что папка **не** содержит `.env` и файлов с ключами (они в `.gitignore`).
2. Создайте репозиторий на GitHub: https://github.com/new
3. Заполните имя (например, `IG_parser`) и сделайте репозиторий **Public**.
4. В терминале выполните:

```powershell
cd c:\Users\user\Desktop\VibeCodingProjects\IG_parser

git init
git add .
git commit -m "Initial commit: IG Parser + Streamlit"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/IG_parser.git
git push -u origin main
```

Замените `YOUR_USERNAME` на ваш логин GitHub.

---

## Шаг 2. Streamlit Cloud

1. Откройте https://share.streamlit.io
2. Войдите через GitHub (Sign in with GitHub).
3. Нажмите **"New app"**.
4. Заполните:
   - **Repository:** `YOUR_USERNAME/IG_parser`
   - **Branch:** `main`
   - **Main file path:** `streamlit_app.py`
   - **App URL:** можно оставить автозаполнённым (например, `ig-parser`) или задать своё.
5. Нажмите **"Advanced settings"** и откройте **Secrets**.

---

## Шаг 3. Secrets (обязательные ключи)

В Secrets вставьте в формате TOML (всё в одном блоке):

```toml
APIFY_TOKEN = "ваш_apify_токен"
OPENAI_API_KEY = "sk-proj-..."
NCBI_EMAIL = "ваш_email@example.com"
NCBI_TOOL = "ig-parser-mvp"
```

Дополнительно для Google Sheets (если нужна выгрузка в таблицу):

```toml
GOOGLE_SHEETS_SPREADSHEET_ID = "id_таблицы"
GOOGLE_SHEETS_WORKSHEET = "ResearchV2"
GOOGLE_SHEETS_CREDENTIALS_JSON = '{"type":"service_account","project_id":"...","private_key_id":"...","private_key":"-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n","client_email":"...@....iam.gserviceaccount.com","client_id":"...","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url":"..."}'
```

Для `GOOGLE_SHEETS_CREDENTIALS_JSON`:
- Откройте ваш `google_credentials.json` (или `google-service-account.json`).
- Скопируйте **весь** JSON и вставьте в кавычках. Для многострочного JSON можно использовать тройные кавычки `'''...'''`.

---

## Шаг 4. Запуск приложения

1. Нажмите **"Deploy"**.
2. Дождитесь сборки (обычно 2–5 минут).
3. Откроется ссылка вида `https://ig-parser-xxx.streamlit.app`.

---

## Перед каждым деплоем

1. **Обновите версию** в `pyproject.toml` и `streamlit_app.py` (APP_VERSION).
2. Коммит, push — Streamlit Cloud подхватит изменения.

---

## Устранение ошибок

| Проблема | Что сделать |
|----------|-------------|
| `APIFY_TOKEN не задан` | Проверьте Secrets, ключ должен быть без лишних пробелов. |
| `ModuleNotFoundError: No module named 'app'` | Убедитесь, что в репозитории есть папка `app/` с `__init__.py`. |
| Ошибка Google Sheets | Проверьте `GOOGLE_SHEETS_CREDENTIALS_JSON` — полный JSON от `{` до `}`. |
