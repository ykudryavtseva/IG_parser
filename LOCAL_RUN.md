# Запуск IG Parser локально (PowerShell)

## 1. Открыть PowerShell

- Нажать `Win + X` → «Терминал» или «Windows PowerShell»
- Либо в VS Code/Cursor: `` Ctrl+` `` для встроенного терминала

## 2. Перейти в папку проекта

```powershell
cd C:\Users\user\Desktop\VibeCodingProjects\IG_parser
```

## 3. Проверить, что установлен Poetry

```powershell
poetry --version
```

Если нет — установить: https://python-poetry.org/docs/#installation  

Кратко (PowerShell):

```powershell
(Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | python -
```

## 4. Установить зависимости

```powershell
poetry install
```

## 5. Создать файл `.env` с переменными окружения

Создайте файл `.env` в корне проекта со следующими переменными (значения скопируйте из вашего `secrets/streamlit_paste_this.txt` или Streamlit Cloud Secrets):

```env
APIFY_TOKEN=ваш_токен_apify
APIFY_ACTOR_ID=apify/instagram-post-scraper
APIFY_SEARCH_ACTOR_ID=apify/instagram-search-scraper
NCBI_EMAIL=ваш_email
NCBI_TOOL=ig-parser-mvp
OPENAI_API_KEY=ваш_ключ_openai
OPENAI_MODEL=gpt-4o-mini
```

Минимально нужны: `APIFY_TOKEN`, `OPENAI_API_KEY`. Остальное — по желанию (Google Sheets, NCBI email).

## 6. Запустить приложение

```powershell
poetry run streamlit run streamlit_app.py
```

## 7. Открыть в браузере

Откроется адрес вида `http://localhost:8501`. Если нет — откройте вручную.

---

**Режим «Последние посты»** — выберите его, укажите источник `dangarnernutrition`, нажмите «Запустить». Картинки должны загружаться с домашнего IP.
