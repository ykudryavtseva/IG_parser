# Деплой IG Parser на VPS (Termius)

Подключитесь к серверу в Termius, затем выполните команды по порядку.

## 1. Обновить систему (опционально)

```bash
sudo apt update && sudo apt upgrade -y
```

## 2. Установить зависимости

```bash
sudo apt install -y python3 python3-pip git
```

## 3. Установить Poetry

```bash
curl -sSL https://install.python-poetry.org | python3 -
```

Добавить Poetry в PATH (если нужно):
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
```

## 4. Клонировать проект

**Если репозиторий на GitHub:**
```bash
cd ~
git clone https://github.com/ВАШ_USERNAME/IG_parser.git
cd IG_parser
```

**Если репо приватное или пока нет — загрузите файлы вручную (например через SFTP в Termius) в `~/IG_parser`.**

## 5. Создать .env

```bash
nano ~/IG_parser/.env
```

Вставьте (замените значения на свои):
```
APIFY_TOKEN=ваш_токен
APIFY_ACTOR_ID=apify/instagram-post-scraper
APIFY_SEARCH_ACTOR_ID=apify/instagram-search-scraper
NCBI_EMAIL=ваш_email
NCBI_TOOL=ig-parser-mvp
OPENAI_API_KEY=ваш_ключ_openai
OPENAI_MODEL=gpt-4o-mini
```

Сохранить: `Ctrl+O`, Enter, `Ctrl+X`.

## 6. Установить зависимости и запустить

```bash
cd ~/IG_parser
poetry install
poetry run streamlit run streamlit_app.py --server.address=0.0.0.0 --server.port=8501
```

Флаг `--server.address=0.0.0.0` нужен, чтобы приложение было доступно извне.

## 7. Проверить

Откройте в браузере: `http://IP_ВАШЕГО_СЕРВЕРА:8501`

## 8. Запуск в фоне (screen)

Чтобы приложение работало после закрытия Termius:

```bash
screen -S igparser
cd ~/IG_parser
poetry run streamlit run streamlit_app.py --server.address=0.0.0.0 --server.port=8501
```

Отсоединиться: `Ctrl+A`, затем `D`.  
Вернуться в сессию: `screen -r igparser`

## 9. Открыть порт в файрволе (если не открыт)

```bash
sudo ufw allow 8501
sudo ufw status
```

## 10. Авто-синхронизация (cron, 8:00 МСК)

Посты из аккаунтов автоматически выгружаются в Google Sheets раз в сутки. Сначала сохраните аккаунты через Streamlit (кнопка «Сохранить аккаунты для мониторинга»).

Добавить задание в crontab:

```bash
crontab -e
```

Строка (8:00 МСК = 5:00 UTC):

```
0 5 * * * cd /root/IG_parser && /root/.local/bin/poetry run python scripts/sync_worker.py >> /root/IG_parser/logs/sync.log 2>&1
```

Создать папку для логов:

```bash
mkdir -p /root/IG_parser/logs
```
