import os
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env (для локального запуска)
# На Replit переменные будут браться из Secrets
load_dotenv()

# Получаем переменные
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
GOOGLE_SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME')
GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH')
GOOGLE_CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')
WEB_APP_URL = os.getenv('WEB_APP_URL')


# Проверяем, что все необходимые переменные определены
if not all([TELEGRAM_TOKEN, GOOGLE_SHEET_NAME, GOOGLE_CREDENTIALS_PATH, GOOGLE_CALENDAR_ID, WEB_APP_URL]):
    raise ValueError("Одна или несколько переменных окружения не установлены. Проверьте ваш .env файл или Secrets на Replit, включая GOOGLE_CALENDAR_ID и WEB_APP_URL.")

