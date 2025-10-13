import os
import json
from dotenv import load_dotenv

# Загружаем переменные окружения из файла .env (для локального запуска)
load_dotenv()

# Получаем переменные
TELEGRAM_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
GOOGLE_SHEET_NAME = os.getenv('GOOGLE_SHEET_NAME')
GOOGLE_CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', 'service_account.json')
GOOGLE_CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')
WEB_APP_URL = os.getenv('WEB_APP_URL')

# Поддержка деплоя: если есть JSON в переменной окружения, создаем файл
if os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON'):
    try:
        credentials_dict = json.loads(os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON'))
        # Сохраняем во временный файл для работы Google API
        with open('service_account.json', 'w') as f:
            json.dump(credentials_dict, f)
        GOOGLE_CREDENTIALS_PATH = 'service_account.json'
        print("✅ Google Service Account создан из переменной окружения")
    except Exception as e:
        print(f"⚠️ Ошибка создания service_account.json: {e}")

# Проверяем, что ОБЯЗАТЕЛЬНЫЕ переменные определены
if not TELEGRAM_TOKEN:
    raise ValueError("TELEGRAM_BOT_TOKEN не установлен. Проверьте переменные окружения.")

# Предупреждения для необязательных переменных
if not GOOGLE_SHEET_NAME:
    print("⚠️ GOOGLE_SHEET_NAME не установлен - Google Sheets будет недоступен")
if not GOOGLE_CREDENTIALS_PATH or not os.path.exists(GOOGLE_CREDENTIALS_PATH):
    print("⚠️ Google Service Account недоступен - Google Sheets будет недоступен")

