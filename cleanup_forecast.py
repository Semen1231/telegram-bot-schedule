#!/usr/bin/env python3
"""
🧹 СКРИПТ ОЧИСТКИ ДУБЛИКАТОВ В ПРОГНОЗЕ

Удаляет из листа "Прогноз" все записи, которые уже есть в листе "Оплачено".
Запускается один раз для очистки старых записей.

Использование:
    python cleanup_forecast.py
"""

import logging
import config
from google_sheets_service import GoogleSheetsService

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    """Основная функция для запуска очистки."""
    try:
        print("=" * 60)
        print("🧹 СКРИПТ ОЧИСТКИ ДУБЛИКАТОВ В ПРОГНОЗЕ")
        print("=" * 60)
        print()
        
        # Инициализируем Google Sheets сервис
        print("📊 Подключение к Google Sheets...")
        sheets_service = GoogleSheetsService(
            config.GOOGLE_CREDENTIALS_PATH,
            config.GOOGLE_SHEET_NAME
        )
        print("✅ Успешно подключено!")
        print()
        
        # Запускаем очистку
        print("🔍 Поиск дубликатов между 'Прогноз' и 'Оплачено'...")
        deleted_count = sheets_service.cleanup_forecast_duplicates()
        
        print()
        print("=" * 60)
        if deleted_count > 0:
            print(f"✅ УСПЕШНО! Удалено дубликатов: {deleted_count}")
        else:
            print("ℹ️ Дубликатов не найдено")
        print("=" * 60)
        
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ ОШИБКА: {e}")
        print("=" * 60)
        logging.error(f"Ошибка при выполнении скрипта: {e}", exc_info=True)

if __name__ == "__main__":
    main()
