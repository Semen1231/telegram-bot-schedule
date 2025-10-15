#!/usr/bin/env python3
"""
Скрипт для тестирования синхронизации Google Calendar
"""

import logging
from google_sheets_service import sheets_service

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def test_sync_functions():
    """Тестирует все функции синхронизации"""
    
    print("🔍 ТЕСТИРОВАНИЕ ФУНКЦИЙ СИНХРОНИЗАЦИИ")
    print("=" * 50)
    
    if not sheets_service:
        print("❌ Google Sheets сервис недоступен!")
        return
    
    print("✅ Google Sheets сервис подключен")
    
    # Тест 1: Проверяем наличие методов
    methods_to_check = [
        'sync_calendar_with_google_calendar',
        'sync_forecast_with_google_calendar', 
        'clean_duplicate_events',
        'update_lesson_mark'
    ]
    
    print("\n🔧 ПРОВЕРКА НАЛИЧИЯ МЕТОДОВ:")
    for method_name in methods_to_check:
        if hasattr(sheets_service, method_name):
            print(f"✅ {method_name} - ЕСТЬ")
        else:
            print(f"❌ {method_name} - ОТСУТСТВУЕТ")
    
    # Тест 2: Пробуем запустить синхронизацию календаря
    print("\n🔄 ТЕСТИРОВАНИЕ СИНХРОНИЗАЦИИ КАЛЕНДАРЯ:")
    try:
        result = sheets_service.sync_calendar_with_google_calendar()
        print(f"✅ Синхронизация календаря: {result[:200]}...")
    except Exception as e:
        print(f"❌ Ошибка синхронизации календаря: {e}")
    
    # Тест 3: Пробуем запустить синхронизацию прогноза
    print("\n💰 ТЕСТИРОВАНИЕ СИНХРОНИЗАЦИИ ПРОГНОЗА:")
    try:
        result = sheets_service.sync_forecast_with_google_calendar()
        print(f"✅ Синхронизация прогноза: {result[:200]}...")
    except Exception as e:
        print(f"❌ Ошибка синхронизации прогноза: {e}")
    
    # Тест 4: Пробуем очистить дубли
    print("\n🧹 ТЕСТИРОВАНИЕ ОЧИСТКИ ДУБЛЕЙ:")
    try:
        result = sheets_service.clean_duplicate_events()
        print(f"✅ Очистка дублей: {result[:200]}...")
    except Exception as e:
        print(f"❌ Ошибка очистки дублей: {e}")
    
    print("\n🎉 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО!")

if __name__ == "__main__":
    test_sync_functions()
