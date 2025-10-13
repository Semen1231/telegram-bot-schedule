#!/usr/bin/env python3
"""
Скрипт для поиска доступных Google Calendar
"""

import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
import config

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def find_calendars():
    """Находит все доступные календари"""
    try:
        print("🔍 ПОИСК ДОСТУПНЫХ GOOGLE КАЛЕНДАРЕЙ")
        print("=" * 50)
        
        # Инициализируем сервис
        scope = ['https://www.googleapis.com/auth/calendar']
        creds = service_account.Credentials.from_service_account_file(
            config.GOOGLE_CREDENTIALS_PATH, 
            scopes=scope
        )
        service = build('calendar', 'v3', credentials=creds)
        
        # Получаем список календарей
        print("📅 Получаю список календарей...")
        calendar_list = service.calendarList().list().execute()
        
        calendars = calendar_list.get('items', [])
        
        if not calendars:
            print("❌ Календари не найдены!")
            return
        
        print(f"✅ Найдено {len(calendars)} календарей:")
        print()
        
        for i, calendar in enumerate(calendars, 1):
            print(f"📅 КАЛЕНДАРЬ #{i}:")
            print(f"   📝 Название: {calendar.get('summary', 'Без названия')}")
            print(f"   🆔 ID: {calendar.get('id')}")
            print(f"   🎨 Цвет: {calendar.get('backgroundColor', 'Не указан')}")
            print(f"   👤 Владелец: {calendar.get('primary', False)}")
            print(f"   🔐 Доступ: {calendar.get('accessRole', 'Не указан')}")
            print()
        
        # Проверяем текущий календарь
        current_id = config.GOOGLE_CALENDAR_ID
        print(f"🎯 ТЕКУЩИЙ КАЛЕНДАРЬ В .env.local:")
        print(f"   🆔 ID: {current_id}")
        
        # Ищем совпадение
        found = False
        for calendar in calendars:
            if calendar.get('id') == current_id:
                print(f"   ✅ НАЙДЕН: {calendar.get('summary')}")
                found = True
                break
        
        if not found:
            print(f"   ❌ НЕ НАЙДЕН в списке доступных календарей!")
            print()
            print("💡 РЕКОМЕНДАЦИИ:")
            print("1. Выберите один из календарей выше")
            print("2. Обновите GOOGLE_CALENDAR_ID в .env.local")
            print("3. Или создайте новый календарь в Google Calendar")
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    find_calendars()
