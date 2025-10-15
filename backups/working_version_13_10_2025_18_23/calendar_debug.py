#!/usr/bin/env python3
"""
Расширенная диагностика Google Calendar API
"""

import logging
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import config

# Настройка логирования
logging.basicConfig(level=logging.INFO)

def test_calendar_access():
    """Тестирует доступ к Google Calendar API"""
    try:
        print("🔍 ДИАГНОСТИКА GOOGLE CALENDAR API")
        print("=" * 50)
        
        # Инициализируем сервис
        scope = ['https://www.googleapis.com/auth/calendar']
        creds = service_account.Credentials.from_service_account_file(
            config.GOOGLE_CREDENTIALS_PATH, 
            scopes=scope
        )
        service = build('calendar', 'v3', credentials=creds)
        
        print("✅ Google Calendar API инициализирован")
        print()
        
        # Тест 1: Получение списка календарей
        print("📅 ТЕСТ 1: Получение списка календарей")
        try:
            calendar_list = service.calendarList().list().execute()
            calendars = calendar_list.get('items', [])
            
            if calendars:
                print(f"✅ Найдено {len(calendars)} календарей:")
                for i, calendar in enumerate(calendars, 1):
                    print(f"   {i}. {calendar.get('summary', 'Без названия')} ({calendar.get('id')})")
            else:
                print("❌ Календари не найдены!")
                print("💡 Возможные причины:")
                print("   1. Service Account не добавлен ни в один календарь")
                print("   2. Неправильные права доступа")
                print("   3. Календарь не расшарен для Service Account")
        except HttpError as e:
            print(f"❌ Ошибка получения списка календарей: {e}")
        
        print()
        
        # Тест 2: Проверка доступа к конкретному календарю
        current_id = config.GOOGLE_CALENDAR_ID
        print(f"📅 ТЕСТ 2: Проверка доступа к календарю")
        print(f"   🆔 ID: {current_id}")
        
        if current_id == "disabled":
            print("   ⚠️ Календарь отключен в конфигурации")
        else:
            try:
                # Пытаемся получить информацию о календаре
                calendar_info = service.calendars().get(calendarId=current_id).execute()
                print(f"   ✅ Календарь найден: {calendar_info.get('summary', 'Без названия')}")
                
                # Пытаемся получить события
                events = service.events().list(
                    calendarId=current_id,
                    maxResults=1
                ).execute()
                print(f"   ✅ Доступ к событиям: есть")
                
            except HttpError as e:
                if e.resp.status == 404:
                    print(f"   ❌ Календарь не найден (404)")
                    print(f"   💡 Проверьте:")
                    print(f"      - Правильность ID календаря")
                    print(f"      - Расшарен ли календарь для Service Account")
                elif e.resp.status == 403:
                    print(f"   ❌ Нет доступа к календарю (403)")
                    print(f"   💡 Добавьте Service Account с правами 'Вносить изменения'")
                else:
                    print(f"   ❌ Ошибка: {e}")
        
        print()
        
        # Тест 3: Проверка primary календаря
        print("📅 ТЕСТ 3: Проверка primary календаря")
        try:
            primary_calendar = service.calendars().get(calendarId='primary').execute()
            print(f"   ✅ Primary календарь: {primary_calendar.get('summary', 'Без названия')}")
            
            events = service.events().list(
                calendarId='primary',
                maxResults=1
            ).execute()
            print(f"   ✅ Доступ к primary календарю: есть")
            
        except HttpError as e:
            print(f"   ❌ Нет доступа к primary календарю: {e}")
        
        print()
        
        # Рекомендации
        print("💡 РЕКОМЕНДАЦИИ:")
        print("1. Откройте Google Calendar: https://calendar.google.com")
        print("2. Найдите нужный календарь в левой панели")
        print("3. Нажмите на 3 точки → 'Настройки и общий доступ'")
        print("4. В разделе 'Поделиться с определенными людьми':")
        print(f"   - Добавьте: telegram-bot-sheets-editor-947@steady-shard-343003.iam.gserviceaccount.com")
        print(f"   - Права: 'Вносить изменения в мероприятия'")
        print("5. Подождите 2-3 минуты для применения изменений")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")

if __name__ == "__main__":
    test_calendar_access()
