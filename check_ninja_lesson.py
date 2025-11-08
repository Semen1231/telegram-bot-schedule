#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Простой скрипт для проверки наличия занятия "Ниндзя" в Google Sheets
"""

import sys
import logging
from google_sheets_service import sheets_service

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

def check_ninja_lesson():
    """Проверяет наличие занятия Ниндзя в календаре."""
    print("\n" + "="*60)
    print("🔍 ПРОВЕРКА ЗАНЯТИЯ 'НИНДЗЯ' В GOOGLE SHEETS")
    print("="*60 + "\n")
    
    if not sheets_service:
        print("❌ ОШИБКА: Google Sheets Service не инициализирован")
        return
    
    try:
        # Получаем лист напрямую
        calendar_sheet = sheets_service.spreadsheet.worksheet("Календарь занятий")
        print("✅ Подключились к листу 'Календарь занятий'\n")
        
        # Получаем ВСЕ данные (включая заголовки)
        all_values = calendar_sheet.get_all_values()
        print(f"📊 Всего строк в листе: {len(all_values)}\n")
        
        if len(all_values) <= 1:
            print("⚠️ Лист пуст или содержит только заголовки")
            return
        
        # Показываем заголовки
        headers = all_values[0]
        print("📋 ЗАГОЛОВКИ:")
        for i, header in enumerate(headers):
            print(f"   Столбец {chr(65+i)}: {header}")
        print()
        
        # Ищем занятие "Ниндзя"
        print("🔎 ПОИСК ЗАНЯТИЯ 'НИНДЗЯ'...\n")
        
        ninja_found = False
        for row_num, row in enumerate(all_values[1:], start=2):  # Начинаем с 2-й строки
            if len(row) > 1:
                lesson_id = row[0] if len(row) > 0 else ''
                subscription_id = row[1] if len(row) > 1 else ''
                date = row[2] if len(row) > 2 else ''
                time_start = row[3] if len(row) > 3 else ''
                status = row[4] if len(row) > 4 else ''
                child = row[5] if len(row) > 5 else ''
                mark = row[6] if len(row) > 6 else ''
                time_end = row[7] if len(row) > 7 else ''
                
                # Проверяем наличие "Ниндзя" в ID абонемента
                if 'Ниндзя' in subscription_id or 'ниндзя' in subscription_id.lower():
                    ninja_found = True
                    print(f"🥷 НАЙДЕНО! Строка #{row_num}:")
                    print(f"   № (A): {lesson_id}")
                    print(f"   ID абонемента (B): {subscription_id}")
                    print(f"   Дата занятия (C): {date}")
                    print(f"   Время начала (D): {time_start}")
                    print(f"   Статус посещения (E): {status}")
                    print(f"   Ребенок (F): {child}")
                    print(f"   Отметка (G): {mark}")
                    print(f"   Время завершения (H): {time_end}")
                    print()
        
        if not ninja_found:
            print("❌ ЗАНЯТИЕ 'НИНДЗЯ' НЕ НАЙДЕНО!\n")
            print("📋 Показываю первые 15 строк для проверки:\n")
            
            for row_num, row in enumerate(all_values[1:16], start=2):
                if len(row) > 1:
                    lesson_id = row[0] if len(row) > 0 else ''
                    subscription_id = row[1] if len(row) > 1 else ''
                    date = row[2] if len(row) > 2 else ''
                    child = row[5] if len(row) > 5 else ''
                    
                    print(f"Строка {row_num}: № {lesson_id} | ID: {subscription_id} | Дата: {date} | Ребенок: {child}")
        
        print("\n" + "="*60)
        print("✅ ПРОВЕРКА ЗАВЕРШЕНА")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {e}\n")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_ninja_lesson()