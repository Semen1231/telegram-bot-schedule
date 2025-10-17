#!/usr/bin/env python3
"""
ПРОВЕРКА ВСЕХ АБОНЕМЕНТОВ - СХОДИМОСТЬ ДАННЫХ
"""

import os
import sys
from dotenv import load_dotenv
from google_sheets_service import sheets_service
import config
import logging

# Загружаем переменные окружения
load_dotenv('.env.local')

def setup_logging():
    """Настройка логирования в консоль"""
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)  # Только ошибки, чтобы не засорять вывод
    formatter = logging.Formatter('%(message)s')
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.addHandler(console_handler)
    logger.setLevel(logging.ERROR)

def check_all_subscriptions():
    """Проверяет сходимость данных для ВСЕХ абонементов"""
    print("🔍 ПРОВЕРКА ВСЕХ АБОНЕМЕНТОВ - СХОДИМОСТЬ ДАННЫХ")
    print("=" * 70)
    
    if not sheets_service:
        print("❌ sheets_service не инициализирован")
        return
    
    setup_logging()
    
    try:
        # Получаем данные из календаря занятий
        print("\n📋 ШАГ 1: ПОЛУЧАЕМ ДАННЫЕ ИЗ КАЛЕНДАРЯ ЗАНЯТИЙ")
        print("-" * 50)
        
        cal_sheet = sheets_service.spreadsheet.worksheet("Календарь занятий")
        cal_data = cal_sheet.get_all_values()
        
        if len(cal_data) <= 1:
            print("❌ Нет данных в календаре занятий")
            return
        
        cal_headers = cal_data[0]
        print(f"📋 Заголовки календаря: {cal_headers}")
        
        # Группируем занятия по ID абонемента
        calendar_stats = {}
        for i, row in enumerate(cal_data[1:], 2):
            if len(row) > 6:
                subscription_id = row[1] if len(row) > 1 else ''  # B - ID абонемента
                status = row[4] if len(row) > 4 else ''  # E - Статус посещения
                mark = row[6] if len(row) > 6 else ''  # G - Отметка
                
                if subscription_id:
                    if subscription_id not in calendar_stats:
                        calendar_stats[subscription_id] = {
                            'total': 0,
                            'zaversheno': 0,
                            'zaplanirovanno': 0,
                            'propusk': 0,
                            'lessons': []
                        }
                    
                    calendar_stats[subscription_id]['total'] += 1
                    calendar_stats[subscription_id]['lessons'].append({
                        'row': i,
                        'status': status,
                        'mark': mark
                    })
                    
                    # Подсчитываем по статусам
                    status_lower = status.lower()
                    if status_lower == 'завершен':
                        calendar_stats[subscription_id]['zaversheno'] += 1
                    elif status_lower == 'запланировано':
                        calendar_stats[subscription_id]['zaplanirovanno'] += 1
                    elif status_lower == 'пропуск':
                        calendar_stats[subscription_id]['propusk'] += 1
        
        print(f"✅ Найдено {len(calendar_stats)} уникальных абонементов в календаре")
        
        # Получаем данные из листа абонементов
        print("\n📊 ШАГ 2: ПОЛУЧАЕМ ДАННЫЕ ИЗ ЛИСТА АБОНЕМЕНТЫ")
        print("-" * 50)
        
        subs_sheet = sheets_service.spreadsheet.worksheet("Абонементы")
        subs_data = subs_sheet.get_all_values()
        
        if len(subs_data) <= 1:
            print("❌ Нет данных в листе абонементов")
            return
        
        subs_headers = subs_data[0]
        print(f"📋 Заголовки абонементов: {subs_headers}")
        
        # Получаем текущие значения из листа абонементов
        subscription_sheet_stats = {}
        for i, row in enumerate(subs_data[1:], 2):
            if len(row) > 1:
                subscription_id = row[1] if len(row) > 1 else ''  # B - ID абонемента
                h_value = row[7] if len(row) > 7 else ''  # H - Прошло занятий
                i_value = row[8] if len(row) > 8 else ''  # I - Осталось занятий
                m_value = row[12] if len(row) > 12 else ''  # M - Пропущено
                j_value = row[9] if len(row) > 9 else ''  # J - Статус
                
                if subscription_id:
                    subscription_sheet_stats[subscription_id] = {
                        'row': i,
                        'h_current': h_value,
                        'i_current': i_value,
                        'm_current': m_value,
                        'j_current': j_value
                    }
        
        print(f"✅ Найдено {len(subscription_sheet_stats)} абонементов в листе")
        
        # Сравниваем данные
        print("\n🔍 ШАГ 3: СРАВНЕНИЕ ДАННЫХ")
        print("-" * 50)
        print(f"{'ID абонемента':<30} {'H(факт/ожид)':<15} {'I(факт/ожид)':<15} {'M(факт/ожид)':<15} {'J(факт/ожид)':<15} {'Статус':<10}")
        print("-" * 110)
        
        correct_count = 0
        incorrect_count = 0
        
        for subscription_id in calendar_stats:
            cal_stats = calendar_stats[subscription_id]
            
            # Ожидаемые значения из календаря
            expected_h = cal_stats['zaversheno']  # Завершенные занятия
            expected_i = cal_stats['zaplanirovanno']  # Запланированные занятия
            expected_m = cal_stats['propusk']  # Пропущенные занятия
            expected_j = 'Завершен' if expected_i == 0 else 'Активен'  # Статус
            
            if subscription_id in subscription_sheet_stats:
                sheet_stats = subscription_sheet_stats[subscription_id]
                
                # Текущие значения из листа
                current_h = sheet_stats['h_current']
                current_i = sheet_stats['i_current']
                current_m = sheet_stats['m_current']
                current_j = sheet_stats['j_current']
                
                # Проверяем сходимость
                h_match = str(current_h) == str(expected_h)
                i_match = str(current_i) == str(expected_i)
                m_match = str(current_m) == str(expected_m)
                j_match = str(current_j).strip().lower() == expected_j.lower()
                
                if h_match and i_match and m_match and j_match:
                    status = "✅ OK"
                    correct_count += 1
                else:
                    status = "❌ НЕ СХОДИТСЯ"
                    incorrect_count += 1
                
                # Отображаем результат
                h_display = f"{current_h}/{expected_h}"
                i_display = f"{current_i}/{expected_i}"
                m_display = f"{current_m}/{expected_m}"
                j_display = f"{current_j}/{expected_j}"
                
                print(f"{subscription_id:<30} {h_display:<15} {i_display:<15} {m_display:<15} {j_display:<15} {status:<10}")
                
                # Показываем детали для несходящихся
                if not (h_match and i_match and m_match and j_match):
                    print(f"  📋 Занятий всего: {cal_stats['total']}")
                    print(f"  📊 Статусы: Завершен={cal_stats['zaversheno']}, Запланировано={cal_stats['zaplanirovanno']}, Пропуск={cal_stats['propusk']}")
                    print(f"  🔍 Ожидаемый статус: {expected_j}")
                    
            else:
                print(f"{subscription_id:<30} {'НЕТ В ЛИСТЕ':<15} {'НЕТ В ЛИСТЕ':<15} {'НЕТ В ЛИСТЕ':<15} {'НЕТ В ЛИСТЕ':<15} {'❌ НЕТ':<10}")
                incorrect_count += 1
        
        # Итоговая статистика
        print("\n" + "=" * 70)
        print(f"📊 ИТОГОВАЯ СТАТИСТИКА:")
        print(f"✅ Правильных абонементов: {correct_count}")
        print(f"❌ Неправильных абонементов: {incorrect_count}")
        print(f"📈 Процент правильных: {correct_count/(correct_count+incorrect_count)*100:.1f}%" if (correct_count+incorrect_count) > 0 else "0%")
        
        if incorrect_count > 0:
            print(f"\n🔧 ИСПРАВЛЯЕМ {incorrect_count} НЕПРАВИЛЬНЫХ АБОНЕМЕНТОВ:")
            print("-" * 50)
            
            fixed_count = 0
            for subscription_id in calendar_stats:
                cal_stats = calendar_stats[subscription_id]
                
                # Ожидаемые значения из календаря
                expected_h = cal_stats['zaversheno']
                expected_i = cal_stats['zaplanirovanno']
                expected_m = cal_stats['propusk']
                
                if subscription_id in subscription_sheet_stats:
                    sheet_stats = subscription_sheet_stats[subscription_id]
                    
                    # Текущие значения из листа
                    current_h = sheet_stats['h_current']
                    current_i = sheet_stats['i_current']
                    current_m = sheet_stats['m_current']
                    
                    # Проверяем нужно ли исправлять
                    h_match = str(current_h) == str(expected_h)
                    i_match = str(current_i) == str(expected_i)
                    m_match = str(current_m) == str(expected_m)
                    expected_j = 'Завершен' if expected_i == 0 else 'Активен'
                    current_j = subscription_sheet_stats[subscription_id]['j_current']
                    j_match = str(current_j).strip().lower() == expected_j.lower()
                    
                    if not (h_match and i_match and m_match and j_match):
                        print(f"🔄 Исправляю {subscription_id}...")
                        try:
                            result = sheets_service.update_subscription_stats(subscription_id)
                            print(f"  ✅ Результат: {result}")
                            fixed_count += 1
                        except Exception as e:
                            print(f"  ❌ Ошибка: {e}")
            
            print(f"\n📊 ИСПРАВЛЕНО: {fixed_count} из {incorrect_count} абонементов")
        else:
            print(f"\n🎉 ВСЕ ДАННЫЕ СХОДЯТСЯ ПРАВИЛЬНО!")
            
    except Exception as e:
        print(f"❌ Ошибка при проверке: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_all_subscriptions()
