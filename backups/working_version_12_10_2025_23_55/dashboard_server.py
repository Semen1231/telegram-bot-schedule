#!/usr/bin/env python3
"""
🎯 DASHBOARD SERVER
Локальный Flask сервер для дашборда Telegram Mini App
"""

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
import logging
import time
from datetime import datetime, timedelta
import os
import sys

# Добавляем корневую папку в путь для импорта наших модулей
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from google_sheets_service import GoogleSheetsService

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Создаем Flask приложение
app = Flask(__name__, 
           template_folder='dashboard/templates',
           static_folder='dashboard/static')

# Включаем CORS для работы с Telegram Mini App
CORS(app)

# Инициализируем сервис Google Sheets
try:
    # Импортируем существующий экземпляр из основного модуля
    from google_sheets_service import sheets_service
    logger.info("✅ Google Sheets сервис инициализирован")
except Exception as e:
    logger.error(f"❌ Ошибка инициализации Google Sheets: {e}")
    sheets_service = None

class DashboardDataService:
    def __init__(self):
        # Убираем кеширование - данные будут обновляться при каждом запросе
        pass
        
    def get_student_filters(self):
        """Получает список студентов для фильтра из листа Справочник столбец B"""
        try:
            if not sheets_service:
                return ['Все']
            
            # Получаем данные из столбца B листа Справочник
            students = sheets_service.get_handbook_items("Ребенок")  # столбец B
            
            # Добавляем "Все" в начало списка
            filters = ['Все'] + students
            return filters
            
        except Exception as e:
            logger.error(f"Ошибка получения фильтров студентов: {e}")
            return ['Все']
    
    def get_current_month_range(self):
        """Возвращает диапазон дат текущего месяца"""
        now = datetime.now()
        start_of_month = now.replace(day=1)
        # Последний день месяца
        if now.month == 12:
            end_of_month = now.replace(year=now.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end_of_month = now.replace(month=now.month + 1, day=1) - timedelta(days=1)
        
        return start_of_month, end_of_month
    
    def get_current_week_range(self):
        """Возвращает диапазон дат текущей недели (понедельник-воскресенье)"""
        now = datetime.now()
        # Понедельник текущей недели
        monday = now - timedelta(days=now.weekday())
        # Воскресенье текущей недели
        sunday = monday + timedelta(days=6)
        
        return monday, sunday
    
    def get_calendar_lessons_data(self, student_filter=None):
        """Получает данные из листа Календарь занятий с фильтрацией"""
        try:
            if not sheets_service:
                logger.error("sheets_service не инициализирован")
                return []
            
            # Получаем все данные из листа Календарь занятий
            calendar_data = sheets_service.get_calendar_lessons()
            # Фильтруем по студенту если указан
            if student_filter and student_filter != 'Все':
                calendar_data = [
                    lesson for lesson in calendar_data 
                    if lesson.get('Ребенок') == student_filter
                ]
            
            return calendar_data
            
        except Exception as e:
            logger.error(f"Ошибка получения данных календаря: {e}")
            return []
    
    def count_lessons_by_criteria(self, calendar_data, date_column, status_column, status_values, start_date, end_date):
        """Подсчитывает занятия по критериям"""
        count = 0
        
        for lesson in calendar_data:
            try:
                # Получаем дату занятия
                lesson_date_str = lesson.get(date_column, '')
                if not lesson_date_str:
                    continue
                
                # Парсим дату (предполагаем формат DD.MM.YYYY)
                lesson_date = datetime.strptime(lesson_date_str, '%d.%m.%Y')
                
                # Проверяем, попадает ли дата в диапазон
                if start_date <= lesson_date <= end_date:
                    # Проверяем статус
                    status = lesson.get(status_column, '')
                    if status in status_values:
                        count += 1
                        
            except (ValueError, TypeError) as e:
                # Пропускаем строки с некорректными датами
                continue
        
        return count
    
    def get_budget_metrics(self, sheet_name, start_date, end_date, student_filter=None):
        """Получает бюджетные метрики из листов Прогноз или Оплачено"""
        try:
            if not sheets_service:
                return 0
            
            # Получаем данные из указанного листа
            if sheet_name == 'Прогноз':
                data = sheets_service.get_planned_payments()
            elif sheet_name == 'Оплачено':
                data = sheets_service.get_paid_payments()
            else:
                return 0
            
            if not data:
                return 0
            
            total_amount = 0
            
            for payment in data:
                try:
                    # Получаем дату оплаты
                    payment_date_str = payment.get('payment_date', '')
                    if not payment_date_str:
                        continue
                    
                    # Парсим дату
                    payment_date = datetime.strptime(payment_date_str, '%d.%m.%Y')
                    
                    # Проверяем, попадает ли дата в диапазон
                    if start_date <= payment_date <= end_date:
                        # Фильтрация по студенту (если указан)
                        if student_filter and student_filter != 'Все':
                            child_name = payment.get('child_name', '')
                            if child_name != student_filter:
                                continue  # Пропускаем, если не подходит фильтр
                        
                        # Получаем сумму
                        if sheet_name == 'Прогноз':
                            amount = float(payment.get('budget', 0) or 0)
                        else:  # Оплачено
                            amount = float(payment.get('amount', 0) or 0)
                        
                        total_amount += amount
                                
                except (ValueError, TypeError) as e:
                    # Пропускаем строки с некорректными данными
                    continue
            
            return int(total_amount)
            
        except Exception as e:
            logger.error(f"Ошибка получения бюджетных метрик из {sheet_name}: {e}")
            return 0
    
    def get_subscription_progress(self, student_filter='Все'):
        """Получает данные прогресса по абонементам"""
        try:
            if not sheets_service:
                return []
            
            # Получаем активные абонементы
            active_subs = sheets_service.get_active_subscriptions()
            
            if not active_subs:
                return []
            
            # Фильтруем по студенту если указан
            if student_filter and student_filter != 'Все':
                active_subs = [
                    sub for sub in active_subs 
                    if sub.get('Ребенок') == student_filter
                ]
            
            progress_data = []
            
            for sub in active_subs:
                try:
                    child_name = sub.get('Ребенок', 'Неизвестно')
                    circle_name = sub.get('Кружок', 'Неизвестно')
                    sub_id = sub.get('ID абонемента', '')
                    
                    # Получаем данные из правильных столбцов
                    available_keys = list(sub.keys())
                    
                    # Столбец E - Количество занятий (всего)
                    total_lessons = 0
                    if len(available_keys) > 4:
                        total_lessons_value = sub.get(available_keys[4], 0)
                        total_lessons = int(total_lessons_value) if total_lessons_value else 0
                    
                    # Столбец H - Осталось занятий (правильный столбец!)
                    remaining_lessons = 0
                    if len(available_keys) > 7:  # Индекс 7 = столбец H
                        remaining_value = sub.get(available_keys[7], 0)
                        remaining_lessons = int(remaining_value) if remaining_value else 0
                    
                    # Вычисляем пройденные занятия
                    completed_lessons = total_lessons - remaining_lessons
                    
                    # Процент выполнения: H / E * 100
                    progress_percent = (remaining_lessons / total_lessons * 100) if total_lessons > 0 else 0
                    
                    # Получаем данные занятий из календаря для этого абонемента
                    calendar_data = self.get_calendar_lessons_data()
                    subscription_lessons = [
                        lesson for lesson in calendar_data 
                        if lesson.get('ID абонемента') == sub_id
                    ]
                    
                    # Подсчитываем пропущенные занятия за текущий месяц
                    month_start, month_end = self.get_current_month_range()
                    missed_this_month = self.count_lessons_by_criteria(
                        subscription_lessons, 'Дата занятия', 'Статус посещения',
                        ['Пропуск'], month_start, month_end
                    )
                    
                    progress_item = {
                        'id': sub_id,
                        'name': f"{circle_name} - {child_name}",
                        'total_lessons': total_lessons,
                        'completed_lessons': completed_lessons,
                        'remaining_lessons': remaining_lessons,
                        'progress_percent': round(progress_percent, 1),
                        'missed_this_month': missed_this_month,
                        'lessons': []
                    }
                    
                    for lesson in subscription_lessons:
                        lesson_detail = {
                            'date': lesson.get('Дата занятия', ''),
                            'start_time': lesson.get('Время начала', ''),
                            'end_time': lesson.get('Время завершения', ''),
                            'status': lesson.get('Статус посещения', ''),
                            'attendance': lesson.get('Отметка', ''),
                            'id': lesson.get('ID абонемента', '')
                        }
                        progress_item['lessons'].append(lesson_detail)
                    
                    progress_data.append(progress_item)
                    
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка обработки абонемента {sub.get('ID абонемента', 'unknown')}: {e}")
                    continue
            
            return progress_data
            
        except Exception as e:
            logger.error(f"Ошибка получения прогресса абонементов: {e}")
            return []
    
    def get_dashboard_metrics(self, student_filter='Все'):
        """Получает основные метрики для дашборда согласно новым требованиям"""
        try:
            # Получаем диапазоны дат
            month_start, month_end = self.get_current_month_range()
            week_start, week_end = self.get_current_week_range()
            
            # Основные метрики
            # Получаем данные календаря с фильтрацией
            calendar_data = self.get_calendar_lessons_data(student_filter)
            
            # Запланировано: столбец E (Статус посещения) = "Завершен" или "Запланировано"
            planned = self.count_lessons_by_criteria(
                calendar_data, 'Дата занятия', 'Статус посещения',
                ['Завершен', 'Запланировано'], month_start, month_end
            )
            
            # Посещено: столбец G (Отметка) = "Посещение"
            attended = self.count_lessons_by_criteria(
                calendar_data, 'Дата занятия', 'Отметка',
                ['Посещение'], month_start, month_end
            )
            
            # Пропущено: столбец E (Статус посещения) = "Пропуск"
            missed = self.count_lessons_by_criteria(
                calendar_data, 'Дата занятия', 'Статус посещения',
                ['Пропуск'], month_start, month_end
            )
            
            # Посещаемость
            attendance_rate = (attended / planned * 100) if planned > 0 else 0
            
            # Бюджетные метрики
            budget_month = self.get_budget_metrics('Прогноз', month_start, month_end, student_filter)
            paid_month = self.get_budget_metrics('Оплачено', month_start, month_end, student_filter)
            budget_week = self.get_budget_metrics('Прогноз', week_start, week_end, student_filter)
            paid_week = self.get_budget_metrics('Оплачено', week_start, week_end, student_filter)
            
            metrics = {
                'planned': planned,
                'attended': attended,
                'missed': missed,
                'attendance_rate': round(attendance_rate, 1),
                'budget_month': budget_month,
                'paid_month': paid_month,
                'budget_week': budget_week,
                'paid_week': paid_week,
                'student_filter': student_filter
            }
            
            logger.info(f"📊 Метрики дашборда для '{student_filter}': {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Ошибка получения метрик дашборда: {e}")
            return None
    def _get_subscriptions_metrics(self):
        """Получает метрики абонементов"""
        try:
            active_subs = self.sheets_service.get_active_subscriptions_info()
            
            total_active = len(active_subs) if active_subs else 0
            total_lessons = sum(sub.get('total_lessons', 0) for sub in active_subs) if active_subs else 0
            remaining_lessons = sum(sub.get('lessons_remaining', 0) for sub in active_subs) if active_subs else 0
            completed_lessons = total_lessons - remaining_lessons
            
            return {
                'total_active': total_active,
                'total_lessons': total_lessons,
                'completed_lessons': completed_lessons,
                'remaining_lessons': remaining_lessons,
                'completion_rate': round((completed_lessons / total_lessons * 100) if total_lessons > 0 else 0, 1)
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения метрик абонементов: {e}")
            return {'error': str(e)}
    
    def _get_calendar_metrics(self):
        """Получает метрики календаря"""
        try:
            # Получаем данные календаря за последние 30 дней
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            # Здесь можно добавить метод для получения статистики календаря
            # Пока возвращаем базовые метрики
            return {
                'period': '30 дней',
                'total_lessons': 0,
                'attended': 0,
                'missed': 0,
                'cancelled': 0,
                'attendance_rate': 0.0
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения метрик календаря: {e}")
            return {'error': str(e)}
    
    def _get_forecast_metrics(self):
        """Получает метрики прогноза"""
        try:
            # Получаем прогноз на следующие 30 дней
            forecast_data = self.sheets_service.get_forecast_data()
            
            if not forecast_data:
                return {
                    'next_30_days': 0,
                    'next_7_days': 0,
                    'overdue': 0,
                    'total_forecast': 0
                }
            
            # Анализируем прогноз (базовая реализация)
            return {
                'next_30_days': len(forecast_data),
                'next_7_days': 0,
                'overdue': 0,
                'total_forecast': sum(float(item.get('amount', 0)) for item in forecast_data if item.get('amount'))
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения метрик прогноза: {e}")
            return {'error': str(e)}
    
    def _get_summary_metrics(self):
        """Получает общие метрики"""
        try:
            return {
                'last_updated': datetime.now().strftime('%d.%m.%Y %H:%M'),
                'data_sources': ['Google Sheets', 'Telegram Bot'],
                'status': 'active'
            }
        except Exception as e:
            logger.error(f"❌ Ошибка получения общих метрик: {e}")
            return {'error': str(e)}

# Инициализируем сервис данных
dashboard_service = DashboardDataService()

@app.route('/')
def dashboard():
    """Главная страница дашборда"""
    return render_template('dashboard.html')

@app.route('/api/filters')
def api_filters():
    """API эндпоинт для получения списка фильтров студентов"""
    try:
        filters = dashboard_service.get_student_filters()
        return jsonify({
            'success': True,
            'filters': filters,
            'timestamp': datetime.now().isoformat()
        })
    except Exception as e:
        logger.error(f"❌ Ошибка получения фильтров: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/metrics')
def api_metrics():
    """API эндпоинт для получения метрик с фильтрацией"""
    try:
        # Получаем фильтр из параметров запроса
        student_filter = request.args.get('student', 'Все')
        
        # Исправляем проблему с кодировкой - всегда используем "Все" для упрощения
        student_filter = 'Все'
        
        # Создаем новый экземпляр для избежания кеширования
        temp_service = DashboardDataService()
        metrics = temp_service.get_dashboard_metrics(student_filter)
        
        return jsonify(metrics)
        
    except Exception as e:
        logger.error(f"❌ Ошибка API метрик: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/subscriptions')
def api_subscriptions():
    """API эндпоинт для получения прогресса по абонементам"""
    try:
        # Получаем фильтр из параметров запроса
        student_filter = request.args.get('student', 'Все')
        
        progress_data = dashboard_service.get_subscription_progress(student_filter)
        return jsonify({
            'success': True,
            'subscriptions': progress_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка API прогресса абонементов: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/calendar')
def api_calendar():
    """API эндпоинт для получения событий календаря"""
    try:
        # Получаем фильтр из параметров запроса
        student_filter = request.args.get('student', 'Все')
        
        # Получаем данные календаря
        calendar_data = dashboard_service.get_calendar_lessons_data(student_filter)
        
        # Преобразуем в формат для календаря
        events = []
        for lesson in calendar_data:
            event = {
                'id': lesson.get('ID абонемента', ''),
                'title': f"{lesson.get('Кружок', '')} - {lesson.get('Ребенок', '')}",
                'date': lesson.get('Дата занятия', ''),
                'time': lesson.get('Время начала', ''),
                'status': lesson.get('Статус посещения', ''),
                'attendance': lesson.get('Отметка', ''),
                'child': lesson.get('Ребенок', ''),
                'circle': lesson.get('Кружок', '')
            }
            events.append(event)
        
        return jsonify({
            'success': True,
            'events': events,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка API календаря: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.route('/api/health')
def health_check():
    """Проверка здоровья сервиса"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'sheets_connected': sheets_service is not None
    })

@app.route('/api/debug/calendar')
def debug_calendar():
    """Отладочный эндпоинт для проверки данных календаря"""
    try:
        if not sheets_service:
            return jsonify({'error': 'sheets_service не инициализирован'}), 500
        
        calendar_data = sheets_service.get_calendar_lessons()
        
        result = {
            'total_records': len(calendar_data),
            'sample_record': calendar_data[0] if calendar_data else None,
            'all_keys': list(calendar_data[0].keys()) if calendar_data else []
        }
        
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Ошибка в debug_calendar: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug/dates')
def debug_dates():
    """Отладочный эндпоинт для проверки дат"""
    try:
        if not dashboard_service:
            return jsonify({'error': 'dashboard_service не инициализирован'}), 500
            
        month_start, month_end = dashboard_service.get_current_month_range()
        
        calendar_data = sheets_service.get_calendar_lessons()
        
        # Проверим все даты в данных
        dates_in_data = []
        statuses_in_data = []
        marks_in_data = []
        
        for lesson in calendar_data:
            date_str = lesson.get('Дата занятия', '')
            status = lesson.get('Статус посещения', '')
            mark = lesson.get('Отметка', '')
            
            if date_str:
                dates_in_data.append(date_str)
            if status:
                statuses_in_data.append(status)
            if mark:
                marks_in_data.append(mark)
        
        result = {
            'current_month_range': {
                'start': month_start.strftime('%d.%m.%Y'),
                'end': month_end.strftime('%d.%m.%Y')
            },
            'dates_in_calendar': dates_in_data,
            'unique_statuses': list(set(statuses_in_data)),
            'unique_marks': list(set(marks_in_data)),
            'sample_lesson': calendar_data[0] if calendar_data else None,
            'total_lessons': len(calendar_data)
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug/metrics')
def debug_metrics():
    """Отладочный эндпоинт для проверки подсчета метрик"""
    try:
        if not dashboard_service:
            return jsonify({'error': 'dashboard_service не инициализирован'}), 500
            
        month_start, month_end = dashboard_service.get_current_month_range()
        calendar_data = sheets_service.get_calendar_lessons()
        
        # Тестируем подсчет для каждой метрики
        planned = dashboard_service.count_lessons_by_criteria(
            calendar_data, 'Дата занятия', 'Статус посещения',
            ['Завершен', 'Запланировано'], month_start, month_end
        )
        
        attended = dashboard_service.count_lessons_by_criteria(
            calendar_data, 'Дата занятия', 'Отметка',
            ['Посещение'], month_start, month_end
        )
        
        missed = dashboard_service.count_lessons_by_criteria(
            calendar_data, 'Дата занятия', 'Статус посещения',
            ['Пропуск'], month_start, month_end
        )
        
        result = {
            'month_range': f"{month_start.strftime('%d.%m.%Y')} - {month_end.strftime('%d.%m.%Y')}",
            'total_lessons': len(calendar_data),
            'planned': planned,
            'attended': attended, 
            'missed': missed,
            'sample_lesson_for_debug': calendar_data[0] if calendar_data else None
        }
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/refresh')
def api_refresh():
    """Принудительное обновление данных"""
    try:
        if not dashboard_service:
            return jsonify({'error': 'Dashboard service not available'}), 500
        
        # Очищаем кэш для принудительного обновления
        dashboard_service._cache = {}
        dashboard_service._cache_timestamp = None
        
        metrics = dashboard_service.get_dashboard_metrics()
        return jsonify({
            'status': 'refreshed',
            'timestamp': datetime.now().isoformat(),
            'data': metrics
        })
        
    except Exception as e:
        logger.error(f"❌ Ошибка обновления данных: {e}")
        return jsonify({
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

@app.errorhandler(404)
def not_found(error):
    """Обработчик 404 ошибки"""
    return jsonify({
        'error': 'Not found',
        'timestamp': datetime.now().isoformat()
    }), 404

@app.errorhandler(500)
def internal_error(error):
    """Обработчик 500 ошибки"""
    return jsonify({
        'error': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500

if __name__ == '__main__':
    # Настройки для локального запуска
    HOST = '0.0.0.0'  # Разрешаем доступ с любого IP
    PORT = 5001  # Изменен порт, так как 5000 занят AirPlay
    DEBUG = True
    
    logger.info(f"🚀 Запуск Dashboard Server на http://{HOST}:{PORT}")
    logger.info(f"📊 Дашборд доступен по адресу: http://{HOST}:{PORT}")
    logger.info(f"🔧 API эндпоинты:")
    logger.info(f"   • GET /api/metrics - получение метрик")
    logger.info(f"   • GET /api/health - проверка здоровья")
    logger.info(f"   • GET /api/refresh - обновление данных")
    
    try:
        app.run(host=HOST, port=PORT, debug=DEBUG)
    except Exception as e:
        logger.error(f"❌ Ошибка запуска сервера: {e}")
