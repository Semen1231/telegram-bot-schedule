import logging
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import config
import pytz

class GoogleCalendarService:
    def __init__(self, credentials_path, calendar_id):
        """Инициализация сервиса Google Calendar."""
        try:
            scope = [
                'https://www.googleapis.com/auth/calendar',
                'https://www.googleapis.com/auth/calendar.events'
            ]
            creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=scope)
            self.service = build('calendar', 'v3', credentials=creds)
            self.calendar_id = calendar_id
            
            logging.info("✅ Успешное подключение к Google Calendar API")
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к Google Calendar: {e}")
            raise

    def get_all_events(self):
        """Получает все события из календаря с повторными попытками."""
        import time
        
        max_retries = 3
        retry_delay = 2  # секунды
        
        for attempt in range(max_retries):
            try:
                events_result = self.service.events().list(
                    calendarId=self.calendar_id,
                    maxResults=2500,
                    singleEvents=True,
                    orderBy='startTime'
                ).execute()
                events = events_result.get('items', [])
                
                logging.info(f"📅 Получено {len(events)} событий из календаря")
                return events
                
            except (ConnectionResetError, ConnectionError, BrokenPipeError) as network_error:
                attempt_num = attempt + 1
                if attempt_num < max_retries:
                    logging.warning(f"🔄 Сетевая ошибка при получении событий (попытка {attempt_num}/{max_retries}): {network_error}")
                    logging.info(f"⏳ Ожидание {retry_delay} секунд перед повторной попыткой...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Экспоненциальная задержка
                    continue
                else:
                    logging.error(f"❌ Все попытки получения событий исчерпаны. Сетевая ошибка: {network_error}")
                    return []
                    
            except Exception as e:
                logging.error(f"❌ Ошибка при получении событий из календаря (попытка {attempt + 1}): {e}")
                if attempt == max_retries - 1:  # Последняя попытка
                    return []
                time.sleep(retry_delay)
                retry_delay *= 2
        
        return []

    def find_event_by_lesson_id(self, lesson_id):
        """Находит событие по ID занятия."""
        try:
            events = self.get_all_events()
            
            for event in events:
                description = event.get('description', '')
                if f"ID занятия: {lesson_id}" in description:
                    return event
            
            return None
            
        except Exception as e:
            logging.error(f"❌ Ошибка при поиске события по ID {lesson_id}: {e}")
            return None

    def get_status_emoji(self, mark):
        """Возвращает эмодзи в зависимости от отметки посещения."""
        mark = str(mark).strip()
        
        emoji_map = {
            'Посещение': '✔️',
            'Перенос': '🔄',
            'Отмена (болезнь)': '🤒',
            'Пропуск (по вине)': '🚫'
        }
        
        return emoji_map.get(mark, '📅')  # По умолчанию календарь, если статус не определен

    def create_event(self, lesson_data, circle_name):
        """Создает новое событие в календаре с повторными попытками."""
        import time
        
        max_retries = 3
        retry_delay = 2  # секунды
        
        for attempt in range(max_retries):
            try:
                # Определяем эмодзи по статусу посещения
                mark = lesson_data.get('mark', '')
                emoji = self.get_status_emoji(mark)
                
                # Формируем название события: Эмодзи Ребенок - Кружок
                summary = f"{emoji} {lesson_data['child']} - {circle_name}"
                
                logging.info(f"📝 Создание события: отметка='{mark}', эмодзи='{emoji}', название='{summary}'")
                
                # Формируем дату и время
                logging.info(f"🕐 Парсинг времени: дата='{lesson_data['date']}', начало='{lesson_data['start_time']}', конец='{lesson_data['end_time']}'")
                
                try:
                    # Парсим дату
                    lesson_date = datetime.strptime(lesson_data['date'], '%d.%m.%Y')
                    
                    # Парсим время начала и окончания
                    start_time_str = lesson_data['start_time'].strip()
                    end_time_str = lesson_data['end_time'].strip()
                    
                    # Пробуем парсить время
                    start_time = datetime.strptime(start_time_str, '%H:%M').time()
                    end_time = datetime.strptime(end_time_str, '%H:%M').time()
                    
                except ValueError as ve:
                    logging.error(f"❌ Не удалось парсить время: начало='{lesson_data['start_time']}', конец='{lesson_data['end_time']}', ошибка: {ve}")
                    return None
                
                # Создаем datetime объекты БЕЗ часового пояса
                start_datetime_naive = datetime.combine(lesson_date.date(), start_time)
                end_datetime_naive = datetime.combine(lesson_date.date(), end_time)
                
                # Определяем часовой пояс (ваш локальный)
                # Используем Asia/Yekaterinburg (UTC+5) вместо Europe/Moscow (UTC+3)
                local_timezone = pytz.timezone('Asia/Yekaterinburg')
                
                # Локализуем время в вашем часовом поясе
                start_datetime = local_timezone.localize(start_datetime_naive)
                end_datetime = local_timezone.localize(end_datetime_naive)
                
                logging.info(f"🕐 Результат парсинга с часовым поясом: {start_datetime} - {end_datetime}")
                
                # Формируем описание с переменными для сравнения
                lesson_id = lesson_data.get('lesson_id', 'N/A')
                subscription_id = lesson_data.get('subscription_id', 'N/A')
                status = lesson_data.get('status', 'N/A')
                child = lesson_data.get('child', 'N/A')
                mark = lesson_data.get('mark', '')
                date = lesson_data.get('date', 'N/A')
                start_time = lesson_data.get('start_time', 'N/A')
                
                description = f"""ID занятия: {lesson_id}
ID абонемента: {subscription_id}
Статус посещения: {status}
Ребенок: {child}
Отметка: {mark}
Дата занятия: {date}
Время начала: {start_time}
Время завершения: {lesson_data.get('end_time', 'N/A')}"""

                event = {
                    'summary': summary,
                    'description': description,
                    'start': {
                        'dateTime': start_datetime.isoformat(),
                        'timeZone': 'Asia/Yekaterinburg',
                    },
                    'end': {
                        'dateTime': end_datetime.isoformat(),
                        'timeZone': 'Asia/Yekaterinburg',
                    },
                }

                created_event = self.service.events().insert(
                    calendarId=self.calendar_id, 
                    body=event
                ).execute()
                
                logging.info(f"✅ Создано событие: {summary} на {lesson_data['date']}")
                return created_event['id']
                
            except (ConnectionResetError, ConnectionError, TimeoutError) as network_error:
                attempt_num = attempt + 1
                if attempt_num < max_retries:
                    logging.warning(f"🔄 Сетевая ошибка при создании события для занятия {lesson_data.get('lesson_id', 'N/A')} (попытка {attempt_num}/{max_retries}): {network_error}")
                    logging.info(f"⏳ Ожидание {retry_delay} секунд перед повторной попыткой...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Увеличиваем задержку для следующей попытки
                    continue
                else:
                    logging.error(f"❌ Все попытки создания события для занятия {lesson_data.get('lesson_id', 'N/A')} исчерпаны. Сетевая ошибка: {network_error}")
                    return None
                    
            except Exception as e:
                logging.error(f"❌ Ошибка при создании события для занятия {lesson_data.get('lesson_id', 'N/A')}: {e}")
                logging.error(f"📊 Данные занятия: {lesson_data}")
                logging.error(f"🎯 Название кружка: {circle_name}")
                import traceback
                logging.error(f"🔍 Полная ошибка: {traceback.format_exc()}")
                return None
        
        return None

    def update_event(self, event_id, lesson_data, circle_name):
        """Обновляет существующее событие."""
        try:
            # Определяем эмодзи по статусу посещения
            mark = lesson_data.get('mark', '')
            emoji = self.get_status_emoji(mark)
            
            # Формируем название события: Эмодзи Ребенок - Кружок
            summary = f"{emoji} {lesson_data['child']} - {circle_name}"
            
            logging.info(f"📝 Обновление события: отметка='{mark}', эмодзи='{emoji}', название='{summary}'")
            
            # Формируем дату и время
            logging.info(f"🕐 Обновление времени: дата='{lesson_data['date']}', начало='{lesson_data['start_time']}', конец='{lesson_data['end_time']}'")
            
            lesson_date = datetime.strptime(lesson_data['date'], '%d.%m.%Y')
            
            # Парсим время с обработкой разных форматов
            try:
                start_time_str = lesson_data['start_time'].strip()
                end_time_str = lesson_data['end_time'].strip()
                
                start_time = datetime.strptime(start_time_str, '%H:%M').time()
                end_time = datetime.strptime(end_time_str, '%H:%M').time()
                
            except ValueError as ve:
                logging.error(f"❌ Не удалось парсить время при обновлении: начало='{lesson_data['start_time']}', конец='{lesson_data['end_time']}', ошибка: {ve}")
                return False
            
            # Создаем datetime объекты БЕЗ часового пояса
            start_datetime_naive = datetime.combine(lesson_date.date(), start_time)
            end_datetime_naive = datetime.combine(lesson_date.date(), end_time)
            
            # Определяем часовой пояс (ваш локальный)
            local_timezone = pytz.timezone('Asia/Yekaterinburg')
            
            # Локализуем время в вашем часовом поясе
            start_datetime = local_timezone.localize(start_datetime_naive)
            end_datetime = local_timezone.localize(end_datetime_naive)
            
            logging.info(f"🕐 Результат парсинга при обновлении с часовым поясом: {start_datetime} - {end_datetime}")
            
            # Формируем описание с переменными для сравнения
            description = f"""ID занятия: {lesson_data['lesson_id']}
ID абонемента: {lesson_data['subscription_id']}
Статус посещения: {lesson_data['status']}
Ребенок: {lesson_data['child']}
Отметка: {lesson_data['mark']}
Дата занятия: {lesson_data['date']}
Время начала: {lesson_data['start_time']}
Время завершения: {lesson_data['end_time']}"""

            event = {
                'summary': summary,
                'description': description,
                'start': {
                    'dateTime': start_datetime.isoformat(),
                    'timeZone': 'Asia/Yekaterinburg',
                },
                'end': {
                    'dateTime': end_datetime.isoformat(),
                    'timeZone': 'Asia/Yekaterinburg',
                },
            }

            updated_event = self.service.events().update(
                calendarId=self.calendar_id,
                eventId=event_id,
                body=event
            ).execute()
            
            logging.info(f"🔄 Обновлено событие: {summary} на {lesson_data['date']}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении события {event_id}: {e}")
            return False

    def delete_event(self, event_id):
        """Удаляет событие из календаря."""
        try:
            self.service.events().delete(
                calendarId=self.calendar_id,
                eventId=event_id
            ).execute()
            logging.info(f"✅ Событие {event_id} удалено из календаря")
            return True
        except Exception as e:
            logging.error(f"❌ Ошибка при удалении события {event_id}: {e}")
            return False

    def delete_subscription_events(self, child_name, circle_name, subscription_id):
        """Удаляет все события абонемента из Google Calendar."""
        if not self.service:
            logging.warning("⚠️ Google Calendar API недоступен - пропускаю удаление событий")
            return {'success': True, 'deleted_count': 0, 'message': 'Calendar API недоступен'}
        
        try:
            logging.info(f"🗑️ Начинаю удаление событий абонемента {subscription_id} ({child_name} - {circle_name})")
            
            deleted_count = 0
            errors = []
            
            # Получаем все события календаря
            events_result = self.service.events().list(
                calendarId=self.calendar_id,
                maxResults=2500,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            logging.info(f"📊 Найдено {len(events)} событий в календаре")
            
            # Ищем события этого абонемента
            for event in events:
                try:
                    summary = event.get('summary', '')
                    description = event.get('description', '')
                    
                    # Проверяем, относится ли событие к удаляемому абонементу
                    if (child_name in summary and circle_name in summary) or \
                       (child_name in description and circle_name in description) or \
                       (subscription_id in description):
                        
                        event_id = event['id']
                        
                        # Удаляем событие
                        self.service.events().delete(
                            calendarId=self.calendar_id,
                            eventId=event_id
                        ).execute()
                        
                        deleted_count += 1
                        logging.info(f"✅ Удалено событие: {summary}")
                        
                        # Небольшая задержка между удалениями
                        import time
                        time.sleep(0.1)
                        
                except Exception as e:
                    error_msg = f"Ошибка при удалении события {event.get('id', 'unknown')}: {e}"
                    logging.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                    continue
            
            # Формируем результат
            if deleted_count > 0:
                message = f"✅ Удалено {deleted_count} событий из Google Calendar"
                if errors:
                    message += f"\n⚠️ Ошибок: {len(errors)}"
            else:
                message = "ℹ️ События абонемента не найдены в Google Calendar"
            
            logging.info(f"🎯 Завершено удаление событий: {deleted_count} удалено, {len(errors)} ошибок")
            
            return {
                'success': True,
                'deleted_count': deleted_count,
                'errors': errors,
                'message': message
            }
            
        except Exception as e:
            error_msg = f"❌ Критическая ошибка при удалении событий абонемента: {e}"
            logging.error(error_msg)
            return {
                'success': False,
                'deleted_count': 0,
                'errors': [str(e)],
                'message': error_msg
            }

    def extract_lesson_variables_from_event(self, event):
        """Извлекает переменные занятия из описания события."""
        try:
            description = event.get('description', '')
            variables = {}
            
            for line in description.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key == 'ID занятия':
                        variables['lesson_id'] = value
                    elif key == 'ID абонемента':
                        variables['subscription_id'] = value
                    elif key == 'Статус посещения':
                        variables['status'] = value
                    elif key == 'Ребенок':
                        variables['child'] = value
                    elif key == 'Отметка':
                        variables['mark'] = value
                    elif key == 'Дата занятия':
                        variables['date'] = value
                    elif key == 'Время начала':
                        variables['start_time'] = value
                    elif key == 'Время завершения':
                        variables['end_time'] = value
            
            return variables
            
        except Exception as e:
            logging.error(f"❌ Ошибка при извлечении переменных из события: {e}")
            return {}

    def compare_lesson_variables(self, sheet_data, event_variables):
        """Сравнивает переменные из таблицы и календаря."""
        try:
            # Нормализуем данные для сравнения
            comparisons = {
                'lesson_id': str(sheet_data.get('lesson_id', '')).strip() == str(event_variables.get('lesson_id', '')).strip(),
                'subscription_id': str(sheet_data.get('subscription_id', '')).strip() == str(event_variables.get('subscription_id', '')).strip(),
                'status': str(sheet_data.get('status', '')).strip() == str(event_variables.get('status', '')).strip(),
                'child': str(sheet_data.get('child', '')).strip() == str(event_variables.get('child', '')).strip(),
                'mark': str(sheet_data.get('mark', '')).strip() == str(event_variables.get('mark', '')).strip(),
                'date': str(sheet_data.get('date', '')).strip() == str(event_variables.get('date', '')).strip(),
                'start_time': str(sheet_data.get('start_time', '')).strip() == str(event_variables.get('start_time', '')).strip(),
                'end_time': str(sheet_data.get('end_time', '')).strip() == str(event_variables.get('end_time', '')).strip(),
            }
            
            all_match = all(comparisons.values())
            
            if not all_match:
                logging.info("🔍 Найдены различия:")
                for key, match in comparisons.items():
                    if not match:
                        sheet_val = sheet_data.get(key, '')
                        event_val = event_variables.get(key, '')
                        logging.info(f"  {key}: Таблица='{sheet_val}' vs Календарь='{event_val}'")
            
            return all_match
            
        except Exception as e:
            logging.error(f"❌ Ошибка при сравнении переменных: {e}")
            return False

    def find_forecast_event_by_id(self, forecast_id):
        """Находит событие прогноза по ID в описании."""
        try:
            events = self.get_all_events()
            
            for event in events:
                description = event.get('description', '')
                if f"ID прогноза: {forecast_id}" in description:
                    return event
            
            return None
            
        except Exception as e:
            logging.error(f"❌ Ошибка при поиске события прогноза по ID {forecast_id}: {e}")
            return None

    def create_forecast_event(self, forecast_data):
        """Создает новое событие прогноза в календаре (на весь день)."""
        import time
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                # Определяем эмодзи по статусу
                emoji = self.get_forecast_status_emoji(forecast_data.get('status', ''))
                
                # Формируем название события: Эмодзи Оплата - Ребенок - Кружок
                summary = f"{emoji} Оплата - {forecast_data['child']} - {forecast_data['circle']}"
                
                logging.info(f"💰 Создание события прогноза: статус='{forecast_data.get('status', '')}', эмодзи='{emoji}', название='{summary}'")
                
                # Парсим дату оплаты
                try:
                    from datetime import datetime
                    payment_date = datetime.strptime(forecast_data['payment_date'], '%d.%m.%Y')
                except ValueError as ve:
                    logging.error(f"❌ Не удалось парсить дату оплаты: '{forecast_data['payment_date']}', ошибка: {ve}")
                    return None
                
                # Формируем описание с переменными для сравнения
                description = f"""ID прогноза: {forecast_data['forecast_id']}
Кружок: {forecast_data['circle']}
Ребенок: {forecast_data['child']}
Дата оплаты: {forecast_data['payment_date']}
Бюджет: {forecast_data['budget']}
Статус: {forecast_data['status']}"""

                # Создаем событие на весь день
                event = {
                    'summary': summary,
                    'description': description,
                    'start': {
                        'date': payment_date.strftime('%Y-%m-%d'),
                        'timeZone': 'Europe/Moscow',
                    },
                    'end': {
                        'date': payment_date.strftime('%Y-%m-%d'),
                        'timeZone': 'Europe/Moscow',
                    },
                }

                created_event = self.service.events().insert(
                    calendarId=self.calendar_id, 
                    body=event
                ).execute()
                
                logging.info(f"✅ Создано событие прогноза: {summary} на {forecast_data['payment_date']}")
                return created_event['id']
                
            except (ConnectionResetError, ConnectionError, TimeoutError) as network_error:
                attempt_num = attempt + 1
                if attempt_num < max_retries:
                    logging.warning(f"🔄 Сетевая ошибка при создании события прогноза {forecast_data.get('forecast_id', 'N/A')} (попытка {attempt_num}/{max_retries}): {network_error}")
                    logging.info(f"⏳ Ожидание {retry_delay} секунд перед повторной попыткой...")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                else:
                    logging.error(f"❌ Все попытки создания события прогноза {forecast_data.get('forecast_id', 'N/A')} исчерпаны. Сетевая ошибка: {network_error}")
                    return None
                    
            except Exception as e:
                logging.error(f"❌ Ошибка при создании события прогноза {forecast_data.get('forecast_id', 'N/A')}: {e}")
                logging.error(f"📊 Данные прогноза: {forecast_data}")
                import traceback
                logging.error(f"🔍 Полная ошибка: {traceback.format_exc()}")
                return None
        
        return None

    def update_forecast_event(self, event_id, forecast_data):
        """Обновляет существующее событие прогноза."""
        try:
            # Определяем эмодзи по статусу
            emoji = self.get_forecast_status_emoji(forecast_data.get('status', ''))
            
            # Формируем название события: Эмодзи Оплата - Ребенок - Кружок
            summary = f"{emoji} Оплата - {forecast_data['child']} - {forecast_data['circle']}"
            
            logging.info(f"💰 Обновление события прогноза: статус='{forecast_data.get('status', '')}', эмодзи='{emoji}', название='{summary}'")
            
            # Парсим дату оплаты
            try:
                from datetime import datetime
                payment_date = datetime.strptime(forecast_data['payment_date'], '%d.%m.%Y')
            except ValueError as ve:
                logging.error(f"❌ Не удалось парсить дату оплаты при обновлении: '{forecast_data['payment_date']}', ошибка: {ve}")
                return False
            
            # Формируем описание с переменными для сравнения
            description = f"""ID прогноза: {forecast_data['forecast_id']}
Кружок: {forecast_data['circle']}
Ребенок: {forecast_data['child']}
Дата оплаты: {forecast_data['payment_date']}
Бюджет: {forecast_data['budget']}
Статус: {forecast_data['status']}"""

            # Обновляем событие на весь день
            event = {
                'summary': summary,
                'description': description,
                'start': {
                    'date': payment_date.strftime('%Y-%m-%d'),
                    'timeZone': 'Europe/Moscow',
                },
                'end': {
                    'date': payment_date.strftime('%Y-%m-%d'),
                    'timeZone': 'Europe/Moscow',
                },
            }

            updated_event = self.service.events().update(
                calendarId=self.calendar_id,
                eventId=event_id,
                body=event
            ).execute()
            
            logging.info(f"🔄 Обновлено событие прогноза: {summary} на {forecast_data['payment_date']}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении события прогноза {event_id}: {e}")
            return False

    def get_forecast_status_emoji(self, status):
        """Возвращает эмодзи в зависимости от статуса прогноза."""
        status = str(status).strip()
        
        emoji_map = {
            'Запланировано': '📅',
            'Оплачено': '✅',
            'Просрочено': '⚠️',
            'Отменено': '❌'
        }
        
        return emoji_map.get(status, '💰')  # По умолчанию деньги

    def extract_forecast_variables_from_event(self, event):
        """Извлекает переменные прогноза из описания события."""
        try:
            description = event.get('description', '')
            variables = {}
            
            for line in description.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key == 'ID прогноза':
                        variables['forecast_id'] = value
                    elif key == 'Кружок':
                        variables['circle'] = value
                    elif key == 'Ребенок':
                        variables['child'] = value
                    elif key == 'Дата оплаты':
                        variables['payment_date'] = value
                    elif key == 'Бюджет':
                        variables['budget'] = value
                    elif key == 'Статус':
                        variables['status'] = value
            
            return variables
            
        except Exception as e:
            logging.error(f"❌ Ошибка при извлечении переменных прогноза из события: {e}")
            return {}

    def compare_forecast_variables(self, sheet_data, event_variables):
        """Сравнивает переменные прогноза из таблицы и календаря."""
        try:
            # Нормализуем данные для сравнения
            comparisons = {
                'forecast_id': str(sheet_data.get('forecast_id', '')).strip() == str(event_variables.get('forecast_id', '')).strip(),
                'circle': str(sheet_data.get('circle', '')).strip() == str(event_variables.get('circle', '')).strip(),
                'child': str(sheet_data.get('child', '')).strip() == str(event_variables.get('child', '')).strip(),
                'payment_date': str(sheet_data.get('payment_date', '')).strip() == str(event_variables.get('payment_date', '')).strip(),
                'budget': str(sheet_data.get('budget', '')).strip() == str(event_variables.get('budget', '')).strip(),
                'status': str(sheet_data.get('status', '')).strip() == str(event_variables.get('status', '')).strip(),
            }
            
            all_match = all(comparisons.values())
            
            if not all_match:
                logging.info("🔍 Найдены различия в прогнозе:")
                for key, match in comparisons.items():
                    if not match:
                        sheet_val = sheet_data.get(key, '')
                        event_val = event_variables.get(key, '')
                        logging.info(f"  {key}: Таблица='{sheet_val}' vs Календарь='{event_val}'")
            
            return all_match
            
        except Exception as e:
            logging.error(f"❌ Ошибка при сравнении переменных прогноза: {e}")
            return False

    def delete_all_forecast_events(self):
        """Удаляет все события прогноза из календаря с обработкой таймаутов."""
        import time
        
        try:
            deleted_count = 0
            max_retries = 3
            retry_delay = 2
            
            for attempt in range(max_retries):
                try:
                    events = self.get_all_events()
                    
                    logging.info(f"🔍 Поиск событий прогноза среди {len(events)} событий в календаре (попытка {attempt + 1}/{max_retries})...")
                    
                    forecast_events = []
                    for event in events:
                        description = event.get('description', '')
                        # Ищем события с "ID прогноза:" в описании
                        if "ID прогноза:" in description:
                            forecast_events.append(event)
                    
                    if not forecast_events:
                        logging.info("📅 События прогноза не найдены")
                        return 0
                    
                    logging.info(f"🎯 Найдено {len(forecast_events)} событий прогноза для удаления")
                    
                    # Удаляем события с небольшими паузами
                    for i, event in enumerate(forecast_events):
                        try:
                            event_id = event['id']
                            event_summary = event.get('summary', 'Без названия')
                            
                            logging.info(f"🗑️ Удаляю событие прогноза ({i+1}/{len(forecast_events)}): {event_summary}")
                            
                            self.service.events().delete(
                                calendarId=self.calendar_id,
                                eventId=event_id
                            ).execute()
                            
                            deleted_count += 1
                            logging.info(f"✅ Удалено событие прогноза: {event_summary}")
                            
                            # Небольшая пауза между удалениями для избежания лимитов API
                            if i < len(forecast_events) - 1:  # Не ждем после последнего
                                time.sleep(0.5)
                                
                        except (ConnectionResetError, ConnectionError, TimeoutError) as network_error:
                            logging.warning(f"🔄 Сетевая ошибка при удалении события {event.get('id', 'N/A')}: {network_error}")
                            continue
                        except Exception as e:
                            logging.error(f"❌ Ошибка при удалении события прогноза {event.get('id', 'N/A')}: {e}")
                            continue
                    
                    logging.info(f"🎉 Удаление завершено: удалено {deleted_count} событий прогноза")
                    return deleted_count
                    
                except (ConnectionResetError, ConnectionError, TimeoutError) as network_error:
                    attempt_num = attempt + 1
                    if attempt_num < max_retries:
                        logging.warning(f"🔄 Сетевая ошибка при удалении событий прогноза (попытка {attempt_num}/{max_retries}): {network_error}")
                        logging.info(f"⏳ Ожидание {retry_delay} секунд перед повторной попыткой...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                    else:
                        logging.error(f"❌ Все попытки удаления событий прогноза исчерпаны. Сетевая ошибка: {network_error}")
                        return deleted_count
                        
                except Exception as e:
                    logging.error(f"❌ Ошибка при удалении всех событий прогноза (попытка {attempt + 1}): {e}")
                    if attempt == max_retries - 1:  # Последняя попытка
                        return deleted_count
                    time.sleep(retry_delay)
                    retry_delay *= 2
            
            return deleted_count
            
        except Exception as e:
            logging.error(f"❌ Критическая ошибка при удалении всех событий прогноза: {e}")
            return 0

    def remove_duplicate_lesson_events(self):
        """Удаляет дублирующиеся события занятий с одинаковым ID занятия."""
        try:
            deleted_count = 0
            events = self.get_all_events()
            
            logging.info(f"🔍 Поиск дублей занятий среди {len(events)} событий...")
            
            # Группируем события по ID занятия
            lesson_events = {}
            for event in events:
                description = event.get('description', '')
                if "ID занятия:" in description:
                    # Извлекаем ID занятия
                    for line in description.split('\n'):
                        if line.startswith('ID занятия:'):
                            lesson_id = line.split(':', 1)[1].strip()
                            if lesson_id not in lesson_events:
                                lesson_events[lesson_id] = []
                            lesson_events[lesson_id].append(event)
                            break
            
            # Ищем дубли
            for lesson_id, events_list in lesson_events.items():
                if len(events_list) > 1:
                    logging.info(f"🔍 Найдено {len(events_list)} дублей для занятия ID {lesson_id}")
                    
                    # Оставляем первое событие, удаляем остальные
                    events_to_delete = events_list[1:]  # Все кроме первого
                    
                    for event in events_to_delete:
                        try:
                            event_summary = event.get('summary', 'Без названия')
                            logging.info(f"🗑️ Удаляю дубль занятия: {event_summary} (ID занятия: {lesson_id})")
                            
                            self.service.events().delete(
                                calendarId=self.calendar_id,
                                eventId=event['id']
                            ).execute()
                            
                            deleted_count += 1
                            logging.info(f"✅ Удален дубль: {event_summary}")
                            
                            # Пауза между удалениями
                            import time
                            time.sleep(0.5)
                            
                        except Exception as e:
                            logging.error(f"❌ Ошибка при удалении дубля события {event.get('id', 'N/A')}: {e}")
            
            logging.info(f"🎉 Очистка дублей завершена: удалено {deleted_count} дублирующихся событий")
            return deleted_count
            
        except Exception as e:
            logging.error(f"❌ Ошибка при удалении дублей занятий: {e}")
            return 0


# Создаем глобальный экземпляр сервиса
try:
    if hasattr(config, 'GOOGLE_CALENDAR_ID') and config.GOOGLE_CALENDAR_ID and config.GOOGLE_CALENDAR_ID != 'disabled':
        calendar_service = GoogleCalendarService(config.GOOGLE_CREDENTIALS_PATH, config.GOOGLE_CALENDAR_ID)
    else:
        calendar_service = None
        logging.info("📅 Google Calendar отключен в конфигурации")
except Exception as e:
    calendar_service = None
    logging.error(f"❌ Не удалось инициализировать Google Calendar: {e}")