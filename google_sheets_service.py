import gspread
from google.oauth2 import service_account
import json
import logging
import os
from datetime import datetime, timedelta
import re
import time
# Google Calendar API импорты
import config

# Импортируем Google Calendar сервис
try:
    from google_calendar_service import GoogleCalendarService
    logging.info("✅ Google Calendar сервис импортирован успешно")
except Exception as e:
    GoogleCalendarService = None
    logging.warning(f"⚠️ Google Calendar сервис не доступен: {e}")

class GoogleSheetsService:
    def __init__(self, credentials_path, sheet_name):
        """Инициализация сервиса Google Sheets."""
        try:
            import time
            # Добавляем задержку для снижения нагрузки на API при инициализации
            time.sleep(5)
            
            scope = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=scope)
            self.client = gspread.authorize(creds)
            
            # Добавляем задержку перед открытием таблицы
            time.sleep(3)
            self.spreadsheet = self.client.open(sheet_name)
            
            # Инициализируем кеш для снижения нагрузки на API
            self._cache = {}
            self._cache_ttl = {}
            self._default_cache_duration = 30  # Кеш на 30 секунд по умолчанию
            
            logging.info("✅ Google Sheets сервис успешно инициализирован (с кешированием)")
            
            # Используем глобальный экземпляр Google Calendar Service
            try:
                from google_calendar_service import calendar_service
                self.calendar_service = calendar_service
                if self.calendar_service:
                    logging.info("Успешное подключение к Google Таблицам и Google Calendar.")
                else:
                    logging.info("Успешное подключение к Google Таблицам. Calendar API отключен.")
            except Exception as e:
                logging.warning(f"⚠️ Google Calendar недоступен: {e}")
                self.calendar_service = None
                logging.info("Успешное подключение к Google Таблицам. Calendar API отключен.")
        except gspread.exceptions.SpreadsheetNotFound:
            print(f"Ошибка: Таблица с названием '{spreadsheet_id}' не найдена. Проверьте GOOGLE_SHEET_NAME в .env файле.")
            raise
        except Exception as e:
            print(f"Ошибка подключения к Google Таблицам: {e}")
            raise
    
    def _get_from_cache(self, key):
        """Получает данные из кеша, если они еще актуальны."""
        if key in self._cache and key in self._cache_ttl:
            if time.time() < self._cache_ttl[key]:
                logging.debug(f"📦 Данные '{key}' получены из кеша")
                return self._cache[key]
            else:
                # Кеш устарел, удаляем
                del self._cache[key]
                del self._cache_ttl[key]
                logging.debug(f"⏰ Кеш '{key}' устарел и удален")
        return None
    
    def _save_to_cache(self, key, data, duration=None):
        """Сохраняет данные в кеш."""
        if duration is None:
            duration = self._default_cache_duration
        self._cache[key] = data
        self._cache_ttl[key] = time.time() + duration
        logging.debug(f"💾 Данные '{key}' сохранены в кеш на {duration} сек")
    
    def _clear_cache(self, key=None):
        """Очищает кеш (полностью или конкретный ключ)."""
        if key:
            if key in self._cache:
                del self._cache[key]
            if key in self._cache_ttl:
                del self._cache_ttl[key]
            logging.debug(f"🗑️ Кеш '{key}' очищен")
        else:
            self._cache.clear()
            self._cache_ttl.clear()
            logging.debug("🗑️ Весь кеш очищен")
    
    def handle_network_error(self, e, operation_name="операции"):
        """Обрабатывает сетевые ошибки и возвращает понятное сообщение."""
        import httpx
        error_msg = str(e)
        
        if isinstance(e, httpx.ReadError) or "httpx.ReadError" in error_msg:
            logging.error(f"🌐 Сетевая ошибка при {operation_name}: {e}")
            return f"🌐 Сетевая ошибка при {operation_name}. Проверьте подключение к интернету и попробуйте снова."
        
        elif "429" in error_msg or "Quota exceeded" in error_msg:
            logging.error(f"📊 Превышена квота API при {operation_name}: {e}")
            return f"📊 Превышена квота Google Sheets API. Подождите 1-2 минуты и попробуйте снова."
        
        elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            logging.error(f"⏰ Таймаут при {operation_name}: {e}")
            return f"⏰ Превышено время ожидания при {operation_name}. Попробуйте еще раз через минуту."
        
        elif "503" in error_msg or "Service Unavailable" in error_msg:
            logging.error(f"🚫 Сервис недоступен при {operation_name}: {e}")
            return f"🚫 Google Sheets временно недоступен. Попробуйте через несколько минут."
        
        else:
            logging.error(f"❌ Неизвестная ошибка при {operation_name}: {e}")
            return f"❌ Произошла ошибка при {operation_name}: {error_msg}"

    def get_active_subscriptions(self):
        """Загружает все абонементы со статусом 'Активен' или 'Ожидает'."""
        try:
            worksheet = self.spreadsheet.worksheet("Абонементы")
            all_values = worksheet.get_all_values()
            if not all_values: return []
            
            headers = all_values[0]
            records = []
            for i, row in enumerate(all_values[1:], start=2):
                record = dict(zip(headers, row))
                record['row_number'] = i
                records.append(record)

            active_subs = [
                sub for sub in records 
                if str(sub.get('Статус', '')).strip().lower() in ['активен', 'ожидает']
            ]
            return active_subs
        except gspread.exceptions.WorksheetNotFound:
            logging.error("Ошибка: Лист 'Абонементы' не найден.")
            return []
        except Exception as e:
            logging.error(f"Ошибка инициализации GoogleSheetsService: {e}")
            self.spreadsheet = None

    def format_time(self, time_str):
        """Форматирует время в формат HH:MM."""
        if not time_str or time_str == '':
            return ''
        
        # Если время уже в правильном формате, возвращаем как есть
        if ':' in time_str and len(time_str.split(':')) == 2:
            try:
                hours, minutes = time_str.split(':')
                # Форматируем с ведущими нулями
                return f"{int(hours):02d}:{int(minutes):02d}"
            except ValueError:
                return time_str
        
        # Если время в другом формате, пытаемся его распознать
        return time_str

    def delete_subscription(self, subscription_id):
        """Полностью удаляет абонемент и все связанные записи из всех листов."""
        try:
            logging.info(f"🗑️ Начинаю полное удаление абонемента {subscription_id}")
            
            # Получаем информацию об абонементе перед удалением для Google Calendar
            subscription_info = self.get_subscription_details(subscription_id)
            child_name = subscription_info.get('child_name', '') if subscription_info else ''
            circle_name = subscription_info.get('circle_name', '') if subscription_info else ''
            
            logging.info(f"📋 Информация об абонементе: child_name='{child_name}', circle_name='{circle_name}'")
            
            deleted_counts = {
                'Абонементы': 0,
                'Календарь занятий': 0,
                'Шаблон расписания': 0,
                'Прогноз': 0,
                'Оплачено': 0,
                'Google Calendar': 0
            }
            
            # 1. Удаляем из листа "Абонементы"
            try:
                subs_sheet = self.spreadsheet.worksheet("Абонементы")
                cell = subs_sheet.find(str(subscription_id))
                subs_sheet.delete_rows(cell.row)
                deleted_counts['Абонементы'] = 1
                logging.info(f"✅ Удален абонемент из листа 'Абонементы'")
            except gspread.exceptions.CellNotFound: 
                logging.warning(f"⚠️ Абонемент {subscription_id} не найден в листе 'Абонементы'")
            except Exception as e:
                logging.error(f"❌ Ошибка при удалении из 'Абонементы': {e}")

            # 2. Удаляем из листа "Календарь занятий"
            try:
                cal_sheet = self.spreadsheet.worksheet("Календарь занятий")
                all_values = cal_sheet.get_all_values()
                rows_to_delete = []
                
                # Ищем по столбцу B (ID абонемента)
                for i, row in enumerate(all_values[1:], start=2):
                    if len(row) > 1 and row[1] == str(subscription_id):
                        rows_to_delete.append(i)
                
                if rows_to_delete:
                    for row_index in sorted(rows_to_delete, reverse=True):
                        cal_sheet.delete_rows(row_index)
                    deleted_counts['Календарь занятий'] = len(rows_to_delete)
                    logging.info(f"✅ Удалено {len(rows_to_delete)} занятий из 'Календарь занятий'")
                else:
                    logging.info("ℹ️ Нет занятий для удаления в 'Календарь занятий'")
            except Exception as e:
                logging.error(f"❌ Ошибка при удалении из 'Календарь занятий': {e}")

            # 3. Удаляем из листа "Шаблон расписания"
            try:
                template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
                all_values = template_sheet.get_all_values()
                rows_to_delete = []
                
                # Ищем по столбцу B (ID абонемента)
                for i, row in enumerate(all_values[1:], start=2):
                    if len(row) > 1 and row[1] == str(subscription_id):
                        rows_to_delete.append(i)
                
                if rows_to_delete:
                    for row_index in sorted(rows_to_delete, reverse=True):
                        template_sheet.delete_rows(row_index)
                    deleted_counts['Шаблон расписания'] = len(rows_to_delete)
                    logging.info(f"✅ Удалено {len(rows_to_delete)} записей из 'Шаблон расписания'")
                else:
                    logging.info("ℹ️ Нет записей для удаления в 'Шаблон расписания'")
            except Exception as e:
                logging.error(f"❌ Ошибка при удалении из 'Шаблон расписания': {e}")

            # 4. Удаляем из листа "Прогноз"
            try:
                forecast_sheet = self.spreadsheet.worksheet("Прогноз")
                all_values = forecast_sheet.get_all_values()
                rows_to_delete = []
                
                logging.info(f"🔍 ДИАГНОСТИКА УДАЛЕНИЯ ИЗ ПРОГНОЗА:")
                logging.info(f"  📋 Ищем записи для: '{child_name}' - '{circle_name}'")
                
                # Ищем по столбцам "Кружок" и "Ребенок"
                if len(all_values) > 1:
                    headers = all_values[0]
                    circle_col = -1
                    child_col = -1
                    
                    logging.info(f"  📊 Заголовки листа 'Прогноз': {headers}")
                    
                    for idx, header in enumerate(headers):
                        if header == 'Кружок':
                            circle_col = idx
                        elif header == 'Ребенок':
                            child_col = idx
                    
                    logging.info(f"  🎯 Столбец 'Кружок': {circle_col}, Столбец 'Ребенок': {child_col}")
                    
                    if circle_col >= 0 and child_col >= 0 and child_name and circle_name:
                        logging.info(f"  🔍 Проверяю {len(all_values)-1} строк данных...")
                        
                        for i, row in enumerate(all_values[1:], start=2):
                            if len(row) > max(circle_col, child_col):
                                row_circle = row[circle_col].strip() if circle_col < len(row) else ""
                                row_child = row[child_col].strip() if child_col < len(row) else ""
                                
                                logging.info(f"    📝 Строка {i}: Кружок='{row_circle}', Ребенок='{row_child}'")
                                
                                # Проверяем точное совпадение
                                if row_circle == circle_name and row_child == child_name:
                                    rows_to_delete.append(i)
                                    logging.info(f"    ✅ НАЙДЕНО СОВПАДЕНИЕ - строка {i} будет удалена")
                        
                        logging.info(f"  📊 Найдено строк для удаления: {len(rows_to_delete)}")
                        
                        if rows_to_delete:
                            for row_index in sorted(rows_to_delete, reverse=True):
                                forecast_sheet.delete_rows(row_index)
                            deleted_counts['Прогноз'] = len(rows_to_delete)
                            logging.info(f"✅ Удалено {len(rows_to_delete)} записей из 'Прогноз'")
                        else:
                            logging.warning("⚠️ Нет записей для удаления в 'Прогноз' - проверьте точность имен")
                    else:
                        logging.warning(f"⚠️ Проблема с поиском: circle_col={circle_col}, child_col={child_col}, child_name='{child_name}', circle_name='{circle_name}'")
                else:
                    logging.warning("⚠️ Лист 'Прогноз' пуст или нет заголовков")
            except Exception as e:
                logging.error(f"❌ Ошибка при удалении из 'Прогноз': {e}")

            # 5. Удаляем из листа "Оплачено"
            try:
                paid_sheet = self.spreadsheet.worksheet("Оплачено")
                all_values = paid_sheet.get_all_values()
                rows_to_delete = []
                
                # Ищем по столбцам "Кружок" и "Ребенок"
                if len(all_values) > 1:
                    headers = all_values[0]
                    circle_col = -1
                    child_col = -1
                    
                    for idx, header in enumerate(headers):
                        if header == 'Кружок':
                            circle_col = idx
                        elif header == 'Ребенок':
                            child_col = idx
                    
                    if circle_col >= 0 and child_col >= 0 and child_name and circle_name:
                        for i, row in enumerate(all_values[1:], start=2):
                            if (len(row) > max(circle_col, child_col) and 
                                row[circle_col] == circle_name and 
                                row[child_col] == child_name):
                                rows_to_delete.append(i)
                        
                        if rows_to_delete:
                            for row_index in sorted(rows_to_delete, reverse=True):
                                paid_sheet.delete_rows(row_index)
                            deleted_counts['Оплачено'] = len(rows_to_delete)
                            logging.info(f"✅ Удалено {len(rows_to_delete)} записей из 'Оплачено'")
                        else:
                            logging.info("ℹ️ Нет записей для удаления в 'Оплачено'")
                    else:
                        logging.warning("⚠️ Не удалось определить столбцы или данные для удаления из 'Оплачено'")
            except Exception as e:
                logging.error(f"❌ Ошибка при удалении из 'Оплачено': {e}")

            # 6. Удаляем события из Google Calendar (по содержимому: имя ребенка, кружок, ID абонемента)
            calendar_errors = []
            try:
                if self.calendar_service and child_name and circle_name:
                    logging.info(f"🗓️ Удаляю события из Google Calendar для {child_name} - {circle_name} (ID: {subscription_id})")
                    
                    # Попытки удаления с повторами при сетевых ошибках
                    max_retries = 3
                    calendar_result = None
                    
                    for attempt in range(max_retries):
                        try:
                            calendar_result = self.calendar_service.delete_subscription_events(
                                child_name, circle_name, subscription_id
                            )
                            break  # Успешно - выходим из цикла
                            
                        except Exception as calendar_error:
                            error_msg = str(calendar_error)
                            if "Connection reset by peer" in error_msg or "timeout" in error_msg.lower():
                                logging.warning(f"🌐 Сетевая ошибка при удалении из Calendar (попытка {attempt + 1}/{max_retries}): {calendar_error}")
                                if attempt < max_retries - 1:
                                    import time
                                    time.sleep(2 ** attempt)  # Экспоненциальная задержка: 1s, 2s, 4s
                                    continue
                            raise calendar_error  # Не сетевая ошибка - пробрасываем дальше
                    
                    if calendar_result:
                        if isinstance(calendar_result, dict):
                            calendar_deleted = calendar_result.get('deleted_count', 0)
                            calendar_success = calendar_result.get('success', True)
                            
                            if calendar_success and calendar_deleted > 0:
                                deleted_counts['Google Calendar'] = calendar_deleted
                                logging.info(f"✅ Удалено {calendar_deleted} событий из Google Calendar")
                            elif not calendar_success:
                                calendar_errors.append(f"Ошибка удаления из Calendar: {calendar_result.get('message', 'Неизвестная ошибка')}")
                                logging.warning(f"⚠️ Не удалось удалить события из Google Calendar")
                            else:
                                logging.info("ℹ️ Нет событий для удаления в Google Calendar")
                        else:
                            # Для обратной совместимости, если возвращается число
                            if calendar_result and calendar_result > 0:
                                deleted_counts['Google Calendar'] = calendar_result
                                logging.info(f"✅ Удалено {calendar_result} событий из Google Calendar")
                            else:
                                logging.info("ℹ️ Нет событий для удаления в Google Calendar")
                    else:
                        calendar_errors.append("Не удалось получить результат удаления из Google Calendar")
                elif not child_name or not circle_name:
                    logging.warning(f"⚠️ Недостаточно данных для удаления из Google Calendar: child_name='{child_name}', circle_name='{circle_name}'")
                    calendar_errors.append(f"Недостаточно данных для поиска событий (ребенок: '{child_name}', кружок: '{circle_name}')")
                else:
                    logging.warning("⚠️ Google Calendar сервис недоступен")
                    calendar_errors.append("Google Calendar сервис недоступен")
                    
            except Exception as e:
                error_message = self.handle_network_error(e, "удалении из Google Calendar")
                calendar_errors.append(error_message)
                logging.error(f"❌ Критическая ошибка при удалении из Google Calendar: {e}")

            # Формируем отчет об удалении
            total_deleted = sum(deleted_counts.values())
            report_lines = [f"✅ Абонемент `{subscription_id}` полностью удален!"]
            
            for sheet_name, count in deleted_counts.items():
                if count > 0:
                    report_lines.append(f"• {sheet_name}: {count} записей")
            
            if total_deleted == 0:
                report_lines.append("⚠️ Данные не найдены в таблицах")
            
            # Добавляем информацию об ошибках Google Calendar
            if calendar_errors:
                report_lines.append("")
                report_lines.append("⚠️ **Проблемы с Google Calendar:**")
                for error in calendar_errors:
                    report_lines.append(f"• {error}")
                report_lines.append("")
                report_lines.append("🔄 **Рекомендация:** Проверьте Google Calendar вручную и удалите события абонемента при необходимости.")
            
            # Возвращаем информацию для Google Calendar
            result_message = "\n".join(report_lines)
            logging.info(f"🎯 Удаление завершено: {total_deleted} записей, ошибок Calendar: {len(calendar_errors)}")
            
            return {
                'success': True,
                'message': result_message,
                'child_name': child_name,
                'circle_name': circle_name,
                'subscription_id': subscription_id,
                'deleted_counts': deleted_counts,
                'calendar_errors': calendar_errors
            }

        except Exception as e:
            # Используем нашу улучшенную обработку ошибок
            error_message = self.handle_network_error(e, "удалении абонемента")
            logging.error(f"❌ Критическая ошибка при удалении абонемента {subscription_id}: {e}")
            return {
                'success': False,
                'message': error_message,
                'child_name': child_name if 'child_name' in locals() else '',
                'circle_name': circle_name if 'circle_name' in locals() else '',
                'subscription_id': subscription_id,
                'deleted_counts': {}
            }
    
    def manual_calendar_cleanup(self, child_name, circle_name):
        """Ручная очистка событий из Google Calendar для конкретного ребенка и кружка."""
        try:
            if not self.calendar_service:
                return "❌ Google Calendar сервис недоступен"
            
            logging.info(f"🧹 Ручная очистка Google Calendar для {child_name} - {circle_name}")
            
            # Получаем все события календаря
            events_result = self.calendar_service.service.events().list(
                calendarId=self.calendar_service.calendar_id,
                maxResults=2500,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            logging.info(f"📊 Найдено {len(events)} событий в календаре")
            
            deleted_count = 0
            errors = []
            
            # Ищем события этого ребенка и кружка
            for event in events:
                try:
                    summary = event.get('summary', '')
                    description = event.get('description', '')
                    
                    # Проверяем, относится ли событие к удаляемому абонементу
                    if (child_name in summary and circle_name in summary) or \
                       (child_name in description and circle_name in description):
                        
                        event_id = event['id']
                        
                        # Удаляем событие
                        self.calendar_service.service.events().delete(
                            calendarId=self.calendar_service.calendar_id,
                            eventId=event_id
                        ).execute()
                        
                        deleted_count += 1
                        logging.info(f"✅ Удалено событие: {summary}")
                        
                        # Небольшая задержка между удалениями
                        import time
                        time.sleep(0.2)
                        
                except Exception as e:
                    error_msg = f"Ошибка при удалении события {event.get('id', 'unknown')}: {e}"
                    logging.error(f"❌ {error_msg}")
                    errors.append(error_msg)
                    continue
            
            # Формируем результат
            if deleted_count > 0:
                message = f"✅ Ручная очистка завершена!\n\n🗑️ Удалено {deleted_count} событий для '{child_name} - {circle_name}'"
                if errors:
                    message += f"\n⚠️ Ошибок: {len(errors)}"
            else:
                message = f"ℹ️ События для '{child_name} - {circle_name}' не найдены в Google Calendar"
            
            logging.info(f"🎯 Ручная очистка завершена: {deleted_count} удалено, {len(errors)} ошибок")
            
            return message
            
        except Exception as e:
            error_msg = f"❌ Критическая ошибка при ручной очистке Google Calendar: {e}"
            logging.error(error_msg)
            return error_msg
    
    def debug_forecast_data(self, child_name=None, circle_name=None):
        """Отладочная функция для просмотра данных в листе Прогноз."""
        try:
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            all_values = forecast_sheet.get_all_values()
            
            if not all_values:
                return "Лист 'Прогноз' пуст"
            
            headers = all_values[0]
            logging.info(f"📊 Заголовки листа 'Прогноз': {headers}")
            
            # Находим столбцы
            circle_col = child_col = -1
            for idx, header in enumerate(headers):
                if header == 'Кружок':
                    circle_col = idx
                elif header == 'Ребенок':
                    child_col = idx
            
            result_lines = [
                f"📋 ДАННЫЕ В ЛИСТЕ 'ПРОГНОЗ':",
                f"🎯 Столбец 'Кружок': {circle_col}, Столбец 'Ребенок': {child_col}",
                ""
            ]
            
            if child_name and circle_name:
                result_lines.append(f"🔍 Ищем записи для: '{child_name}' - '{circle_name}'")
                result_lines.append("")
            
            # Показываем все данные
            for i, row in enumerate(all_values[1:], start=2):
                if len(row) > max(circle_col, child_col) if circle_col >= 0 and child_col >= 0 else True:
                    row_circle = row[circle_col].strip() if circle_col >= 0 and circle_col < len(row) else "N/A"
                    row_child = row[child_col].strip() if child_col >= 0 and child_col < len(row) else "N/A"
                    
                    match_indicator = ""
                    if child_name and circle_name:
                        if row_circle == circle_name and row_child == child_name:
                            match_indicator = " ✅ СОВПАДЕНИЕ"
                        else:
                            match_indicator = " ❌"
                    
                    result_lines.append(f"Строка {i}: Кружок='{row_circle}', Ребенок='{row_child}'{match_indicator}")
            
            return "\n".join(result_lines)
            
        except Exception as e:
            return f"❌ Ошибка при чтении листа 'Прогноз': {e}"
    
    def get_subscription_deletion_preview(self, subscription_id):
        """Показывает предварительный просмотр того, что будет удалено."""
        try:
            logging.info(f"🔍 Анализ данных для удаления абонемента {subscription_id}")
            
            # Получаем информацию об абонементе
            subscription_info = self.get_subscription_details(subscription_id)
            if not subscription_info:
                return {
                    'success': False,
                    'message': f"❌ Абонемент {subscription_id} не найден"
                }
            
            child_name = subscription_info.get('Ребенок', '')
            circle_name = subscription_info.get('Кружок', '')
            
            preview_counts = {
                'Абонементы': 0,
                'Календарь занятий': 0,
                'Шаблон расписания': 0,
                'Прогноз': 0,
                'Оплачено': 0
            }
            
            # Подсчитываем записи в каждом листе
            sheets_to_check = [
                ('Календарь занятий', 'B', subscription_id),
                ('Шаблон расписания', 'B', subscription_id)
            ]
            
            for sheet_name, column, search_value in sheets_to_check:
                try:
                    sheet = self.spreadsheet.worksheet(sheet_name)
                    all_values = sheet.get_all_values()
                    count = 0
                    
                    for row in all_values[1:]:  # Пропускаем заголовки
                        if len(row) > 1 and row[1] == str(search_value):
                            count += 1
                    
                    preview_counts[sheet_name] = count
                except Exception as e:
                    logging.error(f"❌ Ошибка при подсчете в {sheet_name}: {e}")
            
            # Подсчитываем в листах Прогноз и Оплачено по имени и кружку
            for sheet_name in ['Прогноз', 'Оплачено']:
                try:
                    sheet = self.spreadsheet.worksheet(sheet_name)
                    all_values = sheet.get_all_values()
                    count = 0
                    
                    if len(all_values) > 1:
                        headers = all_values[0]
                        circle_col = child_col = -1
                        
                        for idx, header in enumerate(headers):
                            if header == 'Кружок':
                                circle_col = idx
                            elif header == 'Ребенок':
                                child_col = idx
                        
                        if circle_col >= 0 and child_col >= 0:
                            for row in all_values[1:]:
                                if (len(row) > max(circle_col, child_col) and 
                                    row[circle_col] == circle_name and 
                                    row[child_col] == child_name):
                                    count += 1
                    
                    preview_counts[sheet_name] = count
                except Exception as e:
                    logging.error(f"❌ Ошибка при подсчете в {sheet_name}: {e}")
            
            # Абонемент всегда 1 (если найден)
            preview_counts['Абонементы'] = 1
            
            # Формируем отчет
            total_to_delete = sum(preview_counts.values())
            preview_lines = [
                f"📋 **Предварительный просмотр удаления абонемента `{subscription_id}`**",
                f"👤 **Ребенок:** {child_name}",
                f"🎨 **Кружок:** {circle_name}",
                "",
                "🗑️ **Будет удалено:**"
            ]
            
            for sheet_name, count in preview_counts.items():
                if count > 0:
                    preview_lines.append(f"• {sheet_name}: {count} записей")
            
            if total_to_delete == 0:
                preview_lines.append("⚠️ Данные для удаления не найдены")
            else:
                preview_lines.append(f"\n📊 **Всего записей к удалению:** {total_to_delete}")
                preview_lines.append("\n⚠️ **Внимание:** Это действие необратимо!")
            
            return {
                'success': True,
                'message': "\n".join(preview_lines),
                'child_name': child_name,
                'circle_name': circle_name,
                'total_to_delete': total_to_delete,
                'preview_counts': preview_counts
            }
            
        except Exception as e:
            error_message = self.handle_network_error(e, "анализе данных для удаления")
            return {
                'success': False,
                'message': error_message
            }
            
    def get_next_lesson_id(self):
        """Получает следующий уникальный ID для занятия."""
        try:
            cal_sheet = self.spreadsheet.worksheet("Календарь занятий")
            data = cal_sheet.get_all_values()
            
            # Находим максимальный существующий ID
            max_id = 0
            for row in data[1:]:  # Пропускаем заголовки
                if row and row[0]:  # Если есть значение в столбце A
                    try:
                        current_id = int(row[0])
                        max_id = max(max_id, current_id)
                    except ValueError:
                        continue
            
            return max_id + 1
        except Exception as e:
            logging.error(f"Ошибка при получении следующего ID занятия: {e}")
            return 1

    def generate_schedule_for_subscription(self, sub_id, child_name, start_date_str, classes_to_generate, template):
        """Генерирует расписание в 'Календарь занятий'."""
        try:
            cal_sheet = self.spreadsheet.worksheet("Календарь занятий")
            
            if classes_to_generate <= 0:
                return None

            new_cal_entries = []
            last_generated_date = None
            
            base_start_date = datetime.strptime(start_date_str, '%d.%m.%Y')
            current_date = base_start_date
            
            # Получаем начальный ID для новых занятий
            next_lesson_id = self.get_next_lesson_id()
            
            # Создаем словарь с полной информацией о расписании
            template_days = {}
            for t in template:
                day_num = int(t['day_num'])
                template_days[day_num] = {
                    'start_time': t['start_time'],
                    'end_time': t['end_time']
                }
            
            classes_added = 0
            for _ in range(365 * 2): # Safety break для предотвращения бесконечного цикла
                if classes_added >= classes_to_generate: 
                    break
                
                day_of_week = current_date.isoweekday()
                if day_of_week in template_days:
                    schedule_info = template_days[day_of_week]
                    new_cal_entries.append([
                        next_lesson_id + classes_added,  # A: № (ID занятия)
                        sub_id,                          # B: ID абонемента
                        current_date.strftime('%d.%m.%Y'), # C: Дата занятия
                        schedule_info['start_time'],     # D: Время начала
                        'Запланировано',                 # E: Статус посещения
                        child_name,                      # F: Ребенок
                        '',                              # G: Отметка
                        schedule_info['end_time']        # H: Время завершения
                    ])
                    classes_added += 1
                    last_generated_date = current_date
                current_date += timedelta(days=1)
            
            if new_cal_entries:
                # Получаем следующий доступный ID для новых занятий
                next_available_id = self.get_next_lesson_id()
                
                # Присваиваем уникальные последовательные ID всем новым занятиям
                for i, entry in enumerate(new_cal_entries):
                    entry[0] = next_available_id + i  # Уникальные ID начиная с next_available_id
                
                cal_sheet.append_rows(new_cal_entries, value_input_option='USER_ENTERED')
                logging.info(f"✅ Создано {len(new_cal_entries)} занятий с уникальными ID от {next_available_id} до {next_available_id + len(new_cal_entries) - 1}")
                logging.info(f"📋 Абонемент {sub_id}: занятия получили фиксированные ID, которые не будут изменяться")

            return last_generated_date
        
        except Exception as e:
            logging.error(f"Ошибка при генерации расписания для ID {sub_id}: {e}", exc_info=True)
            raise

    def create_full_subscription(self, sub_data):
        """Создает новый абонемент, шаблон и расписание."""
        try:
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            
            ru_months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
            start_date = sub_data['start_date']
            date_part = f"{start_date.day}{ru_months[start_date.month - 1]}"
            clean_child_name = ''.join(filter(str.isalnum, sub_data['child_name']))
            clean_circle_name = ''.join(filter(str.isalnum, sub_data['circle_name']))
            next_row_num = len(subs_sheet.get_all_values()) + 1
            sub_id = f"{date_part}.{clean_child_name}{clean_circle_name}-{next_row_num}"
            
            # Получаем следующий доступный ID для шаблона
            existing_rows = len(template_sheet.get_all_values())
            next_template_id = existing_rows  # Начинаем с количества существующих строк
            
            template_entries = []
            for item in sub_data['schedule']:
                template_entries.append([
                    next_template_id, sub_id, item['day_num'], 
                    self.format_time(item['start_time']), self.format_time(item['end_time'])
                ])
                next_template_id += 1  # Увеличиваем для следующей записи
            if template_entries:
                template_sheet.append_rows(template_entries, value_input_option='USER_ENTERED')
                logging.info(f"✅ Создано {len(template_entries)} записей в шаблоне расписания для абонемента {sub_id}")
                for i, entry in enumerate(template_entries):
                    logging.info(f"  📋 Запись {i+1}: ID={entry[0]}, День={entry[2]}, Время={entry[3]}-{entry[4]}")

            # Определяем количество занятий для генерации
            # Если есть оставшиеся занятия, используем их, иначе общее количество
            classes_to_generate = sub_data.get('remaining_classes', sub_data['total_classes'])
            
            last_class_date = self.generate_schedule_for_subscription(
                sub_id, sub_data['child_name'], start_date.strftime('%d.%m.%Y'), 
                classes_to_generate, sub_data['schedule']
            )

            payment_type = sub_data.get('payment_type', '')
            logging.info(f"🔍 Тип оплаты для нового абонемента: '{payment_type}'")
            
            new_row = [
                next_row_num - 1, sub_id, sub_data['child_name'], sub_data['circle_name'],
                sub_data['total_classes'], start_date.strftime('%d.%m.%Y'), 
                last_class_date.strftime('%d.%m.%Y') if last_class_date else '',
                0, sub_data['remaining_classes'], "Ожидает", sub_data['cost'],
                last_class_date.strftime('%d.%m.%Y') if last_class_date else '',
                0, sub_data['sub_type'], payment_type
            ]
            
            logging.info(f"📋 Создаю строку абонемента: столбец O (индекс 14) = '{payment_type}'")
            
            subs_sheet.append_row(new_row, value_input_option='USER_ENTERED')
            
            # Создаем прогноз оплат только для НЕ разовых абонементов
            if sub_data['sub_type'].lower() != 'разовый':
                logging.info(f"📊 Создаю прогноз оплат для абонемента типа '{sub_data['sub_type']}'")
                self.create_payment_forecast(sub_id, sub_data)
            else:
                logging.info(f"🎯 Пропускаю создание прогноза для разового абонемента '{sub_id}'")
            
            return f"✅ Абонемент и расписание успешно созданы!\n\nID: `{sub_id}`"

        except Exception as e:
            import httpx
            error_msg = str(e)
            
            # Обработка специфичных сетевых ошибок
            if isinstance(e, httpx.ReadError) or "httpx.ReadError" in error_msg:
                logging.error(f"🌐 Сетевая ошибка при создании абонемента: {e}")
                return f"❌ Сетевая ошибка при сохранении в Google Sheets.\n\n🔄 Попробуйте создать абонемент еще раз через 30-60 секунд.\n\n📡 Возможные причины:\n• Временные проблемы с интернетом\n• Перегрузка Google Sheets API\n• Таймаут соединения"
            
            elif "429" in error_msg or "Quota exceeded" in error_msg:
                logging.error(f"📊 Превышена квота API при создании абонемента: {e}")
                return f"❌ Превышена квота Google Sheets API.\n\n⏰ Подождите 1-2 минуты и попробуйте снова.\n\n📈 Система временно ограничивает количество запросов."
            
            elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                logging.error(f"⏰ Таймаут при создании абонемента: {e}")
                return f"❌ Превышено время ожидания ответа от Google Sheets.\n\n🔄 Попробуйте еще раз через минуту.\n\n⚡ Возможно, сервер временно перегружен."
            
            else:
                logging.error(f"❌ Критическая ошибка при создании абонемента: {e}", exc_info=True)
                return f"❌ Произошла ошибка при создании абонемента.\n\n🔧 Детали: {error_msg}\n\n📞 Обратитесь к администратору, если ошибка повторяется."
    
    def create_payment_forecast(self, sub_id, sub_data):
        """Создает прогноз оплат для абонемента - обновление прогноза выполняется в фоне."""
        try:
            # Прогноз будет обновлен в фоновых обновлениях после создания абонемента
            logging.info(f"Прогноз для абонемента {sub_id} будет создан в фоновых обновлениях")
            
        except Exception as e:
            logging.error(f"Ошибка при подготовке прогноза для {sub_id}: {e}", exc_info=True)
            # Не прерываем создание абонемента из-за ошибки прогноза
    
    def update_full_forecast(self):
        """Полное обновление прогноза оплат согласно ТЗ."""
        try:
            from datetime import datetime, timedelta
            import time
            
            # Добавляем задержку для снижения нагрузки на API
            time.sleep(2)
            
            logging.info("=== НАЧАЛО ФОРМИРОВАНИЯ ПРОГНОЗА БЮДЖЕТА ===")
            
            # Шаг 1: Подготовка
            # Получаем или создаем лист "Прогноз" с обработкой ошибки 429
            try:
                forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            except Exception as e:
                if "429" in str(e) or "Quota exceeded" in str(e):
                    logging.warning("⚠️ Превышена квота Google Sheets API при получении листа 'Прогноз'. Пропускаю обновление.")
                    return 0, ["⚠️ Прогноз пропущен из-за превышения квоты API"]
                else:
                    # Создаем лист если его нет
                    try:
                        forecast_sheet = self.spreadsheet.add_worksheet(title="Прогноз", rows=1000, cols=5)
                        headers = ["Кружок", "Ребенок", "Дата оплаты", "Бюджет", "Статус"]
                        forecast_sheet.append_row(headers)
                    except Exception as e2:
                        if "429" in str(e2) or "Quota exceeded" in str(e2):
                            logging.warning("⚠️ Превышена квота Google Sheets API при создании листа 'Прогноз'. Пропускаю обновление.")
                            return 0, ["⚠️ Прогноз пропущен из-за превышения квоты API"]
                        else:
                            raise e2
            
            # Сохранение ID событий удалено (Google Calendar отключен)
            
            # Полностью очищаем лист "Прогноз" от старых данных (начиная со второй строки)
            try:
                if forecast_sheet.row_count > 1:
                    forecast_sheet.delete_rows(2, forecast_sheet.row_count)
                    logging.info(f"Очищены строки 2-{forecast_sheet.row_count} в листе 'Прогноз'")
            except Exception as e:
                if "429" in str(e) or "Quota exceeded" in str(e):
                    logging.warning("⚠️ Превышена квота Google Sheets API при очистке листа 'Прогноз'. Пропускаю обновление.")
                    return 0, ["⚠️ Прогноз пропущен из-за превышения квоты API"]
                
                logging.warning(f"Не удалось удалить строки: {e}")
                try:
                    if forecast_sheet.row_count > 1:
                        range_to_clear = f"A2:E{forecast_sheet.row_count}"
                        forecast_sheet.batch_clear([range_to_clear])
                        logging.info(f"Очищен диапазон {range_to_clear} в листе 'Прогноз'")
                except Exception as e2:
                    if "429" in str(e2) or "Quota exceeded" in str(e2):
                        logging.warning("⚠️ Превышена квота Google Sheets API при очистке диапазона 'Прогноз'. Пропускаю обновление.")
                        return 0, ["⚠️ Прогноз пропущен из-за превышения квоты API"]
                    logging.error(f"Не удалось очистить лист 'Прогноз': {e2}")
            
            # Определяем временные рамки: с первого числа текущего месяца до последнего числа следующего месяца
            today = datetime.now()
            start_of_period = datetime(today.year, today.month, 1)
            
            if today.month == 12:
                end_of_period = datetime(today.year + 1, 2, 1) - timedelta(days=1)
            else:
                next_month = today.month + 1
                if next_month == 12:
                    end_of_period = datetime(today.year + 1, 1, 1) - timedelta(days=1)
                else:
                    end_of_period = datetime(today.year, next_month + 1, 1) - timedelta(days=1)
            
            end_of_period = end_of_period.replace(hour=23, minute=59, second=59)
            
            logging.info(f"Период прогноза: {start_of_period.strftime('%d.%m.%Y')} - {end_of_period.strftime('%d.%m.%Y')}")
            
            # Получаем данные из листов с обработкой ошибки 429
            try:
                subs_sheet = self.spreadsheet.worksheet("Абонементы")
                calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
                
                subs_data = subs_sheet.get_all_values()
                calendar_data = calendar_sheet.get_all_values()
            except Exception as e:
                if "429" in str(e) or "Quota exceeded" in str(e):
                    logging.warning("⚠️ Превышена квота Google Sheets API при формировании прогноза. Пропускаю обновление.")
                    return 0, ["⚠️ Прогноз пропущен из-за превышения квоты API"]
                else:
                    raise e
            
            logging.info(f"Получено данных: абонементы={len(subs_data)}, календарь={len(calendar_data)}")
            
            if len(subs_data) < 2:
                logging.info("Нет данных абонементов для создания прогноза")
                return 0, ["Нет данных абонементов"]
                
            if len(calendar_data) < 2:
                logging.info("Нет данных календаря занятий для создания прогноза")
                return 0, ["Нет данных календаря занятий"]
            
            # Шаг 2: Анализ истории абонементов
            # Группируем все записи по уникальному ключу: "Имя ребенка" + "Название кружка"
            grouped_subscriptions = {}
            subscriptions_by_id = {}  # Словарь для поиска абонементов по ID
            logging.info("Шаг 2: Анализ истории абонементов...")
            
            for i, row in enumerate(subs_data[1:], 2):
                if len(row) < 12:  # Минимум до столбца L
                    continue
                
                # Согласно ТЗ: C:C (Ребенок), D:D (Кружок), E:E (К-во занятий), K:K (Стоимость), L:L (Дата окончания прогноз), B:B (ID абонемента)
                child_name = str(row[2]).strip()  # C:C = индекс 2
                circle_name = str(row[3]).strip()  # D:D = индекс 3
                total_classes = int(row[4]) if row[4] and str(row[4]).isdigit() else 0  # E:E = индекс 4
                # K:K = индекс 10, очищаем от неразрывных пробелов и других символов
                cost_str = str(row[10]).replace('\xa0', '').replace(' ', '').replace(',', '.') if len(row) > 10 and row[10] else "0"
                try:
                    cost = float(cost_str)
                except (ValueError, TypeError):
                    logging.warning(f"Строка {i}: некорректная стоимость '{row[10]}', используем 0")
                    cost = 0
                end_date_str = str(row[11]).strip() if len(row) > 11 else ""  # L:L = индекс 11
                sub_id = str(row[1]).strip()  # B:B = индекс 1
                subscription_type = str(row[13]).strip().lower() if len(row) > 13 else ""  # N:N = индекс 13 (Тип абонемента)
                
                # Пропускаем разовые абонементы - для них прогноз не создается
                if subscription_type == 'разовый':
                    logging.info(f"🎯 Пропускаю разовый абонемент {sub_id} при создании прогноза")
                    continue
                
                if not child_name or not circle_name or not end_date_str or not sub_id:
                    logging.debug(f"Строка {i}: пропускаем из-за пустых данных - ребенок:'{child_name}', кружок:'{circle_name}', дата:'{end_date_str}', ID:'{sub_id}'")
                    continue
                
                try:
                    end_date = datetime.strptime(end_date_str, '%d.%m.%Y')
                except ValueError:
                    logging.debug(f"Строка {i}: некорректная дата окончания: {end_date_str}")
                    continue
                
                # Создаем уникальный ключ "Ребенок|Кружок"
                key = f"{child_name}|{circle_name}"
                
                if key not in grouped_subscriptions:
                    grouped_subscriptions[key] = []
                
                subscription_data = {
                    'child_name': child_name,
                    'circle_name': circle_name,
                    'total_classes': total_classes,
                    'cost': cost,
                    'end_date': end_date,
                    'sub_id': sub_id
                }
                
                grouped_subscriptions[key].append(subscription_data)
                
                # Также сохраняем по ID для быстрого поиска
                subscriptions_by_id[sub_id] = subscription_data
                
                logging.info(f"✅ Добавлен в группу '{key}': {sub_id} (окончание: {end_date_str}, стоимость: {cost}, занятий: {total_classes})")
            
            logging.info(f"Найдено {len(grouped_subscriptions)} уникальных пар 'Ребенок-Кружок'")
            
            if not grouped_subscriptions:
                logging.warning("❌ Не найдено ни одного валидного абонемента для прогнозирования")
                return 0, ["Не найдено валидных абонементов"]
            
            # Для каждой группы находим абонемент с самой поздней "Дата окончания прогноз"
            latest_subscriptions = {}
            for key, subs in grouped_subscriptions.items():
                latest_sub = max(subs, key=lambda x: x['end_date'])
                latest_subscriptions[key] = latest_sub
                logging.info(f"📋 Последний абонемент для '{key}': {latest_sub['sub_id']} (окончание: {latest_sub['end_date'].strftime('%d.%m.%Y')})")
            
            logging.info(f"Определены последние абонементы для {len(latest_subscriptions)} групп")
            
            # Получаем данные из листа "Шаблон расписания"
            try:
                template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
                template_data = template_sheet.get_all_values()
            except Exception as e:
                logging.error(f"❌ Не удалось получить лист 'Шаблон расписания': {e}")
                return 0, ["Ошибка доступа к листу 'Шаблон расписания'"]
            
            # Парсим шаблон расписания для получения дней недели по абонементам
            subscription_schedule = {}
            logging.info("Анализ шаблона расписания для определения дней недели...")
            
            for i, row in enumerate(template_data[1:], 2):
                if len(row) < 3:
                    continue
                
                # Получаем ID абонемента и день недели из шаблона
                sub_id = str(row[1]).strip() if len(row) > 1 else ""  # B:B - ID абонемента (как в get_subscription_schedule)
                day_str = str(row[2]).strip() if len(row) > 2 else ""  # C:C - День недели как число
                
                if not sub_id or not day_str:
                    continue
                
                # Проверяем, есть ли такой ID в наших абонементах
                found = False
                for key, latest_sub in latest_subscriptions.items():
                    if latest_sub['sub_id'] == sub_id:
                        found = True
                        break
                if not found:
                    continue
                
                # Преобразуем день недели из числа в формат Python (0=понедельник, 6=воскресенье)
                try:
                    day_of_week_num = int(day_str)  # 1=понедельник, 7=воскресенье
                    day_of_week = (day_of_week_num - 1) % 7  # Конвертируем в Python формат
                except (ValueError, TypeError):
                    logging.warning(f"Некорректный день недели '{day_str}' для абонемента {sub_id}")
                    continue
                
                if day_of_week is not None:
                    if sub_id not in subscription_schedule:
                        subscription_schedule[sub_id] = set()
                    subscription_schedule[sub_id].add(day_of_week)
                    logging.debug(f"Найден шаблон: {sub_id} -> {day_str} (день недели: {day_of_week})")
            
            # Конвертируем множества в списки
            for sub_id in subscription_schedule:
                subscription_schedule[sub_id] = list(subscription_schedule[sub_id])
            
            logging.info(f"Найдены шаблоны расписания для {len(subscription_schedule)} абонементов")
            
            # Находим дату последнего занятия для каждого абонемента из календаря занятий
            last_lesson_dates = {}
            logging.info("Поиск дат последних занятий в календаре...")
            
            for i, row in enumerate(calendar_data[1:], 2):
                if len(row) < 3:
                    continue
                
                sub_id = str(row[1]).strip() if len(row) > 1 else ""  # B:B - ID абонемента
                date_str = str(row[2]).strip() if len(row) > 2 else ""  # C:C - Дата занятия
                
                if not sub_id or not date_str:
                    continue
                
                # Проверяем, есть ли такой ID в наших абонементах
                found = False
                for key, latest_sub in latest_subscriptions.items():
                    if latest_sub['sub_id'] == sub_id:
                        found = True
                        break
                if not found:
                    continue
                
                try:
                    lesson_date = datetime.strptime(date_str, '%d.%m.%Y')
                    
                    if sub_id not in last_lesson_dates:
                        last_lesson_dates[sub_id] = lesson_date
                    else:
                        if lesson_date > last_lesson_dates[sub_id]:
                            last_lesson_dates[sub_id] = lesson_date
                    
                    logging.debug(f"Найдено занятие: {sub_id} -> {date_str}")
                    
                except ValueError:
                    continue
            
            logging.info(f"Найдены последние занятия для {len(last_lesson_dates)} абонементов")
            
            # Шаг 3: Циклическое прогнозирование для каждой пары "Ребенок-Кружок"
            forecast_rows = []
            skipped_forecasts = []
            added_payments = set()  # Для предотвращения дубликатов: (кружок, ребенок, дата)
            
            logging.info("Шаг 3: Циклическое прогнозирование...")
            
            for key, latest_sub in latest_subscriptions.items():
                logging.info(f"=== Обрабатываю группу: {key} ===")
                
                # Проверяем наличие шаблона расписания для этого абонемента
                if latest_sub['sub_id'] not in subscription_schedule:
                    error_msg = f"{latest_sub['child_name']} - {latest_sub['circle_name']}: не найден шаблон расписания для ID {latest_sub['sub_id']}"
                    skipped_forecasts.append(error_msg)
                    logging.warning(error_msg)
                    continue
                
                # Проверяем наличие последнего занятия в календаре
                if latest_sub['sub_id'] not in last_lesson_dates:
                    error_msg = f"{latest_sub['child_name']} - {latest_sub['circle_name']}: не найдены занятия в календаре для ID {latest_sub['sub_id']}"
                    skipped_forecasts.append(error_msg)
                    logging.warning(error_msg)
                    continue
                
                if not latest_sub['total_classes'] or latest_sub['total_classes'] <= 0:
                    error_msg = f"{latest_sub['child_name']} - {latest_sub['circle_name']}: не указано количество занятий"
                    skipped_forecasts.append(error_msg)
                    logging.warning(error_msg)
                    continue
                
                # НОВАЯ ЛОГИКА: Начальная точка - дата последнего занятия из календаря
                last_lesson_date = last_lesson_dates[latest_sub['sub_id']]
                lesson_days = subscription_schedule[latest_sub['sub_id']]
                total_classes = latest_sub['total_classes']
                
                logging.info(f"Последнее занятие: {last_lesson_date.strftime('%d.%m.%Y')}, дни недели из шаблона: {lesson_days}, занятий: {total_classes}")
                
                # НОВАЯ ЛОГИКА: Циклическое прогнозирование на основе последнего занятия
                current_date = last_lesson_date
                loop_counter = 0
                max_loops = 12  # Защита от бесконечного цикла
                
                while loop_counter < max_loops:
                    loop_counter += 1
                    logging.debug(f"Итерация {loop_counter} для {key}")
                    
                    # Поиск даты следующего занятия (= дата оплаты нового абонемента)
                    search_date = current_date + timedelta(days=1)
                    next_payment_date = None
                    
                    # Ищем первый подходящий день недели из шаблона расписания
                    for _ in range(14):  # Максимум 2 недели поиска
                        if search_date.weekday() in lesson_days:
                            next_payment_date = search_date
                            break
                        search_date += timedelta(days=1)
                    
                    if not next_payment_date:
                        logging.warning(f"Не удалось найти следующую дату оплаты для {key}")
                        break
                    
                    logging.debug(f"Найдена дата следующей оплаты: {next_payment_date.strftime('%d.%m.%Y')}")
                    
                    # Проверка периода прогноза
                    if next_payment_date > end_of_period:
                        logging.debug(f"Дата {next_payment_date.strftime('%d.%m.%Y')} выходит за период прогноза")
                        break
                    
                    # Проверка и запись в прогноз (только если дата в нужном периоде)
                    if start_of_period <= next_payment_date <= end_of_period:
                        # Создаем уникальный ключ для проверки дубликатов
                        payment_key = (latest_sub['circle_name'], latest_sub['child_name'], next_payment_date.strftime('%d.%m.%Y'))
                        
                        if payment_key not in added_payments:
                            # Используем стоимость последнего абонемента
                            cost_to_use = latest_sub['cost']
                            
                            forecast_rows.append([
                                latest_sub['circle_name'],  # A:A (Кружок)
                                latest_sub['child_name'],   # B:B (Ребенок)
                                next_payment_date.strftime('%d.%m.%Y'),  # C:C (Дата оплаты)
                                cost_to_use,  # D:D (Бюджет) - правильная стоимость по ID
                                "Оплата запланирована"  # E:E (Статус)
                            ])
                            added_payments.add(payment_key)
                            logging.info(f"✅ Добавлена дата оплаты: {next_payment_date.strftime('%d.%m.%Y')} для {key} с бюджетом {cost_to_use}")
                        else:
                            logging.debug(f"⚠️ Дубликат пропущен: {next_payment_date.strftime('%d.%m.%Y')} для {key}")
                    else:
                        logging.debug(f"Дата {next_payment_date.strftime('%d.%m.%Y')} вне периода прогноза")
                    
                    # Расчет "виртуального" абонемента - определяем дату последнего занятия
                    virtual_last_lesson_date = next_payment_date
                    classes_found = 1  # Первое занятие = дата оплаты
                    
                    # Считаем оставшиеся занятия от даты начала виртуального абонемента
                    calc_date = next_payment_date + timedelta(days=1)
                    safety_counter = 0
                    
                    while classes_found < total_classes and safety_counter < 365:
                        if calc_date.weekday() in lesson_days:
                            classes_found += 1
                            virtual_last_lesson_date = calc_date
                        
                        calc_date += timedelta(days=1)
                        safety_counter += 1
                    
                    if classes_found == total_classes:
                        current_date = virtual_last_lesson_date
                        logging.debug(f"Виртуальный абонемент: последнее занятие {virtual_last_lesson_date.strftime('%d.%m.%Y')}")
                    else:
                        logging.warning(f"Не удалось рассчитать окончание виртуального абонемента для {key}")
                        break
                
                logging.info(f"Завершено прогнозирование для {key} за {loop_counter} итераций")
            
            # Шаг 4: Завершение и обратная связь
            # Записываем все найденные прогнозные даты в лист "Прогноз"
            if forecast_rows:
                # Используем RAW для предотвращения форматирования (жирного текста)
                forecast_sheet.append_rows(forecast_rows, value_input_option='RAW')
                logging.info(f"✅ Записано {len(forecast_rows)} строк в лист 'Прогноз'")
            else:
                logging.info("ℹ️ Нет данных для записи в прогноз")
            
            # Шаг 5: Обновляем "Дата окончания прогноз" в листе "Абонементы"
            logging.info("Шаг 5: Обновление дат окончания прогноз в абонементах...")
            
            # Находим первую дату оплаты для каждого абонемента и обновляем столбец L
            subscription_next_payment = {}
            for row in forecast_rows:
                circle_name = row[0]  # A:A (Кружок)
                child_name = row[1]   # B:B (Ребенок)
                payment_date_str = row[2]  # C:C (Дата оплаты)
                
                key = f"{child_name}|{circle_name}"
                if key not in subscription_next_payment:
                    subscription_next_payment[key] = payment_date_str
                else:
                    # Сравниваем даты и берем более раннюю
                    try:
                        current_date = datetime.strptime(subscription_next_payment[key], '%d.%m.%Y')
                        new_date = datetime.strptime(payment_date_str, '%d.%m.%Y')
                        if new_date < current_date:
                            subscription_next_payment[key] = payment_date_str
                    except ValueError:
                        pass
            
            # Обновляем столбец L в листе "Абонементы"
            updated_subscriptions = 0
            for key, next_payment_date in subscription_next_payment.items():
                child_name, circle_name = key.split('|')
                
                # Находим соответствующий абонемент в latest_subscriptions
                for latest_key, latest_sub in latest_subscriptions.items():
                    if latest_key == key:
                        try:
                            # Находим строку абонемента в листе
                            for i, row in enumerate(subs_data[1:], 2):
                                if (len(row) > 11 and 
                                    str(row[1]).strip() == latest_sub['sub_id'] and  # B:B - ID абонемента
                                    str(row[2]).strip() == child_name and  # C:C - Ребенок
                                    str(row[3]).strip() == circle_name):   # D:D - Кружок
                                    
                                    # Обновляем столбец L (индекс 11) - Дата окончания прогноз
                                    subs_sheet.update_cell(i, 12, next_payment_date)  # L:L = колонка 12
                                    updated_subscriptions += 1
                                    logging.info(f"✅ Обновлена дата окончания прогноз для {latest_sub['sub_id']}: {next_payment_date}")
                                    break
                        except Exception as e:
                            logging.error(f"Ошибка обновления даты окончания прогноз для {key}: {e}")
            
            logging.info(f"📅 Обновлено дат окончания прогноз: {updated_subscriptions}")
            
            logging.info("=== ЗАВЕРШЕНИЕ ФОРМИРОВАНИЯ ПРОГНОЗА БЮДЖЕТА ===")
            logging.info(f"📊 Найдено ожидаемых платежей: {len(forecast_rows)}")
            
            if skipped_forecasts:
                logging.warning(f"⚠️ Пропущено абонементов: {len(skipped_forecasts)}")
                for error in skipped_forecasts:
                    logging.warning(f"  • {error}")
            
            return len(forecast_rows), skipped_forecasts
            
        except Exception as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                logging.warning("⚠️ Превышена квота Google Sheets API при формировании прогноза бюджета. Пропускаю обновление.")
                return 0, ["⚠️ Прогноз пропущен из-за превышения квоты API"]
            else:
                logging.error(f"❌ Критическая ошибка при формировании прогноза бюджета: {e}", exc_info=True)
                return 0, [f"Ошибка: {e}"]

    def update_all_calendars(self):
        """Обновляет календарь занятий для всех активных абонементов."""
        try:
            from datetime import datetime, timedelta
            
            logging.info("=== НАЧАЛО ОБНОВЛЕНИЯ КАЛЕНДАРЕЙ ЗАНЯТИЙ ===")
            
            # Получаем все абонементы
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            subs_data = subs_sheet.get_all_values()
            
            if len(subs_data) < 2:
                logging.info("Нет абонементов для обновления календарей")
                return 0, []
            
            updated_count = 0
            errors = []
            
            # Проходим по всем абонементам
            for i, row in enumerate(subs_data[1:], 2):
                if len(row) < 12:
                    continue
                
                sub_id = str(row[1]).strip()  # B:B = индекс 1
                child_name = str(row[2]).strip()  # C:C = индекс 2
                circle_name = str(row[3]).strip()  # D:D = индекс 3
                status = str(row[9]).strip().lower() if len(row) > 9 else ""  # J:J = индекс 9
                
                # Пропускаем завершенные абонементы
                if status == "завершен":
                    logging.debug(f"Пропускаем завершенный абонемент: {sub_id}")
                    continue
                
                if not sub_id or not child_name or not circle_name:
                    continue
                
                try:
                    # Получаем данные абонемента для создания календаря
                    start_date_str = str(row[5]).strip() if len(row) > 5 else ""  # F:F = индекс 5
                    total_classes = int(row[4]) if row[4] and str(row[4]).isdigit() else 0  # E:E = индекс 4
                    remaining_classes = int(row[7]) if len(row) > 7 and row[7] and str(row[7]).isdigit() else total_classes  # H:H = индекс 7
                    
                    if not start_date_str or not total_classes:
                        logging.debug(f"Пропускаем абонемент {sub_id}: нет даты начала или количества занятий")
                        continue
                    
                    start_date = datetime.strptime(start_date_str, '%d.%m.%Y')
                    
                    # Получаем расписание абонемента
                    schedule_data = self.get_subscription_schedule(sub_id)
                    if not schedule_data:
                        logging.debug(f"Пропускаем абонемент {sub_id}: нет расписания")
                        continue
                    
                    # Обновляем календарь для этого абонемента
                    success = self.update_subscription_calendar(sub_id, start_date, remaining_classes, schedule_data)
                    if success:
                        updated_count += 1
                        logging.info(f"✅ Обновлен календарь для {sub_id} ({child_name} - {circle_name})")
                    else:
                        errors.append(f"Ошибка обновления календаря для {sub_id}")
                        
                except Exception as e:
                    error_msg = f"Ошибка при обновлении календаря {sub_id}: {e}"
                    errors.append(error_msg)
                    logging.error(error_msg)
            
            logging.info(f"=== ЗАВЕРШЕНИЕ ОБНОВЛЕНИЯ КАЛЕНДАРЕЙ: {updated_count} обновлено ===")
            return updated_count, errors
            
        except Exception as e:
            logging.error(f"Критическая ошибка при обновлении календарей: {e}", exc_info=True)
            return 0, [f"Критическая ошибка: {e}"]

    def update_subscription_calendar(self, sub_id, start_date, classes_count, schedule_data):
        """Обновляет календарь занятий для конкретного абонемента."""
        try:
            from datetime import datetime, timedelta
            
            # Получаем или создаем лист календаря
            try:
                calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            except:
                calendar_sheet = self.spreadsheet.add_worksheet(title="Календарь занятий", rows=1000, cols=8)
                headers = ["№", "ID абонемента", "Дата занятия", "Время начала", "Статус посещения", "Ребенок", "Отметка", "Время завершения"]
                calendar_sheet.append_row(headers)
            
            # Удаляем существующие записи для этого абонемента
            all_values = calendar_sheet.get_all_values()
            rows_to_delete = []
            
            for i, row in enumerate(all_values[1:], 2):  # Начинаем с 2-й строки
                if len(row) > 1 and str(row[1]).strip() == str(sub_id).strip():  # B:B - ID абонемента
                    rows_to_delete.append(i)
            
            # Удаляем строки в обратном порядке
            for row_num in reversed(rows_to_delete):
                calendar_sheet.delete_rows(row_num)
                logging.debug(f"Удалена строка {row_num} для абонемента {sub_id}")
            
            # Создаем новые записи календаря
            calendar_rows = []
            current_date = start_date
            classes_generated = 0
            
            # Получаем максимальный существующий ID для генерации уникальных ID
            max_id = self._get_next_unique_lesson_id(calendar_sheet) - 1
            logging.info(f"🔢 Максимальный существующий ID в календаре: {max_id}")
            
            # Получаем данные абонемента для заполнения
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            sub_cell = subs_sheet.find(str(sub_id))
            if not sub_cell:
                return False
            
            sub_row_values = subs_sheet.row_values(sub_cell.row)
            child_name = sub_row_values[2] if len(sub_row_values) > 2 else ""
            circle_name = sub_row_values[3] if len(sub_row_values) > 3 else ""
            
            # Генерируем календарь на основе расписания
            safety_counter = 0
            while classes_generated < classes_count and safety_counter < 365:
                day_of_week = current_date.weekday()  # 0=понедельник, 6=воскресенье
                
                # Проверяем, есть ли занятие в этот день недели
                for schedule_item in schedule_data:
                    schedule_day = schedule_item['day']
                    if day_of_week == schedule_day:
                        # Добавляем занятие в календарь согласно правильной структуре
                        unique_id = max_id + classes_generated + 1  # Генерируем уникальный ID
                        calendar_rows.append([
                            str(unique_id),  # A:A - № (уникальный ID)
                            sub_id,  # B:B - ID абонемента
                            current_date.strftime('%d.%m.%Y'),  # C:C - Дата занятия
                            self.format_time(schedule_item.get('start_time', '')),  # D:D - Время начала
                            'Запланировано',  # E:E - Статус посещения
                            child_name,  # F:F - Ребенок
                            '',  # G:G - Отметка
                            self.format_time(schedule_item.get('end_time', ''))  # H:H - Время завершения
                        ])
                        classes_generated += 1
                        logging.debug(f"🔢 Создано занятие с уникальным ID: {unique_id}")
                        break
                
                current_date += timedelta(days=1)
                safety_counter += 1
            
            # Записываем все строки календаря
            if calendar_rows:
                calendar_sheet.append_rows(calendar_rows, value_input_option='RAW')
                logging.debug(f"Добавлено {len(calendar_rows)} занятий для абонемента {sub_id}")
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при обновлении календаря абонемента {sub_id}: {e}")
            return False

    def get_subscription_schedule(self, sub_id):
        """Получает расписание абонемента из листа 'Шаблон расписания'."""
        try:
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            template_data = template_sheet.get_all_values()
            
            logging.info(f"🔍 Ищу расписание для абонемента: '{sub_id}'")
            logging.info(f"📋 Загружено строк из шаблона расписания: {len(template_data)}")
            
            # Логируем заголовки
            if template_data:
                logging.info(f"📋 Заголовки: {template_data[0]}")
            
            schedule_items = []
            found_rows = []
            
            for i, row in enumerate(template_data[1:], 2):
                if len(row) < 3:  # Минимум нужно: ID абонемента, День недели, Время начала
                    logging.info(f"⚠️ Пропускаю строку {i}: недостаточно столбцов ({len(row)})")
                    continue
                
                row_sub_id = str(row[1]).strip()  # B:B = индекс 1
                found_rows.append(f"Строка {i}: ID='{row_sub_id}'")
                
                if row_sub_id == str(sub_id).strip():
                    logging.info(f"✅ Найдено совпадение в строке {i}: '{row_sub_id}' == '{sub_id}'")
                    
                    try:
                        day_of_week = int(row[2])  # C:C = индекс 2
                        start_time = str(row[3]).strip() if len(row) > 3 else ""  # D:D = индекс 3
                        end_time = str(row[4]).strip() if len(row) > 4 else ""  # E:E = индекс 4
                        
                        logging.info(f"📅 Парсинг строки {i}: день={day_of_week}, время={start_time}-{end_time}")
                        
                        # Конвертируем день недели в формат Python (0=понедельник, 6=воскресенье)
                        python_day = (day_of_week - 1) % 7
                        
                        schedule_items.append({
                            'day': python_day,
                            'start_time': start_time,
                            'end_time': end_time
                        })
                        
                        logging.info(f"📅 Добавлено расписание: день {python_day}, {start_time}-{end_time}")
                        
                    except (ValueError, IndexError) as e:
                        logging.error(f"❌ Ошибка парсинга строки {i}: {e}")
                        continue
            
            # Логируем все найденные ID для диагностики
            logging.info(f"🔍 Все ID в шаблоне расписания:")
            for found_row in found_rows[:10]:  # Показываем первые 10
                logging.info(f"  {found_row}")
            if len(found_rows) > 10:
                logging.info(f"  ... и еще {len(found_rows) - 10} строк")
            
            logging.info(f"📊 Найдено записей расписания для '{sub_id}': {len(schedule_items)}")
            return schedule_items
            
        except Exception as e:
            logging.error(f"Ошибка при получении расписания для {sub_id}: {e}")
            return []

    def update_subscriptions_statistics(self):
        """Обновляет статистику всех абонементов согласно ТЗ версии 2.0."""
        try:
            from datetime import datetime, timedelta
            import traceback
            
            logging.info("🔒 БЕЗОПАСНАЯ ФУНКЦИЯ update_subscriptions_statistics() - ID в календаре НЕ изменяются!")
            logging.info("✅ ИСПРАВЛЕНО: Функция больше НЕ удаляет и НЕ пересоздает календарь занятий")
            
            logging.info("=== НАЧАЛО ОБНОВЛЕНИЯ СТАТИСТИКИ АБОНЕМЕНТОВ ===")
            
            # Шаг 1: Подготовка и загрузка данных
            logging.info("Шаг 1: Подготовка и загрузка данных...")
            
            # Загружаем данные из всех листов
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            
            subs_data = subs_sheet.get_all_values()
            calendar_data = calendar_sheet.get_all_values()
            template_data = template_sheet.get_all_values()
            
            if len(subs_data) < 2:
                return 0, ["Нет данных абонементов"]
            
            logging.info(f"Загружено: абонементы={len(subs_data)-1}, календарь={len(calendar_data)-1}, шаблоны={len(template_data)-1}")
            
            # Создаем структуры данных для быстрого доступа
            subscriptions = {}
            for i, row in enumerate(subs_data[1:], 2):
                if len(row) < 15:
                    continue
                sub_id = str(row[1]).strip()  # B:B
                if sub_id:
                    subscriptions[sub_id] = {
                        'row_index': i,
                        'data': row,
                        'child_name': str(row[2]).strip(),  # C:C
                        'circle_name': str(row[3]).strip(),  # D:D
                        'total_classes': int(row[4]) if row[4] and str(row[4]).isdigit() else 0,  # E:E
                        'subscription_type': str(row[13]).strip() if len(row) > 13 else "",  # N:N
                        'status': str(row[9]).strip() if len(row) > 9 else ""  # J:J
                    }
            
            # Парсим шаблоны расписания
            templates = {}
            for row in template_data[1:]:
                if len(row) < 5:
                    continue
                sub_id = str(row[1]).strip()  # B:B
                if sub_id:
                    if sub_id not in templates:
                        templates[sub_id] = []
                    try:
                        day_of_week = int(row[2])  # C:C
                        start_time = str(row[3]).strip() if len(row) > 3 else ""  # D:D
                        end_time = str(row[4]).strip() if len(row) > 4 else ""  # E:E
                        
                        templates[sub_id].append({
                            'day': (day_of_week - 1) % 7,  # Конвертируем в Python формат
                            'start_time': start_time,
                            'end_time': end_time
                        })
                    except (ValueError, IndexError):
                        continue
            
            logging.info(f"Найдено шаблонов для {len(templates)} абонементов")
            
            # Шаг 2: Обработка "прошлых" занятий
            logging.info("Шаг 2: Обработка прошлых занятий...")
            
            new_calendar = []  # Только данные занятий, без заголовков
            subscription_stats = {}  # Статистика по абонементам
            
            # Инициализируем статистику
            for sub_id in subscriptions:
                subscription_stats[sub_id] = {
                    'attended': 0,
                    'missed': 0,
                    'used_classes': 0,  # Количество "сгоревших" занятий
                    'last_lesson_date': None
                }
            
            # Обрабатываем каждое занятие из календаря
            for i, row in enumerate(calendar_data[1:], 2):
                if len(row) < 8:
                    continue
                
                sub_id = str(row[1]).strip()  # B:B - ID абонемента
                mark = str(row[6]).strip() if len(row) > 6 else ""  # G:G - Отметка
                
                if not sub_id or sub_id not in subscriptions:
                    continue
                
                # Если есть отметка - это "прошлое" занятие
                if mark:
                    # Обновляем статус посещения
                    updated_row = row.copy()
                    if mark.lower() == "посещение":
                        updated_row[4] = "Завершен"  # E:E - Статус посещения
                        subscription_stats[sub_id]['attended'] += 1
                    else:
                        updated_row[4] = "Пропуск"  # E:E - Статус посещения
                        subscription_stats[sub_id]['missed'] += 1
                    
                    # Определяем, "сгорает" ли занятие согласно ТЗ 2.0
                    subscription_type = subscriptions[sub_id]['subscription_type'].lower()
                    mark_lower = mark.lower()
                    
                    # Занятие "сгорает" если:
                    # 1. Отметка = "Посещение" (для всех типов)
                    # 2. Отметка = "Пропуск (по вине)" (для всех типов) 
                    # 3. Тип = "Фиксированный" и любая отметка
                    if (mark_lower == "посещение" or 
                        mark_lower == "пропуск (по вине)" or
                        (subscription_type == "фиксированный")):
                        subscription_stats[sub_id]['used_classes'] += 1
                    
                    # Занятие НЕ "сгорает" если:
                    # Тип = "С переносами" и отметка = "Перенос" или "Отмена (болезнь)"
                    # (в этом случае used_classes не увеличивается)
                    
                    # Запоминаем дату последнего занятия
                    try:
                        lesson_date = datetime.strptime(str(row[2]).strip(), '%d.%m.%Y')  # C:C - Дата занятия
                        if (subscription_stats[sub_id]['last_lesson_date'] is None or 
                            lesson_date > subscription_stats[sub_id]['last_lesson_date']):
                            subscription_stats[sub_id]['last_lesson_date'] = lesson_date
                    except ValueError:
                        pass
                    
                    # Добавляем обработанное занятие в новый календарь с сохранением оригинального ID
                    # Сохраняем существующий ID занятия (он будет обработан позже в шаге 5)
                    new_calendar.append(updated_row)
                    logging.debug(f"Обработано прошлое занятие: {sub_id} - {mark}")
                
                # Если отметки нет - это "будущее" занятие, игнорируем его
            
            logging.info(f"Обработано {len(new_calendar)} прошлых занятий")
            
            # Шаг 3: Перестроение "будущего" расписания
            logging.info("Шаг 3: Перестроение будущего расписания...")
            
            updated_subscriptions = 0
            errors = []
            subscription_updates = []  # Для пакетного обновления
            
            for sub_id, sub_info in subscriptions.items():
                if sub_info['status'].lower() == "завершен":
                    logging.debug(f"Пропускаем завершенный абонемент: {sub_id}")
                    continue
                
                try:
                    # Вычисляем сколько занятий осталось провести
                    total_classes = sub_info['total_classes']
                    used_classes = subscription_stats[sub_id]['used_classes']
                    remaining_classes = max(0, total_classes - used_classes)
                    
                    logging.info(f"Абонемент {sub_id}: всего={total_classes}, использовано={used_classes}, осталось={remaining_classes}")
                    
                    last_generated_date = subscription_stats[sub_id]['last_lesson_date']
                    
                    # Для разовых абонементов не генерируем дополнительные занятия
                    subscription_type = sub_info.get('subscription_type', '').lower()
                    if subscription_type == 'разовый':
                        logging.info(f"🎯 Пропускаю генерацию дополнительных занятий для разового абонемента {sub_id}")
                    elif remaining_classes > 0 and sub_id in templates:
                        # Определяем дату начала генерации
                        start_date = subscription_stats[sub_id]['last_lesson_date']
                        if start_date is None:
                            # Если занятий еще не было, берем дату начала абонемента
                            start_date_str = str(sub_info['data'][5]).strip()  # F:F - Дата начала
                            if start_date_str:
                                start_date = datetime.strptime(start_date_str, '%d.%m.%Y')
                            else:
                                start_date = datetime.now()
                        
                        # Генерируем будущие занятия
                        current_date = start_date + timedelta(days=1)
                        classes_generated = 0
                        safety_counter = 0
                        
                        while classes_generated < remaining_classes and safety_counter < 365:
                            day_of_week = current_date.weekday()
                            
                            # Проверяем, есть ли занятие в этот день недели
                            for template_item in templates[sub_id]:
                                if day_of_week == template_item['day']:
                                    # Добавляем занятие в календарь согласно правильной структуре
                                    # ID будет присвоен позже в шаге 5 (новым занятиям присваиваются новые ID)
                                    new_calendar.append([
                                        None,  # A: № (будет присвоен позже)
                                        sub_id,  # B: ID абонемента
                                        current_date.strftime('%d.%m.%Y'),  # C: Дата занятия
                                        self.format_time(template_item['start_time']),  # D: Время начала
                                        'Запланировано',  # E: Статус посещения
                                        sub_info['child_name'],  # F: Ребенок
                                        '',  # G: Отметка
                                        self.format_time(template_item['end_time'])  # H: Время завершения
                                    ])
                                    classes_generated += 1
                                    last_generated_date = current_date
                                    logging.debug(f"Сгенерировано занятие: {sub_id} - {current_date.strftime('%d.%m.%Y')}")
                                    break
                            
                            current_date += timedelta(days=1)
                            safety_counter += 1
                    
                    # Подготавливаем данные для обновления абонемента
                    attended = subscription_stats[sub_id]['attended']
                    missed = subscription_stats[sub_id]['missed']
                    
                    # Определяем новый статус на основе реального столбца I (Осталось занятий)
                    current_remaining_from_sheet = 0
                    try:
                        # Получаем текущее значение из столбца I (индекс 8)
                        if len(sub_info['data']) > 8 and sub_info['data'][8]:
                            current_remaining_from_sheet = int(sub_info['data'][8])
                    except (ValueError, IndexError):
                        current_remaining_from_sheet = 0
                    
                    new_status = sub_info['status']
                    if current_remaining_from_sheet <= 0:
                        new_status = "Завершен"
                    elif attended > 0 and sub_info['status'].lower() == "ожидает":
                        new_status = "Активен"
                    
                    # Подготавливаем обновление строки
                    row_data = sub_info['data'].copy()
                    while len(row_data) < 15:
                        row_data.append('')
                    
                    # Обновляем согласно логике ТЗ:
                    row_data[7] = str(attended)  # H:H - Прошло занятий (количество "Посещение")
                    # ВАЖНО: НЕ обновляем столбец I (Осталось занятий) - он должен оставаться независимым!
                    # row_data[8] = str(remaining_classes)  # I:I - НЕ ТРОГАЕМ этот столбец!
                    row_data[12] = str(missed)  # M:M - Пропущено (все виды пропусков)
                    row_data[9] = new_status  # J:J - Статус (автоматически)
                    
                    if last_generated_date:
                        row_data[11] = last_generated_date.strftime('%d.%m.%Y')  # L:L - Дата окончания прогноз
                    
                    subscription_updates.append({
                        'row_index': sub_info['row_index'],
                        'data': row_data[:15]  # Ограничиваем до 15 столбцов
                    })
                    
                    updated_subscriptions += 1
                    logging.info(f"Подготовлено обновление для {sub_id}: посещено={attended}, пропущено={missed}, осталось={remaining_classes}, статус={new_status}")
                        
                except Exception as e:
                    error_msg = f"Ошибка обработки абонемента {sub_id}: {e}"
                    errors.append(error_msg)
                    logging.error(error_msg)
            
            # Шаг 4: Пакетное обновление статистики в листе "Абонементы"
            logging.info("Шаг 4: Пакетное обновление статистики абонементов...")
            
            if subscription_updates:
                try:
                    # Пакетное обновление всех строк абонементов
                    for update in subscription_updates:
                        row_index = update['row_index']
                        row_data = update['data']
                        
                        # Обновляем всю строку целиком
                        range_name = f"A{row_index}:O{row_index}"
                        subs_sheet.update(range_name, [row_data], value_input_option='RAW')
                        
                    logging.info(f"Обновлено {len(subscription_updates)} строк в листе 'Абонементы'")
                    
                except Exception as e:
                    error_msg = f"Ошибка при пакетном обновлении абонементов: {e}"
                    errors.append(error_msg)
                    logging.error(error_msg)
            
            # Шаг 5: Запись данных в календарь с сохранением ID событий
            logging.info("Шаг 5: Запись обновленного календаря с сохранением ID событий...")
            
            # ИСПРАВЛЕНО: НЕ УДАЛЯЕМ СУЩЕСТВУЮЩИЕ ДАННЫЕ, ТОЛЬКО ОБНОВЛЯЕМ СТАТИСТИКУ
            logging.info("🔒 ЗАЩИТА ID: Обновляем только статистику абонементов, НЕ трогая календарь занятий")
            
            # Получаем текущие данные календаря для проверки
            all_data = calendar_sheet.get_all_values()
            logging.info(f"📊 Текущее состояние календаря: {len(all_data)-1} занятий (ID сохранены)")
            
            # ВАЖНО: Календарь занятий НЕ пересоздается, ID остаются неизменными!
            # Статистика обновляется только в листе "Абонементы"
            
            logging.info("=== ЗАВЕРШЕНИЕ ОБНОВЛЕНИЯ СТАТИСТИКИ АБОНЕМЕНТОВ ===")
            logging.info(f"Обновлено абонементов: {updated_subscriptions}")
            
            return updated_subscriptions, errors
            
        except Exception as e:
            logging.error(f"Критическая ошибка при обновлении статистики абонементов: {e}", exc_info=True)
            return 0, [f"Критическая ошибка: {e}"]
    
    def verify_lesson_ids_integrity(self):
        """Проверяет целостность ID в календаре занятий (не изменяет данные)."""
        try:
            logging.info("🔍 Проверка целостности ID в календаре занятий...")
            
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            data = calendar_sheet.get_all_values()
            
            if len(data) <= 1:
                return {"status": "empty", "message": "Календарь занятий пуст"}
            
            ids = []
            duplicates = []
            invalid_ids = []
            
            for i, row in enumerate(data[1:], 2):  # Начинаем с 2-й строки
                if len(row) > 0:
                    lesson_id = str(row[0]).strip()
                    if lesson_id:
                        if lesson_id.isdigit():
                            id_num = int(lesson_id)
                            if id_num in ids:
                                duplicates.append({"row": i, "id": id_num})
                            else:
                                ids.append(id_num)
                        else:
                            invalid_ids.append({"row": i, "id": lesson_id})
                    else:
                        invalid_ids.append({"row": i, "id": "пустой"})
            
            result = {
                "status": "checked",
                "total_lessons": len(data) - 1,
                "valid_ids": len(ids),
                "duplicates": duplicates,
                "invalid_ids": invalid_ids,
                "id_range": f"{min(ids) if ids else 0}-{max(ids) if ids else 0}"
            }
            
            if duplicates or invalid_ids:
                logging.warning(f"⚠️ Найдены проблемы с ID: дубли={len(duplicates)}, невалидные={len(invalid_ids)}")
            else:
                logging.info("✅ Все ID в календаре занятий корректны и уникальны")
            
            return result
            
        except Exception as e:
            logging.error(f"❌ Ошибка при проверке целостности ID: {e}")
            return {"status": "error", "message": str(e)}

    def create_visual_calendar(self):
        """Создает визуальный календарь согласно ТЗ."""
        try:
            from datetime import datetime, timedelta, date
            import calendar
            
            logging.info("=== НАЧАЛО СОЗДАНИЯ ВИЗУАЛЬНОГО КАЛЕНДАРЯ ===")
            
            # Шаг 1: Подготовка
            logging.info("Шаг 1: Подготовка листа и временных рамок...")
            
            # Находим или создаем лист "Обзор календаря"
            try:
                overview_sheet = self.spreadsheet.worksheet("Обзор календаря")
                # Полная очистка содержимого и форматирования
                overview_sheet.clear()
                overview_sheet.clear_basic_filter()
                
                # Очищаем все форматирование через API
                requests = [{
                    'repeatCell': {
                        'range': {
                            'sheetId': overview_sheet.id
                        },
                        'cell': {},
                        'fields': 'userEnteredFormat'
                    }
                }]
                
                body = {'requests': requests}
                self.spreadsheet.batch_update(body)
                logging.info("Полная очистка листа 'Обзор календаря' выполнена")
                
            except:
                overview_sheet = self.spreadsheet.add_worksheet(title="Обзор календаря", rows=100, cols=70)
            
            # Определяем временные рамки: текущий и следующий месяц
            today = date.today()
            current_month_start = date(today.year, today.month, 1)
            
            # Следующий месяц
            if today.month == 12:
                next_month_start = date(today.year + 1, 1, 1)
                next_month_end = date(today.year + 1, 1, calendar.monthrange(today.year + 1, 1)[1])
            else:
                next_month_start = date(today.year, today.month + 1, 1)
                next_month_end = date(today.year, today.month + 1, calendar.monthrange(today.year, today.month + 1)[1])
            
            current_month_end = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
            
            logging.info(f"Период отчета: {current_month_start} - {next_month_end}")
            
            # Шаг 2: Сбор и обработка данных
            logging.info("Шаг 2: Загрузка и обработка данных...")
            
            # Загружаем данные из всех листов
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            
            subs_data = subs_sheet.get_all_values()
            calendar_data = calendar_sheet.get_all_values()
            forecast_data = forecast_sheet.get_all_values()
            template_data = template_sheet.get_all_values()
            
            # Создаем карты данных
            marks_map = {}  # "Ребенок|Кружок|Дата" -> {время, отметка, статус}
            budget_map = {}  # "Ребенок|Кружок|Дата" -> сумма
            template_map = {}  # ID абонемента -> {день недели, время}
            
            # Заполняем карту отметок из календаря занятий
            for row in calendar_data[1:]:
                if len(row) < 7:
                    continue
                
                sub_id = str(row[1]).strip()  # B:B - ID абонемента
                date_str = str(row[2]).strip()  # C:C - Дата занятия
                start_time = str(row[3]).strip()  # D:D - Время начала
                status = str(row[4]).strip()  # E:E - Статус посещения
                child = str(row[5]).strip()  # F:F - Ребенок
                mark = str(row[6]).strip() if len(row) > 6 else ""  # G:G - Отметка
                
                if not date_str or not child:
                    continue
                
                # Находим кружок по ID абонемента
                circle = ""
                for sub_row in subs_data[1:]:
                    if len(sub_row) > 3 and str(sub_row[1]).strip() == sub_id:
                        circle = str(sub_row[3]).strip()  # D:D - Кружок
                        break
                
                if circle:
                    key = f"{child}|{circle}|{date_str}"
                    marks_map[key] = {
                        'time': start_time,
                        'mark': mark,
                        'status': status
                    }
            
            # Заполняем карту бюджетов из прогноза
            for row in forecast_data[1:]:
                if len(row) < 4:
                    continue
                
                circle = str(row[0]).strip()  # A:A - Кружок
                child = str(row[1]).strip()  # B:B - Ребенок
                date_str = str(row[2]).strip()  # C:C - Дата оплаты
                budget_str = str(row[3]).strip()  # D:D - Бюджет
                
                if not date_str or not child or not circle:
                    continue
                
                try:
                    budget = float(budget_str.replace(',', '').replace(' ', ''))
                    key = f"{child}|{circle}|{date_str}"
                    
                    if key in budget_map:
                        budget_map[key] += budget
                    else:
                        budget_map[key] = budget
                except ValueError:
                    continue
            
            # Заполняем карту шаблонов
            for row in template_data[1:]:
                if len(row) < 4:
                    continue
                
                sub_id = str(row[1]).strip()  # B:B - ID абонемента
                try:
                    day_of_week = int(row[2])  # C:C - День недели
                    start_time = str(row[3]).strip()  # D:D - Время начала
                    
                    template_map[sub_id] = {
                        'day': (day_of_week - 1) % 7,  # Конвертируем в Python формат
                        'time': start_time
                    }
                except (ValueError, IndexError):
                    continue
            
            logging.info(f"Загружено: отметки={len(marks_map)}, бюджеты={len(budget_map)}, шаблоны={len(template_map)}")
            
            # Шаг 3: Построение структуры отчета
            logging.info("Шаг 3: Построение структуры отчета...")
            
            # Формируем заголовок (даты)
            header_row = [""]  # Первая ячейка пустая
            
            # Добавляем даты текущего месяца
            current_date = current_month_start
            while current_date <= current_month_end:
                header_row.append(current_date.strftime('%d.%m'))
                current_date += timedelta(days=1)
            
            # Пустой столбец для разделения
            header_row.append("")
            
            # Добавляем даты следующего месяца
            current_date = next_month_start
            while current_date <= next_month_end:
                header_row.append(current_date.strftime('%d.%m'))
                current_date += timedelta(days=1)
            
            # Формируем боковую колонку (дети и кружки)
            children_circles = {}
            for row in subs_data[1:]:
                if len(row) < 4:
                    continue
                
                child = str(row[2]).strip()  # C:C - Ребенок
                circle = str(row[3]).strip()  # D:D - Кружок
                
                if child and circle:
                    if child not in children_circles:
                        children_circles[child] = set()
                    children_circles[child].add(circle)
            
            # Сортируем детей и их кружки
            sorted_children = sorted(children_circles.keys())
            
            # Создаем структуру строк
            rows_data = [header_row]  # Первая строка - заголовки
            row_labels = []  # Метки строк для форматирования
            
            for child in sorted_children:
                circles = sorted(list(children_circles[child]))
                
                # Добавляем имя ребенка (жирным)
                child_row = [child] + [""] * (len(header_row) - 1)
                rows_data.append(child_row)
                row_labels.append(('child', child))
                
                # Добавляем кружки ребенка
                for circle in circles:
                    circle_row = [f"  {circle}"] + [""] * (len(header_row) - 1)
                    rows_data.append(circle_row)
                    row_labels.append(('circle', child, circle))
                
                # Добавляем строку "Бюджет"
                budget_row = ["  Бюджет"] + [""] * (len(header_row) - 1)
                rows_data.append(budget_row)
                row_labels.append(('budget', child))
            
            logging.info(f"Создана структура: {len(rows_data)} строк, {len(header_row)} столбцов")
            
            # Шаг 4: Заполнение сетки календаря
            logging.info("Шаг 4: Заполнение данных...")
            
            # Создаем карту дат для быстрого поиска индексов столбцов
            date_to_col = {}
            col_index = 1  # Начинаем с 1 (после столбца с именами)
            
            # Текущий месяц
            current_date = current_month_start
            while current_date <= current_month_end:
                date_to_col[current_date.strftime('%d.%m.%Y')] = col_index
                col_index += 1
                current_date += timedelta(days=1)
            
            # Пропускаем пустой столбец
            col_index += 1
            
            # Следующий месяц
            current_date = next_month_start
            while current_date <= next_month_end:
                date_to_col[current_date.strftime('%d.%m.%Y')] = col_index
                col_index += 1
                current_date += timedelta(days=1)
            
            # Заполняем данные по строкам
            for row_idx, (row_type, *params) in enumerate(row_labels, 1):  # +1 потому что первая строка - заголовки
                if row_type == 'circle':
                    child, circle = params
                    
                    # Проходим по всем датам
                    for date_str, col_idx in date_to_col.items():
                        key = f"{child}|{circle}|{date_str}"
                        
                        # Приоритет 1: Дата оплаты
                        if key in budget_map:
                            rows_data[row_idx][col_idx] = int(budget_map[key])
                            continue
                        
                        # Приоритет 2: Прошедшее занятие с отметкой
                        if key in marks_map and marks_map[key]['mark']:
                            mark_info = marks_map[key]
                            if mark_info['mark'].lower() == 'посещение':
                                # Форматируем время как текст в формате HH:MM
                                time_str = mark_info['time']
                                if ':' in time_str and len(time_str.split(':')) == 2:
                                    try:
                                        hours, minutes = time_str.split(':')
                                        formatted_time = f"{int(hours):02d}:{int(minutes):02d}"
                                        rows_data[row_idx][col_idx] = formatted_time
                                    except ValueError:
                                        rows_data[row_idx][col_idx] = time_str
                                else:
                                    rows_data[row_idx][col_idx] = time_str
                            else:
                                rows_data[row_idx][col_idx] = 'x'
                            continue
                        
                        # Приоритет 3: Запланированное занятие
                        if key in marks_map and not marks_map[key]['mark']:
                            # Форматируем время как текст в формате HH:MM
                            time_str = marks_map[key]['time']
                            if ':' in time_str and len(time_str.split(':')) == 2:
                                try:
                                    hours, minutes = time_str.split(':')
                                    formatted_time = f"{int(hours):02d}:{int(minutes):02d}"
                                    rows_data[row_idx][col_idx] = formatted_time
                                except ValueError:
                                    rows_data[row_idx][col_idx] = time_str
                            else:
                                rows_data[row_idx][col_idx] = time_str
                            continue
                
                elif row_type == 'budget':
                    child = params[0]
                    
                    # Суммируем бюджеты по дням для ребенка
                    for date_str, col_idx in date_to_col.items():
                        daily_budget = 0
                        
                        # Проходим по всем кружкам ребенка
                        if child in children_circles:
                            for circle in children_circles[child]:
                                key = f"{child}|{circle}|{date_str}"
                                if key in budget_map:
                                    daily_budget += budget_map[key]
                        
                        if daily_budget > 0:
                            rows_data[row_idx][col_idx] = int(daily_budget)
            
            # Шаг 5: Запись данных в лист
            logging.info("Шаг 5: Запись данных в Google Sheets...")
            
            # Записываем все данные
            if rows_data:
                overview_sheet.append_rows(rows_data, value_input_option='RAW')
            
            # Шаг 6: Форматирование
            logging.info("Шаг 6: Применение форматирования...")
            
            # Получаем размеры таблицы
            num_rows = len(rows_data)
            num_cols = len(header_row)
            
            # Применяем сетку начиная со второй строки
            if num_rows > 1:
                grid_range = f"A2:{chr(ord('A') + num_cols - 1)}{num_rows}"
                
                # Создаем запрос на форматирование сетки
                requests = []
                
                # Добавляем границы сетки
                requests.append({
                    'updateBorders': {
                        'range': {
                            'sheetId': overview_sheet.id,
                            'startRowIndex': 1,  # Начинаем со второй строки (индекс 1)
                            'endRowIndex': num_rows,
                            'startColumnIndex': 0,
                            'endColumnIndex': num_cols
                        },
                        'top': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.95, 'green': 0.95, 'blue': 0.95}},
                        'bottom': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.95, 'green': 0.95, 'blue': 0.95}},
                        'left': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.95, 'green': 0.95, 'blue': 0.95}},
                        'right': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.95, 'green': 0.95, 'blue': 0.95}},
                        'innerHorizontal': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.95, 'green': 0.95, 'blue': 0.95}},
                        'innerVertical': {'style': 'SOLID', 'width': 1, 'color': {'red': 0.95, 'green': 0.95, 'blue': 0.95}}
                    }
                })
                
                # Форматируем имена детей жирным шрифтом
                for row_idx, (row_type, *params) in enumerate(row_labels, 2):  # +2 потому что начинаем с A2
                    if row_type == 'child':
                        requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': overview_sheet.id,
                                    'startRowIndex': row_idx - 1,
                                    'endRowIndex': row_idx,
                                    'startColumnIndex': 0,
                                    'endColumnIndex': 1
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'textFormat': {
                                            'bold': True
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat.textFormat.bold'
                            }
                        })
                
                # Обрабатываем форматирование для каждой строки кружка
                for row_idx, (row_type, *params) in enumerate(row_labels, 2):
                    if row_type == 'circle':
                        child, circle = params
                        
                        # Проходим по всем ячейкам строки кружка
                        for date_str, col_idx in date_to_col.items():
                            key = f"{child}|{circle}|{date_str}"
                            cell_value = rows_data[row_idx - 1][col_idx] if col_idx < len(rows_data[row_idx - 1]) else ""
                            
                            # Проверяем, что в ячейке есть значение
                            if cell_value and cell_value != '':
                                # Если это бюджет (число) - красный фон + формат числа
                                if key in budget_map:
                                    requests.append({
                                        'repeatCell': {
                                            'range': {
                                                'sheetId': overview_sheet.id,
                                                'startRowIndex': row_idx - 1,
                                                'endRowIndex': row_idx,
                                                'startColumnIndex': col_idx,
                                                'endColumnIndex': col_idx + 1
                                            },
                                            'cell': {
                                                'userEnteredFormat': {
                                                    'numberFormat': {
                                                        'type': 'NUMBER',
                                                        'pattern': '#,##0'
                                                    },
                                                    'backgroundColor': {
                                                        'red': 0.96,
                                                        'green': 0.8,
                                                        'blue': 0.8
                                                    }
                                                }
                                            },
                                            'fields': 'userEnteredFormat.numberFormat,userEnteredFormat.backgroundColor'
                                        }
                                    })
                                
                                # Если это время посещения - зеленый фон
                                elif (key in marks_map and marks_map[key]['mark'] and 
                                      marks_map[key]['mark'].lower() == 'посещение' and
                                      cell_value != 'x'):
                                    requests.append({
                                        'repeatCell': {
                                            'range': {
                                                'sheetId': overview_sheet.id,
                                                'startRowIndex': row_idx - 1,
                                                'endRowIndex': row_idx,
                                                'startColumnIndex': col_idx,
                                                'endColumnIndex': col_idx + 1
                                            },
                                            'cell': {
                                                'userEnteredFormat': {
                                                    'backgroundColor': {
                                                        'red': 0.85,
                                                        'green': 0.92,
                                                        'blue': 0.83
                                                    }
                                                }
                                            },
                                            'fields': 'userEnteredFormat.backgroundColor'
                                        }
                                    })
                
                # Форматируем строки "Бюджет" в формате с разделителями тысяч
                for row_idx, (row_type, *params) in enumerate(row_labels, 2):
                    if row_type == 'budget':
                        requests.append({
                            'repeatCell': {
                                'range': {
                                    'sheetId': overview_sheet.id,
                                    'startRowIndex': row_idx - 1,
                                    'endRowIndex': row_idx,
                                    'startColumnIndex': 1,
                                    'endColumnIndex': num_cols
                                },
                                'cell': {
                                    'userEnteredFormat': {
                                        'numberFormat': {
                                            'type': 'NUMBER',
                                            'pattern': '#,##0'
                                        }
                                    }
                                },
                                'fields': 'userEnteredFormat.numberFormat'
                            }
                        })
                
                # Выполняем все запросы форматирования
                if requests:
                    body = {'requests': requests}
                    self.spreadsheet.batch_update(body)
                    logging.info(f"Применено {len(requests)} запросов форматирования")
            
            logging.info("=== ЗАВЕРШЕНИЕ СОЗДАНИЯ ВИЗУАЛЬНОГО КАЛЕНДАРЯ ===")
            return True
            
        except Exception as e:
            logging.error(f"Критическая ошибка при создании визуального календаря: {e}", exc_info=True)
            return False

    def get_subscriptions(self):
        """Получает все абонементы."""
        try:
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            data = subs_sheet.get_all_records()
            return data
        except Exception as e:
            logging.error(f"Ошибка при получении абонементов: {e}")
            return []
    
    def get_current_subscription_by_child_circle(self, child_name, circle_name):
        """Получает последний абонемент для ребенка и кружка (включая завершенные)."""
        try:
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            data = subs_sheet.get_all_records()
            
            # Ищем все абонементы для данной пары ребенок-кружок
            matching_subs = []
            for sub in data:
                if (str(sub.get('Ребенок', '')).strip() == str(child_name).strip() and 
                    str(sub.get('Кружок', '')).strip() == str(circle_name).strip()):
                    matching_subs.append(sub)
            
            if not matching_subs:
                return None
            
            # Возвращаем последний абонемент (по дате создания или ID)
            # Сортируем по ID абонемента (который содержит дату)
            matching_subs.sort(key=lambda x: str(x.get('ID абонемента', '')), reverse=True)
            return matching_subs[0]
            
        except Exception as e:
            logging.error(f"Ошибка при получении абонемента для {child_name} - {circle_name}: {e}")
            return None
    
    def transfer_forecast_to_paid(self, subscription_key, payment_date=None):
        """Переносит конкретную прогнозную оплату в лист 'Оплачено' при продлении абонемента."""
        try:
            child_name, circle_name = subscription_key.split("|")
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            paid_sheet = self.spreadsheet.worksheet("Оплачено")
            
            # Получаем все данные из прогноза
            all_data = forecast_sheet.get_all_values()
            if not all_data:
                return "✅ Лист прогноза пуст"
            
            # Находим строки для переноса
            rows_to_transfer = []
            rows_to_delete = []
            
            for i, row in enumerate(all_data[1:], 2):  # Начинаем с 2 (пропускаем заголовки)
                if len(row) >= 4:
                    row_circle = str(row[0]).strip()  # A: Кружок
                    row_child = str(row[1]).strip()   # B: Ребенок
                    row_payment_date = str(row[2]).strip()  # C: Дата оплаты
                    
                    # Проверяем совпадение ребенка, кружка и конкретной даты (если указана)
                    if (row_circle == circle_name and row_child == child_name):
                        if payment_date is None or row_payment_date == payment_date:
                            # Подготавливаем данные для переноса в "Оплачено"
                            budget = str(row[3]).strip()        # D: Бюджет
                            
                            # Формат для листа "Оплачено": [Кружок, Ребенок, Дата оплаты, Бюджет, Статус]
                            paid_row = [circle_name, child_name, row_payment_date, budget, "Оплачено"]
                            rows_to_transfer.append(paid_row)
                            rows_to_delete.append(i)
                            
                            # Если указана конкретная дата, переносим только её и выходим
                            if payment_date is not None:
                                break
            
            # Переносим в "Оплачено"
            if rows_to_transfer:
                for row in rows_to_transfer:
                    paid_sheet.append_row(row, value_input_option='USER_ENTERED')
                
                # Удаляем из "Прогноз" (в обратном порядке)
                for row_index in sorted(rows_to_delete, reverse=True):
                    forecast_sheet.delete_rows(row_index)
                
                logging.info(f"✅ Перенесено {len(rows_to_transfer)} оплат из Прогноз в Оплачено для {child_name} - {circle_name}")
                return f"✅ Перенесено {len(rows_to_transfer)} оплат в 'Оплачено'"
            else:
                return "ℹ️ Нет данных для переноса"
            
        except Exception as e:
            logging.error(f"Ошибка при переносе оплат для {subscription_key}: {e}")
            return f"❌ Ошибка переноса: {e}"

    def delete_forecast_payments_by_key(self, subscription_key):
        """Удаляет прогнозные оплаты для конкретного абонемента."""
        try:
            child_name, circle_name = subscription_key.split("|")
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            
            # Получаем все данные
            all_data = forecast_sheet.get_all_values()
            if not all_data:
                return "✅ Лист прогноза пуст"
            
            # Находим строки для удаления
            rows_to_delete = []
            for i, row in enumerate(all_data[1:], 2):  # Начинаем с 2 (пропускаем заголовки)
                if len(row) >= 2:
                    row_circle = str(row[0]).strip()  # A: Кружок
                    row_child = str(row[1]).strip()   # B: Ребенок
                    
                    if row_circle == circle_name and row_child == child_name:
                        rows_to_delete.append(i)
            
            # Удаляем строки (в обратном порядке, чтобы не сбить индексы)
            deleted_count = 0
            for row_index in sorted(rows_to_delete, reverse=True):
                forecast_sheet.delete_rows(row_index)
                deleted_count += 1
            
            return f"✅ Удалено {deleted_count} прогнозных оплат для {child_name} - {circle_name}"
            
        except Exception as e:
            logging.error(f"Ошибка при удалении прогнозных оплат для {subscription_key}: {e}")
            return f"❌ Ошибка при удалении: {e}"
    
    def create_schedule_template_for_new_subscription(self, new_sub_id, old_sub_id):
        """Создает шаблон расписания для нового абонемента на основе старого."""
        try:
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            
            # Получаем расписание старого абонемента
            old_schedule = self.get_subscription_schedule(old_sub_id)
            if not old_schedule:
                logging.error(f"Не найдено расписание для абонемента {old_sub_id}")
                return False
            
            # Создаем новые записи в шаблоне расписания
            new_template_rows = []
            for schedule_item in old_schedule:
                # Преобразуем Python день недели (0-6) в формат таблицы (1-7)
                day_of_week = schedule_item['day'] + 1
                if day_of_week == 7:  # Воскресенье в Python = 6, в таблице = 7
                    day_of_week = 7
                
                new_row = [
                    '',  # A: Пустая колонка
                    new_sub_id,  # B: ID абонемента (новый)
                    day_of_week,  # C: День недели
                    self.format_time(schedule_item['start_time']),  # D: Время начала
                    self.format_time(schedule_item['end_time']),    # E: Время окончания
                    ''   # F: Пустая колонка
                ]
                new_template_rows.append(new_row)
            
            # Добавляем новые строки в шаблон
            if new_template_rows:
                template_sheet.append_rows(new_template_rows, value_input_option='RAW')
                logging.info(f"✅ Создано {len(new_template_rows)} записей в шаблоне расписания для абонемента {new_sub_id}")
                return True
            else:
                logging.error(f"Нет данных для создания шаблона расписания для {new_sub_id}")
                return False
                
        except Exception as e:
            logging.error(f"Ошибка при создании шаблона расписания для {new_sub_id}: {e}")
            return False

    def get_calendar_lessons(self):
        """Получает все занятия из календаря занятий (с кешированием)."""
        try:
            # Проверяем кеш
            cache_key = 'calendar_lessons'
            cached_data = self._get_from_cache(cache_key)
            if cached_data is not None:
                return cached_data
            
            logging.info("📋 Подключение к листу 'Календарь занятий'...")
            
            # Попытка с повтором при превышении квоты
            max_retries = 3
            calendar_sheet = None
            
            for attempt in range(max_retries):
                try:
                    calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
                    break
                except gspread.exceptions.APIError as e:
                    if "429" in str(e) and attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 15  # 15, 30, 45 секунд
                        logging.warning(f"⚠️ Квота Google Sheets превышена, ожидание {wait_time} секунд... (попытка {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise
            
            if not calendar_sheet:
                logging.error("❌ Не удалось подключиться к листу после всех попыток")
                return []
            
            logging.info("📊 Загрузка данных из листа...")
            
            # Аналогично для загрузки данных
            data = None
            for attempt in range(max_retries):
                try:
                    data = calendar_sheet.get_all_records()
                    break
                except gspread.exceptions.APIError as e:
                    if "429" in str(e) and attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 15
                        logging.warning(f"⚠️ Квота превышена при загрузке данных, ожидание {wait_time} секунд...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise
            
            if data is not None:
                logging.info(f"✅ Успешно загружено {len(data)} записей из календаря")
                if data:
                    logging.info(f"📝 Пример первой записи: {data[0]}")
                
                # Сохраняем в кеш на 30 секунд
                self._save_to_cache(cache_key, data, duration=30)
                
                return data
            else:
                return []
                
        except Exception as e:
            logging.error(f"❌ Ошибка при получении календаря занятий: {e}", exc_info=True)
            return []

    def get_lessons_by_subscription(self, subscription_id):
        """Получает занятия для конкретного абонемента с номерами строк."""
        try:
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            all_data = calendar_sheet.get_all_values()
            
            if not all_data:
                return []
            
            headers = all_data[0]
            lessons_with_rows = []
            
            for i, row in enumerate(all_data[1:], 2):  # Начинаем с строки 2 (1 - заголовки)
                if len(row) > 1:  # Проверяем что есть данные
                    lesson_subscription_id = row[1] if len(row) > 1 else ''  # B - ID абонемента
                    
                    if str(lesson_subscription_id).strip() == str(subscription_id).strip():
                        # Создаем словарь с данными занятия
                        lesson = {}
                        for j, header in enumerate(headers):
                            lesson[header] = row[j] if j < len(row) else ''
                        
                        # Добавляем номер строки для правильного сопоставления
                        lesson['_row_number'] = i
                        lessons_with_rows.append(lesson)
            
            logging.info(f"📋 Найдено {len(lessons_with_rows)} занятий для абонемента {subscription_id}")
            for lesson in lessons_with_rows:
                logging.info(f"   Строка {lesson['_row_number']}: {lesson.get('Дата занятия', '')} - {lesson.get('Статус посещения', '')}")
            
            return lessons_with_rows
        except Exception as e:
            logging.error(f"Ошибка при получении занятий для абонемента {subscription_id}: {e}")
            return []

    def update_lesson_mark(self, lesson_id, mark):
        """Обновляет отметку посещения для занятия по ID."""
        try:
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            data = calendar_sheet.get_all_values()
            
            # Ищем строку с нужным ID занятия
            lesson_row = None
            
            # Если ID составной (дата_индекс_имя), разбираем его
            if '_' in str(lesson_id) and len(str(lesson_id).split('_')) >= 3:
                parts = str(lesson_id).split('_')
                target_date = parts[0]
                target_index = int(parts[1])
                target_child = '_'.join(parts[2:])  # Имя может содержать _
                
                # Ищем по дате и имени ребенка
                current_index = 0
                for i, row in enumerate(data):
                    if i == 0:  # Пропускаем заголовки
                        continue
                    if len(row) > 5:  # Проверяем что есть дата и ребенок
                        row_date = str(row[2]).strip() if len(row) > 2 else ''  # C:C - Дата занятия
                        row_child = str(row[5]).strip() if len(row) > 5 else ''  # F:F - Ребенок
                        
                        if row_date == target_date and row_child == target_child:
                            if current_index == target_index:
                                lesson_row = i + 1
                                break
                            current_index += 1
            else:
                # Сначала пробуем найти по ID в столбце A
                for i, row in enumerate(data):
                    if i == 0:  # Пропускаем заголовки
                        continue
                    if len(row) > 0 and str(row[0]).strip() == str(lesson_id).strip():  # A:A - № (ID занятия)
                        lesson_row = i + 1  # +1 потому что индексы в Google Sheets начинаются с 1
                        break
                
                # Если не найдено по ID, пробуем использовать lesson_id как номер строки
                if not lesson_row and lesson_id.isdigit():
                    potential_row = int(lesson_id)
                    if 2 <= potential_row <= len(data):  # Проверяем что строка существует (начиная с 2, т.к. 1 - заголовки)
                        lesson_row = potential_row
                        logging.info(f"Используем lesson_id {lesson_id} как номер строки")
                    else:
                        logging.error(f"Номер строки {lesson_id} вне диапазона (2-{len(data)})")
            
            if not lesson_row:
                logging.error(f"Занятие с ID {lesson_id} не найдено")
                return False
            
            # Обновляем столбец G (отметка)
            calendar_sheet.update_cell(lesson_row, 7, mark)
            logging.info(f"✅ Обновлена отметка для занятия (строка {lesson_row}, столбец G): {mark}")
            
            # Обновляем статус посещения в столбце E
            status_map = {
                'посещение': 'Завершен',
                'пропуск (по вине)': 'Пропуск',
                'отмена (болезнь)': 'Пропуск',
                'перенос': 'Пропуск'
            }
            new_status = status_map.get(mark.lower(), 'Запланировано')
            calendar_sheet.update_cell(lesson_row, 5, new_status)
            logging.info(f"✅ Обновлен статус для занятия (строка {lesson_row}, столбец E): {new_status}")
            
            # Получаем данные занятия для проверки типа абонемента
            lesson_data_row = data[lesson_row - 1]  # -1 потому что lesson_row начинается с 1
            subscription_id = lesson_data_row[1]  # B:B - ID абонемента
            child_name = lesson_data_row[5]  # F:F - Ребенок
            
            # Проверяем тип абонемента
            subscription_info = self.get_subscription_details(subscription_id)
            if subscription_info:
                subscription_type = subscription_info.get('subscription_type', '').lower()
                
                # Для разовых абонементов показываем выбор переноса ТОЛЬКО для трех конкретных отметок
                if subscription_type == 'разовый':
                    # Проверяем, нужен ли выбор переноса для этой отметки
                    transfer_marks = ['пропуск (по вине)', 'отмена (болезнь)', 'перенос']
                    
                    if mark.lower() in transfer_marks:
                        logging.info(f"🎯 Разовый абонемент {subscription_id} - показываем выбор переноса для отметки '{mark}'")
                        
                        # Обновляем статистику пропущенных занятий
                        self._update_razoviy_missed_classes_stats(subscription_id)
                        
                        # Возвращаем специальный статус для выбора переноса
                        return {
                            'status': 'needs_transfer_choice',
                            'subscription_id': subscription_id,
                            'child_name': child_name,
                            'lesson_data': {
                                'lesson_id': lesson_id,
                                'date': lesson_data_row[2],  # C:C - Дата занятия
                                'start_time': lesson_data_row[3],  # D:D - Время начала
                                'end_time': lesson_data_row[7],  # H:H - Время завершения
                                'mark': mark
                            }
                        }
                    else:
                        # Для отметки "Посещение" просто обновляем без выбора переноса
                        logging.info(f"🎯 Разовый абонемент {subscription_id} - отметка '{mark}' без выбора переноса")
            
            # Проверяем, нужно ли создать новое занятие при отмене или переносе (для обычных абонементов)
            # ВАЖНО: "Пропуск (по вине)" НЕ создает замещающее занятие!
            if mark.lower() in ['отмена (болезнь)', 'отмена (другое)', 'перенос']:
                # Проверяем тип абонемента (если не разовый)
                if subscription_info and subscription_info.get('subscription_type', '').lower() != 'разовый':
                    logging.info(f"Обнаружена отмена/перенос занятия {lesson_id}, создаю замещающее занятие...")
                    
                    # Создаем новое занятие взамен отмененного
                    replacement_created = self._create_replacement_lesson(subscription_id, child_name)
                    if replacement_created:
                        logging.info(f"✅ Создано замещающее занятие для абонемента {subscription_id}")
                    else:
                        logging.warning(f"⚠️ Не удалось создать замещающее занятие для абонемента {subscription_id}")
                elif not subscription_info:
                    logging.warning(f"⚠️ Не удалось получить информацию об абонементе {subscription_id}")
            elif mark.lower() == 'пропуск (по вине)':
                # Для "Пропуск (по вине)" только обновляем статистику без создания замещающего занятия
                logging.info(f"🚫 Пропуск (по вине) для занятия {lesson_id} - замещающее занятие НЕ создается")
            
            # Столбец I (Осталось занятий) будет автоматически пересчитан в update_subscription_stats()
            # на основе количества занятий со статусом "Запланировано"
            
            # Синхронизация календаря будет выполнена в фоновом процессе
            logging.info("✅ Отметка сохранена, столбцы H/I/M будут обновлены в update_subscription_stats()")
            
            # Очищаем кеш календаря занятий после изменения
            self._clear_cache('calendar_lessons')
            logging.debug("🗑️ Кеш календаря очищен после обновления отметки")
            
            # Возвращаем subscription_id для обновления статистики
            return {'success': True, 'subscription_id': subscription_id}
        except Exception as e:
            import httpx
            error_msg = str(e)
            
            # Обработка специфичных сетевых ошибок
            if isinstance(e, httpx.ReadError) or "httpx.ReadError" in error_msg:
                logging.error(f"🌐 Сетевая ошибка при обновлении отметки занятия {lesson_id}: {e}")
                return False
            elif "429" in error_msg or "Quota exceeded" in error_msg:
                logging.error(f"📊 Превышена квота API при обновлении отметки занятия {lesson_id}: {e}")
                return False
            elif "timeout" in error_msg.lower():
                logging.error(f"⏰ Таймаут при обновлении отметки занятия {lesson_id}: {e}")
                return False
            else:
                logging.error(f"❌ Ошибка при обновлении отметки занятия {lesson_id}: {e}")
                return False

    def _update_remaining_lessons(self, subscription_id, change):
        """Обновляет количество оставшихся занятий в столбце I листа 'Абонементы'."""
        try:
            logging.info(f"🔍 Обновление столбца I для {subscription_id}, изменение: {change}")
            
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            cell = subs_sheet.find(str(subscription_id))
            
            if cell:
                headers = subs_sheet.row_values(1)
                row_values = subs_sheet.row_values(cell.row)
                
                if 'Осталось занятий' in headers:
                    remaining_col = headers.index('Осталось занятий') + 1
                    
                    # Получаем текущее значение
                    current_remaining = 0
                    if len(row_values) > remaining_col - 1:
                        try:
                            current_remaining = int(row_values[remaining_col - 1]) if row_values[remaining_col - 1] else 0
                        except ValueError:
                            current_remaining = 0
                    
                    # Применяем изменение
                    new_remaining = max(0, current_remaining + change)  # Не даем уйти в минус
                    
                    # Обновляем значение
                    subs_sheet.update_cell(cell.row, remaining_col, new_remaining)
                    
                    # Обновляем статус на основе оставшихся занятий
                    if 'Статус' in headers:
                        status_col = headers.index('Статус') + 1
                        if new_remaining <= 0:
                            subs_sheet.update_cell(cell.row, status_col, 'Завершен')
                        else:
                            subs_sheet.update_cell(cell.row, status_col, 'Активен')
                    
                    logging.info(f"📊 Обновлено 'Осталось занятий' для {subscription_id}: {current_remaining} → {new_remaining}")
                    return True
                else:
                    logging.error(f"Столбец 'Осталось занятий' не найден в листе 'Абонементы'")
                    return False
            else:
                logging.error(f"Абонемент {subscription_id} не найден в листе 'Абонементы'")
                return False
                
        except Exception as e:
            logging.error(f"Ошибка при обновлении оставшихся занятий для {subscription_id}: {e}")
            return False

    def refresh_all_subscriptions_data(self):
        """Полное обновление всех данных после изменения столбца I (Осталось занятий)."""
        try:
            logging.info("🔄 Запуск полного обновления данных абонементов...")
            
            # 1. Получаем все активные абонементы
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            subs_data = subs_sheet.get_all_records()
            
            updated_count = 0
            
            for sub in subs_data:
                subscription_id = sub.get('ID абонемента', '')
                remaining_lessons = sub.get('Осталось занятий', 0)
                
                if subscription_id and remaining_lessons:
                    try:
                        remaining_int = int(remaining_lessons)
                        
                        # Обновляем статус на основе оставшихся занятий
                        if remaining_int > 0:
                            # 2. Создаем занятия в календаре на основе шаблона
                            self._create_lessons_from_template(subscription_id, remaining_int)
                            
                            # 3. Создаем записи в прогнозе
                            self._create_forecast_entries(subscription_id)
                            
                            updated_count += 1
                            logging.info(f"✅ Обновлен абонемент {subscription_id} ({remaining_int} занятий)")
                        else:
                            logging.info(f"⏭️ Пропущен завершенный абонемент {subscription_id}")
                            
                    except ValueError:
                        logging.warning(f"⚠️ Некорректное значение 'Осталось занятий' для {subscription_id}: {remaining_lessons}")
                        continue
            
            logging.info(f"🎯 Обновлено {updated_count} абонементов")
            return f"✅ Обновлено {updated_count} абонементов"
            
        except Exception as e:
            logging.error(f"❌ Ошибка при полном обновлении данных: {e}")
            return f"❌ Ошибка: {str(e)}"

    def _create_lessons_from_template(self, subscription_id, lessons_count):
        """Создает занятия в календаре на основе шаблона расписания."""
        try:
            # Получаем информацию об абонементе
            sub_info = self.get_subscription_details(subscription_id)
            if not sub_info:
                logging.error(f"Не найдена информация об абонементе {subscription_id}")
                return False
            
            # Получаем шаблон расписания
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            template_data = template_sheet.get_all_records()
            
            # Ищем шаблон для данного абонемента
            template_row = None
            for row in template_data:
                if str(row.get('ID абонемента', '')).strip() == str(subscription_id).strip():
                    template_row = row
                    break
            
            if not template_row:
                logging.error(f"Не найден шаблон расписания для {subscription_id}")
                return False
            
            # Создаем занятия на основе шаблона
            from datetime import datetime, timedelta
            
            # Получаем расписание из шаблона
            schedule = {}
            days_map = {
                'Понедельник': 0, 'Вторник': 1, 'Среда': 2, 'Четверг': 3,
                'Пятница': 4, 'Суббота': 5, 'Воскресенье': 6
            }
            
            for day, day_num in days_map.items():
                if template_row.get(day):
                    time_range = template_row.get(day, '').strip()
                    if time_range and '-' in time_range:
                        start_time, end_time = time_range.split('-')
                        schedule[day_num] = {
                            'start_time': start_time.strip(),
                            'end_time': end_time.strip()
                        }
            
            if not schedule:
                logging.error(f"Не найдено расписание в шаблоне для {subscription_id}")
                return False
            
            # Создаем занятия начиная с ближайшей даты
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            
            # Получаем максимальный существующий ID для генерации уникальных ID
            max_id = self._get_next_unique_lesson_id(calendar_sheet) - 1
            logging.info(f"🔢 Максимальный существующий ID для разовых занятий: {max_id}")
            
            current_date = datetime.now().date()
            created_lessons = 0
            
            while created_lessons < lessons_count:
                weekday = current_date.weekday()
                
                if weekday in schedule:
                    # Создаем занятие на эту дату с уникальным ID
                    unique_id = max_id + created_lessons + 1
                    lesson_data = [
                        str(unique_id),  # A: № (уникальный ID)
                        subscription_id,  # B: ID абонемента
                        current_date.strftime('%d.%m.%Y'),  # C: Дата занятия
                        schedule[weekday]['start_time'],  # D: Время начала
                        'Запланировано',  # E: Статус посещения
                        sub_info.get('child_name', ''),  # F: Ребенок
                        '',  # G: Отметка
                        schedule[weekday]['end_time'],  # H: Время завершения
                        sub_info.get('circle_name', ''),  # I: Кружок
                    ]
                    
                    calendar_sheet.append_row(lesson_data)
                    created_lessons += 1
                    logging.info(f"📅 Создано занятие {subscription_id} на {current_date} с ID {unique_id}")
                
                current_date += timedelta(days=1)
                
                # Защита от бесконечного цикла
                if (current_date - datetime.now().date()).days > 365:
                    logging.warning(f"Превышен лимит поиска дат для {subscription_id}")
                    break
            
            logging.info(f"✅ Создано {created_lessons} занятий для {subscription_id}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при создании занятий из шаблона для {subscription_id}: {e}")
            return False

    def _create_forecast_entries(self, subscription_id):
        """Создает записи в листе Прогноз для абонемента."""
        try:
            # Получаем информацию об абонементе
            sub_info = self.get_subscription_details(subscription_id)
            if not sub_info:
                return False
            
            # Получаем данные о занятиях из календаря
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            calendar_data = calendar_sheet.get_all_records()
            
            # Фильтруем занятия по ID абонемента
            subscription_lessons = [
                lesson for lesson in calendar_data 
                if str(lesson.get('ID абонемента', '')).strip() == str(subscription_id).strip()
            ]
            
            if not subscription_lessons:
                logging.info(f"Нет занятий в календаре для {subscription_id}")
                return True
            
            # Группируем занятия по месяцам для создания записей в прогнозе
            from datetime import datetime
            from collections import defaultdict
            
            monthly_groups = defaultdict(list)
            
            for lesson in subscription_lessons:
                lesson_date_str = lesson.get('Дата занятия', '')
                if lesson_date_str:
                    try:
                        lesson_date = datetime.strptime(lesson_date_str, '%d.%m.%Y')
                        month_key = lesson_date.strftime('%Y-%m')
                        monthly_groups[month_key].append(lesson)
                    except ValueError:
                        continue
            
            # Создаем записи в прогнозе для каждого месяца
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            
            for month_key, lessons in monthly_groups.items():
                # Вычисляем дату оплаты (первое число месяца)
                year, month = month_key.split('-')
                payment_date = f"01.{month}.{year}"
                
                # Вычисляем бюджет (количество занятий * стоимость)
                lessons_count = len(lessons)
                cost_per_lesson = sub_info.get('cost', 0)
                try:
                    cost_per_lesson = float(cost_per_lesson) if cost_per_lesson else 0
                except ValueError:
                    cost_per_lesson = 0
                
                total_budget = lessons_count * cost_per_lesson
                
                # Создаем запись в прогнозе
                forecast_data = [
                    sub_info.get('circle_name', ''),  # A: Кружок
                    sub_info.get('child_name', ''),   # B: Ребенок
                    payment_date,                     # C: Дата оплаты
                    total_budget,                     # D: Бюджет
                    'Оплата запланирована',           # E: Статус
                ]
                
                forecast_sheet.append_row(forecast_data)
                logging.info(f"💰 Создана запись прогноза для {subscription_id}: {month_key}, {total_budget} руб.")
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при создании записей прогноза для {subscription_id}: {e}")
            return False

    def _create_replacement_lesson(self, subscription_id, child_name):
        """Создает замещающее занятие при отмене, основываясь на шаблоне расписания."""
        try:
            from datetime import datetime, timedelta
            
            # Получаем шаблон расписания для абонемента
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            template_data = template_sheet.get_all_values()
            
            # Ищем шаблон для данного абонемента
            schedule_template = []
            for row in template_data[1:]:  # Пропускаем заголовки
                if len(row) >= 5 and str(row[1]).strip() == subscription_id:  # B:B - ID абонемента
                    try:
                        day_of_week = int(row[2]) - 1  # C:C - День недели (конвертируем в Python формат 0-6)
                        start_time = str(row[3]).strip()  # D:D - Время начала
                        end_time = str(row[4]).strip()  # E:E - Время завершения
                        
                        schedule_template.append({
                            'day': day_of_week % 7,  # Обеспечиваем корректный диапазон 0-6
                            'start_time': start_time,
                            'end_time': end_time
                        })
                    except (ValueError, IndexError):
                        continue
            
            if not schedule_template:
                logging.warning(f"Шаблон расписания для абонемента {subscription_id} не найден")
                return False
            
            # Получаем календарь занятий для определения последней даты
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            calendar_data = calendar_sheet.get_all_values()
            
            # Находим последнее занятие этого абонемента
            last_lesson_date = None
            for row in calendar_data[1:]:  # Пропускаем заголовки
                if len(row) >= 6 and str(row[1]).strip() == subscription_id:  # B:B - ID абонемента
                    try:
                        lesson_date_str = str(row[2]).strip()  # C:C - Дата занятия
                        lesson_date = datetime.strptime(lesson_date_str, '%d.%m.%Y')
                        if last_lesson_date is None or lesson_date > last_lesson_date:
                            last_lesson_date = lesson_date
                    except ValueError:
                        continue
            
            # Если нет предыдущих занятий, начинаем с сегодняшней даты
            if last_lesson_date is None:
                start_date = datetime.now()
            else:
                start_date = last_lesson_date + timedelta(days=1)
            
            # Ищем следующую подходящую дату по шаблону
            current_date = start_date
            max_attempts = 60  # Ищем не более 60 дней вперед
            attempts = 0
            
            while attempts < max_attempts:
                day_of_week = current_date.weekday()
                
                # Проверяем, есть ли занятие в этот день недели по шаблону
                for template_item in schedule_template:
                    if day_of_week == template_item['day']:
                        # Создаем новое занятие
                        return self._add_lesson_to_calendar(
                            subscription_id,
                            child_name,
                            current_date,
                            template_item['start_time'],
                            template_item['end_time']
                        )
                
                current_date += timedelta(days=1)
                attempts += 1
            
            logging.warning(f"Не удалось найти подходящую дату для замещающего занятия абонемента {subscription_id}")
            return False
            
        except Exception as e:
            logging.error(f"❌ Ошибка при создании замещающего занятия: {e}")
            return False

    def _get_next_unique_lesson_id(self, calendar_sheet=None):
        """Получает следующий уникальный ID для занятия."""
        try:
            if calendar_sheet is None:
                calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            
            all_data = calendar_sheet.get_all_values()
            
            # Находим максимальный существующий ID в столбце A
            max_id = 0
            for i, row in enumerate(all_data):
                if i == 0:  # Пропускаем заголовки
                    continue
                if len(row) > 0 and row[0]:  # Проверяем что есть значение в столбце A
                    try:
                        current_id = int(str(row[0]).strip())
                        if current_id > max_id:
                            max_id = current_id
                    except (ValueError, TypeError):
                        continue  # Пропускаем нечисловые значения
            
            # Следующий ID = максимальный + 1
            next_id = max_id + 1
            logging.debug(f"🔢 Генерируем уникальный ID: максимальный существующий = {max_id}, новый = {next_id}")
            return next_id
            
        except Exception as e:
            logging.error(f"Ошибка при получении уникального ID: {e}")
            return 1  # Fallback ID

    def _add_lesson_to_calendar(self, subscription_id, child_name, lesson_date, start_time, end_time):
        """Добавляет новое занятие в календарь занятий."""
        try:
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            
            # Получаем следующий уникальный ID для занятия
            next_id = self._get_next_unique_lesson_id(calendar_sheet)
            logging.info(f"🔢 Создаем замещающее занятие с уникальным ID: {next_id}")
            
            # Получаем информацию об абонементе для определения кружка
            subscription_info = self.get_subscription_details(subscription_id)
            circle_name = subscription_info.get('circle_name', '') if subscription_info else ''
            
            # Формируем новую строку занятия
            new_lesson = [
                str(next_id),  # A:A - № (ID занятия)
                subscription_id,  # B:B - ID абонемента
                lesson_date.strftime('%d.%m.%Y'),  # C:C - Дата занятия
                self.format_time(start_time),  # D:D - Время начала
                'Запланировано',  # E:E - Статус посещения
                child_name,  # F:F - Ребенок
                '',  # G:G - Отметка (пустая для нового занятия)
                self.format_time(end_time)  # H:H - Время завершения
            ]
            
            # Добавляем строку в календарь
            calendar_sheet.append_row(new_lesson)
            
            logging.info(f"✅ Добавлено замещающее занятие: ID={next_id}, Дата={lesson_date.strftime('%d.%m.%Y')}, Ребенок={child_name}")
            
            # Создаем событие в Google Calendar, если сервис доступен
            if self.calendar_service:
                try:
                    # Формируем данные для создания события в формате, который ожидает create_event
                    lesson_data = {
                        'lesson_id': str(next_id),  # Добавляем ID занятия
                        'subscription_id': subscription_id,  # Добавляем ID абонемента
                        'child': child_name,
                        'date': lesson_date.strftime('%d.%m.%Y'),
                        'start_time': start_time,
                        'end_time': end_time,
                        'status': 'Запланировано',  # Статус для нового занятия
                        'mark': ''  # Пустая отметка для нового занятия
                    }
                    
                    # Сначала проверяем, нет ли уже события для этого занятия
                    existing_event = self.calendar_service.find_event_by_lesson_id(str(next_id))
                    if not existing_event:
                        existing_event = self.calendar_service.find_event_by_lesson_details(lesson_data, circle_name)
                    
                    if existing_event:
                        logging.info(f"⚠️ Событие для замещающего занятия уже существует, пропускаю создание")
                    else:
                        event_created = self.calendar_service.create_event(lesson_data, circle_name)
                        if event_created:
                            logging.info(f"✅ Создано событие в Google Calendar для замещающего занятия с ID {next_id}")
                except Exception as calendar_error:
                    logging.warning(f"⚠️ Не удалось создать событие в Google Calendar: {calendar_error}")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Ошибка при добавлении занятия в календарь: {e}")
            return False

    def _update_razoviy_missed_classes_stats(self, subscription_id):
        """Обновляет статистику пропущенных занятий для разового абонемента."""
        try:
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            data = subs_sheet.get_all_values()
            
            # Находим строку с нужным абонементом
            for i, row in enumerate(data[1:], 2):  # Начинаем с 2-й строки
                if len(row) > 1 and str(row[1]).strip() == str(subscription_id).strip():  # B:B - ID абонемента
                    # Получаем текущее значение пропущенных занятий (столбец M = индекс 12)
                    current_missed = int(row[12]) if len(row) > 12 and row[12] and str(row[12]).isdigit() else 0
                    new_missed = current_missed + 1
                    
                    # Обновляем столбец M (пропущенные занятия)
                    subs_sheet.update_cell(i, 13, new_missed)  # 13 = столбец M
                    logging.info(f"✅ Обновлена статистика разового абонемента {subscription_id}: пропущено {new_missed}")
                    return True
            
            logging.warning(f"⚠️ Абонемент {subscription_id} не найден для обновления статистики")
            return False
            
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении статистики разового абонемента {subscription_id}: {e}")
            return False

    def create_razoviy_replacement_lesson(self, subscription_id, child_name, selected_date, original_lesson_data):
        """Создает замещающее занятие для разового абонемента на выбранную дату."""
        try:
            from datetime import datetime
            
            # Парсим выбранную дату
            if isinstance(selected_date, str):
                lesson_date = datetime.strptime(selected_date, '%d.%m.%Y')
            else:
                lesson_date = selected_date
            
            # Получаем информацию об абонементе для определения кружка
            subscription_info = self.get_subscription_details(subscription_id)
            circle_name = subscription_info.get('circle_name', '') if subscription_info else ''
            
            # Используем время из оригинального занятия
            start_time = original_lesson_data.get('start_time', '10:00')
            end_time = original_lesson_data.get('end_time', '10:30')
            
            # Создаем новое занятие
            return self._add_lesson_to_calendar(
                subscription_id,
                child_name,
                lesson_date,
                start_time,
                end_time
            )
            
        except Exception as e:
            logging.error(f"❌ Ошибка при создании замещающего занятия для разового абонемента: {e}")
            return False

    def get_subscription_details(self, subscription_id):
        """Получает детальную информацию об абонементе."""
        try:
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            data = subs_sheet.get_all_records()
            
            for sub in data:
                if str(sub.get('ID абонемента', '')).strip() == str(subscription_id).strip():
                    return {
                        'child_name': sub.get('Ребенок', ''),
                        'circle_name': sub.get('Кружок', ''),
                        'start_date': sub.get('Дата начала', ''),
                        'end_date_forecast': sub.get('Дата окончания прогноз', ''),
                        'total_classes': sub.get('К-во занятий', ''),
                        'attended_classes': sub.get('Прошло занятий', ''),
                        'remaining_classes': sub.get('Осталось занятий', ''),
                        'missed_classes': sub.get('Пропущено', ''),
                        'cost': sub.get('Стоимость', ''),
                        'subscription_type': sub.get('Тип абонемента', '')
                    }
            return None
        except Exception as e:
            logging.error(f"Ошибка при получении данных абонемента {subscription_id}: {e}")
            return None

    def get_forecast_payment_dates(self, child_name, circle_name):
        """Получает прогнозные даты оплат для ребенка и кружка."""
        try:
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            data = forecast_sheet.get_all_records()
            
            payment_dates = []
            for row in data:
                if (str(row.get('Ребенок', '')).strip() == str(child_name).strip() and 
                    str(row.get('Кружок', '')).strip() == str(circle_name).strip()):
                    payment_dates.append(row.get('Дата оплаты', ''))
            
            return payment_dates
        except Exception as e:
            logging.error(f"Ошибка при получении прогнозных дат для {child_name} - {circle_name}: {e}")
            return []

    def get_lesson_info_by_id(self, lesson_id):
        """Получает информацию о занятии по ID."""
        try:
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            data = calendar_sheet.get_all_records()
            
            for lesson in data:
                if str(lesson.get('№', '')).strip() == str(lesson_id).strip():
                    return lesson
            
            return None
        except Exception as e:
            logging.error(f"Ошибка при получении информации о занятии {lesson_id}: {e}")
            return None

    def get_lessons_by_subscription_with_marks(self, subscription_id):
        """Получает все занятия по абонементу с отметками."""
        try:
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            data = calendar_sheet.get_all_records()
            
            lessons = []
            for lesson in data:
                if lesson.get('ID абонемента', '') == subscription_id:
                    lessons.append(lesson)
            
            # Сортируем по дате
            lessons.sort(key=lambda x: x.get('Дата занятия', ''))
            return lessons
        except Exception as e:
            logging.error(f"Ошибка при получении занятий абонемента {subscription_id}: {e}")
            return []

    def get_forecast_budget_for_child_circle(self, child_name, circle_name):
        """Получает общий прогнозируемый бюджет для ребенка и кружка."""
        try:
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            data = forecast_sheet.get_all_records()
            
            total_budget = 0
            for row in data:
                if (row.get('Ребенок', '') == child_name and 
                    row.get('Кружок', '') == circle_name):
                    budget = row.get('Бюджет', 0)
                    if isinstance(budget, (int, float)):
                        total_budget += budget
                    elif isinstance(budget, str) and budget.replace('.', '').isdigit():
                        total_budget += float(budget)
            
            return total_budget if total_budget > 0 else None
        except Exception as e:
            logging.error(f"Ошибка при получении прогнозируемого бюджета: {e}")
            return None

    def get_handbook_items(self, header_name):
        """Получает список уникальных значений из столбца в 'Справочнике'."""
        try:
            worksheet = self.spreadsheet.worksheet("Справочник")
            headers = worksheet.row_values(1)
            logging.info(f"Заголовки в Справочнике: {headers}")
            
            if header_name not in headers:
                logging.error(f"Header '{header_name}' not found in 'Справочник'. Available headers: {headers}")
                return []
            
            col_index = headers.index(header_name) + 1
            values = worksheet.col_values(col_index)[1:] 
            logging.info(f"Значения из столбца '{header_name}': {values}")
            
            filtered_values = sorted(list(set(filter(None, values))))
            logging.info(f"Отфильтрованные значения: {filtered_values}")
            return filtered_values
        except Exception as e:
            logging.error(f"Ошибка при получении данных из Справочника: {e}")
            return []

    def add_handbook_item(self, header_name, value):
        try:
            worksheet = self.spreadsheet.worksheet("Справочник")
            headers = worksheet.row_values(1)
            if header_name not in headers:
                return False, f"Столбец '{header_name}' не найден."
            
            col_index = headers.index(header_name) + 1
            all_values = worksheet.col_values(col_index)
            first_empty_row = len(all_values) + 1
            worksheet.update_cell(first_empty_row, col_index, value)
            return True, f"Значение '{value}' успешно добавлено."
        except Exception as e:
            return False, f"Ошибка при добавлении: {e}"

    def edit_handbook_item(self, header_name, old_value, new_value):
        try:
            worksheet = self.spreadsheet.worksheet("Справочник")
            cell = worksheet.find(old_value)
            if not cell:
                return False, f"Значение '{old_value}' не найдено."
            worksheet.update_cell(cell.row, cell.col, new_value)
            return True, f"'{old_value}' успешно изменено на '{new_value}'."
        except Exception as e:
            return False, f"Ошибка при редактировании: {e}"

    def delete_handbook_item(self, header_name, value):
        try:
            worksheet = self.spreadsheet.worksheet("Справочник")
            cell = worksheet.find(value)
            if not cell:
                return False, f"Значение '{value}' не найдено."
            worksheet.update_cell(cell.row, cell.col, "")
            return True, f"Значение '{value}' успешно удалено."
        except Exception as e:
            return False, f"Ошибка при удалении: {e}"

    def get_children_list(self):
        """Получает список детей из справочника."""
        return self.get_handbook_items("Ребенок")

    def get_circles_list(self):
        """Получает список кружков из справочника."""
        return self.get_handbook_items("Название кружка")

    def get_subscription_types(self):
        """Получает список типов абонементов из справочника."""
        return self.get_handbook_items('Тип абонемента')
    
    def get_payment_types(self):
        """Получает список типов оплаты из справочника."""
        return self.get_handbook_items('Оплата')
    
    def update_subscription_stats(self, subscription_id):
        """Обновляет статистику абонемента на основе данных из календаря с проверкой сходимости."""
        try:
            logging.info(f"🔄 Обновление статистики для абонемента {subscription_id}")
            
            # Используем новую функцию проверки и исправления данных
            result = self.validate_subscription_data_consistency(subscription_id)
            logging.info(f"📊 Результат проверки сходимости: {result}")
            
            return result
                
        except Exception as e:
            logging.error(f"Ошибка при обновлении статистики для {subscription_id}: {e}")
            return f"❌ Ошибка: {e}"

    def get_planned_payments(self):
        """Получает все запланированные оплаты из листа 'Прогноз' со статусом 'Оплата запланирована'."""
        try:
            logging.info("📊 Получение запланированных оплат...")
            
            logging.info("📋 Подключение к листу 'Прогноз'...")
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            
            logging.info("📋 Загрузка данных из листа...")
            all_data = forecast_sheet.get_all_values()
            logging.info(f"📋 Загружено {len(all_data)} строк данных")
            
            if len(all_data) <= 1:
                logging.info("Нет данных в листе 'Прогноз'")
                return []
            
            headers = all_data[0]
            planned_payments = []
            
            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) >= 4:  # Нужно минимум 4 столбца (A, B, C, D)
                    circle_name = str(row[0]).strip()      # A: Кружок
                    child_name = str(row[1]).strip()       # B: Ребенок
                    payment_date = str(row[2]).strip()     # C: Дата оплаты
                    budget = str(row[3]).strip()           # D: Бюджет
                    status = str(row[4]).strip() if len(row) >= 5 else ''  # E: Статус (опционально)
                    
                    # Убрана фильтрация по статусу - считаем ВСЕ строки с датами
                    if (circle_name and child_name and payment_date and budget):
                        try:
                            # Очищаем строку от неразрывных пробелов и других символов
                            budget_clean = budget.replace('\xa0', '').replace(' ', '').replace(',', '.').strip()
                            # Проверяем что budget это число
                            budget_value = float(budget_clean) if budget_clean else 0
                            if budget_value > 0:
                                planned_payments.append({
                                    'row_index': row_index,
                                    'circle_name': circle_name,
                                    'child_name': child_name,
                                    'payment_date': payment_date,
                                    'budget': budget_clean,  # Сохраняем очищенную строку
                                    'status': status,
                                    'key': f"{child_name}|{circle_name}"
                                })
                        except (ValueError, AttributeError):
                            continue  # Пропускаем строки с некорректным бюджетом
            
            logging.info(f"Найдено {len(planned_payments)} запланированных оплат")
            return planned_payments
            
        except Exception as e:
            logging.error(f"Ошибка при получении запланированных оплат: {e}")
            return []

    def get_paid_payments(self):
        """Получает все оплаченные платежи из листа 'Оплачено' со статусом 'Оплачено'."""
        try:
            logging.info("📊 Получение оплаченных платежей...")
            
            paid_sheet = self.spreadsheet.worksheet("Оплачено")
            all_data = paid_sheet.get_all_values()
            
            if len(all_data) <= 1:
                logging.info("Нет данных в листе 'Оплачено'")
                return []
            
            headers = all_data[0]
            paid_payments = []
            
            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) >= 4:  # Нужно минимум 4 столбца (A, B, C, D)
                    circle_name = str(row[0]).strip()      # A: Кружок
                    child_name = str(row[1]).strip()       # B: Ребенок
                    payment_date = str(row[2]).strip()     # C: Дата оплаты
                    amount = str(row[3]).strip()           # D: Бюджет/Сумма
                    status = str(row[4]).strip() if len(row) >= 5 else ''  # E: Статус (опционально)
                    
                    # Убрана фильтрация по статусу - считаем ВСЕ строки с датами
                    if (circle_name and child_name and payment_date and amount):
                        try:
                            # Очищаем строку от неразрывных пробелов и других символов
                            amount_clean = amount.replace('\xa0', '').replace(' ', '').replace(',', '.').strip()
                            # Проверяем что amount это число
                            amount_value = float(amount_clean) if amount_clean else 0
                            if amount_value > 0:
                                paid_payments.append({
                                    'row_index': row_index,
                                    'circle_name': circle_name,
                                    'child_name': child_name,
                                    'payment_date': payment_date,
                                    'amount': amount_clean,  # Сохраняем очищенную строку
                                    'status': status,
                                    'key': f"{child_name}|{circle_name}"
                                })
                        except (ValueError, AttributeError):
                            continue  # Пропускаем строки с некорректной суммой
            
            logging.info(f"Найдено {len(paid_payments)} оплаченных платежей")
            return paid_payments
            
        except Exception as e:
            logging.error(f"Ошибка при получении оплаченных платежей: {e}")
            return []

    def get_budget_forecast_by_weeks(self):
        """Получает прогноз бюджета по месяцам и неделям с данными из листов 'Прогноз' и 'Оплачено'."""
        try:
            from datetime import datetime, timedelta
            import calendar
            
            logging.info("📊 Получение прогноза бюджета по неделям...")
            
            # Получаем данные из обоих листов
            planned_payments = self.get_planned_payments()
            paid_payments = self.get_paid_payments()
            
            # Получаем текущий месяц и следующий
            now = datetime.now()
            current_month = now.month
            current_year = now.year
            
            # Следующий месяц
            if current_month == 12:
                next_month = 1
                next_year = current_year + 1
            else:
                next_month = current_month + 1
                next_year = current_year
            
            months_data = {}
            
            # Русские названия месяцев
            russian_months = {
                1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
                5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
                9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
            }
            
            # Обрабатываем оба месяца
            for month, year in [(current_month, current_year), (next_month, next_year)]:
                month_name = russian_months[month]
                month_key = f"{year}-{month:02d}"
                
                months_data[month_key] = {
                    'name': f"{month_name} {year}",
                    'weeks': {},
                    'total_planned': 0,
                    'total_paid': 0
                }
                
                # Получаем все дни месяца
                _, days_in_month = calendar.monthrange(year, month)
                
                # Группируем по неделям
                for day in range(1, days_in_month + 1):
                    date = datetime(year, month, day)
                    week_number = date.isocalendar()[1]  # Номер недели в году
                    week_key = f"week_{week_number}"
                    
                    if week_key not in months_data[month_key]['weeks']:
                        # Определяем диапазон недели
                        week_start = date - timedelta(days=date.weekday())
                        week_end = week_start + timedelta(days=6)
                        
                        months_data[month_key]['weeks'][week_key] = {
                            'number': week_number,
                            'start_date': week_start.strftime('%d.%m'),
                            'end_date': week_end.strftime('%d.%m'),
                            'planned': 0,
                            'paid': 0
                        }
            
            # Обрабатываем запланированные платежи
            for payment in planned_payments:
                try:
                    # Парсим дату (формат может быть разный)
                    payment_date_str = payment['payment_date']
                    payment_date = self._parse_date(payment_date_str)
                    
                    if payment_date:
                        month_key = f"{payment_date.year}-{payment_date.month:02d}"
                        if month_key in months_data:
                            week_number = payment_date.isocalendar()[1]
                            week_key = f"week_{week_number}"
                            
                            budget = float(payment['budget']) if payment['budget'] else 0
                            
                            if week_key in months_data[month_key]['weeks']:
                                months_data[month_key]['weeks'][week_key]['planned'] += budget
                                months_data[month_key]['total_planned'] += budget
                                
                except Exception as e:
                    logging.warning(f"Ошибка при обработке запланированного платежа: {e}")
            
            # Обрабатываем оплаченные платежи
            for payment in paid_payments:
                try:
                    # Парсим дату
                    payment_date_str = payment['payment_date']
                    payment_date = self._parse_date(payment_date_str)
                    
                    if payment_date:
                        month_key = f"{payment_date.year}-{payment_date.month:02d}"
                        if month_key in months_data:
                            week_number = payment_date.isocalendar()[1]
                            week_key = f"week_{week_number}"
                            
                            amount = float(payment['amount']) if payment['amount'] else 0
                            
                            if week_key in months_data[month_key]['weeks']:
                                months_data[month_key]['weeks'][week_key]['paid'] += amount
                                months_data[month_key]['total_paid'] += amount
                                
                except Exception as e:
                    logging.warning(f"Ошибка при обработке оплаченного платежа: {e}")
            
            logging.info(f"✅ Обработано данных по {len(months_data)} месяцам")
            return months_data
            
        except Exception as e:
            logging.error(f"Ошибка при получении прогноза по неделям: {e}")
            # Возвращаем пустые данные, но не ломаем функцию
            if "Connection aborted" in str(e) or "Connection reset" in str(e):
                logging.warning("⚠️ Проблема с подключением к Google Sheets. Возвращаю пустые данные.")
            return {}
    
    def _parse_date(self, date_str):
        """Парсит дату из строки в различных форматах."""
        try:
            # Пробуем различные форматы
            formats = ['%d.%m.%Y', '%d/%m/%Y', '%Y-%m-%d', '%d.%m.%y']
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            logging.warning(f"Не удалось распарсить дату: {date_str}")
            return None
            
        except Exception as e:
            logging.warning(f"Ошибка при парсинге даты '{date_str}': {e}")
            return None

    def mark_payments_as_paid(self, subscription_key):
        """Отмечает все оплаты для указанного абонемента как 'Оплачено'."""
        try:
            logging.info(f"📝 Отмечаю оплаты как оплаченные для {subscription_key}")
            
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            all_data = forecast_sheet.get_all_values()
            
            if len(all_data) <= 1:
                return False, "Нет данных в листе 'Прогноз'"
            
            updated_count = 0
            
            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) >= 5:
                    circle_name = str(row[0]).strip()      # A: Кружок
                    child_name = str(row[1]).strip()       # B: Ребенок
                    payment_date = str(row[2]).strip()     # C: Дата оплаты
                    budget = str(row[3]).strip()           # D: Бюджет
                    status = str(row[4]).strip()           # E: Статус
                    
                    current_key = f"{child_name}|{circle_name}"
                    
                    if (current_key == subscription_key and 
                        status == "Оплата запланирована"):
                        
                        # Обновляем статус на "Оплачено"
                        forecast_sheet.update_cell(row_index, 5, "Оплачено")
                        updated_count += 1
                        logging.info(f"Обновлен статус для {payment_date}: Оплачено")
            
            logging.info(f"Обновлено {updated_count} оплат для {subscription_key}")
            return True, f"Отмечено как оплаченные: {updated_count} платежей"
            
        except Exception as e:
            logging.error(f"Ошибка при отметке оплат как оплаченных: {e}")
            return False, f"Ошибка: {e}"

    def mark_single_payment_as_paid(self, row_index):
        """Отмечает одну конкретную оплату как 'Оплачено' по номеру строки."""
        try:
            logging.info(f"📝 Отмечаю оплату в строке {row_index} как оплаченную")
            
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            
            # Получаем текущее значение статуса
            current_status = forecast_sheet.cell(row_index, 5).value
            
            if current_status == "Оплата запланирована":
                # Обновляем статус на "Оплачено"
                forecast_sheet.update_cell(row_index, 5, "Оплачено")
                
                # Получаем информацию об оплате для логирования
                row_data = forecast_sheet.row_values(row_index)
                if len(row_data) >= 3:
                    child_name = row_data[1]
                    circle_name = row_data[0]
                    payment_date = row_data[2]
                    logging.info(f"Оплата отмечена: {child_name} - {circle_name}, дата {payment_date}")
                
                return True, "Оплата отмечена как оплаченная"
            else:
                return False, f"Оплата уже имеет статус: {current_status}"
            
        except Exception as e:
            logging.error(f"Ошибка при отметке отдельной оплаты: {e}")
            return False, f"Ошибка: {e}"

    def move_payment_to_paid(self, row_index):
        """Перемещает оплату из листа 'Прогноз' в лист 'Оплачено'."""
        try:
            logging.info(f"📝 Перемещаю оплату из строки {row_index} в лист 'Оплачено'")
            
            # Получаем лист "Прогноз"
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            
            # Получаем данные строки
            row_data = forecast_sheet.row_values(row_index)
            if len(row_data) < 4:
                return False, "Недостаточно данных в строке"
            
            circle_name = row_data[0]  # A: Кружок
            child_name = row_data[1]   # B: Ребенок
            payment_date = row_data[2] # C: Дата оплаты
            budget = row_data[3]       # D: Бюджет
            
            # Проверяем, что это запланированная оплата
            current_status = row_data[4] if len(row_data) > 4 else ""
            if current_status != "Оплата запланирована":
                return False, f"Оплата имеет статус '{current_status}', а не 'Оплата запланирована'"
            
            # Получаем или создаем лист "Оплачено"
            try:
                paid_sheet = self.spreadsheet.worksheet("Оплачено")
            except:
                # Создаем лист "Оплачено" если его нет
                paid_sheet = self.spreadsheet.add_worksheet(title="Оплачено", rows=1000, cols=5)
                # Добавляем заголовки
                headers = ["Кружок", "Ребенок", "Дата оплаты", "Бюджет", "Статус"]
                paid_sheet.update('A1:E1', [headers])
                logging.info("Создан лист 'Оплачено' с заголовками")
            
            # Добавляем запись в лист "Оплачено"
            new_row = [circle_name, child_name, payment_date, budget, "Оплачено"]
            paid_sheet.append_row(new_row)
            
            # Удаляем строку из листа "Прогноз"
            forecast_sheet.delete_rows(row_index)
            
            logging.info(f"✅ Оплата перемещена: {child_name} - {circle_name}, дата {payment_date}")
            return True, f"Оплата перемещена в лист 'Оплачено': {child_name} - {circle_name}, {payment_date}"
            
        except Exception as e:
            logging.error(f"Ошибка при перемещении оплаты: {e}")
            return False, f"Ошибка: {e}"

    def forecast_budget(self):
        """Рассчитывает прогноз бюджета на 2 месяца вперед."""
        try:
            # Сначала пытаемся использовать новый лист "Прогноз"
            try:
                forecast_sheet = self.spreadsheet.worksheet("Прогноз")
                return self._get_forecast_from_forecast_sheet(forecast_sheet)
            except:
                # Если листа "Прогноз" нет, используем старую логику
                logging.info("Лист 'Прогноз' не найден, используем старую логику расчета")
                return self._get_forecast_from_calendar()
        
        except Exception as e:
            logging.error(f"Ошибка при получении прогноза бюджета: {e}")
            return None
    
    def _get_forecast_from_forecast_sheet(self, forecast_sheet):
        """Получает прогноз из листа 'Прогноз'."""
        try:
            from datetime import datetime, timedelta
            
            # Получаем все данные как список списков
            all_values = forecast_sheet.get_all_values()
            if len(all_values) < 2:
                return []
            
            # Первая строка - заголовки
            headers = all_values[0]
            
            current_date = datetime.now()
            end_date = current_date + timedelta(days=60)  # 2 месяца
            
            forecast_items = []
            for row in all_values[1:]:
                if len(row) < 4:
                    continue
                
                try:
                    # Структура: ["Кружок", "Ребенок", "Дата", "Стоимость"]
                    circle = row[0] if len(row) > 0 else ''
                    child = row[1] if len(row) > 1 else ''
                    date_str = row[2] if len(row) > 2 else ''
                    cost_str = row[3] if len(row) > 3 else '0'
                    
                    if not date_str:
                        continue
                    
                    record_date = datetime.strptime(date_str, '%d.%m.%Y')
                    if current_date <= record_date <= end_date:
                        try:
                            cost = float(cost_str) if cost_str else 0
                        except ValueError:
                            cost = 0
                        
                        forecast_items.append({
                            'date': date_str,
                            'child': child,
                            'circle': circle,
                            'cost': cost
                        })
                except (ValueError, TypeError, IndexError):
                    continue
            
            return sorted(forecast_items, key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y'))
            
        except Exception as e:
            logging.error(f"Ошибка при получении прогноза из листа 'Прогноз': {e}")
            return []
    
    def _get_forecast_from_calendar(self):
        """Старая логика получения прогноза из календаря занятий."""
        try:
            from datetime import datetime, timedelta
            
            cal_sheet = self.spreadsheet.worksheet("Календарь занятий")
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            
            # Получаем данные календаря
            all_cal_values = cal_sheet.get_all_values()
            if not all_cal_values:
                return []
            
            cal_headers = all_cal_values[0]
            cal_records = []
            for row in all_cal_values[1:]:
                record = dict(zip(cal_headers, row))
                cal_records.append(record)
            
            # Получаем данные абонементов для получения стоимости
            all_subs_values = subs_sheet.get_all_values()
            if not all_subs_values:
                return []
            
            subs_headers = all_subs_values[0]
            subs_records = {}
            for row in all_subs_values[1:]:
                record = dict(zip(subs_headers, row))
                sub_id = record.get('ID абонемента')
                if sub_id:
                    subs_records[sub_id] = record
            
            # Определяем диапазон дат (2 месяца вперед)
            today = datetime.now()
            end_date = today + timedelta(days=60)
            
            forecasts = []
            for record in cal_records:
                try:
                    class_date_str = record.get('Дата', '')
                    if not class_date_str:
                        continue
                        
                    class_date = datetime.strptime(class_date_str, '%d.%m.%Y')
                    
                    # Проверяем, попадает ли дата в диапазон прогноза
                    if today <= class_date <= end_date:
                        sub_id = record.get('ID абонемента', '')
                        if sub_id in subs_records:
                            sub_info = subs_records[sub_id]
                            
                            # Рассчитываем стоимость одного занятия
                            try:
                                total_cost = float(sub_info.get('Стоимость', 0))
                                total_classes = int(sub_info.get('Всего занятий', 1))
                                cost_per_class = total_cost / total_classes if total_classes > 0 else 0
                            except (ValueError, ZeroDivisionError):
                                cost_per_class = 0
                            
                            forecasts.append({
                                'date': class_date_str,
                                'child': record.get('Ребенок', 'N/A'),
                                'circle': sub_info.get('Кружок', 'N/A'),
                                'cost': round(cost_per_class, 2)
                            })
                            
                except ValueError:
                    # Пропускаем записи с некорректными датами
                    continue
            
            # Сортируем по дате
            forecasts.sort(key=lambda x: datetime.strptime(x['date'], '%d.%m.%Y'))
            return forecasts
            
        except Exception as e:
            logging.error(f"Ошибка при расчете прогноза бюджета: {e}")
            return None

    def sync_with_google_calendar_DISABLED(self):
        """Google Calendar синхронизация отключена."""
        logging.info("Google Calendar синхронизация отключена")
        return True

    def _delete_old_sync_events_DISABLED(self, start_date, end_date):
        """Удаляет старые события с тегом #schedule_sync."""
        try:
            time_min = start_date.isoformat() + 'Z'
            time_max = end_date.isoformat() + 'Z'
            
            events_result = self.calendar_service.events().list(
                calendarId=config.GOOGLE_CALENDAR_ID,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            deleted_count = 0
            
            for event in events:
                description = event.get('description', '')
                if '#schedule_sync' in description:
                    self.calendar_service.events().delete(
                        calendarId=config.GOOGLE_CALENDAR_ID,
                        eventId=event['id']
                    ).execute()
                    deleted_count += 1
            
            logging.info(f"Удалено {deleted_count} старых событий")
            
        except HttpError as e:
            if e.resp.status == 403:
                logging.error("Google Calendar API не настроен или отключен. Пропускаем удаление старых событий.")
            else:
                logging.error(f"Ошибка Calendar API при удалении событий: {e}")
        except Exception as e:
            logging.error(f"Ошибка при удалении старых событий: {e}")

    def _get_existing_sync_events(self, start_date, end_date):
        """Получает существующие события с тегом #schedule_sync для сравнения."""
        try:
            time_min = start_date.isoformat() + 'Z'
            time_max = end_date.isoformat() + 'Z'
            
            events_result = self.calendar_service.events().list(
                calendarId=config.GOOGLE_CALENDAR_ID,
                timeMin=time_min,
                timeMax=time_max,
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            sync_events = {}
            all_sync_events = []  # Для поиска дублей
            
            for event in events:
                description = event.get('description', '')
                if '#schedule_sync' in description:
                    all_sync_events.append(event)
                    # Создаем уникальный ключ для события
                    event_key = self._extract_event_key_from_event(event)
                    if event_key:
                        # Если ключ уже существует, сохраняем самое старое событие
                        if event_key not in sync_events:
                            sync_events[event_key] = event
                        else:
                            # Сравниваем даты создания и оставляем более старое
                            existing_created = sync_events[event_key].get('created', '')
                            current_created = event.get('created', '')
                            if current_created < existing_created:
                                sync_events[event_key] = event
            
            logging.info(f"Найдено {len(sync_events)} уникальных событий синхронизации из {len(all_sync_events)} общих")
            
            # Передаем все события для поиска дублей
            sync_events['_all_events'] = all_sync_events
            return sync_events
            
        except HttpError as e:
            if e.resp.status == 403:
                logging.error("Google Calendar API не настроен. Возвращаем пустой список событий.")
                return {}
            else:
                logging.error(f"Ошибка Calendar API при получении событий: {e}")
                return {}
        except Exception as e:
            logging.error(f"Ошибка при получении существующих событий: {e}")
            return {}

    def _remove_duplicate_events(self, existing_events):
        """Находит и удаляет дублированные события в календаре."""
        try:
            duplicates_removed = 0
            event_groups = {}
            
            # Получаем полный список событий
            all_events = existing_events.pop('_all_events', [])
            
            # Группируем все события по ключам
            for event in all_events:
                event_key = self._extract_event_key_from_event(event)
                if event_key:
                    if event_key not in event_groups:
                        event_groups[event_key] = []
                    event_groups[event_key].append(event)
            
            # Ищем группы с дублями (больше одного события с одинаковым ключом)
            for event_key, events in event_groups.items():
                if len(events) > 1:
                    logging.info(f"Найдены дубли для ключа '{event_key}': {len(events)} событий")
                    
                    # Выбираем самое актуальное событие для сохранения
                    # Приоритет: 1) с непустой отметкой, 2) последнее по времени обновления
                    best_event = None
                    best_priority = -1
                    
                    for event in events:
                        description = event.get('description', '')
                        mark = ''
                        
                        # Извлекаем отметку из описания
                        for line in description.split('\n'):
                            if line.startswith('Отметка:'):
                                mark = line.split(':', 1)[1].strip()
                                break
                        
                        # Определяем приоритет события
                        priority = 0
                        if mark and mark not in ['', 'N/A']:
                            priority = 2  # Высокий приоритет для событий с отметкой
                        else:
                            priority = 1  # Низкий приоритет для событий без отметки
                        
                        # Если приоритет выше или равен, проверяем время обновления
                        if priority > best_priority:
                            best_event = event
                            best_priority = priority
                        elif priority == best_priority:
                            # Если приоритет одинаковый, выбираем последнее обновленное
                            if best_event:
                                best_updated = best_event.get('updated', '')
                                current_updated = event.get('updated', '')
                                if current_updated > best_updated:
                                    best_event = event
                            else:
                                best_event = event
                    
                    logging.info(f"✅ Выбрано для сохранения: {best_event.get('summary', 'Без названия')} (приоритет: {best_priority})")
                    
                    # Удаляем все события кроме лучшего
                    events_to_delete = [e for e in events if e['id'] != best_event['id']]
                    
                    for duplicate_event in events_to_delete:
                        try:
                            self.calendar_service.events().delete(
                                calendarId=config.GOOGLE_CALENDAR_ID,
                                eventId=duplicate_event['id']
                            ).execute()
                            
                            event_title = duplicate_event.get('summary', 'Без названия')
                            logging.info(f"Удален дубль события: {event_title}")
                            duplicates_removed += 1
                            
                            # Удаляем из словаря existing_events
                            if event_key in existing_events and existing_events[event_key]['id'] == duplicate_event['id']:
                                # Если это был основной элемент в словаре, заменяем его на лучшее (с отметкой)
                                existing_events[event_key] = best_event
                            
                        except HttpError as e:
                            if e.resp.status == 403:
                                logging.error("Google Calendar API не настроен. Дубль не удален.")
                            elif e.resp.status == 404:
                                logging.warning(f"Событие уже удалено: {duplicate_event.get('id')}")
                            else:
                                logging.error(f"Ошибка Calendar API при удалении дубля: {e}")
                        except Exception as e:
                            logging.error(f"Ошибка при удалении дубля события: {e}")
            
            if duplicates_removed > 0:
                logging.info(f"Удалено {duplicates_removed} дублированных событий")
            else:
                logging.info("Дублированные события не найдены")
            
            return duplicates_removed
            
        except Exception as e:
            logging.error(f"Ошибка при поиске и удалении дублей: {e}")
            return 0

    def _get_forecast_data(self):
        """Получает данные из листа Прогноз."""
        try:
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            return forecast_sheet.get_all_records()
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных прогноза: {e}")
            return []

    def _get_schedule_templates(self):
        """Получает данные из листа Шаблон расписания."""
        try:
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            return template_sheet.get_all_records()
        except Exception as e:
            logging.error(f"Ошибка при загрузке шаблонов расписания: {e}")
            return []

    def _get_subscriptions_data(self):
        """Получает данные из листа Абонементы."""
        try:
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            return subs_sheet.get_all_records()
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных абонементов: {e}")
            return []

    def _create_forecast_map(self, forecast_data):
        """Создает карту прогнозов: ключ 'Ребенок|Кружок|дата', значение - бюджет."""
        forecast_map = {}
        for row in forecast_data:
            child = row.get('Ребенок', '')
            circle = row.get('Кружок', '')
            date = row.get('Дата оплаты', '')
            budget = row.get('Бюджет', 0)
            
            if child and circle and date:
                # Преобразуем дату в формат YYYY-MM-DD
                try:
                    date_obj = datetime.strptime(date, '%d.%m.%Y')
                    date_key = date_obj.strftime('%Y-%m-%d')
                    key = f"{child}|{circle}|{date_key}"
                    forecast_map[key] = budget
                except ValueError:
                    continue
        
        return forecast_map

    def _create_schedule_map(self, schedule_templates):
        """Создает карту шаблонов расписания."""
        schedule_map = {}
        for row in schedule_templates:
            sub_id = row.get('ID абонемента', '')
            if sub_id:
                if sub_id not in schedule_map:
                    schedule_map[sub_id] = []
                schedule_map[sub_id].append({
                    'day': row.get('День недели', ''),
                    'start_time': row.get('Время начала', ''),
                    'end_time': row.get('Время завершения', '')
                })
        
        return schedule_map

    def _create_circle_names_map(self, subscriptions_data):
        """Создает карту названий кружков по ID абонемента."""
        circle_map = {}
        for row in subscriptions_data:
            # Пробуем разные варианты названий полей ID
            sub_id = row.get('ID', '') or row.get('ID абонемента', '') or row.get('№', '')
            circle = row.get('Кружок', '')
            
            if sub_id and circle:
                circle_map[sub_id] = circle
                logging.info(f"Добавлен кружок в карту: {sub_id} -> {circle}")
        
        logging.info(f"Создана карта кружков: {circle_map}")
        return circle_map

    def _create_lesson_event(self, lesson, forecast_map, schedule_map, circle_names_map, processed_payment_dates):
        """Создает событие для занятия."""
        try:
            sub_id = lesson.get('ID абонемента', '')
            child_name = lesson.get('Ребенок', '')
            lesson_date = lesson.get('Дата занятия', '')
            
            # Получаем название кружка
            circle_name = circle_names_map.get(sub_id, 'Неизвестный кружок')
            logging.info(f"Для абонемента {sub_id} найден кружок: {circle_name}")
            
            # Сначала пробуем получить время из самого занятия
            # Пробуем разные варианты названий полей времени
            start_time = str(lesson.get('Время начала', '') or lesson.get('Начало', '') or lesson.get('Start Time', '')).strip()
            end_time = str(lesson.get('Время завершения', '') or lesson.get('Окончание', '') or lesson.get('End Time', '') or lesson.get('Время окончания', '')).strip()
            
            logging.info(f"Поля занятия: {list(lesson.keys())}")
            logging.info(f"Время из занятия: начало='{start_time}', окончание='{end_time}'")
            
            # Если времени нет в занятии, берем из шаблона расписания
            if not start_time or not end_time:
                schedule = schedule_map.get(sub_id, [])
                if schedule:
                    template = schedule[0]
                    if not start_time:
                        start_time = str(template.get('start_time', '10:00')).strip()
                    if not end_time:
                        end_time = str(template.get('end_time', '11:00')).strip()
                else:
                    logging.warning(f"Не найден шаблон расписания для абонемента {sub_id}")
                    if not start_time:
                        start_time = '10:00'
                    if not end_time:
                        end_time = '11:00'
            
            logging.info(f"Время для занятия {sub_id}: {start_time} - {end_time}")
            
            # Преобразуем дату занятия
            try:
                lesson_date_obj = datetime.strptime(lesson_date, '%d.%m.%Y')
            except ValueError:
                logging.error(f"Неверный формат даты: {lesson_date}")
                return False
            
            # Проверяем, есть ли оплата на эту дату
            date_key = lesson_date_obj.strftime('%Y-%m-%d')
            payment_key = f"{child_name}|{circle_name}|{date_key}"
            is_payment_day = payment_key in forecast_map
            
            # Формируем данные события
            if is_payment_day:
                title = f"ОПЛАТА: {circle_name} - {child_name}"
                description = f"Требуется оплата за следующий абонемент: {forecast_map[payment_key]} руб.\n\n#schedule_sync"
                processed_payment_dates.add(payment_key)
            else:
                title = f"{circle_name} - {child_name}"
                description = "#schedule_sync"
            
            # Проверяем и форматируем время
            if not start_time or start_time == '':
                start_time = '10:00'
            if not end_time or end_time == '':
                end_time = '11:00'
            
            # Убираем лишние пробелы и проверяем формат времени
            try:
                # Создаем datetime объекты для начала и конца
                start_datetime = datetime.strptime(f"{lesson_date} {start_time}", '%d.%m.%Y %H:%M')
                end_datetime = datetime.strptime(f"{lesson_date} {end_time}", '%d.%m.%Y %H:%M')
            except ValueError as e:
                logging.error(f"Ошибка парсинга времени для занятия {lesson_date}: start_time='{start_time}', end_time='{end_time}'. Ошибка: {e}")
                # Используем время по умолчанию
                start_datetime = datetime.strptime(f"{lesson_date} 10:00", '%d.%m.%Y %H:%M')
                end_datetime = datetime.strptime(f"{lesson_date} 11:00", '%d.%m.%Y %H:%M')
            
            # Создаем событие
            event = {
                'summary': title,
                'description': description,
                'start': {
                    'dateTime': start_datetime.isoformat(),
                    'timeZone': 'Europe/Moscow',
                },
                'end': {
                    'dateTime': end_datetime.isoformat(),
                    'timeZone': 'Europe/Moscow',
                },
            }
            
            try:
                created_event = self.calendar_service.events().insert(
                    calendarId=config.GOOGLE_CALENDAR_ID,
                    body=event
                ).execute()
                
                logging.info(f"Создано событие: {title} на {lesson_date}")
                return True
            except HttpError as e:
                if e.resp.status == 403:
                    logging.error("Google Calendar API не настроен. Событие не создано.")
                    return False
                else:
                    logging.error(f"Ошибка Calendar API при создании события: {e}")
                    return False
            
        except Exception as e:
            logging.error(f"Ошибка при создании события для занятия: {e}")
            return False

    def _create_future_payment_events(self, forecast_map, processed_payment_dates):
        """Создает события для будущих оплат, которые еще не обработаны."""
        created_count = 0
        
        for payment_key, budget in forecast_map.items():
            if payment_key not in processed_payment_dates:
                try:
                    child, circle, date_str = payment_key.split('|')
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    
                    # Создаем событие на весь день
                    event = {
                        'summary': f"ОПЛАТА: {circle} - {child}",
                        'description': f"Требуется оплата за следующий абонемент: {budget} руб.\n\n#schedule_sync",
                        'start': {
                            'date': date_str,
                        },
                        'end': {
                            'date': date_str,
                        },
                    }
                    
                    try:
                        created_event = self.calendar_service.events().insert(
                            calendarId=config.GOOGLE_CALENDAR_ID,
                            body=event
                        ).execute()
                        
                        logging.info(f"Создано событие оплаты: {circle} - {child} на {date_str}")
                        created_count += 1
                    except HttpError as e:
                        if e.resp.status == 403:
                            logging.error("Google Calendar API не настроен. Событие оплаты не создано.")
                        else:
                            logging.error(f"Ошибка Calendar API при создании события оплаты: {e}")
                    
                except Exception as e:
                    logging.error(f"Ошибка при создании события оплаты {payment_key}: {e}")
        
        return created_count

    def _generate_event_key(self, lesson, circle_names_map):
        """Генерирует уникальный ключ для события на основе ID занятия."""
        lesson_id = lesson.get('№', '')  # ID занятия из столбца A
        
        # Формат ключа: "lesson_id_[ID]"
        return f"lesson_id_{lesson_id}"

    def _extract_event_key_from_event(self, event):
        """Извлекает ключ события из существующего события календаря."""
        try:
            description = event.get('description', '')
            summary = event.get('summary', '')
            
            # Ищем ID занятия в описании события
            if '#schedule_sync' in description:
                # Ищем строку вида "lesson_id:123" в описании
                import re
                lesson_id_match = re.search(r'lesson_id:(\d+)', description)
                if lesson_id_match:
                    lesson_id = lesson_id_match.group(1)
                    return f"lesson_id_{lesson_id}"
                
                # Если это событие оплаты
                if summary.startswith('ОПЛАТА: ') or summary.startswith('💵ОПЛАТА: '):
                    start = event.get('start', {})
                    if 'date' in start:
                        event_date = datetime.strptime(start['date'], '%Y-%m-%d').strftime('%d.%m.%Y')
                        # Убираем эмодзи из названия
                        clean_summary = summary.replace('💵ОПЛАТА: ', '').replace('ОПЛАТА: ', '')
                        parts = clean_summary.split(' - ')
                        if len(parts) == 2:
                            circle_name, child_name = parts
                            return f"payment_{event_date}|{child_name}|{circle_name}"
                
                # Для старых событий без lesson_id - создаем ключ по дате и названию
                start = event.get('start', {})
                if 'dateTime' in start:
                    event_date = datetime.fromisoformat(start['dateTime'].replace('Z', '+00:00')).strftime('%d.%m.%Y')
                    # Убираем эмодзи из названия
                    clean_summary = re.sub(r'^[✔️🔄🤒✖️💵]*', '', summary)
                    parts = clean_summary.split(' - ')
                    if len(parts) == 2:
                        circle_name, child_name = parts
                        return f"legacy_{event_date}|{child_name}|{circle_name}"
            
            return None
        except Exception as e:
            logging.error(f"Ошибка при извлечении ключа из события: {e}")
            return None

    def _sync_lesson_event(self, lesson, forecast_map, schedule_map, circle_names_map, processed_payment_dates, existing_events):
        """Синхронизирует событие занятия (создает новое или обновляет существующее)."""
        try:
            event_key = self._generate_event_key(lesson, circle_names_map)
            
            # Формируем данные нового события
            event_data = self._prepare_lesson_event_data(lesson, forecast_map, schedule_map, circle_names_map, processed_payment_dates)
            if not event_data:
                return 'skipped'
            
            # Проверяем существование события более тщательно
            existing_event = None
            
            # Сначала проверяем по ключу
            if event_key in existing_events:
                existing_event = existing_events[event_key]
                logging.info(f"Найдено существующее событие по ключу {event_key}")
            else:
                # Дополнительная проверка: ищем по legacy ключу и названию с датой
                lesson_date = lesson.get('Дата занятия', '')
                child_name = lesson.get('Ребенок', '')
                circle_name = circle_names_map.get(lesson.get('ID абонемента', ''), '')
                
                # Проверяем legacy ключ
                legacy_key = f"legacy_{lesson_date}|{child_name}|{circle_name}"
                if legacy_key in existing_events:
                    existing_event = existing_events[legacy_key]
                    logging.info(f"Найдено существующее событие по legacy ключу {legacy_key}")
                else:
                    # Последняя проверка: ищем по названию и дате среди всех событий
                    for key, event in existing_events.items():
                        if key.startswith(('lesson_id_', 'legacy_')):
                            event_summary = event.get('summary', '')
                            event_start = event.get('start', {})
                            
                            # Убираем эмодзи из названия события для сравнения
                            import re
                            clean_summary = re.sub(r'^[✔️🔄🤒✖️💵]*', '', event_summary)
                            expected_title = f"{circle_name} - {child_name}"
                            
                            if clean_summary == expected_title:
                                # Проверяем совпадение по дате
                                if 'dateTime' in event_start:
                                    event_date = datetime.fromisoformat(event_start['dateTime'].replace('Z', '+00:00')).strftime('%d.%m.%Y')
                                    if event_date == lesson_date:
                                        existing_event = event
                                        logging.info(f"Найдено существующее событие по названию и дате: {event_summary}")
                                        break
            
            if existing_event:
                # Событие существует - проверяем, нужно ли обновление
                if self._event_needs_update(existing_event, event_data):
                    # Обновляем существующее событие
                    self._update_calendar_event(existing_event['id'], event_data)
                    logging.info(f"Обновлено событие: {event_data['summary']}")
                    return 'updated'
                else:
                    logging.info(f"Событие не требует обновления: {event_data['summary']}")
                    return 'unchanged'
            else:
                # Создаем новое событие
                self._create_calendar_event(event_data)
                logging.info(f"Создано новое событие: {event_data['summary']}")
                return 'created'
                
        except Exception as e:
            logging.error(f"Ошибка при синхронизации события занятия: {e}")
            return 'error'

    def _prepare_lesson_event_data(self, lesson, forecast_map, schedule_map, circle_names_map, processed_payment_dates):
        """Подготавливает данные события для занятия."""
        # Используем существующую логику из _create_lesson_event
        # но возвращаем данные события вместо создания
        try:
            sub_id = lesson.get('ID абонемента', '')
            child_name = lesson.get('Ребенок', '')
            lesson_date = lesson.get('Дата занятия', '')
            lesson_id = lesson.get('№', '')  # ID занятия из столбца A
            lesson_mark = lesson.get('Отметка', '')  # Отметка посещения
            lesson_status = lesson.get('Статус посещения', '')  # Статус посещения
            
            # Получаем название кружка
            circle_name = circle_names_map.get(sub_id, 'Неизвестный кружок')
            logging.info(f"Для абонемента {sub_id} найден кружок: {circle_name}")
            
            # Получаем время
            start_time = str(lesson.get('Время начала', '') or lesson.get('Начало', '') or lesson.get('Start Time', '')).strip()
            end_time = str(lesson.get('Время завершения', '') or lesson.get('Окончание', '') or lesson.get('End Time', '') or lesson.get('Время окончания', '')).strip()
            
            logging.info(f"Поля занятия: {list(lesson.keys())}")
            logging.info(f"Время из занятия: начало='{start_time}', окончание='{end_time}'")
            
            # Если времени нет в занятии, берем из шаблона расписания
            if not start_time or not end_time:
                schedule = schedule_map.get(sub_id, [])
                if schedule:
                    template = schedule[0]
                    if not start_time:
                        start_time = str(template.get('start_time', '10:00')).strip()
                    if not end_time:
                        end_time = str(template.get('end_time', '11:00')).strip()
                else:
                    logging.warning(f"Не найден шаблон расписания для абонемента {sub_id}")
                    if not start_time:
                        start_time = '10:00'
                    if not end_time:
                        end_time = '11:00'
            
            logging.info(f"Время для занятия {sub_id}: {start_time} - {end_time}")
            
            # Преобразуем дату занятия
            try:
                lesson_date_obj = datetime.strptime(lesson_date, '%d.%m.%Y')
            except ValueError:
                logging.error(f"Неверный формат даты: {lesson_date}")
                return None
            
            # Проверяем, есть ли оплата на эту дату
            date_key = lesson_date_obj.strftime('%Y-%m-%d')
            payment_key = f"{child_name}|{circle_name}|{date_key}"
            is_payment_day = payment_key in forecast_map
            
            # Получаем эмодзи для статуса
            status_emoji = self._get_status_emoji(lesson_mark, lesson_status, is_payment_day)
            
            # Формируем данные события
            if is_payment_day:
                title = f"{status_emoji}ОПЛАТА: {circle_name} - {child_name}"
                description = f"Требуется оплата за следующий абонемент: {forecast_map[payment_key]} руб.\n\nlesson_id:{lesson_id}\n#schedule_sync"
                processed_payment_dates.add(payment_key)
            else:
                title = f"{status_emoji}{circle_name} - {child_name}".strip()
                description = f"lesson_id:{lesson_id}\n#schedule_sync"
            
            # Проверяем и форматируем время
            if not start_time or start_time == '':
                start_time = '10:00'
            if not end_time or end_time == '':
                end_time = '11:00'
            
            # Убираем лишние пробелы и проверяем формат времени
            try:
                # Создаем datetime объекты для начала и конца
                start_datetime = datetime.strptime(f"{lesson_date} {start_time}", '%d.%m.%Y %H:%M')
                end_datetime = datetime.strptime(f"{lesson_date} {end_time}", '%d.%m.%Y %H:%M')
            except ValueError as e:
                logging.error(f"Ошибка парсинга времени для занятия {lesson_date}: start_time='{start_time}', end_time='{end_time}'. Ошибка: {e}")
                # Используем время по умолчанию
                start_datetime = datetime.strptime(f"{lesson_date} 10:00", '%d.%m.%Y %H:%M')
                end_datetime = datetime.strptime(f"{lesson_date} 11:00", '%d.%m.%Y %H:%M')
            
            # Возвращаем данные события
            return {
                'summary': title,
                'description': description,
                'start': {
                    'dateTime': start_datetime.isoformat(),
                    'timeZone': 'Europe/Moscow',
                },
                'end': {
                    'dateTime': end_datetime.isoformat(),
                    'timeZone': 'Europe/Moscow',
                },
            }
            
        except Exception as e:
            logging.error(f"Ошибка при подготовке данных события: {e}")
            return None

    def _event_needs_update(self, existing_event, new_event_data):
        """Проверяет, нужно ли обновлять существующее событие."""
        try:
            # Сравниваем основные поля
            if existing_event.get('summary') != new_event_data.get('summary'):
                return True
            
            if existing_event.get('description') != new_event_data.get('description'):
                return True
            
            # Сравниваем время начала
            existing_start = existing_event.get('start', {}).get('dateTime', '')
            new_start = new_event_data.get('start', {}).get('dateTime', '')
            if existing_start != new_start:
                return True
            
            # Сравниваем время окончания
            existing_end = existing_event.get('end', {}).get('dateTime', '')
            new_end = new_event_data.get('end', {}).get('dateTime', '')
            if existing_end != new_end:
                return True
            
            return False
        except Exception as e:
            logging.error(f"Ошибка при сравнении событий: {e}")
            return True  # В случае ошибки лучше обновить

    def _create_calendar_event_DISABLED(self, event_data):
        """Создает новое событие в календаре."""
        try:
            created_event = self.calendar_service.events().insert(
                calendarId=config.GOOGLE_CALENDAR_ID,
                body=event_data
            ).execute()
            return True
        except HttpError as e:
            if e.resp.status == 403:
                logging.error("Google Calendar API не настроен. Событие не создано.")
                return False
            else:
                logging.error(f"Ошибка Calendar API при создании события: {e}")
                return False

    def _update_calendar_event_DISABLED(self, event_id, event_data):
        """Обновляет существующее событие в календаре."""
        try:
            updated_event = self.calendar_service.events().update(
                calendarId=config.GOOGLE_CALENDAR_ID,
                eventId=event_id,
                body=event_data
            ).execute()
            return True
        except HttpError as e:
            if e.resp.status == 403:
                logging.error("Google Calendar API не настроен. Событие не обновлено.")
                return False
            else:
                logging.error(f"Ошибка Calendar API при обновлении события: {e}")
                return False

    def _sync_future_payment_events(self, forecast_map, processed_payment_dates, existing_events, events_to_keep):
        """Синхронизирует события будущих оплат."""
        created_count = 0
        updated_count = 0
        
        for payment_key, budget in forecast_map.items():
            if payment_key not in processed_payment_dates:
                try:
                    child, circle, date_str = payment_key.split('|')
                    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
                    
                    # Генерируем ключ события для оплаты
                    event_date = date_obj.strftime('%d.%m.%Y')
                    event_key = f"payment_{event_date}|{child}|{circle}"
                    events_to_keep.add(event_key)
                    
                    # Создаем данные события на весь день с эмодзи оплаты
                    payment_emoji = self._get_status_emoji(None, None, is_payment=True)
                    event_data = {
                        'summary': f"{payment_emoji}ОПЛАТА: {circle} - {child}",
                        'description': f"Требуется оплата за следующий абонемент: {budget} руб.\n\n#schedule_sync",
                        'start': {
                            'date': date_str,
                        },
                        'end': {
                            'date': date_str,
                        },
                    }
                    
                    if event_key in existing_events:
                        # Обновляем существующее событие
                        existing_event = existing_events[event_key]
                        if self._event_needs_update(existing_event, event_data):
                            self._update_calendar_event(existing_event['id'], event_data)
                            logging.info(f"Обновлено событие оплаты: {circle} - {child} на {date_str}")
                            updated_count += 1
                    else:
                        # Создаем новое событие
                        self._create_calendar_event(event_data)
                        logging.info(f"Создано событие оплаты: {circle} - {child} на {date_str}")
                        created_count += 1
                    
                except Exception as e:
                    logging.error(f"Ошибка при синхронизации события оплаты {payment_key}: {e}")
        
        return {'created': created_count, 'updated': updated_count}

    def _delete_obsolete_events(self, existing_events, events_to_keep):
        """Удаляет устаревшие события, которых больше нет в расписании."""
        deleted_count = 0
        
        for event_key, event in existing_events.items():
            if event_key not in events_to_keep:
                try:
                    self.calendar_service.events().delete(
                        calendarId=config.GOOGLE_CALENDAR_ID,
                        eventId=event['id']
                    ).execute()
                    logging.info(f"Удалено устаревшее событие: {event.get('summary', 'Без названия')}")
                    deleted_count += 1
                except HttpError as e:
                    if e.resp.status == 403:
                        logging.error("Google Calendar API не настроен. Событие не удалено.")
                    else:
                        logging.error(f"Ошибка Calendar API при удалении события: {e}")
                except Exception as e:
                    logging.error(f"Ошибка при удалении события: {e}")
        
        return deleted_count

    def _get_status_emoji(self, lesson_mark, lesson_status, is_payment=False):
        """Возвращает эмодзи в зависимости от статуса занятия или типа события."""
        if is_payment:
            return "💰"  # Оплата
        
        # Проверяем отметку посещения (приоритет)
        if lesson_mark and lesson_mark.strip():
            mark_lower = lesson_mark.lower().strip()
            if 'посещение' in mark_lower:
                return "✔️"  # Галочка для посещения
            elif 'перенос' in mark_lower:
                return "🔄"  # Стрелки обновления для переноса
            elif 'отмена (болезнь)' in mark_lower or 'болезнь' in mark_lower:
                return "🤒"  # Больной смайлик для болезни
            elif 'пропуск (по вине)' in mark_lower or 'пропуск' in mark_lower:
                return "🚫"  # Знак запрета для пропуска по вине
            # Убираем обработку просто "отмена" - такой отметки не должно быть
        
        # Если отметки нет, проверяем статус посещения
        if lesson_status and lesson_status.strip():
            status_lower = lesson_status.lower().strip()
            if status_lower == 'завершен':
                return "✔️"  # Завершенное занятие
            elif status_lower == 'пропуск':
                return "🚫"  # Пропущенное занятие
            elif status_lower == 'запланировано':
                return ""  # БЕЗ эмодзи для запланированных
        
        return ""  # По умолчанию БЕЗ эмодзи для новых занятий

    def _extract_lesson_variables_from_event(self, event):
        """Извлекает переменные занятия из события Google Calendar для сравнения."""
        try:
            summary = event.get('summary', '')
            description = event.get('description', '')
            start = event.get('start', {})
            end = event.get('end', {})
            
            logging.info(f"    🔍 Извлекаем переменные из события:")
            logging.info(f"      📝 Название: {summary}")
            logging.info(f"      📄 Описание: {description}")
            
            # Извлекаем ВСЕ переменные из описания события
            lesson_id = ''
            event_date = ''
            start_time = ''
            end_time = ''
            status = ''
            mark = ''
            
            # Парсим описание построчно для извлечения переменных
            if description:
                logging.info(f"      🔍 Парсим описание построчно:")
                for line in description.split('\n'):
                    line = line.strip()
                    logging.info(f"        📄 Строка: '{line}'")
                    if line.startswith('lesson_id:'):
                        lesson_id = line.split('lesson_id:')[1].strip()
                        logging.info(f"          🆔 Найден lesson_id: '{lesson_id}'")
                    elif line.startswith('date:'):
                        event_date = line.split('date:')[1].strip()
                        logging.info(f"          📅 Найдена date: '{event_date}'")
                    elif line.startswith('start_time:'):
                        start_time = line.split('start_time:')[1].strip()
                        logging.info(f"          🕐 Найдено start_time: '{start_time}'")
                    elif line.startswith('end_time:'):
                        end_time = line.split('end_time:')[1].strip()
                        logging.info(f"          🕐 Найдено end_time: '{end_time}'")
                    elif line.startswith('status:'):
                        status = line.split('status:')[1].strip()
                        logging.info(f"          📊 Найден status: '{status}'")
                    elif line.startswith('mark:'):
                        mark = line.split('mark:')[1].strip()
                        logging.info(f"          ✏️ Найдена mark: '{mark}'")
            
            # Если переменные не найдены в описании, пытаемся извлечь из других полей
            if not event_date and 'dateTime' in start:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start['dateTime'].replace('Z', '+00:00'))
                event_date = start_dt.strftime('%d.%m.%Y')
            elif not event_date and 'date' in start:
                from datetime import datetime
                date_obj = datetime.strptime(start['date'], '%Y-%m-%d')
                event_date = date_obj.strftime('%d.%m.%Y')
            
            if not start_time and 'dateTime' in start:
                from datetime import datetime
                start_dt = datetime.fromisoformat(start['dateTime'].replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end['dateTime'].replace('Z', '+00:00'))
                start_time = start_dt.strftime('%H:%M')
                end_time = end_dt.strftime('%H:%M')
            
            return {
                'lesson_id': lesson_id,
                'date': event_date,
                'start_time': start_time,
                'end_time': end_time,
                'status': status,
                'mark': mark,
                'summary': summary,
                'description': description
            }
            
        except Exception as e:
            logging.error(f"Ошибка при извлечении переменных из события: {e}")
            return None

    def _extract_forecast_variables_from_event(self, event):
        """Извлекает переменные прогноза из события Google Calendar для сравнения."""
        try:
            summary = event.get('summary', '')
            description = event.get('description', '')
            start = event.get('start', {})
            
            # Извлекаем ВСЕ переменные из описания события
            circle_name = ''
            child_name = ''
            event_date = ''
            budget = ''
            status = ''
            
            # Парсим описание построчно для извлечения переменных
            if description:
                for line in description.split('\n'):
                    line = line.strip()
                    if line.startswith('circle_name:'):
                        circle_name = line.split('circle_name:')[1].strip()
                    elif line.startswith('child_name:'):
                        child_name = line.split('child_name:')[1].strip()
                    elif line.startswith('date:'):
                        event_date = line.split('date:')[1].strip()
                    elif line.startswith('budget:'):
                        budget = line.split('budget:')[1].strip()
                    elif line.startswith('status:'):
                        status = line.split('status:')[1].strip()
            
            # Если переменные не найдены в описании, пытаемся извлечь из других полей
            if not event_date and 'date' in start:
                from datetime import datetime
                date_obj = datetime.strptime(start['date'], '%Y-%m-%d')
                event_date = date_obj.strftime('%d.%m.%Y')
            
            if not circle_name or not child_name:
                # Извлекаем из названия как fallback
                if 'ОПЛАТА: ' in summary:
                    parts = summary.split('ОПЛАТА: ')[1].split(' - ')
                    if len(parts) == 2:
                        if not circle_name:
                            circle_name = parts[0].strip()
                        if not child_name:
                            child_name = parts[1].strip()
            
            return {
                'circle_name': circle_name,
                'child_name': child_name,
                'date': event_date,
                'budget': budget,
                'status': status,
                'summary': summary,
                'description': description
            }
            
        except Exception as e:
            logging.error(f"Ошибка при извлечении переменных прогноза из события: {e}")
            return None

    def _compare_lesson_variables(self, lesson_sheet_data, event_data):
        """Сравнивает переменные занятия из листа и события Google Calendar."""
        if not lesson_sheet_data or not event_data:
            return False
        
        # Сравниваем ключевые переменные
        sheet_date = lesson_sheet_data.get('Дата занятия', '').strip()
        sheet_start = lesson_sheet_data.get('Время начала', '').strip()
        sheet_end = lesson_sheet_data.get('Время завершения', '').strip()
        sheet_status = lesson_sheet_data.get('Статус посещения', '').strip().lower()
        sheet_mark = lesson_sheet_data.get('Отметка', '').strip()
        
        event_date = event_data.get('date', '').strip()
        event_start = event_data.get('start_time', '').strip()
        event_end = event_data.get('end_time', '').strip()
        event_status = event_data.get('status', '').strip().lower()
        event_mark = event_data.get('mark', '').strip()
        
        # Проверяем каждую переменную
        variables_match = (
            sheet_date == event_date and
            sheet_start == event_start and
            sheet_end == event_end and
            sheet_status == event_status and
            sheet_mark == event_mark
        )
        
        if not variables_match:
            logging.info(f"Найдены расхождения в занятии:")
            logging.info(f"  Дата: '{sheet_date}' vs '{event_date}'")
            logging.info(f"  Время начала: '{sheet_start}' vs '{event_start}'")
            logging.info(f"  Время окончания: '{sheet_end}' vs '{event_end}'")
            logging.info(f"  Статус: '{sheet_status}' vs '{event_status}'")
            logging.info(f"  Отметка: '{sheet_mark}' vs '{event_mark}'")
        
        return variables_match

    def _compare_forecast_variables(self, forecast_sheet_data, event_data):
        """Сравнивает переменные прогноза из листа и события Google Calendar."""
        if not forecast_sheet_data or not event_data:
            return False
        
        # Сравниваем ключевые переменные
        sheet_circle = forecast_sheet_data.get('Кружок', '').strip()
        sheet_child = forecast_sheet_data.get('Ребенок', '').strip()
        sheet_date = forecast_sheet_data.get('Дата оплаты', '').strip()
        sheet_budget = str(forecast_sheet_data.get('Бюджет', '')).strip()
        sheet_status = forecast_sheet_data.get('Статус', '').strip().lower()
        
        event_circle = event_data.get('circle_name', '').strip()
        event_child = event_data.get('child_name', '').strip()
        event_date = event_data.get('date', '').strip()
        event_budget = event_data.get('budget', '').strip()
        event_status = event_data.get('status', '').strip().lower()
        
        # Проверяем каждую переменную
        variables_match = (
            sheet_circle == event_circle and
            sheet_child == event_child and
            sheet_date == event_date and
            sheet_budget == event_budget and
            sheet_status == event_status
        )
        
        if not variables_match:
            logging.info(f"Найдены расхождения в прогнозе:")
            logging.info(f"  Кружок: '{sheet_circle}' vs '{event_circle}'")
            logging.info(f"  Ребенок: '{sheet_child}' vs '{event_child}'")
            logging.info(f"  Дата: '{sheet_date}' vs '{event_date}'")
            logging.info(f"  Бюджет: '{sheet_budget}' vs '{event_budget}'")
            logging.info(f"  Статус: '{sheet_status}' vs '{event_status}'")
        
        return variables_match

    def professional_calendar_sync_DISABLED(self):
        """Google Calendar синхронизация отключена."""
        logging.info("Google Calendar синхронизация отключена")
        return True

    def sync_calendar_with_google_calendar(self):
        """
        НОВАЯ ФУНКЦИЯ: Синхронизация Google Календаря с листом 'Календарь занятий'
        
        Логика работы:
        1. Читает все строки из листа "Календарь занятий"
        2. Для каждой строки ищет событие в Google Calendar по ID занятия
        3. Если событие найдено - сравнивает все переменные
        4. Если переменные отличаются - обновляет событие
        5. Если событие не найдено - создает новое
        6. Если все переменные совпадают - игнорирует
        """
        try:
            import time
            start_time = time.time()
            
            if not self.calendar_service:
                return "❌ Google Calendar не настроен. Проверьте GOOGLE_CALENDAR_ID в .env файле."
            
            logging.info("🔄 Начинаю синхронизацию Google Calendar...")
            
            # Получаем данные из листа "Календарь занятий"
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            try:
                calendar_data = calendar_sheet.get_all_values()
            except Exception as e:
                if "429" in str(e) or "Quota exceeded" in str(e):
                    logging.warning("⚠️ Превышена квота Google Sheets API. Пропускаю синхронизацию календаря.")
                    return "⚠️ Синхронизация пропущена из-за превышения квоты API"
                raise e
            
            if len(calendar_data) <= 1:
                return "❌ Лист 'Календарь занятий' пуст или содержит только заголовки."
            
            headers = calendar_data[0]
            
            # Проверяем наличие всех необходимых столбцов
            required_columns = {
                'lesson_id': '№',
                'subscription_id': 'ID абонемента', 
                'date': 'Дата занятия',
                'start_time': 'Время начала',
                'status': 'Статус посещения',
                'child': 'Ребенок',
                'mark': 'Отметка',
                'end_time': 'Время завершения'
            }
            
            col_indices = {}
            for key, header in required_columns.items():
                try:
                    col_indices[key] = headers.index(header)
                except ValueError:
                    return f"❌ Не найден столбец '{header}' в листе 'Календарь занятий'"
            
            # Получаем данные абонементов для определения названий кружков
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            subs_data = subs_sheet.get_all_values()
            
            # Создаем словарь ID абонемента -> название кружка
            circle_names_map = {}
            if len(subs_data) > 1:
                subs_headers = subs_data[0]
                logging.info(f"📋 Заголовки листа 'Абонементы': {subs_headers}")
                
                # Находим индексы нужных столбцов (B = ID, D = Кружок)
                try:
                    id_col_index = 1  # Столбец B (индекс 1)
                    circle_col_index = 3  # Столбец D (индекс 3)
                    
                    for row in subs_data[1:]:
                        if len(row) > max(id_col_index, circle_col_index):
                            sub_id = str(row[id_col_index]).strip()
                            circle_name = str(row[circle_col_index]).strip()
                            if sub_id and circle_name:
                                circle_names_map[sub_id] = circle_name
                                logging.info(f"📝 Маппинг: ID {sub_id} -> {circle_name}")
                except Exception as e:
                    logging.error(f"❌ Ошибка при чтении абонементов: {e}")
            
            logging.info(f"📊 Создан маппинг кружков: {circle_names_map}")
            
            # Счетчики для статистики
            created_count = 0
            updated_count = 0
            ignored_count = 0
            errors = []
            
            # Обрабатываем каждую строку календаря занятий
            for row_index, row in enumerate(calendar_data[1:], start=2):
                try:
                    # Извлекаем данные из строки
                    lesson_data = {
                        'lesson_id': str(row[col_indices['lesson_id']]).strip(),
                        'subscription_id': str(row[col_indices['subscription_id']]).strip(),
                        'date': str(row[col_indices['date']]).strip(),
                        'start_time': str(row[col_indices['start_time']]).strip(),
                        'status': str(row[col_indices['status']]).strip(),
                        'child': str(row[col_indices['child']]).strip(),
                        'mark': str(row[col_indices['mark']]).strip(),
                        'end_time': str(row[col_indices['end_time']]).strip()
                    }
                    
                    # Пропускаем строки без ID занятия
                    if not lesson_data['lesson_id']:
                        continue
                    
                    # Разовые абонементы теперь тоже синхронизируются с Google Calendar
                    # (убрали проверку, чтобы события создавались для всех типов абонементов)
                    
                    # Получаем название кружка
                    circle_name = circle_names_map.get(lesson_data['subscription_id'], 'Неизвестный кружок')
                    
                    # Ищем событие в Google Calendar по ID занятия
                    existing_event = None
                    
                    # Сначала пробуем найти по ID занятия (если ID не пустой и не N/A)
                    if lesson_data['lesson_id'] and lesson_data['lesson_id'] not in ['', 'N/A']:
                        existing_event = self.calendar_service.find_event_by_lesson_id(lesson_data['lesson_id'])
                    
                    # Если не найдено по ID, ищем по деталям (дата, время, ребенок, кружок)
                    if not existing_event:
                        existing_event = self.calendar_service.find_event_by_lesson_details(lesson_data, circle_name)
                        if existing_event:
                            logging.info(f"🔍 Найдено событие по деталям для занятия {lesson_data['lesson_id']}")
                    
                    if existing_event:
                        # Событие существует - сравниваем переменные
                        event_variables = self.calendar_service.extract_lesson_variables_from_event(existing_event)
                        
                        logging.info(f"🔍 Сравнение для занятия {lesson_data['lesson_id']}:")
                        logging.info(f"   📊 Данные из таблицы: {lesson_data}")
                        logging.info(f"   📅 Данные из календаря: {event_variables}")
                        
                        if self.calendar_service.compare_lesson_variables(lesson_data, event_variables):
                            # Все переменные совпадают - игнорируем
                            ignored_count += 1
                            logging.info(f"✅ Занятие {lesson_data['lesson_id']}: все данные совпадают, пропускаем")
                        else:
                            # Есть различия - обновляем событие
                            logging.info(f"🔄 Занятие {lesson_data['lesson_id']}: найдены различия, обновляем событие")
                            if self.calendar_service.update_event(existing_event['id'], lesson_data, circle_name):
                                updated_count += 1
                                logging.info(f"✅ Занятие {lesson_data['lesson_id']}: событие успешно обновлено")
                            else:
                                errors.append(f"Ошибка обновления события для занятия {lesson_data['lesson_id']}")
                                logging.error(f"❌ Ошибка обновления события для занятия {lesson_data['lesson_id']}")
                    else:
                        # Событие не найдено - создаем новое
                        logging.info(f"🆕 Занятие {lesson_data['lesson_id']}: событие не найдено, создаю новое")
                        logging.info(f"📊 Данные для создания: {lesson_data}")
                        logging.info(f"🎯 Название кружка: {circle_name}")
                        
                        event_id = self.calendar_service.create_event(lesson_data, circle_name)
                        if event_id:
                            created_count += 1
                            logging.info(f"✅ Занятие {lesson_data['lesson_id']}: создано новое событие с ID {event_id}")
                        else:
                            error_msg = f"Ошибка создания события для занятия {lesson_data['lesson_id']}"
                            errors.append(error_msg)
                            logging.error(f"❌ {error_msg}")
                
                except Exception as e:
                    error_msg = f"Ошибка обработки строки {row_index}: {e}"
                    errors.append(error_msg)
                    logging.error(error_msg)
            
            # Очищаем дубли занятий после синхронизации
            logging.info("🧹 Проверяю и удаляю дубли занятий...")
            duplicates_removed = self.calendar_service.remove_duplicate_lesson_events()
            if duplicates_removed > 0:
                logging.info(f"🗑️ Удалено {duplicates_removed} дублирующихся событий занятий")
            
            # Вычисляем время выполнения
            end_time = time.time()
            execution_time = round(end_time - start_time, 2)
            total_api_calls = 3 + created_count + updated_count + duplicates_removed  # 3 базовых + создание + обновление + удаление дублей
            
            # Формируем отчет
            result = f"""📅 **Синхронизация Google Calendar завершена**

📊 **Статистика:**
• ✅ Создано событий: {created_count}
• 🔄 Обновлено событий: {updated_count}  
• ⏭️ Пропущено (без изменений): {ignored_count}
• 🧹 Удалено дублей: {duplicates_removed}
• ❌ Ошибок: {len(errors)}

⚡ **Производительность:**
• 🕐 Время выполнения: {execution_time} сек
• 📡 API запросов: {total_api_calls}"""

            if errors:
                result += f"\n\n❌ **Ошибки:**\n" + "\n".join(f"• {error}" for error in errors[:5])
                if len(errors) > 5:
                    result += f"\n• ... и еще {len(errors) - 5} ошибок"
            
            logging.info(f"🎉 Синхронизация завершена: создано {created_count}, обновлено {updated_count}, пропущено {ignored_count}, удалено дублей {duplicates_removed}, время: {execution_time}с, API: {total_api_calls}")
            return result
            
        except Exception as e:
            error_msg = f"❌ Критическая ошибка при синхронизации календаря: {e}"
            logging.error(error_msg, exc_info=True)
            return error_msg

    def sync_forecast_with_google_calendar(self):
        """
        НОВАЯ ФУНКЦИЯ: Синхронизация прогноза оплат с Google Calendar
        
        Логика работы:
        1. Читает все строки из листа "Прогноз"
        2. Для каждой строки ищет событие в Google Calendar по уникальному ID
        3. Если событие найдено - сравнивает все переменные
        4. Если переменные отличаются - обновляет событие
        5. Если событие не найдено - создает новое (на весь день)
        6. Если все переменные совпадают - игнорирует
        """
        try:
            import time
            start_time = time.time()
            
            if not self.calendar_service:
                return "❌ Google Calendar не настроен. Проверьте GOOGLE_CALENDAR_ID в .env файле."
            
            logging.info("💰 Начинаю синхронизацию прогноза оплат с Google Calendar...")
            
            # Получаем данные из листа "Прогноз"
            try:
                forecast_sheet = self.spreadsheet.worksheet("Прогноз")
                forecast_data = forecast_sheet.get_all_values()
            except Exception as e:
                return f"❌ Ошибка при чтении листа 'Прогноз': {e}"
            
            if len(forecast_data) <= 1:
                # Лист пуст - удаляем все события прогноза из календаря
                logging.info("📭 Лист 'Прогноз' пуст, удаляю все события прогноза из календаря...")
                deleted_count = self.calendar_service.delete_all_forecast_events()
                
                if deleted_count > 0:
                    return f"🗑️ **Лист 'Прогноз' пуст**\n\n✅ Удалено {deleted_count} событий прогноза из календаря\n📅 Календарь очищен от всех прогнозов оплат"
                else:
                    return "ℹ️ **Лист 'Прогноз' пуст**\n\n📅 В календаре не найдено событий прогноза для удаления"
            
            logging.info(f"📊 Найдено {len(forecast_data)-1} строк в листе 'Прогноз'")
            
            # Счетчики для статистики
            created_count = 0
            updated_count = 0
            ignored_count = 0
            deleted_count = 0
            errors = []
            
            # Получаем все существующие события прогноза из календаря
            all_calendar_events = self.calendar_service.get_all_events()
            existing_forecast_events = []
            for event in all_calendar_events:
                description = event.get('description', '')
                if "ID прогноза:" in description:
                    existing_forecast_events.append(event)
            
            logging.info(f"📅 Найдено {len(existing_forecast_events)} существующих событий прогноза в календаре")
            
            # Собираем ID всех прогнозов из таблицы для последующего сравнения
            table_forecast_ids = set()
            
            # Обрабатываем каждую строку прогноза (пропускаем заголовки)
            for row_index, row in enumerate(forecast_data[1:], 2):
                try:
                    if len(row) < 5:  # Минимум 5 столбцов: Кружок, Ребенок, Дата оплаты, Бюджет, Статус
                        continue
                    
                    # Извлекаем данные из строки прогноза
                    circle = str(row[0]).strip()
                    child = str(row[1]).strip()
                    payment_date = str(row[2]).strip()
                    budget = str(row[3]).strip()
                    status = str(row[4]).strip()
                    
                    # Создаем уникальный ID на основе ключевых данных (без номера строки)
                    # Это позволит избежать дублей при изменении порядка строк
                    import hashlib
                    unique_key = f"{circle}_{child}_{payment_date}_{budget}"
                    forecast_id = f"forecast_{hashlib.md5(unique_key.encode()).hexdigest()[:8]}"
                    
                    forecast_data_item = {
                        'circle': circle,                   # A: Кружок
                        'child': child,                     # B: Ребенок
                        'payment_date': payment_date,       # C: Дата оплаты
                        'budget': budget,                   # D: Бюджет
                        'status': status,                   # E: Статус
                        'forecast_id': forecast_id          # Уникальный ID на основе данных
                    }
                    
                    # Пропускаем строки с пустыми ключевыми данными
                    if not forecast_data_item['child'] or not forecast_data_item['payment_date']:
                        continue
                    
                    # Добавляем ID в множество для отслеживания
                    table_forecast_ids.add(forecast_data_item['forecast_id'])
                    
                    logging.info(f"💰 Обрабатываю прогноз: {forecast_data_item['child']} - {forecast_data_item['circle']} на {forecast_data_item['payment_date']} (ID: {forecast_data_item['forecast_id']})")
                    
                    # Ищем существующее событие по ID прогноза
                    existing_event = None
                    
                    # Сначала пробуем найти по ID прогноза
                    existing_event = self.calendar_service.find_forecast_event_by_id(forecast_data_item['forecast_id'])
                    
                    # Если не найдено по ID, ищем по деталям (дата, ребенок, кружок)
                    if not existing_event:
                        existing_event = self.calendar_service.find_forecast_event_by_details(forecast_data_item)
                        if existing_event:
                            logging.info(f"🔍 Найдено событие прогноза по деталям для {forecast_data_item['child']} - {forecast_data_item['circle']}")
                    
                    if existing_event:
                        # Событие найдено - сравниваем данные
                        event_variables = self.calendar_service.extract_forecast_variables_from_event(existing_event)
                        
                        logging.info(f"🔍 Сравнение для прогноза {forecast_data_item['forecast_id']}:")
                        logging.info(f"   📊 Данные из таблицы: {forecast_data_item}")
                        logging.info(f"   📅 Данные из календаря: {event_variables}")
                        
                        # Сравниваем переменные
                        if self.calendar_service.compare_forecast_variables(forecast_data_item, event_variables):
                            # Все данные совпадают - пропускаем
                            ignored_count += 1
                            logging.info(f"✅ Прогноз {forecast_data_item['forecast_id']}: все данные совпадают, пропускаем")
                        else:
                            # Есть различия - обновляем событие
                            logging.info(f"🔄 Прогноз {forecast_data_item['forecast_id']}: найдены различия, обновляем событие")
                            if self.calendar_service.update_forecast_event(existing_event['id'], forecast_data_item):
                                updated_count += 1
                                logging.info(f"✅ Прогноз {forecast_data_item['forecast_id']}: событие успешно обновлено")
                            else:
                                errors.append(f"Ошибка обновления события для прогноза {forecast_data_item['forecast_id']}")
                                logging.error(f"❌ Ошибка обновления события для прогноза {forecast_data_item['forecast_id']}")
                    else:
                        # Событие не найдено - создаем новое
                        logging.info(f"🆕 Прогноз {forecast_data_item['forecast_id']}: событие не найдено, создаю новое")
                        logging.info(f"📊 Данные для создания: {forecast_data_item}")
                        
                        event_id = self.calendar_service.create_forecast_event(forecast_data_item)
                        if event_id:
                            created_count += 1
                            logging.info(f"✅ Прогноз {forecast_data_item['forecast_id']}: создано новое событие с ID {event_id}")
                        else:
                            error_msg = f"Ошибка создания события для прогноза {forecast_data_item['forecast_id']}"
                            errors.append(error_msg)
                            logging.error(f"❌ {error_msg}")
                
                except Exception as e:
                    error_msg = f"Ошибка обработки строки прогноза {row_index}: {e}"
                    errors.append(error_msg)
                    logging.error(error_msg)
            
            # Удаляем события прогноза, которых нет в таблице
            logging.info(f"🔍 Поиск событий для удаления...")
            logging.info(f"📊 ID в таблице: {len(table_forecast_ids)} штук")
            
            for event in existing_forecast_events:
                try:
                    event_variables = self.calendar_service.extract_forecast_variables_from_event(event)
                    event_forecast_id = event_variables.get('forecast_id', '')
                    
                    if event_forecast_id and event_forecast_id not in table_forecast_ids:
                        # Событие есть в календаре, но нет в таблице - удаляем
                        event_summary = event.get('summary', 'Без названия')
                        logging.info(f"🗑️ Удаляю лишнее событие прогноза: {event_summary} (ID: {event_forecast_id})")
                        
                        try:
                            self.calendar_service.service.events().delete(
                                calendarId=self.calendar_service.calendar_id,
                                eventId=event['id']
                            ).execute()
                            
                            deleted_count += 1
                            logging.info(f"✅ Удалено лишнее событие: {event_summary}")
                            
                        except Exception as delete_error:
                            error_msg = f"Ошибка удаления события {event_forecast_id}: {delete_error}"
                            errors.append(error_msg)
                            logging.error(error_msg)
                            
                except Exception as e:
                    logging.error(f"❌ Ошибка при проверке события для удаления: {e}")
            
            # Вычисляем время выполнения
            end_time = time.time()
            execution_time = round(end_time - start_time, 2)
            total_api_calls = 3 + created_count + updated_count  # 3 базовых + создание + обновление
            
            # Формируем отчет
            result = f"""💰 **Синхронизация прогноза оплат завершена**

📊 **Статистика:**
• ✅ Создано событий: {created_count}
• 🔄 Обновлено событий: {updated_count}  
• ⏭️ Пропущено (без изменений): {ignored_count}
• 🗑️ Удалено лишних событий: {deleted_count}
• ❌ Ошибок: {len(errors)}

⚡ **Производительность:**
• 🕐 Время выполнения: {execution_time} сек
• 📡 API запросов: {total_api_calls}"""

            if errors:
                result += f"\n\n❌ **Ошибки:**\n" + "\n".join(f"• {error}" for error in errors[:5])
                if len(errors) > 5:
                    result += f"\n• ... и еще {len(errors) - 5} ошибок"
            
            logging.info(f"🎉 Синхронизация прогноза завершена: создано {created_count}, обновлено {updated_count}, пропущено {ignored_count}, удалено {deleted_count}, время: {execution_time}с, API: {total_api_calls}")
            return result
            
        except Exception as e:
            error_msg = f"❌ Критическая ошибка при синхронизации прогноза: {e}"
            logging.error(error_msg, exc_info=True)
            return error_msg

    def professional_calendar_sync_DISABLED_OLD(self):
        """
        ПРОФЕССИОНАЛЬНАЯ СИНХРОНИЗАЦИЯ КАЛЕНДАРЯ
        
        Логика работы:
        1. Читает все строки из листов "Календарь занятий" и "Прогноз"
        2. Для каждой строки проверяет ID События в Календаре (столбец I/E)
        3. Если ID есть - находит событие в Google Calendar и сравнивает ВСЕ переменные
        4. Если переменные совпадают - ИГНОРИРУЕТ
        5. Если есть расхождения - ОБНОВЛЯЕТ событие по ID
        6. Если ID нет или событие не найдено - СОЗДАЕТ новое и записывает ID
        7. НИКОГДА не изменяет порядок строк и их привязку к данным
        """
        try:
            logging.info("🔄 ПРОФЕССИОНАЛЬНАЯ СИНХРОНИЗАЦИЯ: Начинаю работу...")
            
            if not config.GOOGLE_CALENDAR_ID:
                logging.error("GOOGLE_CALENDAR_ID не указан в конфигурации")
                return False
            
            # Получаем существующие события из Google Calendar
            from datetime import datetime, timedelta
            now = datetime.now()
            start_date = now.replace(day=1)
            end_date = now.replace(month=now.month + 6, day=1) if now.month <= 6 else now.replace(year=now.year + 1, month=6, day=1)
            
            existing_events_map = self._get_existing_events_map(start_date, end_date)
            logging.info(f"📊 Найдено существующих событий в Google Calendar: {len(existing_events_map)}")
            
            # Логируем все найденные события для отладки
            if existing_events_map:
                logging.info(f"🔍 СПИСОК ВСЕХ НАЙДЕННЫХ СОБЫТИЙ В GOOGLE CALENDAR:")
                for event_id, event in existing_events_map.items():
                    logging.info(f"  🆔 ID: {event_id}")
                    logging.info(f"    📝 Название: {event.get('summary', 'Без названия')}")
                    logging.info(f"    📄 Описание: {event.get('description', 'Без описания')[:100]}...")
            else:
                logging.info(f"⚠️ НЕ НАЙДЕНО НИ ОДНОГО СОБЫТИЯ В GOOGLE CALENDAR!")
            
            # Подготавливаем данные для синхронизации
            subscriptions_data = self._get_subscriptions_data()
            circle_names_map = self._create_circle_names_map(subscriptions_data)
            
            stats = {'lessons_created': 0, 'lessons_updated': 0, 'lessons_ignored': 0, 
                    'forecast_created': 0, 'forecast_updated': 0, 'forecast_ignored': 0, 'errors': 0}
            
            # === СИНХРОНИЗАЦИЯ КАЛЕНДАРЯ ЗАНЯТИЙ ===
            logging.info("📅 Синхронизация календаря занятий...")
            
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            calendar_data = calendar_sheet.get_all_values()
            
            if len(calendar_data) <= 1:
                logging.info("Нет данных в календаре занятий")
            else:
                # Ожидаемые заголовки: №, ID абонемента, Дата занятия, Время начала, Статус посещения, Ребенок, Отметка, Время завершения, ID События в Календаре
                headers = calendar_data[0]
                logging.info(f"📋 Заголовки календаря: {headers}")
                
                # Находим индексы нужных столбцов
                col_indices = {}
                expected_columns = {
                    'lesson_num': '№',
                    'subscription_id': 'ID абонемента', 
                    'lesson_date': 'Дата занятия',
                    'start_time': 'Время начала',
                    'status': 'Статус посещения',
                    'child_name': 'Ребенок',
                    'mark': 'Отметка',
                    'end_time': 'Время завершения',
                    'event_id': 'ID События в Календаре'
                }
                
                for key, expected_header in expected_columns.items():
                    try:
                        col_indices[key] = headers.index(expected_header)
                    except ValueError:
                        logging.error(f"Не найден столбец '{expected_header}' в календаре занятий")
                        return False
                
                # Обрабатываем каждую строку календаря занятий
                for row_index, row in enumerate(calendar_data[1:], start=2):
                    try:
                        # Извлекаем данные из строки
                        lesson_data = {}
                        for key, col_index in col_indices.items():
                            lesson_data[key] = row[col_index] if col_index < len(row) else ''
                        
                        logging.info(f"🔍 СТРОКА {row_index}: Обрабатываем занятие")
                        logging.info(f"  📋 Данные строки: {lesson_data}")
                        
                        # Пропускаем строки без даты
                        if not lesson_data['lesson_date'] or not lesson_data['lesson_date'].strip():
                            logging.info(f"  ⏭️ Пропускаем строку {row_index} - нет даты")
                            continue
                        
                        # Получаем название кружка
                        circle_name = circle_names_map.get(lesson_data['subscription_id'], 'Неизвестный кружок')
                        logging.info(f"  🎨 Кружок: {circle_name}")
                        
                        # Проверяем ID события
                        current_event_id = lesson_data['event_id'].strip()
                        logging.info(f"  🆔 ID События в Календаре: '{current_event_id}' (длина: {len(current_event_id)})")
                        
                        if current_event_id and current_event_id in existing_events_map:
                            logging.info(f"  ✅ СЛУЧАЙ 1: ID найден в Google Calendar - проверяем переменные")
                            # ЕСТЬ ID И СОБЫТИЕ СУЩЕСТВУЕТ - ПРОВЕРЯЕМ ПЕРЕМЕННЫЕ
                            event = existing_events_map[current_event_id]
                            logging.info(f"  📅 Найдено событие в Google Calendar: {event.get('summary', 'Без названия')}")
                            
                            event_variables = self._extract_lesson_variables_from_event(event)
                            logging.info(f"  🔍 Извлеченные переменные из события: {event_variables}")
                            
                            if event_variables and self._compare_lesson_data_with_event(lesson_data, circle_name, event_variables):
                                # ВСЕ ПЕРЕМЕННЫЕ СОВПАДАЮТ - ИГНОРИРУЕМ
                                stats['lessons_ignored'] += 1
                                logging.info(f"  ✅ Все переменные совпадают - ИГНОРИРУЕМ")
                            else:
                                # ЕСТЬ РАСХОЖДЕНИЯ - ОБНОВЛЯЕМ ПО ID
                                logging.info(f"  🔄 Найдены расхождения - ОБНОВЛЯЕМ ПО ID")
                                if self._update_lesson_event_by_id(current_event_id, lesson_data, circle_name):
                                    stats['lessons_updated'] += 1
                                    logging.info(f"  ✅ Обновлено событие занятия {lesson_data['lesson_num']} по ID {current_event_id}")
                                else:
                                    stats['errors'] += 1
                                    logging.error(f"  ❌ Ошибка обновления события {current_event_id}")
                                    
                        elif current_event_id and current_event_id not in existing_events_map:
                            logging.info(f"  ⚠️ СЛУЧАЙ 2: ID есть, но событие НЕ найдено в Google Calendar")
                            logging.info(f"  🧹 Очищаем ID и создаем новое событие")
                            # ЕСТЬ ID, НО СОБЫТИЕ НЕ НАЙДЕНО - ОЧИЩАЕМ ID И СОЗДАЕМ НОВОЕ
                            calendar_sheet.update_cell(row_index, col_indices['event_id'] + 1, '')
                            logging.info(f"  🗑️ Очищен ID в строке {row_index}, столбец {col_indices['event_id'] + 1}")
                            
                            new_event_id = self._create_lesson_event(lesson_data, circle_name)
                            if new_event_id:
                                calendar_sheet.update_cell(row_index, col_indices['event_id'] + 1, new_event_id)
                                stats['lessons_created'] += 1
                                logging.info(f"  ✨ Создано новое событие {new_event_id} и записано в строку {row_index}")
                            else:
                                stats['errors'] += 1
                                logging.error(f"  ❌ Ошибка создания нового события для строки {row_index}")
                                
                        else:
                            logging.info(f"  🆕 СЛУЧАЙ 3: НЕТ ID - создаем новое событие")
                            # НЕТ ID - СОЗДАЕМ НОВОЕ СОБЫТИЕ
                            new_event_id = self._create_lesson_event(lesson_data, circle_name)
                            if new_event_id:
                                logging.info(f"  💾 ЗАПИСЫВАЕМ ID В GOOGLE SHEETS:")
                                logging.info(f"    📍 Строка: {row_index}")
                                logging.info(f"    📍 Столбец: {col_indices['event_id'] + 1}")
                                logging.info(f"    🆔 ID для записи: {new_event_id}")
                                
                                calendar_sheet.update_cell(row_index, col_indices['event_id'] + 1, new_event_id)
                                
                                # Проверяем, что ID действительно записался
                                updated_value = calendar_sheet.cell(row_index, col_indices['event_id'] + 1).value
                                logging.info(f"    ✅ Проверка записи: в ячейке теперь '{updated_value}'")
                                
                                stats['lessons_created'] += 1
                                logging.info(f"  ✨ Создано новое событие {new_event_id} и записано в строку {row_index}, столбец {col_indices['event_id'] + 1}")
                            else:
                                stats['errors'] += 1
                                logging.error(f"  ❌ Ошибка создания события для строки {row_index}")
                                
                    except Exception as e:
                        logging.error(f"Ошибка при обработке строки {row_index} календаря занятий: {e}")
                        stats['errors'] += 1
            
            # === СИНХРОНИЗАЦИЯ ПРОГНОЗА ===
            logging.info("💰 Синхронизация прогноза оплат...")
            
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            forecast_data = forecast_sheet.get_all_values()
            
            if len(forecast_data) <= 1:
                logging.info("Нет данных в прогнозе")
            else:
                # Ожидаемые заголовки: Кружок, Ребенок, Дата оплаты, Бюджет, ID События в Календаре, Статус
                forecast_headers = forecast_data[0]
                logging.info(f"📋 Заголовки прогноза: {forecast_headers}")
                
                # Находим индексы нужных столбцов
                forecast_col_indices = {}
                expected_forecast_columns = {
                    'circle_name': 'Кружок',
                    'child_name': 'Ребенок',
                    'payment_date': 'Дата оплаты',
                    'budget': 'Бюджет',
                    'event_id': 'ID События в Календаре',
                    'status': 'Статус'
                }
                
                for key, expected_header in expected_forecast_columns.items():
                    try:
                        forecast_col_indices[key] = forecast_headers.index(expected_header)
                    except ValueError:
                        logging.error(f"Не найден столбец '{expected_header}' в прогнозе")
                        return False
                
                # Обрабатываем каждую строку прогноза
                for row_index, row in enumerate(forecast_data[1:], start=2):
                    try:
                        # Извлекаем данные из строки
                        forecast_row_data = {}
                        for key, col_index in forecast_col_indices.items():
                            forecast_row_data[key] = row[col_index] if col_index < len(row) else ''
                        
                        # Пропускаем строки без даты или со статусом отличным от "Оплата запланирована"
                        if (not forecast_row_data['payment_date'] or not forecast_row_data['payment_date'].strip() or
                            forecast_row_data['status'].strip() != 'Оплата запланирована'):
                            continue
                        
                        # Проверяем ID события
                        current_event_id = forecast_row_data['event_id'].strip()
                        
                        if current_event_id and current_event_id in existing_events_map:
                            # ЕСТЬ ID И СОБЫТИЕ СУЩЕСТВУЕТ - ПРОВЕРЯЕМ ПЕРЕМЕННЫЕ
                            event = existing_events_map[current_event_id]
                            event_variables = self._extract_forecast_variables_from_event(event)
                            
                            if event_variables and self._compare_forecast_data_with_event(forecast_row_data, event_variables):
                                # ВСЕ ПЕРЕМЕННЫЕ СОВПАДАЮТ - ИГНОРИРУЕМ
                                stats['forecast_ignored'] += 1
                                logging.debug(f"✅ Прогноз {forecast_row_data['payment_date']}: все переменные совпадают, игнорируется")
                            else:
                                # ЕСТЬ РАСХОЖДЕНИЯ - ОБНОВЛЯЕМ ПО ID
                                if self._update_forecast_event_by_id(current_event_id, forecast_row_data):
                                    stats['forecast_updated'] += 1
                                    logging.info(f"🔄 Обновлено событие прогноза {forecast_row_data['payment_date']} по ID {current_event_id}")
                                else:
                                    stats['errors'] += 1
                                    
                        elif current_event_id and current_event_id not in existing_events_map:
                            # ЕСТЬ ID, НО СОБЫТИЕ НЕ НАЙДЕНО - ОЧИЩАЕМ ID И СОЗДАЕМ НОВОЕ
                            forecast_sheet.update_cell(row_index, forecast_col_indices['event_id'] + 1, '')
                            new_event_id = self._create_forecast_event(forecast_row_data)
                            if new_event_id:
                                forecast_sheet.update_cell(row_index, forecast_col_indices['event_id'] + 1, new_event_id)
                                stats['forecast_created'] += 1
                                logging.info(f"✨ Создано новое событие прогноза {forecast_row_data['payment_date']} (старое не найдено)")
                            else:
                                stats['errors'] += 1
                                
                        else:
                            # НЕТ ID - СОЗДАЕМ НОВОЕ СОБЫТИЕ
                            new_event_id = self._create_forecast_event(forecast_row_data)
                            if new_event_id:
                                forecast_sheet.update_cell(row_index, forecast_col_indices['event_id'] + 1, new_event_id)
                                stats['forecast_created'] += 1
                                logging.info(f"✨ Создано новое событие прогноза {forecast_row_data['payment_date']}")
                            else:
                                stats['errors'] += 1
                                
                    except Exception as e:
                        logging.error(f"Ошибка при обработке строки {row_index} прогноза: {e}")
                        stats['errors'] += 1
            
            # Выводим финальную статистику
            logging.info(f"📊 ПРОФЕССИОНАЛЬНАЯ СИНХРОНИЗАЦИЯ ЗАВЕРШЕНА:")
            logging.info(f"  📅 Занятия: создано {stats['lessons_created']}, обновлено {stats['lessons_updated']}, проигнорировано {stats['lessons_ignored']}")
            logging.info(f"  💰 Прогнозы: создано {stats['forecast_created']}, обновлено {stats['forecast_updated']}, проигнорировано {stats['forecast_ignored']}")
            logging.info(f"  ❌ Ошибок: {stats['errors']}")
            
            return stats['errors'] == 0
            
        except Exception as e:
            logging.error(f"Критическая ошибка при профессиональной синхронизации: {e}", exc_info=True)
            return False
            
            # === СИНХРОНИЗАЦИЯ ПРОГНОЗОВ ===
            if forecast_data:
                logging.info("🔄 Синхронизация прогнозов...")
                forecast_sheet = self.spreadsheet.worksheet("Прогноз")
                forecast_data_raw = forecast_sheet.get_all_values()
                
                forecast_headers = forecast_data_raw[0]
                forecast_event_id_col = None
                
                for i, header in enumerate(forecast_headers):
                    if header == 'ID События в Календаре':
                        forecast_event_id_col = i
                        break
                
                if forecast_event_id_col is not None:
                    for forecast in forecast_data:
                        forecast_date = forecast.get('Дата оплаты', '')
                        
                        if not forecast_date or not forecast_date.strip():
                            continue
                        
                        # Находим строку в таблице прогноза
                        row_index = None
                        current_event_id = ''
                        
                        for i, row in enumerate(forecast_data_raw[1:], start=2):
                            if (len(row) >= 3 and 
                                str(row[0]).strip() == forecast.get('Кружок', '').strip() and
                                str(row[1]).strip() == forecast.get('Ребенок', '').strip() and
                                str(row[2]).strip() == forecast_date.strip()):
                                row_index = i
                                current_event_id = row[forecast_event_id_col] if forecast_event_id_col < len(row) else ''
                                break
                        
                        if not row_index:
                            continue
                        
                        # Логика синхронизации прогнозов по ID
                        if current_event_id and current_event_id in existing_events_map:
                            # ЕСТЬ ID И СОБЫТИЕ СУЩЕСТВУЕТ - ПРОВЕРЯЕМ ПЕРЕМЕННЫЕ
                            event = existing_events_map[current_event_id]
                            event_variables = self._extract_forecast_variables_from_event(event)
                            
                            if event_variables and self._compare_forecast_variables(forecast, event_variables):
                                # ВСЕ ПЕРЕМЕННЫЕ СОВПАДАЮТ - ИГНОРИРУЕМ
                                stats['ignored'] += 1
                                logging.debug(f"Прогноз {forecast_date}: все переменные совпадают, игнорируется")
                            else:
                                # ЕСТЬ РАСХОЖДЕНИЯ - ОБНОВЛЯЕМ ПО ID
                                result = self._update_event_by_id(current_event_id, forecast, None, None, 'forecast')
                                if result:
                                    stats['updated'] += 1
                                    logging.info(f"✅ Обновлено событие прогноза {forecast_date} по ID {current_event_id}")
                                else:
                                    stats['errors'] += 1
                                    
                        elif current_event_id and current_event_id not in existing_events_map:
                            # ЕСТЬ ID, НО СОБЫТИЕ НЕ НАЙДЕНО - ОЧИЩАЕМ ID И СОЗДАЕМ НОВОЕ
                            forecast_sheet.update_cell(row_index, forecast_event_id_col + 1, '')
                            new_event_id = self._create_new_forecast_event(forecast)
                            if new_event_id:
                                forecast_sheet.update_cell(row_index, forecast_event_id_col + 1, new_event_id)
                                stats['created'] += 1
                                logging.info(f"✅ Создано новое событие прогноза {forecast_date} (старое не найдено)")
                            else:
                                stats['errors'] += 1
                                
                        else:
                            # НЕТ ID - СОЗДАЕМ НОВОЕ СОБЫТИЕ
                            new_event_id = self._create_new_forecast_event(forecast)
                            if new_event_id:
                                forecast_sheet.update_cell(row_index, forecast_event_id_col + 1, new_event_id)
                                stats['created'] += 1
                                logging.info(f"✅ Создано новое событие прогноза {forecast_date}")
                            else:
                                stats['errors'] += 1
            
            logging.info(f"📊 Статистика профессиональной синхронизации:")
            logging.info(f"  Создано: {stats['created']}")
            logging.info(f"  Обновлено: {stats['updated']}")
            logging.info(f"  Проигнорировано (совпадают): {stats['ignored']}")
            logging.info(f"  Ошибок: {stats['errors']}")
            
            return stats['errors'] == 0
            
        except Exception as e:
            logging.error(f"Ошибка при профессиональной синхронизации: {e}", exc_info=True)
            return False

    def _compare_lesson_data_with_event(self, lesson_data, circle_name, event_variables):
        """Сравнивает данные занятия из Google Sheets с переменными из события Google Calendar."""
        try:
            logging.info(f"    🔍 ДЕТАЛЬНОЕ СРАВНЕНИЕ ПЕРЕМЕННЫХ:")
            
            # Нормализуем данные для сравнения
            sheet_lesson_num = str(lesson_data.get('lesson_num', '')).strip()
            sheet_date = str(lesson_data.get('lesson_date', '')).strip()
            sheet_start_time = str(lesson_data.get('start_time', '')).strip()
            sheet_end_time = str(lesson_data.get('end_time', '')).strip()
            sheet_status = str(lesson_data.get('status', '')).strip()
            sheet_mark = str(lesson_data.get('mark', '')).strip()
            sheet_child_name = str(lesson_data.get('child_name', '')).strip()
            
            # Данные из события
            event_lesson_num = str(event_variables.get('lesson_id', '')).strip()
            event_date = str(event_variables.get('date', '')).strip()
            event_start_time = str(event_variables.get('start_time', '')).strip()
            event_end_time = str(event_variables.get('end_time', '')).strip()
            event_status = str(event_variables.get('status', '')).strip()
            event_mark = str(event_variables.get('mark', '')).strip()
            
            logging.info(f"      📊 ДАННЫЕ ИЗ GOOGLE SHEETS:")
            logging.info(f"        🆔 Номер занятия: '{sheet_lesson_num}'")
            logging.info(f"        📅 Дата: '{sheet_date}'")
            logging.info(f"        🕐 Время начала: '{sheet_start_time}'")
            logging.info(f"        🕐 Время окончания: '{sheet_end_time}'")
            logging.info(f"        📊 Статус: '{sheet_status}'")
            logging.info(f"        ✏️ Отметка: '{sheet_mark}'")
            
            logging.info(f"      📊 ДАННЫЕ ИЗ GOOGLE CALENDAR:")
            logging.info(f"        🆔 Номер занятия: '{event_lesson_num}'")
            logging.info(f"        📅 Дата: '{event_date}'")
            logging.info(f"        🕐 Время начала: '{event_start_time}'")
            logging.info(f"        🕐 Время окончания: '{event_end_time}'")
            logging.info(f"        📊 Статус: '{event_status}'")
            logging.info(f"        ✏️ Отметка: '{event_mark}'")
            
            # Сравниваем каждую переменную отдельно
            lesson_num_match = sheet_lesson_num == event_lesson_num
            date_match = sheet_date == event_date
            start_time_match = sheet_start_time == event_start_time
            end_time_match = sheet_end_time == event_end_time
            status_match = sheet_status == event_status
            mark_match = sheet_mark == event_mark
            
            logging.info(f"      ✅ РЕЗУЛЬТАТЫ СРАВНЕНИЯ:")
            logging.info(f"        🆔 Номер занятия: {lesson_num_match} ({'✅' if lesson_num_match else '❌'})")
            logging.info(f"        📅 Дата: {date_match} ({'✅' if date_match else '❌'})")
            logging.info(f"        🕐 Время начала: {start_time_match} ({'✅' if start_time_match else '❌'})")
            logging.info(f"        🕐 Время окончания: {end_time_match} ({'✅' if end_time_match else '❌'})")
            logging.info(f"        📊 Статус: {status_match} ({'✅' if status_match else '❌'})")
            logging.info(f"        ✏️ Отметка: {mark_match} ({'✅' if mark_match else '❌'})")
            
            # Сравниваем все переменные
            variables_match = (
                lesson_num_match and
                date_match and
                start_time_match and
                end_time_match and
                status_match and
                mark_match
            )
            
            logging.info(f"      🎯 ИТОГОВЫЙ РЕЗУЛЬТАТ: {variables_match} ({'✅ ВСЕ СОВПАДАЕТ' if variables_match else '❌ ЕСТЬ РАСХОЖДЕНИЯ'})")
            
            return variables_match
            
        except Exception as e:
            logging.error(f"Ошибка при сравнении данных занятия: {e}")
            return False

    def _compare_forecast_data_with_event(self, forecast_data, event_variables):
        """Сравнивает данные прогноза из Google Sheets с переменными из события Google Calendar."""
        try:
            # Нормализуем данные для сравнения
            sheet_circle = str(forecast_data.get('circle_name', '')).strip()
            sheet_child = str(forecast_data.get('child_name', '')).strip()
            sheet_date = str(forecast_data.get('payment_date', '')).strip()
            sheet_budget = str(forecast_data.get('budget', '')).strip()
            sheet_status = str(forecast_data.get('status', '')).strip()
            
            # Данные из события
            event_circle = str(event_variables.get('circle_name', '')).strip()
            event_child = str(event_variables.get('child_name', '')).strip()
            event_date = str(event_variables.get('date', '')).strip()
            event_budget = str(event_variables.get('budget', '')).strip()
            event_status = str(event_variables.get('status', '')).strip()
            
            # Сравниваем все переменные
            variables_match = (
                sheet_circle == event_circle and
                sheet_child == event_child and
                sheet_date == event_date and
                sheet_budget == event_budget and
                sheet_status == event_status
            )
            
            if not variables_match:
                logging.info(f"Найдены расхождения в прогнозе {sheet_date}:")
                logging.info(f"  Кружок: '{sheet_circle}' vs '{event_circle}'")
                logging.info(f"  Ребенок: '{sheet_child}' vs '{event_child}'")
                logging.info(f"  Дата: '{sheet_date}' vs '{event_date}'")
                logging.info(f"  Бюджет: '{sheet_budget}' vs '{event_budget}'")
                logging.info(f"  Статус: '{sheet_status}' vs '{event_status}'")
            
            return variables_match
            
        except Exception as e:
            logging.error(f"Ошибка при сравнении данных прогноза: {e}")
            return False

    def _update_event_by_id(self, event_id, data, circle_names_map, forecast_map, event_type):
        """Обновляет событие по ID с новыми данными."""
        try:
            if event_type == 'lesson':
                event_data = self._prepare_lesson_event_data(data, circle_names_map, forecast_map)
            else:  # forecast
                event_data = self._prepare_forecast_event_data(data)
            
            if not event_data:
                return False
            
            # Обновляем событие по ID
            updated_event = self.calendar_service.events().update(
                calendarId=config.GOOGLE_CALENDAR_ID,
                eventId=event_id,
                body=event_data
            ).execute()
            
            logging.info(f"✅ Обновлено событие по ID {event_id}: {updated_event.get('summary')}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при обновлении события {event_id}: {e}")
            return False

    def _update_lesson_event_by_id(self, event_id, lesson_data, circle_name):
        """Обновляет событие занятия по ID."""
        try:
            # Подготавливаем данные события
            event_data = self._prepare_lesson_event_data_from_row(lesson_data, circle_name)
            if not event_data:
                return False
            
            # Обновляем событие
            updated_event = self.calendar_service.events().update(
                calendarId=config.GOOGLE_CALENDAR_ID,
                eventId=event_id,
                body=event_data
            ).execute()
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при обновлении события занятия {event_id}: {e}")
            return False

    def _update_forecast_event_by_id(self, event_id, forecast_data):
        """Обновляет событие прогноза по ID."""
        try:
            # Подготавливаем данные события
            event_data = self._prepare_forecast_event_data_from_row(forecast_data)
            if not event_data:
                return False
            
            # Обновляем событие
            updated_event = self.calendar_service.events().update(
                calendarId=config.GOOGLE_CALENDAR_ID,
                eventId=event_id,
                body=event_data
            ).execute()
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при обновлении события прогноза {event_id}: {e}")
            return False

    def _create_lesson_event(self, lesson_data, circle_name):
        """Создает новое событие занятия."""
        try:
            logging.info(f"    🆕 СОЗДАНИЕ НОВОГО СОБЫТИЯ:")
            logging.info(f"      📋 Данные занятия: {lesson_data}")
            logging.info(f"      🎨 Кружок: {circle_name}")
            
            # Подготавливаем данные события
            event_data = self._prepare_lesson_event_data_from_row(lesson_data, circle_name)
            if not event_data:
                logging.error(f"      ❌ Не удалось подготовить данные события")
                return None
            
            logging.info(f"      📝 Подготовленные данные события:")
            logging.info(f"        📝 Название: {event_data.get('summary', '')}")
            logging.info(f"        📄 Описание: {event_data.get('description', '')}")
            logging.info(f"        🕐 Начало: {event_data.get('start', {})}")
            logging.info(f"        🕐 Конец: {event_data.get('end', {})}")
            
            # Создаем событие
            logging.info(f"      🚀 Отправляем запрос на создание события в Google Calendar...")
            created_event = self.calendar_service.events().insert(
                calendarId=config.GOOGLE_CALENDAR_ID,
                body=event_data
            ).execute()
            
            event_id = created_event.get('id')
            logging.info(f"      ✅ Событие успешно создано с ID: {event_id}")
            logging.info(f"      📝 Название созданного события: {created_event.get('summary', '')}")
            
            return event_id
            
        except Exception as e:
            logging.error(f"      ❌ Ошибка при создании события занятия: {e}")
            return None

    def _create_forecast_event(self, forecast_data):
        """Создает новое событие прогноза."""
        try:
            # Подготавливаем данные события
            event_data = self._prepare_forecast_event_data_from_row(forecast_data)
            if not event_data:
                return None
            
            # Создаем событие
            created_event = self.calendar_service.events().insert(
                calendarId=config.GOOGLE_CALENDAR_ID,
                body=event_data
            ).execute()
            
            return created_event.get('id')
            
        except Exception as e:
            logging.error(f"Ошибка при создании события прогноза: {e}")
            return None

    def _prepare_lesson_event_data_from_row(self, lesson_data, circle_name):
        """Подготавливает данные события занятия из строки таблицы."""
        try:
            from datetime import datetime
            
            lesson_num = lesson_data.get('lesson_num', '')
            lesson_date = lesson_data.get('lesson_date', '')
            start_time = lesson_data.get('start_time', '')
            end_time = lesson_data.get('end_time', '')
            status = lesson_data.get('status', '')
            mark = lesson_data.get('mark', '')
            child_name = lesson_data.get('child_name', '')
            
            # Получаем эмодзи для статуса
            status_emoji = self._get_status_emoji(mark, status, False)
            
            # Формируем название
            title = f"{status_emoji}{circle_name} - {child_name}".strip()
            
            # Формируем описание с ПОЛНЫМИ переменными
            description = ""
            if mark:
                description = f"Отметка: {mark}\n\n"
            
            description += f"lesson_id:{lesson_num}\n"
            description += f"date:{lesson_date}\n"
            description += f"start_time:{start_time}\n"
            description += f"end_time:{end_time}\n"
            description += f"status:{status}\n"
            description += f"mark:{mark}\n"
            description += f"#schedule_sync"
            
            # Парсим дату и время
            lesson_date_obj = datetime.strptime(lesson_date, '%d.%m.%Y')
            
            # Формируем время начала и окончания
            if start_time and start_time.strip():
                try:
                    start_hour, start_minute = map(int, start_time.split(':'))
                    start_datetime = lesson_date_obj.replace(hour=start_hour, minute=start_minute)
                except:
                    start_datetime = lesson_date_obj.replace(hour=9, minute=0)
            else:
                start_datetime = lesson_date_obj.replace(hour=9, minute=0)
            
            if end_time and end_time.strip():
                try:
                    end_hour, end_minute = map(int, end_time.split(':'))
                    end_datetime = lesson_date_obj.replace(hour=end_hour, minute=end_minute)
                except:
                    end_datetime = start_datetime.replace(hour=start_datetime.hour + 1)
            else:
                end_datetime = start_datetime.replace(hour=start_datetime.hour + 1)
            
            # Формируем данные события
            event_data = {
                'summary': title,
                'description': description,
                'start': {
                    'dateTime': start_datetime.isoformat(),
                    'timeZone': 'Asia/Almaty',
                },
                'end': {
                    'dateTime': end_datetime.isoformat(),
                    'timeZone': 'Asia/Almaty',
                },
            }
            
            return event_data
            
        except Exception as e:
            logging.error(f"Ошибка при подготовке данных события занятия: {e}")
            return None

    def _prepare_forecast_event_data_from_row(self, forecast_data):
        """Подготавливает данные события прогноза из строки таблицы."""
        try:
            from datetime import datetime
            
            circle_name = forecast_data.get('circle_name', '')
            child_name = forecast_data.get('child_name', '')
            payment_date = forecast_data.get('payment_date', '')
            budget = str(forecast_data.get('budget', ''))
            status = forecast_data.get('status', 'ожидается')
            
            # Парсим дату оплаты
            payment_date_obj = datetime.strptime(payment_date, '%d.%m.%Y')
            
            # Формируем описание с ПОЛНЫМИ переменными для синхронизации
            description = f"Требуется оплата за следующий абонемент: {budget} руб.\n\n"
            description += f"circle_name:{circle_name}\n"
            description += f"child_name:{child_name}\n"
            description += f"date:{payment_date}\n"
            description += f"budget:{budget}\n"
            description += f"status:{status}\n"
            description += f"#schedule_sync"
            
            # Формируем данные события на весь день
            event_data = {
                'summary': f"💰ОПЛАТА: {circle_name} - {child_name}",
                'description': description,
                'start': {
                    'date': payment_date_obj.strftime('%Y-%m-%d'),
                },
                'end': {
                    'date': payment_date_obj.strftime('%Y-%m-%d'),
                },
            }
            
            return event_data
            
        except Exception as e:
            logging.error(f"Ошибка при подготовке данных события прогноза: {e}")
            return None

    def update_calendar_events_after_attendance_mark_DISABLED(self, lesson_id):
        """Google Calendar синхронизация отключена."""
        logging.info("Google Calendar синхронизация отключена")
        return True

    def update_calendar_events_after_attendance_mark_DISABLED_OLD(self, lesson_id):
        """
        Детальная синхронизация событий Google Calendar с проверкой всех переменных.
        Сравнивает каждую переменную между Google Sheets и Google Calendar.
        """
        try:
            logging.info(f"🔄 Начинаю детальную синхронизацию после отметки для занятия {lesson_id}")
            
            if not config.GOOGLE_CALENDAR_ID:
                logging.error("GOOGLE_CALENDAR_ID не указан в конфигурации")
                return False
            
            # Получаем все данные из листов
            calendar_lessons = self.get_calendar_lessons()
            forecast_data = self._get_forecast_data()
            
            if not calendar_lessons:
                logging.warning("Нет данных в календаре занятий")
                return True
            
            # Получаем существующие события из Google Calendar
            from datetime import datetime, timedelta
            now = datetime.now()
            start_date = now.replace(day=1)  # Первый день текущего месяца
            end_date = now.replace(month=now.month + 2, day=1) - timedelta(days=1) if now.month <= 10 else now.replace(year=now.year + 1, month=2, day=1) - timedelta(days=1)
            
            existing_events_map = self._get_existing_events_map(start_date, end_date)
            logging.info(f"Найдено существующих событий в Google Calendar: {len(existing_events_map)}")
            
            # Подготавливаем данные для синхронизации
            subscriptions_data = self._get_subscriptions_data()
            circle_names_map = self._create_circle_names_map(subscriptions_data)
            forecast_map = self._create_forecast_map(forecast_data)
            
            # Получаем листы для обновления ID событий
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            
            # Находим индексы столбцов ID событий
            calendar_data = calendar_sheet.get_all_values()
            forecast_data_raw = forecast_sheet.get_all_values()
            
            calendar_headers = calendar_data[0]
            forecast_headers = forecast_data_raw[0]
            
            calendar_event_id_col = None
            forecast_event_id_col = None
            
            for i, header in enumerate(calendar_headers):
                if header == 'ID События в Календаре':
                    calendar_event_id_col = i
                    break
            
            for i, header in enumerate(forecast_headers):
                if header == 'ID События в Календаре':
                    forecast_event_id_col = i
                    break
            
            if calendar_event_id_col is None:
                logging.error("Не найден столбец 'ID События в Календаре' в листе 'Календарь занятий'")
                return False
            
            stats = {'lessons_updated': 0, 'lessons_created': 0, 'forecasts_updated': 0, 'forecasts_created': 0, 'deleted': 0, 'ignored': 0}
            
            # Создаем множество для отслеживания обработанных событий
            processed_event_ids = set()
            
            # === СИНХРОНИЗАЦИЯ ЗАНЯТИЙ ===
            logging.info("🔄 Синхронизация занятий из 'Календарь занятий'...")
            
            for lesson in calendar_lessons:
                lesson_num = lesson.get('№', '')
                lesson_date = lesson.get('Дата занятия', '')
                
                if not lesson_date or not lesson_date.strip():
                    continue
                
                # Находим строку в таблице
                row_index = None
                for i, row in enumerate(calendar_data[1:], start=2):
                    if len(row) > 0 and str(row[0]).strip() == str(lesson_num):
                        row_index = i
                        break
                
                if not row_index:
                    continue
                
                # Получаем ID события из таблицы
                event_id = calendar_data[row_index - 1][calendar_event_id_col] if calendar_event_id_col < len(calendar_data[row_index - 1]) else ''
                
                if event_id and event_id in existing_events_map:
                    # Событие существует - проверяем переменные
                    event = existing_events_map[event_id]
                    event_variables = self._extract_lesson_variables_from_event(event)
                    
                    if event_variables and self._compare_lesson_variables(lesson, event_variables):
                        # Все переменные совпадают - игнорируем
                        stats['ignored'] += 1
                        processed_event_ids.add(event_id)
                        logging.debug(f"Занятие {lesson_num}: переменные совпадают, игнорируется")
                    else:
                        # Есть расхождения - обновляем
                        result = self._update_existing_lesson_event(lesson, event_id, event, circle_names_map, forecast_map)
                        if result:
                            stats['lessons_updated'] += 1
                            processed_event_ids.add(event_id)
                            logging.info(f"✅ Обновлено событие занятия {lesson_num} из-за расхождений")
                        
                elif event_id and event_id not in existing_events_map:
                    # ID есть, но событие не найдено - очищаем ID и создаем новое
                    calendar_sheet.update_cell(row_index, calendar_event_id_col + 1, '')
                    new_event_id = self._create_new_lesson_event(lesson, circle_names_map, forecast_map)
                    if new_event_id:
                        calendar_sheet.update_cell(row_index, calendar_event_id_col + 1, new_event_id)
                        stats['lessons_created'] += 1
                        processed_event_ids.add(new_event_id)
                        logging.info(f"✅ Создано новое событие занятия {lesson_num} (старое не найдено)")
                        
                else:
                    # ID нет - создаем новое событие
                    new_event_id = self._create_new_lesson_event(lesson, circle_names_map, forecast_map)
                    if new_event_id:
                        calendar_sheet.update_cell(row_index, calendar_event_id_col + 1, new_event_id)
                        stats['lessons_created'] += 1
                        processed_event_ids.add(new_event_id)
                        logging.info(f"✅ Создано новое событие занятия {lesson_num}")
            
            # === СИНХРОНИЗАЦИЯ ПРОГНОЗОВ ===
            if forecast_event_id_col is not None:
                logging.info("🔄 Синхронизация событий из 'Прогноз'...")
                
                for forecast in forecast_data:
                    forecast_date = forecast.get('Дата оплаты', '')
                    
                    if not forecast_date or not forecast_date.strip():
                        continue
                    
                    # Находим строку в таблице прогноза
                    row_index = None
                    for i, row in enumerate(forecast_data_raw[1:], start=2):
                        if (len(row) >= 3 and 
                            str(row[0]).strip() == forecast.get('Кружок', '').strip() and
                            str(row[1]).strip() == forecast.get('Ребенок', '').strip() and
                            str(row[2]).strip() == forecast_date.strip()):
                            row_index = i
                            break
                    
                    if not row_index:
                        continue
                    
                    # Получаем ID события из таблицы прогноза
                    event_id = forecast_data_raw[row_index - 1][forecast_event_id_col] if forecast_event_id_col < len(forecast_data_raw[row_index - 1]) else ''
                    
                    if event_id and event_id in existing_events_map:
                        # Событие существует - проверяем переменные
                        event = existing_events_map[event_id]
                        event_variables = self._extract_forecast_variables_from_event(event)
                        
                        if event_variables and self._compare_forecast_variables(forecast, event_variables):
                            # Все переменные совпадают - игнорируем
                            stats['ignored'] += 1
                            processed_event_ids.add(event_id)
                            logging.debug(f"Прогноз {forecast_date}: переменные совпадают, игнорируется")
                        else:
                            # Есть расхождения - обновляем
                            result = self._update_existing_forecast_event(forecast, event_id, event)
                            if result:
                                stats['forecasts_updated'] += 1
                                processed_event_ids.add(event_id)
                                logging.info(f"✅ Обновлено событие прогноза {forecast_date} из-за расхождений")
                                
                    elif event_id and event_id not in existing_events_map:
                        # ID есть, но событие не найдено - очищаем ID и создаем новое
                        forecast_sheet.update_cell(row_index, forecast_event_id_col + 1, '')
                        new_event_id = self._create_new_forecast_event(forecast)
                        if new_event_id:
                            forecast_sheet.update_cell(row_index, forecast_event_id_col + 1, new_event_id)
                            stats['forecasts_created'] += 1
                            processed_event_ids.add(new_event_id)
                            logging.info(f"✅ Создано новое событие прогноза {forecast_date} (старое не найдено)")
                            
                    else:
                        # ID нет - создаем новое событие
                        new_event_id = self._create_new_forecast_event(forecast)
                        if new_event_id:
                            forecast_sheet.update_cell(row_index, forecast_event_id_col + 1, new_event_id)
                            stats['forecasts_created'] += 1
                            processed_event_ids.add(new_event_id)
                            logging.info(f"✅ Создано новое событие прогноза {forecast_date}")
            
            # === УДАЛЕНИЕ ЛИШНИХ СОБЫТИЙ ===
            logging.info("🔄 Проверка и удаление лишних событий...")
            
            for event_id, event in existing_events_map.items():
                if event_id not in processed_event_ids:
                    # Событие не найдено в таблицах - удаляем
                    try:
                        self.calendar_service.events().delete(
                            calendarId=config.GOOGLE_CALENDAR_ID,
                            eventId=event_id
                        ).execute()
                        stats['deleted'] += 1
                        logging.info(f"🗑️ Удалено лишнее событие: {event.get('summary', 'Без названия')}")
                    except Exception as e:
                        logging.error(f"Ошибка при удалении события {event_id}: {e}")
            
            logging.info(f"📊 Статистика синхронизации:")
            logging.info(f"  Занятия: создано {stats['lessons_created']}, обновлено {stats['lessons_updated']}")
            logging.info(f"  Прогнозы: создано {stats['forecasts_created']}, обновлено {stats['forecasts_updated']}")
            logging.info(f"  Удалено лишних: {stats['deleted']}")
            logging.info(f"  Проигнорировано (совпадают): {stats['ignored']}")
            
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при детальной синхронизации: {e}", exc_info=True)
            return False

    def fix_duplicate_lesson_ids(self):
        """Исправляет дублированные ID занятий в календаре, сохраняя уникальные ID."""
        try:
            logging.info("🔧 Начинаю исправление дублированных ID занятий...")
            
            cal_sheet = self.spreadsheet.worksheet("Календарь занятий")
            data = cal_sheet.get_all_values()
            
            if len(data) < 2:
                logging.info("Календарь занятий пуст или содержит только заголовки")
                return True
            
            headers = data[0]
            rows = data[1:]
            
            # Анализируем существующие ID и находим дубли
            id_counts = {}
            existing_ids = set()
            
            for row in rows:
                if len(row) > 0 and row[0] and str(row[0]).strip():
                    try:
                        lesson_id = int(row[0])
                        id_counts[lesson_id] = id_counts.get(lesson_id, 0) + 1
                        existing_ids.add(lesson_id)
                    except ValueError:
                        pass
            
            # Находим дубли
            duplicates = {id_val: count for id_val, count in id_counts.items() if count > 1}
            
            if not duplicates:
                logging.info("✅ Дублированных ID не найдено")
                return True
            
            logging.info(f"🔍 Найдены дубли ID: {duplicates}")
            
            # Находим следующий доступный ID для переназначения
            max_id = max(existing_ids) if existing_ids else 0
            next_available_id = max_id + 1
            
            # Исправляем дубли (без заголовков - они уже есть в листе)
            fixed_data = []
            used_ids = set()
            
            for row in rows:
                if len(row) > 0:
                    fixed_row = row.copy()
                    
                    try:
                        current_id = int(row[0]) if row[0] and str(row[0]).strip() else None
                        
                        # Если ID уже использован (дубль) или невалидный
                        if current_id is None or current_id in used_ids:
                            # Присваиваем новый уникальный ID
                            while next_available_id in existing_ids or next_available_id in used_ids:
                                next_available_id += 1
                            fixed_row[0] = str(next_available_id)
                            used_ids.add(next_available_id)
                            next_available_id += 1
                            logging.info(f"🔧 Переназначен ID {current_id} → {fixed_row[0]}")
                        else:
                            # ID уникальный, сохраняем его
                            used_ids.add(current_id)
                            
                    except ValueError:
                        # Невалидный ID, присваиваем новый
                        while next_available_id in existing_ids or next_available_id in used_ids:
                            next_available_id += 1
                        fixed_row[0] = str(next_available_id)
                        used_ids.add(next_available_id)
                        next_available_id += 1
                        logging.info(f"🔧 Присвоен новый ID: {fixed_row[0]}")
                    
                    fixed_data.append(fixed_row)
            
            # ИСПРАВЛЕНО: Обновляем только дублированные ID, НЕ пересоздавая всю таблицу
            logging.info("🔒 ЗАЩИТА ID: Обновляем только дублированные ID точечно")
            
            # Обновляем только те строки, где ID был изменен
            updates_made = 0
            for i, (original_row, fixed_row) in enumerate(zip(rows, fixed_data)):
                if original_row[0] != fixed_row[0]:  # ID изменился
                    row_number = i + 2  # +2 потому что строки начинаются с 1, и есть заголовок
                    try:
                        cal_sheet.update_cell(row_number, 1, fixed_row[0])  # Обновляем только столбец A (ID)
                        updates_made += 1
                        logging.info(f"🔧 Обновлен ID в строке {row_number}: {original_row[0]} → {fixed_row[0]}")
                    except Exception as e:
                        logging.error(f"❌ Ошибка обновления строки {row_number}: {e}")
            
            logging.info(f"✅ Точечно обновлено {updates_made} ID (вместо пересоздания всей таблицы)")
            
            logging.info(f"✅ Исправлено {len(duplicates)} типов дублированных ID")
            logging.info(f"📊 Всего занятий: {len(rows)}, уникальных ID: {len(used_ids)}")
            return True
            
        except Exception as e:
            logging.error(f"❌ Ошибка при исправлении дублированных ID: {e}")
            return False

    def _force_remove_duplicates_by_content(self, existing_events):
        """Принудительно удаляет дубли по содержимому (название + дата + время)."""
        try:
            content_groups = {}
            duplicates_removed = 0
            
            # Группируем события по содержимому
            for event_key, event in existing_events.items():
                if event_key == '_all_events':
                    continue
                    
                summary = event.get('summary', '')
                start = event.get('start', {})
                
                # Создаем ключ содержимого
                if 'dateTime' in start:
                    content_key = f"{summary}|{start['dateTime']}"
                elif 'date' in start:
                    content_key = f"{summary}|{start['date']}"
                else:
                    continue
                
                if content_key not in content_groups:
                    content_groups[content_key] = []
                content_groups[content_key].append((event_key, event))
            
            # Удаляем дубли (оставляем самое актуальное - с отметкой)
            for content_key, events_list in content_groups.items():
                if len(events_list) > 1:
                    logging.info(f"Найдены дубли по содержимому '{content_key}': {len(events_list)} событий")
                    
                    # Выбираем самое актуальное событие для сохранения
                    # Приоритет: 1) с непустой отметкой, 2) последнее по времени обновления
                    best_event = None
                    best_event_key = None
                    best_priority = -1
                    
                    for event_key, event in events_list:
                        description = event.get('description', '')
                        mark = ''
                        
                        # Извлекаем отметку из описания
                        for line in description.split('\n'):
                            if line.startswith('Отметка:'):
                                mark = line.split(':', 1)[1].strip()
                                break
                        
                        # Определяем приоритет события
                        priority = 0
                        if mark and mark not in ['', 'N/A']:
                            priority = 2  # Высокий приоритет для событий с отметкой
                        else:
                            priority = 1  # Низкий приоритет для событий без отметки
                        
                        # Если приоритет выше или равен, проверяем время обновления
                        if priority > best_priority:
                            best_event = event
                            best_event_key = event_key
                            best_priority = priority
                        elif priority == best_priority:
                            # Если приоритет одинаковый, выбираем последнее обновленное
                            if best_event:
                                best_updated = best_event.get('updated', '')
                                current_updated = event.get('updated', '')
                                if current_updated > best_updated:
                                    best_event = event
                                    best_event_key = event_key
                            else:
                                best_event = event
                                best_event_key = event_key
                    
                    logging.info(f"✅ Выбрано для сохранения: {best_event.get('summary', 'Без названия')} (приоритет: {best_priority})")
                    
                    # Удаляем все кроме лучшего
                    for event_key, event in events_list:
                        if event_key == best_event_key:
                            continue  # Пропускаем лучшее событие
                        try:
                            self.calendar_service.events().delete(
                                calendarId=config.GOOGLE_CALENDAR_ID,
                                eventId=event['id']
                            ).execute()
                            logging.info(f"Удален дубль события: {event.get('summary', '')}")
                            duplicates_removed += 1
                            
                            # Удаляем из словаря existing_events
                            if event_key in existing_events:
                                del existing_events[event_key]
                                
                        except Exception as e:
                            logging.error(f"Ошибка при удалении дубля: {e}")
            
            return duplicates_removed
            
        except Exception as e:
            logging.error(f"Ошибка при принудительном удалении дублей: {e}")
            return 0

    def auto_sync_calendar_after_changes_DISABLED(self):
        """Автоматическая синхронизация календаря после изменений в Google Sheets."""
        try:
            logging.info("🔄 Запуск автоматической синхронизации календаря...")
            
            # Запускаем синхронизацию в фоне, чтобы не блокировать основные операции
            result = self.sync_with_google_calendar()
            
            if result:
                logging.info("✅ Автоматическая синхронизация календаря завершена успешно")
            else:
                logging.warning("⚠️ Автоматическая синхронизация календаря завершена с ошибками")
                
            return result
            
        except Exception as e:
            logging.error(f"❌ Ошибка при автоматической синхронизации календаря: {e}")
            return False

    def _get_existing_events_map(self, start_date, end_date):
        """
        Получает карту существующих событий из Google Calendar с тегом #schedule_sync.
        Возвращает словарь {event_id: event_object}.
        """
        try:
            events_map = {}
            
            # Проверяем доступность Calendar API
            if not self.calendar_service:
                logging.warning("Calendar API недоступен")
                return {}
            
            # Запрашиваем события из Google Calendar
            events_result = self.calendar_service.service.events().list(
                calendarId=config.GOOGLE_CALENDAR_ID,
                timeMin=start_date.isoformat() + 'Z',
                timeMax=end_date.isoformat() + 'Z',
                singleEvents=True,
                orderBy='startTime'
            ).execute()
            
            events = events_result.get('items', [])
            
            for event in events:
                description = event.get('description', '')
                if '#schedule_sync' in description:
                    event_id = event.get('id')
                    if event_id:
                        events_map[event_id] = event
            
            logging.info(f"Загружено {len(events_map)} существующих событий с тегом #schedule_sync")
            return events_map
            
        except Exception as e:
            logging.error(f"Ошибка при получении существующих событий: {e}")
            return {}

    def _sync_calendar_lessons_DISABLED(self, calendar_lessons, circle_names_map, forecast_map, existing_events_map):
        """
        Синхронизирует занятия из листа 'Календарь занятий' с Google Calendar.
        Использует столбец I 'ID События в Календаре' для отслеживания событий.
        """
        stats = {'created': 0, 'updated': 0, 'errors': 0}
        
        try:
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            all_data = calendar_sheet.get_all_values()
            
            if len(all_data) <= 1:
                logging.info("Нет данных для синхронизации в календаре занятий")
                return stats
            
            headers = all_data[0]
            
            # Находим индексы нужных столбцов
            event_id_col_index = None
            for i, header in enumerate(headers):
                if header == 'ID События в Календаре':
                    event_id_col_index = i
                    break
            
            if event_id_col_index is None:
                logging.error("Не найден столбец 'ID События в Календаре' в листе 'Календарь занятий'")
                return stats
            
            # Обрабатываем каждую строку занятий
            for row_index, lesson in enumerate(calendar_lessons, start=2):  # Начинаем с 2-й строки (после заголовков)
                try:
                    lesson_status = lesson.get('Статус посещения', '').lower()
                    lesson_date = lesson.get('Дата занятия', '')
                    
                    # Синхронизируем ВСЕ занятия с валидными датами (для обновления эмодзи)
                    if not lesson_date or not lesson_date.strip():
                        continue
                    
                    # Получаем ID события из столбца I
                    if row_index - 2 < len(all_data) - 1:
                        event_id = all_data[row_index - 1][event_id_col_index] if event_id_col_index < len(all_data[row_index - 1]) else ''
                    else:
                        event_id = ''
                    
                    if event_id and event_id in existing_events_map:
                        # Сценарий А: ID есть и событие найдено - обновляем
                        result = self._update_existing_lesson_event(lesson, event_id, existing_events_map[event_id], circle_names_map, forecast_map)
                        if result:
                            stats['updated'] += 1
                            # Удаляем из карты - событие обработано
                            del existing_events_map[event_id]
                        else:
                            stats['errors'] += 1
                    
                    elif event_id and event_id not in existing_events_map:
                        # ID есть, но событие не найдено (удалено вручную) - очищаем ID и создаем новое
                        logging.info(f"Событие с ID {event_id} не найдено в календаре, создаем новое")
                        calendar_sheet.update_cell(row_index, event_id_col_index + 1, '')  # Очищаем ID
                        
                        # Создаем новое событие
                        new_event_id = self._create_new_lesson_event(lesson, circle_names_map, forecast_map)
                        if new_event_id:
                            # Записываем новый ID обратно в таблицу
                            calendar_sheet.update_cell(row_index, event_id_col_index + 1, new_event_id)
                            stats['created'] += 1
                        else:
                            stats['errors'] += 1
                    
                    else:
                        # Сценарий Б: ID нет - создаем новое событие
                        new_event_id = self._create_new_lesson_event(lesson, circle_names_map, forecast_map)
                        if new_event_id:
                            # Записываем новый ID в таблицу
                            calendar_sheet.update_cell(row_index, event_id_col_index + 1, new_event_id)
                            stats['created'] += 1
                        else:
                            stats['errors'] += 1
                
                except Exception as e:
                    logging.error(f"Ошибка при обработке занятия в строке {row_index}: {e}")
                    stats['errors'] += 1
            
            logging.info(f"Синхронизация занятий: создано {stats['created']}, обновлено {stats['updated']}, ошибок {stats['errors']}")
            return stats
            
        except Exception as e:
            logging.error(f"Ошибка при синхронизации занятий: {e}")
            return stats

    def _sync_forecast_events(self, forecast_data, existing_events_map):
        """
        Синхронизирует события оплат из листа 'Прогноз' с Google Calendar.
        Использует столбец E 'ID События в Календаре' для отслеживания событий.
        """
        stats = {'created': 0, 'updated': 0, 'errors': 0}
        
        try:
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            all_data = forecast_sheet.get_all_values()
            
            if len(all_data) <= 1:
                logging.info("Нет данных для синхронизации в прогнозе")
                return stats
            
            headers = all_data[0]
            
            # Находим индекс столбца E 'ID События в Календаре'
            event_id_col_index = None
            for i, header in enumerate(headers):
                if header == 'ID События в Календаре':
                    event_id_col_index = i
                    break
            
            if event_id_col_index is None:
                logging.error("Не найден столбец 'ID События в Календаре' в листе 'Прогноз'")
                return stats
            
            # Обрабатываем каждую строку прогноза
            for row_index, forecast in enumerate(forecast_data, start=2):
                try:
                    # Получаем ID события из столбца E
                    if row_index - 2 < len(all_data) - 1:
                        event_id = all_data[row_index - 1][event_id_col_index] if event_id_col_index < len(all_data[row_index - 1]) else ''
                    else:
                        event_id = ''
                    
                    if event_id and event_id in existing_events_map:
                        # ID есть и событие найдено - обновляем
                        result = self._update_existing_forecast_event(forecast, event_id, existing_events_map[event_id])
                        if result:
                            stats['updated'] += 1
                            # Удаляем из карты - событие обработано
                            del existing_events_map[event_id]
                        else:
                            stats['errors'] += 1
                    
                    elif event_id and event_id not in existing_events_map:
                        # ID есть, но событие не найдено - очищаем ID и создаем новое
                        logging.info(f"Событие оплаты с ID {event_id} не найдено в календаре, создаем новое")
                        forecast_sheet.update_cell(row_index, event_id_col_index + 1, '')
                        
                        # Создаем новое событие
                        new_event_id = self._create_new_forecast_event(forecast)
                        if new_event_id:
                            forecast_sheet.update_cell(row_index, event_id_col_index + 1, new_event_id)
                            stats['created'] += 1
                        else:
                            stats['errors'] += 1
                    
                    else:
                        # ID нет - создаем новое событие
                        new_event_id = self._create_new_forecast_event(forecast)
                        if new_event_id:
                            forecast_sheet.update_cell(row_index, event_id_col_index + 1, new_event_id)
                            stats['created'] += 1
                        else:
                            stats['errors'] += 1
                
                except Exception as e:
                    logging.error(f"Ошибка при обработке прогноза в строке {row_index}: {e}")
                    stats['errors'] += 1
            
            logging.info(f"Синхронизация прогнозов: создано {stats['created']}, обновлено {stats['updated']}, ошибок {stats['errors']}")
            return stats
            
        except Exception as e:
            logging.error(f"Ошибка при синхронизации прогнозов: {e}")
            return stats

    def _cleanup_unused_events(self, existing_events_map):
        """
        Удаляет события, которые больше не имеют соответствующих записей в таблицах.
        """
        deleted_count = 0
        
        try:
            for event_id, event in existing_events_map.items():
                try:
                    self.calendar_service.events().delete(
                        calendarId=config.GOOGLE_CALENDAR_ID,
                        eventId=event_id
                    ).execute()
                    
                    deleted_count += 1
                    logging.info(f"Удалено устаревшее событие: {event.get('summary', 'Без названия')}")
                    
                except Exception as e:
                    logging.error(f"Ошибка при удалении события {event_id}: {e}")
            
            return deleted_count
            
        except Exception as e:
            logging.error(f"Ошибка при очистке устаревших событий: {e}")
            return deleted_count

    def _update_existing_lesson_event(self, lesson, event_id, existing_event, circle_names_map, forecast_map):
        """Обновляет существующее событие занятия в Google Calendar."""
        try:
            # Подготавливаем новые данные события
            event_data = self._prepare_lesson_event_data(lesson, circle_names_map, forecast_map)
            if not event_data:
                return False
            
            # Обновляем событие
            updated_event = self.calendar_service.events().update(
                calendarId=config.GOOGLE_CALENDAR_ID,
                eventId=event_id,
                body=event_data
            ).execute()
            
            logging.info(f"✅ Обновлено событие занятия: {updated_event.get('summary')}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при обновлении события занятия {event_id}: {e}")
            return False

    def _update_existing_forecast_event(self, forecast, event_id, existing_event):
        """Обновляет существующее событие оплаты в Google Calendar."""
        try:
            # Подготавливаем данные события оплаты
            event_data = self._prepare_forecast_event_data(forecast)
            if not event_data:
                return False
            
            # Обновляем событие
            updated_event = self.calendar_service.events().update(
                calendarId=config.GOOGLE_CALENDAR_ID,
                eventId=event_id,
                body=event_data
            ).execute()
            
            logging.info(f"✅ Обновлено событие оплаты: {updated_event.get('summary')}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при обновлении события оплаты {event_id}: {e}")
            return False

    def _create_new_lesson_event(self, lesson, circle_names_map, forecast_map):
        """Создает новое событие занятия в Google Calendar и возвращает его ID."""
        try:
            # Подготавливаем данные события
            event_data = self._prepare_lesson_event_data(lesson, circle_names_map, forecast_map)
            if not event_data:
                return None
            
            # Создаем событие
            created_event = self.calendar_service.events().insert(
                calendarId=config.GOOGLE_CALENDAR_ID,
                body=event_data
            ).execute()
            
            event_id = created_event.get('id')
            logging.info(f"✅ Создано новое событие занятия: {created_event.get('summary')} (ID: {event_id})")
            return event_id
            
        except Exception as e:
            logging.error(f"Ошибка при создании события занятия: {e}")
            return None

    def _create_new_forecast_event(self, forecast):
        """Создает новое событие оплаты в Google Calendar и возвращает его ID."""
        try:
            # Подготавливаем данные события оплаты
            event_data = self._prepare_forecast_event_data(forecast)
            if not event_data:
                return None
            
            # Создаем событие на весь день
            created_event = self.calendar_service.events().insert(
                calendarId=config.GOOGLE_CALENDAR_ID,
                body=event_data
            ).execute()
            
            event_id = created_event.get('id')
            logging.info(f"✅ Создано новое событие оплаты: {created_event.get('summary')} (ID: {event_id})")
            return event_id
            
        except Exception as e:
            logging.error(f"Ошибка при создании события оплаты: {e}")
            return None

    def _prepare_lesson_event_data(self, lesson, circle_names_map, forecast_map):
        """Подготавливает данные для события занятия."""
        try:
            from datetime import datetime
            
            # Извлекаем данные занятия
            lesson_id = lesson.get('№', '')
            sub_id = lesson.get('ID абонемента', '')
            child_name = lesson.get('Ребенок', '')
            lesson_date = lesson.get('Дата занятия', '')
            lesson_mark = lesson.get('Отметка', '')
            lesson_status = lesson.get('Статус посещения', '')
            start_time = lesson.get('Время начала', '')
            end_time = lesson.get('Время завершения', '')
            
            # Получаем название кружка
            circle_name = circle_names_map.get(sub_id, 'Неизвестный кружок')
            
            # Проверяем, есть ли оплата на эту дату
            date_key = lesson_date.replace('.', '')
            if len(date_key) == 8:  # ddmmyyyy
                formatted_date = f"{date_key[0:2]}.{date_key[2:4]}.{date_key[4:8]}"
            else:
                formatted_date = lesson_date
            
            payment_key = f"{child_name}|{circle_name}|{formatted_date}"
            is_payment_day = payment_key in forecast_map
            
            # Получаем эмодзи для статуса
            status_emoji = self._get_status_emoji(lesson_mark, lesson_status, is_payment_day)
            
            # Формируем данные события с ПОЛНЫМИ переменными для синхронизации
            if is_payment_day:
                title = f"{status_emoji}ОПЛАТА: {circle_name} - {child_name}"
                description = f"Требуется оплата за следующий абонемент: {forecast_map[payment_key]} руб.\n\n"
            else:
                title = f"{status_emoji}{circle_name} - {child_name}".strip()
                description = ""
                if lesson_mark:
                    description = f"Отметка: {lesson_mark}\n\n"
            
            # ДОБАВЛЯЕМ ВСЕ ПЕРЕМЕННЫЕ ДЛЯ СИНХРОНИЗАЦИИ
            description += f"lesson_id:{lesson_id}\n"
            description += f"date:{lesson_date}\n"
            description += f"start_time:{start_time}\n"
            description += f"end_time:{end_time}\n"
            description += f"status:{lesson_status}\n"
            description += f"mark:{lesson_mark}\n"
            description += f"#schedule_sync"
            
            # Парсим дату и время
            lesson_date_obj = datetime.strptime(lesson_date, '%d.%m.%Y')
            
            # Формируем время начала и окончания
            if start_time and start_time.strip():
                try:
                    start_hour, start_minute = map(int, start_time.split(':'))
                    start_datetime = lesson_date_obj.replace(hour=start_hour, minute=start_minute)
                except:
                    start_datetime = lesson_date_obj.replace(hour=9, minute=0)
            else:
                start_datetime = lesson_date_obj.replace(hour=9, minute=0)
            
            if end_time and end_time.strip():
                try:
                    end_hour, end_minute = map(int, end_time.split(':'))
                    end_datetime = lesson_date_obj.replace(hour=end_hour, minute=end_minute)
                except:
                    end_datetime = start_datetime.replace(hour=start_datetime.hour + 1)
            else:
                end_datetime = start_datetime.replace(hour=start_datetime.hour + 1)
            
            # Формируем данные события
            event_data = {
                'summary': title,
                'description': description,
                'start': {
                    'dateTime': start_datetime.isoformat(),
                    'timeZone': 'Asia/Almaty',
                },
                'end': {
                    'dateTime': end_datetime.isoformat(),
                    'timeZone': 'Asia/Almaty',
                },
            }
            
            return event_data
            
        except Exception as e:
            logging.error(f"Ошибка при подготовке данных события занятия: {e}")
            return None

    def _prepare_forecast_event_data(self, forecast):
        """Подготавливает данные для события оплаты (на весь день)."""
        try:
            from datetime import datetime
            
            child_name = forecast.get('Ребенок', '')
            circle_name = forecast.get('Кружок', '')
            payment_date = forecast.get('Дата оплаты', '')
            budget = str(forecast.get('Бюджет', ''))
            status = forecast.get('Статус', 'ожидается')
            
            # Парсим дату оплаты
            payment_date_obj = datetime.strptime(payment_date, '%d.%m.%Y')
            
            # Формируем описание с ПОЛНЫМИ переменными для синхронизации
            description = f"Требуется оплата за следующий абонемент: {budget} руб.\n\n"
            description += f"circle_name:{circle_name}\n"
            description += f"child_name:{child_name}\n"
            description += f"date:{payment_date}\n"
            description += f"budget:{budget}\n"
            description += f"status:{status}\n"
            description += f"#schedule_sync"
            
            # Формируем данные события на весь день
            event_data = {
                'summary': f"💰ОПЛАТА: {circle_name} - {child_name}",
                'description': description,
                'start': {
                    'date': payment_date_obj.strftime('%Y-%m-%d'),
                },
                'end': {
                    'date': payment_date_obj.strftime('%Y-%m-%d'),
                },
            }
            
            return event_data
            
        except Exception as e:
            logging.error(f"Ошибка при подготовке данных события оплаты: {e}")
            return None

    def update_single_lesson_in_calendar_DISABLED(self, lesson_id):
        """Google Calendar синхронизация отключена."""
        logging.info("Google Calendar синхронизация отключена")
        return True

    def update_single_lesson_in_calendar_DISABLED_OLD(self, lesson_id):
        """
        Обновляет только конкретное событие в Google Календаре по lesson_id.
        Использует новую логику с ID событий из столбца I.
        """
        try:
            logging.info(f"🔄 Обновление события для занятия ID {lesson_id} (новая логика)...")
            
            if not config.GOOGLE_CALENDAR_ID:
                logging.error("GOOGLE_CALENDAR_ID не указан в конфигурации")
                return False
            
            # Получаем данные конкретного занятия
            lesson_data = self.get_lesson_info_by_id(lesson_id)
            if not lesson_data:
                logging.error(f"Занятие с ID {lesson_id} не найдено")
                return False
            
            # Получаем ID события из столбца I "ID События в Календаре"
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            all_data = calendar_sheet.get_all_values()
            
            if len(all_data) <= 1:
                logging.error("Нет данных в календаре занятий")
                return False
            
            headers = all_data[0]
            
            # Находим индекс столбца I "ID События в Календаре"
            event_id_col_index = None
            for i, header in enumerate(headers):
                if header == 'ID События в Календаре':
                    event_id_col_index = i
                    break
            
            if event_id_col_index is None:
                logging.error("Не найден столбец 'ID События в Календаре' в листе 'Календарь занятий'")
                return False
            
            # Ищем строку с нужным lesson_id
            lesson_row_index = None
            event_id = None
            
            for row_index, row in enumerate(all_data[1:], start=2):  # Пропускаем заголовки
                if len(row) > 0 and str(row[0]).strip() == str(lesson_id).strip():  # Столбец A - № (ID занятия)
                    lesson_row_index = row_index
                    if event_id_col_index < len(row):
                        event_id = row[event_id_col_index].strip()
                    break
            
            if lesson_row_index is None:
                logging.error(f"Не найдена строка для занятия ID {lesson_id}")
                return False
            
            # Создаем карты для подготовки данных события
            subscriptions_data = self._get_subscriptions_data()
            forecast_data = self._get_forecast_data()
            circle_names_map = self._create_circle_names_map(subscriptions_data)
            forecast_map = self._create_forecast_map(forecast_data)
            
            if event_id:
                # ID события есть - обновляем существующее событие
                try:
                    # Получаем событие из Google Calendar
                    existing_event = self.calendar_service.events().get(
                        calendarId=config.GOOGLE_CALENDAR_ID,
                        eventId=event_id
                    ).execute()
                    
                    # Обновляем событие
                    result = self._update_existing_lesson_event(lesson_data, event_id, existing_event, circle_names_map, forecast_map)
                    if result:
                        logging.info(f"✅ Событие для занятия {lesson_id} успешно обновлено")
                        return True
                    else:
                        logging.error(f"❌ Не удалось обновить событие для занятия {lesson_id}")
                        return False
                        
                except Exception as e:
                    if 'not found' in str(e).lower():
                        # Событие не найдено - очищаем ID и создаем новое
                        logging.info(f"Событие с ID {event_id} не найдено, создаем новое")
                        calendar_sheet.update_cell(lesson_row_index, event_id_col_index + 1, '')
                        
                        # Создаем новое событие
                        new_event_id = self._create_new_lesson_event(lesson_data, circle_names_map, forecast_map)
                        if new_event_id:
                            calendar_sheet.update_cell(lesson_row_index, event_id_col_index + 1, new_event_id)
                            logging.info(f"✅ Создано новое событие для занятия {lesson_id} (ID: {new_event_id})")
                            return True
                        else:
                            logging.error(f"❌ Не удалось создать новое событие для занятия {lesson_id}")
                            return False
                    else:
                        logging.error(f"❌ Ошибка при получении события {event_id}: {e}")
                        return False
            else:
                # ID события нет - создаем новое событие
                new_event_id = self._create_new_lesson_event(lesson_data, circle_names_map, forecast_map)
                if new_event_id:
                    calendar_sheet.update_cell(lesson_row_index, event_id_col_index + 1, new_event_id)
                    logging.info(f"✅ Создано новое событие для занятия {lesson_id} (ID: {new_event_id})")
                    return True
                else:
                    logging.error(f"❌ Не удалось создать новое событие для занятия {lesson_id}")
                    return False
                
        except Exception as e:
            logging.error(f"❌ Ошибка при обновлении события занятия {lesson_id}: {e}")
            return False

    def _prepare_single_lesson_event_data(self, lesson_data):
        """Подготавливает данные для обновления одного события занятия."""
        try:
            from datetime import datetime
            
            # Извлекаем данные занятия
            lesson_id = lesson_data.get('№', '')
            sub_id = lesson_data.get('ID абонемента', '')
            child_name = lesson_data.get('Ребенок', '')
            lesson_date = lesson_data.get('Дата занятия', '')
            lesson_mark = lesson_data.get('Отметка', '')
            lesson_status = lesson_data.get('Статус посещения', '')
            start_time = lesson_data.get('Время начала', '')
            end_time = lesson_data.get('Время завершения', '')
            
            # Получаем название кружка из абонемента
            sub_details = self.get_subscription_details(sub_id)
            circle_name = sub_details.get('circle_name', 'Неизвестный кружок') if sub_details else 'Неизвестный кружок'
            
            # Получаем эмодзи для статуса
            status_emoji = self._get_status_emoji(lesson_mark, lesson_status, False)
            
            # Формируем заголовок события
            title = f"{status_emoji}{circle_name} - {child_name}".strip()
            
            # Формируем описание с lesson_id
            description = f"lesson_id:{lesson_id}\n#schedule_sync"
            if lesson_mark:
                description = f"Отметка: {lesson_mark}\n\n{description}"
            
            # Парсим дату занятия
            try:
                lesson_date_obj = datetime.strptime(lesson_date, '%d.%m.%Y')
            except ValueError:
                logging.error(f"Неверный формат даты: {lesson_date}")
                return None
            
            # Формируем время начала и окончания
            if start_time and start_time.strip():
                try:
                    start_hour, start_minute = map(int, start_time.split(':'))
                    start_datetime = lesson_date_obj.replace(hour=start_hour, minute=start_minute)
                except:
                    start_datetime = lesson_date_obj.replace(hour=9, minute=0)
            else:
                start_datetime = lesson_date_obj.replace(hour=9, minute=0)
            
            if end_time and end_time.strip():
                try:
                    end_hour, end_minute = map(int, end_time.split(':'))
                    end_datetime = lesson_date_obj.replace(hour=end_hour, minute=end_minute)
                except:
                    end_datetime = start_datetime.replace(hour=start_datetime.hour + 1)
            else:
                end_datetime = start_datetime.replace(hour=start_datetime.hour + 1)
            
            # Формируем данные события
            event_data = {
                'summary': title,
                'description': description,
                'start': {
                    'dateTime': start_datetime.isoformat(),
                    'timeZone': 'Asia/Almaty',
                },
                'end': {
                    'dateTime': end_datetime.isoformat(),
                    'timeZone': 'Asia/Almaty',
                },
            }
            
            logging.info(f"📝 Подготовлены данные события: {title}")
            return event_data
            
        except Exception as e:
            logging.error(f"❌ Ошибка при подготовке данных события: {e}")
            return None

    def get_subscription_full_stats(self, sub_id):
        """Получает полную статистику абонемента из всех листов."""
        try:
            stats = {
                'subscription': {},
                'schedule_template': [],
                'calendar_lessons': [],
                'forecast_payments': []
            }
            
            # 1. Данные абонемента
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            subs_data = subs_sheet.get_all_records()
            for sub in subs_data:
                if str(sub.get('ID абонемента', '')).strip() == str(sub_id).strip():
                    stats['subscription'] = sub
                    break
            
            # 2. Шаблон расписания
            template_sheet = self.spreadsheet.worksheet("Шаблон расписания")
            template_data = template_sheet.get_all_records()
            for template in template_data:
                if str(template.get('ID абонемента', '')).strip() == str(sub_id).strip():
                    stats['schedule_template'].append(template)
            
            # 3. Календарь занятий
            calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
            calendar_data = calendar_sheet.get_all_records()
            for lesson in calendar_data:
                if str(lesson.get('ID абонемента', '')).strip() == str(sub_id).strip():
                    stats['calendar_lessons'].append(lesson)
            
            # 4. Прогноз оплат
            if stats['subscription']:
                child_name = stats['subscription'].get('Ребенок', '')
                circle_name = stats['subscription'].get('Кружок', '')
                
                forecast_sheet = self.spreadsheet.worksheet("Прогноз")
                forecast_data = forecast_sheet.get_all_records()
                for forecast in forecast_data:
                    if (str(forecast.get('Ребенок', '')).strip() == str(child_name).strip() and
                        str(forecast.get('Кружок', '')).strip() == str(circle_name).strip()):
                        stats['forecast_payments'].append(forecast)
            
            return stats
            
        except Exception as e:
            logging.error(f"Ошибка при получении статистики абонемента {sub_id}: {e}")
            return None

    def get_notification_time(self):
        """Получает настроенное время уведомлений из ячейки N2 листа Справочник."""
        try:
            handbook_sheet = self.spreadsheet.worksheet("Справочник")
            notification_time = handbook_sheet.acell('N2').value
            
            if notification_time and notification_time.strip():
                return notification_time.strip()
            else:
                return None
                
        except Exception as e:
            logging.error(f"Ошибка при получении времени уведомлений: {e}")
            return None
    
    def set_notification_time(self, time_str):
        """Устанавливает время уведомлений в ячейку N2 листа Справочник."""
        try:
            handbook_sheet = self.spreadsheet.worksheet("Справочник")
            # Исправляем: передаем список значений вместо строки
            handbook_sheet.update('N2', [[time_str]])
            logging.info(f"Время уведомлений установлено: {time_str}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при установке времени уведомлений: {e}")
            return False
    
    def set_notification_chat_id(self, chat_id):
        """Устанавливает chat_id для уведомлений в ячейку O2 листа Справочник."""
        try:
            handbook_sheet = self.spreadsheet.worksheet("Справочник")
            # Исправляем: передаем список значений вместо строки
            handbook_sheet.update('O2', [[str(chat_id)]])
            logging.info(f"Chat ID для уведомлений установлен: {chat_id}")
            return True
            
        except Exception as e:
            logging.error(f"Ошибка при установке chat_id уведомлений: {e}")
            return False
    
    def get_notification_chat_id(self):
        """Получает chat_id для уведомлений из ячейки O2 листа Справочник."""
        try:
            handbook_sheet = self.spreadsheet.worksheet("Справочник")
            chat_id = handbook_sheet.acell('O2').value
            
            if chat_id and chat_id.strip():
                return chat_id.strip()
            else:
                return None
                
        except Exception as e:
            logging.error(f"Ошибка при получении chat_id уведомлений: {e}")
            return None

    def get_weekly_summary(self):
        """Получает сводку на текущую неделю."""
        try:
            from datetime import datetime, timedelta
            import time
            
            # Добавляем задержку для снижения нагрузки на API
            time.sleep(2)
            
            logging.info("🔄 Начинаю получение еженедельной сводки...")
            
            # Определяем текущую неделю (понедельник - воскресенье)
            # ВАЖНО: Обнуляем время для корректного сравнения дат
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            monday = today - timedelta(days=today.weekday())
            sunday = monday + timedelta(days=6)
            
            logging.info(f"📅 Период недели: {monday.strftime('%d.%m.%Y')} - {sunday.strftime('%d.%m.%Y')}")
            
            summary = {
                'week_start': monday.strftime('%d.%m.%Y'),
                'week_end': sunday.strftime('%d.%m.%Y'),
                'lessons_this_week': [],
                'payments_this_week': [],
                'attendance_stats': {},
                'total_budget': 0
            }
            
            # Получаем занятия на эту неделю (принудительно обновляем данные)
            try:
                calendar_sheet = self.spreadsheet.worksheet("Календарь занятий")
                
                # Принудительно обновляем данные из Google Sheets (очищаем кэш)
                try:
                    # Сначала получаем сырые данные для принудительного обновления
                    raw_data = calendar_sheet.get_all_values()
                    # Затем получаем структурированные данные
                    calendar_data = calendar_sheet.get_all_records()
                    logging.info(f"📋 Принудительно загружено {len(calendar_data)} записей из Календаря занятий")
                except Exception as e:
                    if "429" in str(e) or "Quota exceeded" in str(e):
                        logging.warning("⚠️ Превышена квота Google Sheets API при получении календаря. Возвращаю пустую сводку.")
                        return None
                    logging.error(f"❌ Ошибка при загрузке календаря: {e}")
                    # Fallback - пробуем еще раз
                    calendar_data = calendar_sheet.get_all_records()
                    logging.info(f"📋 Загружено {len(calendar_data)} записей из Календаря занятий (fallback)")
                
                # Получаем данные абонементов для определения кружков
                subs_sheet = self.spreadsheet.worksheet("Абонементы")
                subs_data = subs_sheet.get_all_records()
            except Exception as e:
                if "429" in str(e) or "Quota exceeded" in str(e):
                    logging.warning("⚠️ Превышена квота Google Sheets API при получении еженедельной сводки. Возвращаю пустую сводку.")
                    return None
                else:
                    raise e
            subs_dict = {str(sub.get('ID абонемента', '')): sub for sub in subs_data}
            logging.info(f"📋 Загружено {len(subs_data)} абонементов")
            
            for lesson in calendar_data:
                lesson_date_str = lesson.get('Дата занятия', '')
                lesson_mark = lesson.get('Отметка', '').strip()  # Столбец G - отметка
                lesson_status = lesson.get('Статус посещения', '').strip()  # Столбец E - статус
                
                if lesson_date_str:
                    try:
                        lesson_date = datetime.strptime(lesson_date_str, '%d.%m.%Y')
                        if monday <= lesson_date <= sunday:
                            sub_id = str(lesson.get('ID абонемента', ''))
                            sub_info = subs_dict.get(sub_id, {})
                            
                            lesson_info = {
                                'date': lesson_date_str,
                                'child': sub_info.get('Ребенок', lesson.get('Ребенок', '')),
                                'circle': sub_info.get('Кружок', 'N/A'),
                                'time': f"{lesson.get('Время начала', '')}-{lesson.get('Время окончания', '')}",
                                'mark': lesson_mark,  # Столбец G
                                'status': lesson_status,  # Столбец E
                                'sub_id': sub_id
                            }
                            
                            summary['lessons_this_week'].append(lesson_info)
                            logging.info(f"📅 Найдено занятие: {lesson_date_str} - {lesson_info['child']} ({lesson_info['circle']}) - Статус E: '{lesson_status}', Отметка G: '{lesson_mark}'")
                    except ValueError:
                        continue
            
            logging.info(f"📊 Всего занятий на неделю: {len(summary['lessons_this_week'])}")
            
            # Получаем прогноз оплат на эту неделю
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            forecast_data = forecast_sheet.get_all_records()
            
            for payment in forecast_data:
                payment_date_str = payment.get('Дата оплаты', '')
                if payment_date_str:
                    try:
                        payment_date = datetime.strptime(payment_date_str, '%d.%m.%Y')
                        if monday <= payment_date <= sunday:
                            budget = float(payment.get('Бюджет', 0) or 0)
                            summary['payments_this_week'].append({
                                'date': payment_date_str,
                                'child': payment.get('Ребенок', ''),
                                'circle': payment.get('Кружок', ''),
                                'amount': budget
                            })
                            summary['total_budget'] += budget
                    except (ValueError, TypeError):
                        continue
            
            # Рассчитываем статистику посещаемости согласно новым правилам
            total_lessons = len(summary['lessons_this_week'])
            
            # ✅ Посещено: столбец G со значением "Посещение"
            attended = sum(1 for l in summary['lessons_this_week'] 
                          if l['mark'].lower() == 'посещение')
            
            # ❌ Пропущено: столбец G со значениями "Пропуск (по вине)", "Отмена (болезнь)", "Перенос"
            missed_marks = ['пропуск (по вине)', 'отмена (болезнь)', 'перенос']
            missed = sum(1 for l in summary['lessons_this_week'] 
                        if l['mark'].lower() in missed_marks)
            
            # 📅 Запланировано: столбец E со значением "Запланировано"
            planned = sum(1 for l in summary['lessons_this_week'] 
                         if l['status'].lower() == 'запланировано')
            
            # 📊 Посещаемость: Посещено / (Посещено + Пропущено) * 100%
            # Показывает процент посещенных занятий от фактически проведенных (посещено + пропущено)
            total_actual = attended + missed  # Фактически проведенные занятия
            attendance_rate = round((attended / max(total_actual, 1)) * 100, 1) if total_actual > 0 else 0
            
            logging.info(f"📊 Статистика посещаемости (исправленная логика):")
            logging.info(f"  • Всего занятий за период: {total_lessons}")
            logging.info(f"  • ✅ Посещено (G='Посещение'): {attended}")
            logging.info(f"  • ❌ Пропущено (G='Пропуск/Отмена/Перенос'): {missed}")
            logging.info(f"  • 📅 Запланировано (E='Запланировано'): {planned}")
            logging.info(f"  • 📊 Посещаемость: {attended}/{total_actual} = {attendance_rate}% (Посещено/Фактически проведенные)")
            
            # Логируем статусы всех занятий для отладки
            for l in summary['lessons_this_week']:
                logging.info(f"  📅 {l['date']} - {l['child']}: E='{l['status']}', G='{l['mark']}'")
            
            summary['attendance_stats'] = {
                'total': total_lessons,
                'attended': attended,
                'missed': missed,
                'planned': planned,
                'attendance_rate': attendance_rate
            }
            
            # Добавляем информацию об активных абонементах
            summary['active_subscriptions'] = self.get_active_subscriptions_info()
            
            logging.info(f"✅ Еженедельная сводка готова")
            
            return summary
            
        except Exception as e:
            logging.error(f"Ошибка при получении еженедельной сводки: {e}")
            return None

    def get_active_subscriptions_info(self):
        """Получает информацию об активных абонементах с датами оплат."""
        try:
            logging.info("📋 Получение информации об активных абонементах...")
            
            # Получаем данные абонементов
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            subs_data = subs_sheet.get_all_records()
            
            # Получаем данные прогноза для дат оплат
            forecast_sheet = self.spreadsheet.worksheet("Прогноз")
            forecast_data = forecast_sheet.get_all_records()
            
            # Создаем словарь прогнозов по ребенку и кружку
            forecast_dict = {}
            for forecast in forecast_data:
                child = str(forecast.get('Ребенок', '')).strip()
                circle = str(forecast.get('Кружок', '')).strip()
                payment_date = str(forecast.get('Дата оплаты', '')).strip()
                
                if child and circle and payment_date:
                    key = f"{child}_{circle}"
                    if key not in forecast_dict:
                        forecast_dict[key] = []
                    forecast_dict[key].append(payment_date)
            
            active_subs = []
            
            for sub in subs_data:
                status = str(sub.get('Статус', '')).strip()
                remaining_lessons = sub.get('Осталось занятий', 0)
                
                # Считаем активными абонементы со статусом "Активен" или с оставшимися занятиями > 0
                if status.lower() == 'активен' or (remaining_lessons and int(remaining_lessons) > 0):
                    child = str(sub.get('Ребенок', '')).strip()
                    circle = str(sub.get('Кружок', '')).strip()
                    
                    # Получаем данные из правильных столбцов
                    completed_lessons = sub.get('Прошло занятий', 0)  # Столбец H
                    remaining = sub.get('Осталось занятий', 0)  # Столбец I
                    
                    # Вычисляем общее количество: прошло + осталось
                    completed = int(completed_lessons) if completed_lessons else 0
                    remaining_int = int(remaining) if remaining else 0
                    total_lessons = completed + remaining_int
                    
                    # Ищем дату оплаты в прогнозе
                    key = f"{child}_{circle}"
                    payment_dates = forecast_dict.get(key, [])
                    next_payment = payment_dates[0] if payment_dates else "Не указана"
                    
                    # Если несколько дат, берем ближайшую
                    if len(payment_dates) > 1:
                        try:
                            from datetime import datetime
                            today = datetime.now()
                            future_dates = []
                            
                            for date_str in payment_dates:
                                try:
                                    date_obj = datetime.strptime(date_str, '%d.%m.%Y')
                                    if date_obj >= today:
                                        future_dates.append((date_obj, date_str))
                                except:
                                    continue
                            
                            if future_dates:
                                future_dates.sort(key=lambda x: x[0])
                                next_payment = future_dates[0][1]
                        except:
                            pass
                    
                    sub_info = {
                        'child': child,
                        'circle': circle,
                        'total_lessons': total_lessons,
                        'remaining_lessons': remaining_int,
                        'completed_lessons': completed,
                        'next_payment_date': next_payment,
                        'status': status
                    }
                    
                    active_subs.append(sub_info)
                    logging.info(f"📋 Активный абонемент: {child} - {circle} ({remaining} занятий осталось, оплата: {next_payment})")
            
            logging.info(f"📊 Найдено {len(active_subs)} активных абонементов")
            return active_subs
            
        except Exception as e:
            logging.error(f"❌ Ошибка при получении информации об активных абонементах: {e}")
            return []

    def clean_duplicate_events(self):
        """Публичная функция для очистки дублированных событий в Google Calendar."""
        try:
            logging.info("🧹 Начинаю очистку дублированных событий в Google Calendar...")
            
            # Проверяем доступность Calendar API
            if not hasattr(self, 'calendar_service') or not self.calendar_service:
                logging.warning("⚠️ Calendar API недоступен - пропускаю очистку дублей")
                return "⚠️ Calendar API недоступен - очистка дублей пропущена"
            
            # Получаем все существующие события за последние 6 месяцев
            from datetime import datetime, timedelta
            now = datetime.now()
            start_date = now - timedelta(days=180)  # 6 месяцев назад
            end_date = now + timedelta(days=180)    # 6 месяцев вперед
            
            existing_events_map = self._get_existing_events_map(start_date, end_date)
            if not existing_events_map:
                logging.info("ℹ️ События в календаре не найдены")
                return "ℹ️ События в календаре не найдены - очистка не требуется"
            
            # Запускаем очистку дублей
            duplicates_removed = self._remove_duplicate_events(list(existing_events_map.values()))
            
            if duplicates_removed > 0:
                result_message = f"✅ Очистка завершена успешно! Удалено дублей: {duplicates_removed}"
                logging.info(result_message)
                return result_message
            else:
                result_message = "ℹ️ Дублированные события не найдены - календарь чист"
                logging.info(result_message)
                return result_message
                
        except Exception as e:
            error_message = f"❌ Ошибка при очистке дублированных событий: {str(e)}"
            logging.error(error_message)
            return error_message

    def validate_subscription_data_consistency(self, subscription_id=None):
        """
        Проверяет и исправляет сходимость данных между календарем и абонементами.
        Если subscription_id указан, проверяет только этот абонемент.
        Иначе проверяет все абонементы.
        """
        try:
            logging.info(f"🔍 Проверка сходимости данных для {'всех абонементов' if not subscription_id else f'абонемента {subscription_id}'}")
            
            # Получаем данные из календаря
            cal_sheet = self.spreadsheet.worksheet("Календарь занятий")
            cal_data = cal_sheet.get_all_values()
            
            if len(cal_data) <= 1:
                return "❌ Нет данных в календаре занятий"
            
            # Группируем по ID абонемента
            calendar_stats = {}
            for i, row in enumerate(cal_data[1:], 2):
                if len(row) > 6:
                    sub_id = row[1] if len(row) > 1 else ''
                    status = row[4] if len(row) > 4 else ''
                    
                    if sub_id and (not subscription_id or sub_id == subscription_id):
                        if sub_id not in calendar_stats:
                            calendar_stats[sub_id] = {'zaversheno': 0, 'zaplanirovanno': 0, 'propusk': 0}
                        
                        status_lower = status.lower()
                        if status_lower == 'завершен':
                            calendar_stats[sub_id]['zaversheno'] += 1
                        elif status_lower == 'запланировано':
                            calendar_stats[sub_id]['zaplanirovanno'] += 1
                        elif status_lower == 'пропуск':
                            calendar_stats[sub_id]['propusk'] += 1
            
            # Получаем данные из листа абонементов
            subs_sheet = self.spreadsheet.worksheet("Абонементы")
            subs_data = subs_sheet.get_all_values()
            
            if len(subs_data) <= 1:
                return "❌ Нет данных в листе абонементов"
            
            # Проверяем и исправляем
            fixed_count = 0
            checked_count = 0
            
            for sub_id in calendar_stats:
                cal_stats = calendar_stats[sub_id]
                
                # Ожидаемые значения
                expected_h = cal_stats['zaversheno']
                expected_i = cal_stats['zaplanirovanno']
                expected_m = cal_stats['propusk']
                
                # Ищем абонемент в листе
                for i, row in enumerate(subs_data[1:], 2):
                    if len(row) > 1 and row[1] == sub_id:
                        current_h = row[7] if len(row) > 7 else ''
                        current_i = row[8] if len(row) > 8 else ''
                        current_m = row[12] if len(row) > 12 else ''
                        
                        # Проверяем сходимость
                        h_match = str(current_h) == str(expected_h)
                        i_match = str(current_i) == str(expected_i)
                        m_match = str(current_m) == str(expected_m)
                        
                        checked_count += 1
                        
                        # Проверяем статус абонемента (столбец J)
                        current_status = row[9] if len(row) > 9 else ''  # J - Статус
                        expected_status = 'Завершен' if expected_i == 0 else 'Активен'
                        status_match = str(current_status).strip().lower() == expected_status.lower()
                        
                        if not (h_match and i_match and m_match and status_match):
                            logging.info(f"🔄 Исправляю данные для {sub_id}:")
                            logging.info(f"   H={current_h}→{expected_h}, I={current_i}→{expected_i}, M={current_m}→{expected_m}, J={current_status}→{expected_status}")
                            
                            # Исправляем данные
                            if not h_match and len(row) > 7:
                                subs_sheet.update_cell(i, 8, expected_h)  # H - столбец 8
                            if not i_match and len(row) > 8:
                                subs_sheet.update_cell(i, 9, expected_i)  # I - столбец 9
                            if not m_match and len(row) > 12:
                                subs_sheet.update_cell(i, 13, expected_m)  # M - столбец 13
                            if not status_match and len(row) > 9:
                                subs_sheet.update_cell(i, 10, expected_status)  # J - столбец 10
                            
                            fixed_count += 1
                        break
            
            if fixed_count > 0:
                logging.info(f"✅ Исправлено {fixed_count} из {checked_count} абонементов")
                return f"✅ Исправлено {fixed_count} абонементов"
            else:
                logging.info(f"✅ Все {checked_count} абонементов имеют правильные данные")
                return f"✅ Все данные корректны ({checked_count} абонементов)"
                
        except Exception as e:
            logging.error(f"❌ Ошибка при проверке сходимости: {e}")
            return f"❌ Ошибка: {e}"

# Глобальный экземпляр сервиса с retry механизмом
def initialize_sheets_service_with_retry():
    """Инициализация Google Sheets с повторными попытками при ошибке 429"""
    import time
    max_retries = 5
    base_delay = 60  # 60 секунд базовая задержка
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Попытка {attempt + 1}/{max_retries} инициализации Google Sheets...")
            service = GoogleSheetsService(config.GOOGLE_CREDENTIALS_PATH, config.GOOGLE_SHEET_NAME)
            logging.info("✅ Google Sheets успешно инициализирован")
            return service
            
        except Exception as e:
            if "429" in str(e) or "Quota exceeded" in str(e):
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Экспоненциальная задержка
                    logging.warning(f"⚠️ Ошибка 429 при инициализации Google Sheets. Повтор через {delay} секунд...")
                    time.sleep(delay)
                    continue
                else:
                    logging.error("❌ Превышено максимальное количество попыток инициализации Google Sheets")
                    return None
            else:
                logging.error(f"❌ Критическая ошибка инициализации Google Sheets: {e}")
                return None
    
    return None

# Инициализируем сервис
sheets_service = initialize_sheets_service_with_retry()

if sheets_service is None:
    logging.critical("❌ Не удалось инициализировать GoogleSheetsService после всех попыток")
else:
    logging.info("✅ GoogleSheetsService успешно инициализирован")

