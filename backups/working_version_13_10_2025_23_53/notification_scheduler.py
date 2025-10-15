import asyncio
import logging
from datetime import datetime, time, timedelta
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from google_sheets_service import sheets_service
import config

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NotificationScheduler:
    """Планировщик уведомлений о занятиях"""
    
    def __init__(self, bot: Bot):
        self.bot = bot
        self.chat_id = None  # Будет установлен динамически
        self.is_running = False
        self.current_task = None
        
    async def start_scheduler(self):
        """Запускает планировщик уведомлений"""
        if self.is_running:
            logging.info("📅 Планировщик уведомлений уже запущен")
            return
            
        self.is_running = True
        logging.info("🚀 Запуск планировщика уведомлений")
        
        # Запускаем основной цикл планировщика
        self.current_task = asyncio.create_task(self._scheduler_loop())
        
    async def stop_scheduler(self):
        """Останавливает планировщик уведомлений"""
        if not self.is_running:
            return
            
        self.is_running = False
        if self.current_task:
            self.current_task.cancel()
            try:
                await self.current_task
            except asyncio.CancelledError:
                pass
                
        logging.info("⏹️ Планировщик уведомлений остановлен")
        
    async def _scheduler_loop(self):
        """Основной цикл планировщика"""
        while self.is_running:
            try:
                # Получаем настроенное время уведомлений
                notification_time = self._get_notification_time()
                
                if notification_time:
                    # Проверяем, пора ли отправлять уведомления
                    if self._is_notification_time(notification_time):
                        await self._send_daily_notifications()
                        
                        # Ждем до следующего дня, чтобы не отправлять дубли
                        await asyncio.sleep(3600)  # Ждем час
                
                # Проверяем каждые 5 минут
                await asyncio.sleep(300)
                
            except Exception as e:
                logging.error(f"❌ Ошибка в планировщике уведомлений: {e}")
                await asyncio.sleep(300)  # Ждем 5 минут перед повтором
                
    def _get_notification_time(self) -> str:
        """Получает настроенное время уведомлений из Справочника"""
        try:
            # Получаем время из ячейки N2 листа "Справочник"
            handbook_sheet = sheets_service.spreadsheet.worksheet("Справочник")
            notification_time = handbook_sheet.acell('N2').value
            
            if notification_time and notification_time.strip():
                logging.info(f"⏰ Настроенное время уведомлений: {notification_time}")
                return notification_time.strip()
            else:
                logging.info("⏰ Время уведомлений не настроено")
                return None
                
        except Exception as e:
            logging.error(f"❌ Ошибка при получении времени уведомлений: {e}")
            return None
            
    def _is_notification_time(self, notification_time: str) -> bool:
        """Проверяет, пора ли отправлять уведомления"""
        try:
            # Парсим время из строки (формат: "HH:MM")
            target_time = datetime.strptime(notification_time, "%H:%M").time()
            current_time = datetime.now().time()
            
            # Проверяем, что текущее время в пределах 5 минут от целевого
            current_minutes = current_time.hour * 60 + current_time.minute
            target_minutes = target_time.hour * 60 + target_time.minute
            
            diff = abs(current_minutes - target_minutes)
            
            # Если разница меньше 5 минут, то пора отправлять
            return diff <= 5
            
        except Exception as e:
            logging.error(f"❌ Ошибка при проверке времени уведомлений: {e}")
            return False
            
    def set_chat_id(self, chat_id):
        """Устанавливает ID чата для отправки уведомлений"""
        self.chat_id = chat_id
        logging.info(f"📱 Установлен chat_id для уведомлений: {chat_id}")
    
    async def _send_daily_notifications(self):
        """Отправляет ежедневные уведомления о занятиях"""
        try:
            logging.info("📬 Отправка ежедневных уведомлений о занятиях")
            
            # Получаем chat_id из базы данных если не установлен
            if not self.chat_id:
                saved_chat_id = sheets_service.get_notification_chat_id()
                if saved_chat_id:
                    self.chat_id = saved_chat_id
                    logging.info(f"📱 Загружен chat_id из базы: {self.chat_id}")
                else:
                    logging.warning("⚠️ Chat ID не найден в базе данных, уведомления не будут отправлены")
                    return
            
            # Получаем занятия на сегодня
            today_lessons = self._get_today_lessons()
            
            if not today_lessons:
                logging.info("📅 На сегодня занятий не найдено")
                return
                
            logging.info(f"📚 Найдено занятий на сегодня: {len(today_lessons)}")
            
            # Отправляем уведомление по каждому занятию
            for lesson in today_lessons:
                await self._send_lesson_notification(lesson)
                await asyncio.sleep(2)  # Небольшая задержка между уведомлениями
                
        except Exception as e:
            logging.error(f"❌ Ошибка при отправке ежедневных уведомлений: {e}")
            
    def _get_today_lessons(self) -> list:
        """Получает список занятий на сегодня"""
        try:
            today = datetime.now().strftime('%d.%m.%Y')
            logging.info(f"📅 Получение занятий на {today}")
            
            # Получаем все занятия из календаря
            calendar_sheet = sheets_service.spreadsheet.worksheet("Календарь занятий")
            all_data = calendar_sheet.get_all_values()
            
            if len(all_data) <= 1:
                return []
                
            headers = all_data[0]
            today_lessons = []
            
            # Находим индексы нужных колонок
            date_col = None
            child_col = None
            circle_col = None
            time_start_col = None
            time_end_col = None
            lesson_id_col = None
            status_col = None
            
            for i, header in enumerate(headers):
                if 'дата' in header.lower():
                    date_col = i
                elif 'ребенок' in header.lower():
                    child_col = i
                elif 'кружок' in header.lower():
                    circle_col = i
                elif 'время начала' in header.lower():
                    time_start_col = i
                elif 'время завершения' in header.lower():
                    time_end_col = i
                elif 'id занятия' in header.lower():
                    lesson_id_col = i
                elif 'отметка' in header.lower() or 'статус' in header.lower():
                    status_col = i
                    
            # Ищем занятия на сегодня
            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) > max(date_col or 0, child_col or 0, circle_col or 0):
                    lesson_date = row[date_col] if date_col is not None else ""
                    
                    if lesson_date == today:
                        # Проверяем, что занятие еще не отмечено
                        status = row[status_col] if status_col is not None and status_col < len(row) else ""
                        
                        if not status or status.strip() == "":
                            # Получаем ID абонемента для поиска названия кружка
                            subscription_id = ""
                            for i, header in enumerate(headers):
                                if 'id абонемента' in header.lower():
                                    subscription_id = row[i] if i < len(row) else ""
                                    break
                            
                            # Получаем название кружка из абонемента
                            circle_name = "Неизвестно"
                            if subscription_id:
                                sub_details = sheets_service.get_subscription_details(subscription_id)
                                if sub_details:
                                    circle_name = sub_details.get('circle_name', 'Неизвестно')
                            
                            # Получаем реальный lesson_id или создаем на основе строки
                            real_lesson_id = row[lesson_id_col] if lesson_id_col is not None and lesson_id_col < len(row) else ""
                            if not real_lesson_id or real_lesson_id.strip() == "":
                                # Если ID занятия пустой, используем номер строки как ID
                                real_lesson_id = str(row_index)
                                logging.warning(f"⚠️ Пустой lesson_id в строке {row_index}, используем номер строки: {real_lesson_id}")
                            
                            lesson_data = {
                                'lesson_id': real_lesson_id,
                                'child_name': row[child_col] if child_col is not None else "Неизвестно",
                                'circle_name': circle_name,
                                'subscription_id': subscription_id,
                                'start_time': row[time_start_col] if time_start_col is not None else "",
                                'end_time': row[time_end_col] if time_end_col is not None else "",
                                'date': lesson_date,
                                'row_index': row_index
                            }
                            today_lessons.append(lesson_data)
                            
            return today_lessons
            
        except Exception as e:
            logging.error(f"❌ Ошибка при получении занятий на сегодня: {e}")
            return []
            
    async def _send_lesson_notification(self, lesson: dict, max_retries: int = 3):
        """Отправляет уведомление о конкретном занятии с retry логикой"""
        for attempt in range(max_retries):
            try:
                lesson_id = lesson['lesson_id']
                child_name = lesson['child_name']
                circle_name = lesson['circle_name']
                start_time = lesson['start_time']
                end_time = lesson['end_time']
                
                # ПОДРОБНЫЕ ЛОГИ ДЛЯ ОТЛАДКИ УВЕДОМЛЕНИЙ
                logging.info("=" * 60)
                logging.info("📬 ОТПРАВКА УВЕДОМЛЕНИЯ О ЗАНЯТИИ")
                logging.info(f"📝 lesson_id: '{lesson_id}'")
                logging.info(f"👤 child_name: '{child_name}'")
                logging.info(f"🎨 circle_name: '{circle_name}'")
                logging.info(f"⏰ start_time: '{start_time}'")
                logging.info(f"⏰ end_time: '{end_time}'")
                logging.info(f"📱 chat_id: '{self.chat_id}'")
                logging.info(f"🔄 Попытка: {attempt + 1}/{max_retries}")
                
                # Формируем текст уведомления - ТОЧНАЯ КОПИЯ из select_lesson_from_date
                message_text = f"✅ *Выберите отметку посещения*\n\n"
                message_text += f"👤 *Ребенок:* {child_name}\n"
                message_text += f"🎨 *Кружок:* {circle_name}\n"
                message_text += f"🆔 *ID абонемента:* {lesson.get('subscription_id', '')}\n"
                message_text += f"📅 *Дата занятия:* {lesson['date']}\n"
                if start_time and end_time:
                    message_text += f"🕐 *Время:* {start_time} - {end_time}\n"
                message_text += "\nВыберите статус посещения:"
                
                # Получаем статусы посещения из Справочника
                attendance_statuses = sheets_service.get_handbook_items("Статусы посещения")
                
                if not attendance_statuses:
                    logging.error("❌ Не найдены статусы посещения в Справочнике")
                    return
                    
                # Создаем кнопки с отметками (используем ту же логику, что в календаре)
                keyboard = []
                
                # Словарь соответствия статусов и эмодзи (точно как в календаре)
                status_emojis = {
                    'посещение': '✅',
                    'пропуск (по вине)': '❌',
                    'пропуск': '❌',
                    'отмена (болезнь)': '🤒',
                    'перенос': '🔄',
                    'отмена': '🚫',
                    'болезнь': '🤒',
                    'уважительная причина': '📋',
                    'неуважительная причина': '⚠️'
                }
                
                for status in attendance_statuses:
                    if status.strip():  # Пропускаем пустые значения
                        # Ищем подходящий эмодзи для статуса
                        emoji = ''
                        status_lower = status.lower().strip()
                        
                        # Проверяем точное совпадение
                        if status_lower in status_emojis:
                            emoji = status_emojis[status_lower]
                        else:
                            # Проверяем частичное совпадение
                            for key, value in status_emojis.items():
                                if key in status_lower:
                                    emoji = value
                                    break
                        
                        # Если эмодзи не найден, используем стандартный
                        if not emoji:
                            emoji = '📝'
                        
                        button_text = f"{emoji} {status}"
                        # Используем ТОЧНО тот же формат callback_data, что в календаре
                        callback_data = f"attendance_mark_{lesson_id}|||{status}"
                        
                        # ПОДРОБНЫЕ ЛОГИ ДЛЯ ОТЛАДКИ КНОПОК
                        logging.info(f"🔘 Создание кнопки уведомления:")
                        logging.info(f"   📝 Текст кнопки: '{button_text}'")
                        logging.info(f"   🔗 Callback data: '{callback_data}'")
                        logging.info(f"   📊 lesson_id: '{lesson_id}'")
                        logging.info(f"   ✏️ status: '{status}'")
                        
                        keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                
                # Добавляем кнопку "Отмена" в отдельной строке
                keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data=f"cancel_notification_{lesson_id}")])
                
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                # Отправляем уведомление с таймаутом - ТОЧНАЯ КОПИЯ из select_lesson_from_date
                await self.bot.send_message(
                    chat_id=self.chat_id,
                    text=message_text,
                    reply_markup=reply_markup,
                    parse_mode='Markdown',  # Изменено на Markdown как в календаре
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30
                )
                
                logging.info(f"✅ УВЕДОМЛЕНИЕ УСПЕШНО ОТПРАВЛЕНО!")
                logging.info(f"📬 Занятие: {child_name} - {circle_name}")
                logging.info(f"🔘 Создано кнопок: {len(keyboard)}")
                logging.info("=" * 60)
                return  # Успешно отправлено, выходим из цикла
                
            except Exception as e:
                attempt_info = f"(попытка {attempt + 1}/{max_retries})"
                
                if "TimedOut" in str(e) or "timeout" in str(e).lower():
                    logging.warning(f"⏰ Таймаут при отправке уведомления {attempt_info}: {e}")
                    
                    if attempt < max_retries - 1:
                        wait_time = (attempt + 1) * 5  # Увеличиваем время ожидания
                        logging.info(f"⏳ Ожидание {wait_time} секунд перед повтором...")
                        await asyncio.sleep(wait_time)
                        continue
                else:
                    logging.error(f"❌ Ошибка при отправке уведомления {attempt_info}: {e}")
                    
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)  # Короткая пауза перед повтором
                        continue
                
                # Если все попытки исчерпаны
                if attempt == max_retries - 1:
                    logging.error(f"❌ Не удалось отправить уведомление после {max_retries} попыток: {child_name} - {circle_name}")
                    break

# Глобальный экземпляр планировщика
notification_scheduler = None

def get_notification_scheduler(bot: Bot = None) -> NotificationScheduler:
    """Получает глобальный экземпляр планировщика уведомлений"""
    global notification_scheduler
    
    if notification_scheduler is None and bot is not None:
        notification_scheduler = NotificationScheduler(bot)
        
    return notification_scheduler
