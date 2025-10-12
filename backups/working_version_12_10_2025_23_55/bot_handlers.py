import logging
import asyncio
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo
from telegram.ext import ContextTypes, ConversationHandler, CommandHandler, CallbackQueryHandler, MessageHandler, filters
from google_sheets_service import sheets_service
from calendar import monthrange
import telegram

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Импорт планировщика уведомлений (будет инициализирован позже)
notification_scheduler = None

async def delete_message_after_delay(bot, chat_id, message_id, delay_seconds):
    """Удаляет сообщение через указанное количество секунд."""
    try:
        await asyncio.sleep(delay_seconds)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logging.info(f"🗑️ Удалено временное уведомление через {delay_seconds} секунд")
    except Exception as e:
        logging.warning(f"⚠️ Не удалось удалить сообщение {message_id}: {e}")

# === Определяем состояния для диалога ===
(
    MAIN_MENU,
    SELECT_SUBSCRIPTION,
    MANAGE_SUBSCRIPTION,
    CONFIRM_DELETE_SUBSCRIPTION,
    
    # Subscription Creation Flow
    CREATE_SUB_CHILD,
    CREATE_SUB_GET_CHILD_NAME,
    CREATE_SUB_CIRCLE,
    CREATE_SUB_GET_CIRCLE_NAME,
    CREATE_SUB_TYPE,
    CREATE_SUB_PAYMENT_TYPE,
    CREATE_SUB_COST,
    CREATE_SUB_TOTAL_CLASSES,
    CREATE_SUB_REMAINING_CLASSES,
    CREATE_SUB_START_DATE_MONTH,
    CREATE_SUB_START_DATE_DAY,
    CREATE_SUB_SCHEDULE_DAY,
    CREATE_SUB_SCHEDULE_START_HOUR,
    CREATE_SUB_SCHEDULE_START_MINUTE,
    CREATE_SUB_SCHEDULE_END_HOUR,
    CREATE_SUB_SCHEDULE_END_MINUTE,
    CREATE_SUB_SCHEDULE_CONFIRM,

    # Calendar States
    CALENDAR_MENU,
    INTERACTIVE_CALENDAR,
    SELECT_CALENDAR_DATE,
    SELECT_LESSON_FROM_DATE,
    SELECT_ATTENDANCE_MARK,

    # Settings States
    SETTINGS_MENU,
    SHOW_CATEGORY_ITEMS,
    ADD_ITEM,
    MANAGE_SINGLE_ITEM,
    GET_NEW_VALUE_FOR_EDIT,
    CONFIRM_DELETE_ITEM,

    # Subscription Renewal States
    RENEWAL_SELECT_DATE_TYPE,
    RENEWAL_SELECT_CUSTOM_DATE,
    RENEWAL_CONFIRM,
    
    # Notification Settings States
    NOTIFICATION_TIME_SETTINGS,
) = range(36)
# === Вспомогательные функции ===
def create_calendar_keyboard(year, month):
    keyboard = []
    ru_months = ["Янв", "Фев", "Мар", "Апр", "Май", "Июн", "Июл", "Авг", "Сен", "Окт", "Ноя", "Дек"]
    
    prev_month_data = f"cal_month_{year}_{month-1}" if month > 1 else f"cal_month_{year-1}_12"
    next_month_data = f"cal_month_{year}_{month+1}" if month < 12 else f"cal_month_{year+1}_1"
    keyboard.append([
        InlineKeyboardButton("<<", callback_data=prev_month_data),
        InlineKeyboardButton(f"{ru_months[month-1]} {year}", callback_data="ignore"),
        InlineKeyboardButton(">>", callback_data=next_month_data)
    ])
    
    week_days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
    keyboard.append([InlineKeyboardButton(day, callback_data="ignore") for day in week_days])
    
    month_calendar = []
    first_day_weekday, num_days = monthrange(year, month)
    
    current_day = 1
    while current_day <= num_days:
        row = []
        for i in range(7):
            if (len(month_calendar) == 0 and i < first_day_weekday) or current_day > num_days:
                row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                row.append(InlineKeyboardButton(str(current_day), callback_data=f"cal_day_{current_day}"))
                current_day += 1
        month_calendar.append(row)
    
    keyboard.extend(month_calendar)
    keyboard.append([InlineKeyboardButton("⏪ Отмена", callback_data="menu_subscriptions")])
    return InlineKeyboardMarkup(keyboard)

def create_time_keyboard(prefix, hour_range=range(8, 22), minute_step=15):
    keyboard = []
    if minute_step == 60: 
        row = []
        for hour in hour_range:
            row.append(InlineKeyboardButton(f"{hour:02d}", callback_data=f"{prefix}_{hour}"))
            if len(row) == 6:
                keyboard.append(row)
                row = []
        if row: keyboard.append(row)
    else:
        row = [InlineKeyboardButton(f"{minute:02d}", callback_data=f"{prefix}_{minute}") for minute in range(0, 60, minute_step)]
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)

async def _display_settings_category_list(update: Update, context: ContextTypes.DEFAULT_TYPE, sender_func):
    """Универсальная функция для отображения списка элементов категории."""
    category_header = context.user_data['settings_category_header']
    items = sheets_service.get_handbook_items(category_header)

    if items is None:
        await sender_func("❌ Ошибка при загрузке списка. Проверьте логи.")
        return SHOW_CATEGORY_ITEMS

    keyboard = [[InlineKeyboardButton(item, callback_data=f"settings_select_item_{item}")] for item in items]
    keyboard.append([InlineKeyboardButton("➕ Добавить новый", callback_data="settings_add")])
    keyboard.append([InlineKeyboardButton("⏪ Назад в настройки", callback_data="menu_settings")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    message_text = f"Управление {context.user_data['settings_category_title']}:"
    if not items: message_text += "\n\nСписок пока пуст."

    if hasattr(sender_func, '__name__') and sender_func.__name__ == 'edit_message_text':
        await sender_func(message_text, reply_markup=reply_markup)
    else:
        await sender_func(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup)
    return SHOW_CATEGORY_ITEMS
# === Основное Меню ===
async def clear_chat_history(context, chat_id, max_messages=50):
    """Очищает историю чата, удаляя последние сообщения."""
    try:
        logging.info(f"🧹 Начинаю очистку чата {chat_id}")
        deleted_count = 0
        
        # Получаем ID последних сообщений и удаляем их
        for i in range(max_messages):
            try:
                # Пытаемся удалить сообщения, начиная с текущего ID и идя назад
                # Telegram не позволяет получить список сообщений, поэтому пробуем удалить по ID
                message_id = context.bot_data.get('last_message_id', 1000) - i
                if message_id > 0:
                    await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
                    deleted_count += 1
            except Exception:
                # Если сообщение не найдено или не может быть удалено, продолжаем
                continue
        
        if deleted_count > 0:
            logging.info(f"🧹 Удалено {deleted_count} сообщений из чата")
        else:
            logging.info("🧹 Нет сообщений для удаления или недостаточно прав")
            
    except Exception as e:
        logging.warning(f"⚠️ Не удалось полностью очистить чат: {e}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик команды /start. Очищает чат и показывает главное меню с еженедельной сводкой."""
    
    # Очищаем чат при команде /start (только если это команда, а не callback)
    loading_message = None
    if update.message and update.message.text == '/start':
        try:
            # Сначала отправляем сообщение о загрузке
            loading_message = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🔄 <b>Загружаю главное меню...</b>\n\n⏳ Пожалуйста, подождите...",
                parse_mode='HTML'
            )
            
            # Удаляем команду /start
            await update.message.delete()
            
            # Пытаемся очистить чат (удаляем последние сообщения)
            chat_id = update.effective_chat.id
            deleted_count = 0
            
            # Пробуем удалить последние 20 сообщений (исключая сообщение загрузки)
            for i in range(1, 21):
                try:
                    message_id_to_delete = update.message.message_id - i
                    # Не удаляем сообщение загрузки
                    if loading_message and message_id_to_delete == loading_message.message_id:
                        continue
                    await context.bot.delete_message(chat_id=chat_id, message_id=message_id_to_delete)
                    deleted_count += 1
                except Exception:
                    # Если сообщение не найдено или не может быть удалено, пропускаем
                    continue
            
            if deleted_count > 0:
                logging.info(f"🧹 Очищен чат: удалено {deleted_count} сообщений")
            
        except Exception as e:
            logging.warning(f"⚠️ Не удалось полностью очистить чат: {e}")
    
    # Показываем индикатор загрузки только если это callback
    if update.callback_query:
        await update.callback_query.answer()
    
    # Формируем главное меню с еженедельной сводкой (если доступна)
    try:
        # Пытаемся получить еженедельную сводку, но не блокируем показ меню
        weekly_summary = sheets_service.get_weekly_summary()
        
        if weekly_summary and weekly_summary.get('attendance_stats'):
            message_text = f"📊 <b>СВОДКА НА НЕДЕЛЮ</b>\n"
            message_text += f"📅 {weekly_summary.get('week_start', 'Неделя')} - {weekly_summary.get('week_end', '')}\n\n"
            
            # Статистика занятий
            stats = weekly_summary['attendance_stats']
            message_text += f"📚 <b>ЗАНЯТИЯ НА НЕДЕЛЮ:</b>\n"
            message_text += f"• Всего: {stats.get('total', 0)}\n"
            message_text += f"• ✅ Посещено: {stats.get('attended', 0)}\n"
            message_text += f"• ❌ Пропущено: {stats.get('missed', 0)}\n"
            message_text += f"• 📅 Запланировано: {stats.get('planned', 0)}\n"
            if stats.get('total', 0) > 0:
                message_text += f"• 📊 Посещаемость: {stats.get('attendance_rate', 0)}%\n"
            
            # Прогноз оплат (кратко)
            if weekly_summary.get('payments_this_week'):
                total_budget = weekly_summary.get('total_budget', 0)
                payments_count = len(weekly_summary['payments_this_week'])
                message_text += f"\n💰 <b>ОПЛАТЫ НА НЕДЕЛЮ:</b> {payments_count} оплат на {total_budget} руб.\n"
            else:
                message_text += f"\n💰 <b>ОПЛАТЫ НА НЕДЕЛЮ:</b> Нет запланированных оплат\n"
            
            message_text += f"\n👋 <b>Выберите действие:</b>"
        else:
            message_text = '👋 <b>Главное меню</b>\n\nВыберите действие:'
            
    except Exception as e:
        logging.error(f"Ошибка при получении еженедельной сводки: {e}")
        message_text = '👋 <b>Главное меню</b>\n\nВыберите действие:'
    
    keyboard = [
        [InlineKeyboardButton("📊 Дашборд", callback_data="menu_dashboard")],
        [InlineKeyboardButton("📄 Абонементы", callback_data="menu_subscriptions")],
        [InlineKeyboardButton("💰 Прогноз бюджета", callback_data="menu_forecast")],
        [InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="menu_settings")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        # Если есть сообщение загрузки от команды /start, заменяем его на главное меню
        if loading_message:
            try:
                await loading_message.edit_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
            except Exception as e:
                logging.warning(f"⚠️ Не удалось отредактировать сообщение загрузки: {e}")
                # Если не удалось отредактировать, удаляем и создаем новое
                try:
                    await loading_message.delete()
                except Exception:
                    pass
                await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            # Если это не команда /start (которую мы уже удалили), удаляем сообщение
            if update.message and update.message.text != '/start':
                try: 
                    await update.message.delete()
                except Exception: 
                    pass
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
        
    return MAIN_MENU

# === Обработчики кнопок меню ===
async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    logging.info(f"🔍 main_menu_handler: получен callback_data = '{query.data}'")
    
    if query.data == 'main_menu':
        logging.info("🏠 Переход в главное меню")
        return await start(update, context)
    elif query.data == 'refresh_main_menu':
        logging.info("🔄 Обновление главного меню")
        return await start(update, context)
    elif query.data == 'force_refresh_data':
        logging.info("🔄 Принудительное обновление данных")
        return await force_refresh_all_data(update, context)
    elif query.data == 'menu_dashboard':
        logging.info("📊 Переход к дашборду")
        return await dashboard_menu(update, context)
    elif query.data == 'menu_subscriptions':
        logging.info("📄 Переход в меню абонементов")
        return await subscriptions_menu(update, context)
    elif query.data == 'menu_calendar':
        logging.info("📅 Переход в меню календаря")
        return await calendar_menu(update, context)
    elif query.data == 'menu_forecast':
        logging.info("📊 Переход в меню прогноза")
        return await forecast_menu_handler(update, context)
    elif query.data == 'menu_settings':
        logging.info("⚙️ Переход в настройки")
        return await settings_menu(update, context)
    elif query.data == 'sync_google_calendar':
        logging.info("🔄 Синхронизация Google Calendar")
        return await sync_google_calendar_handler(update, context)
    elif query.data == 'sync_google_forecast':
        logging.info("💰 Синхронизация Google прогноза")
        return await sync_google_forecast_handler(update, context)
    elif query.data == 'clean_duplicates':
        logging.info("🧹 Очистка дублей")
        return await clean_duplicates_handler(update, context)
    else:
        logging.warning(f"❓ Неизвестная команда: {query.data}")
        await query.answer("Функция в разработке", show_alert=True)
        return MAIN_MENU

async def sync_google_calendar_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик синхронизации Google Calendar."""
    query = update.callback_query
    await query.answer("🔄 Запускаю синхронизацию...")
    
    print("🔄 ОБРАБОТЧИК СИНХРОНИЗАЦИИ ВЫЗВАН!")
    logging.info("🔄 Запуск обработчика синхронизации Google Calendar")
    
    # Показываем сообщение о начале синхронизации
    await query.edit_message_text("🔄 **Синхронизация Google Calendar**\n\n⏳ Начинаю синхронизацию...\nЭто может занять несколько минут.", parse_mode='Markdown')
    
    try:
        logging.info("📞 Вызываю функцию синхронизации...")
        
        # Показываем промежуточное сообщение
        await query.edit_message_text("🔄 **Синхронизация Google Calendar**\n\n📊 Читаю данные из таблицы...", parse_mode='Markdown')
        
        # Запускаем синхронизацию
        result = sheets_service.sync_calendar_with_google_calendar()
        logging.info(f"✅ Синхронизация завершена, результат: {result[:100]}...")
        
        # Показываем результат с уведомлением
        message_text = f"🎉 **Синхронизация завершена!**\n\n{result}\n\n📱 *Проверьте Google Calendar - события обновлены*"
        
        # Отправляем временное уведомление
        notification_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="🔔 **Уведомление**\n\n✅ Google Calendar успешно обновлен!\n📅 Все изменения применены.",
            parse_mode='Markdown'
        )
        
        # Удаляем уведомление через 5 секунд
        import asyncio
        asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, notification_msg.message_id, 5))
        
    except Exception as e:
        logging.error(f"❌ Ошибка при синхронизации календаря: {e}", exc_info=True)
        message_text = f"❌ **Ошибка при синхронизации календаря**\n\n```\n{str(e)}\n```\n\n🔧 Попробуйте позже или обратитесь к администратору."
        
        # Отправляем временное уведомление об ошибке
        error_notification_msg = await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="🔔 **Уведомление**\n\n❌ Произошла ошибка при синхронизации календаря.",
            parse_mode='Markdown'
        )
        
        # Удаляем уведомление об ошибке через 8 секунд (дольше, чтобы пользователь успел прочитать)
        asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, error_notification_msg.message_id, 8))
    
    # Добавляем кнопку возврата в главное меню
    keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
    return MAIN_MENU

async def sync_google_forecast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик синхронизации прогноза оплат с Google Calendar."""
    query = update.callback_query
    
    try:
        await query.answer("💰 Запускаю синхронизацию прогноза...")
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при answer в sync_google_forecast: {e}")
    
    print("💰 ОБРАБОТЧИК СИНХРОНИЗАЦИИ ПРОГНОЗА ВЫЗВАН!")
    logging.info("💰 Запуск обработчика синхронизации Google прогноза")
    
    # Показываем сообщение о начале синхронизации
    try:
        await query.edit_message_text("💰 **Синхронизация Google прогноза**\n\n⏳ Начинаю синхронизацию прогноза оплат...\nЭто может занять несколько минут.", parse_mode='Markdown')
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при редактировании сообщения синхронизации прогноза: {e}")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="💰 **Синхронизация Google прогноза**\n\n⏳ Начинаю синхронизацию прогноза оплат...\nЭто может занять несколько минут.",
                parse_mode='Markdown'
            )
        except:
            pass
    
    try:
        logging.info("📞 Вызываю функцию синхронизации прогноза...")
        
        # Показываем промежуточное сообщение
        try:
            await query.edit_message_text("💰 **Синхронизация Google прогноза**\n\n📊 Читаю данные из листа 'Прогноз'...", parse_mode='Markdown')
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при обновлении промежуточного сообщения прогноза: {e}")
        
        # Запускаем синхронизацию прогноза
        result = sheets_service.sync_forecast_with_google_calendar()
        logging.info(f"✅ Синхронизация прогноза завершена, результат: {result[:100]}...")
        
        # Показываем результат с уведомлением
        message_text = f"🎉 **Синхронизация прогноза завершена!**\n\n{result}\n\n💰 *Проверьте Google Calendar - прогнозы оплат обновлены*"
        
        # Отправляем временное уведомление
        try:
            notification_msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🔔 **Уведомление**\n\n✅ Google прогноз успешно обновлен!\n💰 Все прогнозы оплат синхронизированы.",
                parse_mode='Markdown'
            )
            
            # Удаляем уведомление через 5 секунд
            import asyncio
            asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, notification_msg.message_id, 5))
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при отправке уведомления о прогнозе: {e}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка при синхронизации прогноза: {e}", exc_info=True)
        message_text = f"❌ **Ошибка при синхронизации прогноза**\n\n```\n{str(e)}\n```\n\n🔧 Попробуйте позже или обратитесь к администратору."
        
        # Отправляем временное уведомление об ошибке
        try:
            error_notification_msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🔔 **Уведомление**\n\n❌ Произошла ошибка при синхронизации прогноза.\n\n🔧 Проверьте логи для подробностей.",
                parse_mode='Markdown'
            )
            
            # Удаляем уведомление об ошибке через 8 секунд
            import asyncio
            asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, error_notification_msg.message_id, 8))
        except Exception as notify_error:
            logging.error(f"❌ Ошибка при отправке уведомления об ошибке прогноза: {notify_error}")
    
    # Добавляем кнопку возврата в главное меню
    keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при редактировании финального сообщения прогноза: {e}")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        except:
            pass
    return MAIN_MENU

async def clean_duplicates_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик очистки дублей в Google Calendar."""
    query = update.callback_query
    
    try:
        await query.answer("🧹 Запускаю очистку дублей...")
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при answer в clean_duplicates: {e}")
    
    print("🧹 ОБРАБОТЧИК ОЧИСТКИ ДУБЛЕЙ ВЫЗВАН!")
    logging.info("🧹 Запуск обработчика очистки дублей")
    
    # Показываем сообщение о начале очистки
    try:
        await query.edit_message_text("🧹 **Очистка дублей в Google Calendar**\n\n⏳ Начинаю поиск и удаление дублирующихся событий...\nЭто может занять несколько минут.", parse_mode='Markdown')
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при редактировании сообщения очистки: {e}")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🧹 **Очистка дублей в Google Calendar**\n\n⏳ Начинаю поиск и удаление дублирующихся событий...\nЭто может занять несколько минут.",
                parse_mode='Markdown'
            )
        except:
            pass
    
    try:
        logging.info("📞 Вызываю функцию очистки дублей...")
        
        # Показываем промежуточное сообщение
        try:
            await query.edit_message_text("🧹 **Очистка дублей в Google Calendar**\n\n🔍 Анализирую события в календаре...", parse_mode='Markdown')
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при обновлении промежуточного сообщения очистки: {e}")
        
        # Запускаем очистку дублей
        result = sheets_service.clean_duplicate_events()
        logging.info(f"✅ Очистка дублей завершена, результат: {result[:100]}...")
        
        # Показываем результат с уведомлением
        message_text = f"🎉 **Очистка дублей завершена!**\n\n{result}\n\n🧹 *Проверьте Google Calendar - дубли удалены*"
        
        # Отправляем временное уведомление
        try:
            notification_msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🔔 **Уведомление**\n\n✅ Очистка дублей завершена!\n🧹 Дублирующиеся события удалены.",
                parse_mode='Markdown'
            )
            
            # Удаляем уведомление через 5 секунд
            import asyncio
            asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, notification_msg.message_id, 5))
        except Exception as e:
            logging.warning(f"⚠️ Ошибка при отправке уведомления об очистке: {e}")
        
    except Exception as e:
        logging.error(f"❌ Ошибка при очистке дублей: {e}", exc_info=True)
        message_text = f"❌ **Ошибка при очистке дублей**\n\n```\n{str(e)}\n```\n\n🔧 Попробуйте позже или обратитесь к администратору."
        
        # Отправляем временное уведомление об ошибке
        try:
            error_notification_msg = await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="🔔 **Уведомление**\n\n❌ Произошла ошибка при очистке дублей.\n\n🔧 Проверьте логи для подробностей.",
                parse_mode='Markdown'
            )
            
            # Удаляем уведомление об ошибке через 8 секунд
            import asyncio
            asyncio.create_task(delete_message_after_delay(context.bot, update.effective_chat.id, error_notification_msg.message_id, 8))
        except Exception as notify_error:
            logging.error(f"❌ Ошибка при отправке уведомления об ошибке очистки: {notify_error}")
    
    # Добавляем кнопку возврата в главное меню
    keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    try:
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception as e:
        logging.warning(f"⚠️ Ошибка при редактировании финального сообщения очистки: {e}")
        try:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=message_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        except:
            pass
    return MAIN_MENU

async def go_back_to_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    return await start(update, context)

async def force_refresh_all_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Принудительно обновляет все данные и возвращает в главное меню с актуальной статистикой."""
    query = update.callback_query
    await query.answer("🔄 Запускаю обновление данных...")
    
    try:
        # Сразу показываем сообщение об успехе и запускаем фоновое обновление
        await query.edit_message_text("✅ <b>Обновление данных запущено!</b>\n\n🔄 Загружаю актуальную статистику...", parse_mode='HTML')
        
        # Запускаем фоновое обновление (без ожидания)
        import asyncio
        asyncio.create_task(update_data_in_background())
        
        # Небольшая задержка и показываем главное меню БЕЗ вызова start (чтобы избежать двойного callback)
        await asyncio.sleep(1)
        
        # Получаем еженедельную статистику
        try:
            weekly_summary = sheets_service.get_weekly_summary()
            if weekly_summary:
                stats = weekly_summary['attendance_stats']
                message_text = f"📊 <b>СВОДКА НА НЕДЕЛЮ</b>\n"
                message_text += f"📅 {weekly_summary['week_start']} - {weekly_summary['week_end']}\n\n"
                
                message_text += f"📚 <b>ЗАНЯТИЯ НА НЕДЕЛЮ:</b>\n"
                message_text += f"• Всего: {stats['total']}\n"
                message_text += f"• ✅ Посещено: {stats['attended']}\n"
                message_text += f"• ❌ Пропущено: {stats['missed']}\n"
                message_text += f"• 📅 Запланировано: {stats['planned']}\n"
                message_text += f"• 📊 Посещаемость: {stats['attendance_rate']}%\n"
                
                # Ближайшие занятия
                if weekly_summary['lessons_this_week']:
                    message_text += f"\n🎯 <b>БЛИЖАЙШИЕ ЗАНЯТИЯ:</b>\n"
                    lessons_sorted = sorted(weekly_summary['lessons_this_week'], key=lambda x: x['date'])
                    for lesson in lessons_sorted[:5]:
                        status_emoji = "✅" if lesson.get('mark') == 'Посещение' else "📅"
                        message_text += f"• {status_emoji} {lesson['date']} {lesson['time']} - {lesson['child']} ({lesson['circle']})\n"
                    
                    if len(lessons_sorted) > 5:
                        message_text += f"• ... и еще {len(lessons_sorted) - 5} занятий\n"
                
                # Прогноз оплат
                if weekly_summary['payments_this_week']:
                    message_text += f"\n💰 <b>ОПЛАТЫ НА НЕДЕЛЮ:</b>\n"
                    for payment in weekly_summary['payments_this_week'][:3]:
                        message_text += f"• {payment['date']}: {payment['child']} - {payment['amount']} руб.\n"
                    
                    if len(weekly_summary['payments_this_week']) > 3:
                        message_text += f"• ... и еще {len(weekly_summary['payments_this_week']) - 3} оплат\n"
                else:
                    message_text += f"\n💰 <b>ОПЛАТЫ НА НЕДЕЛЮ:</b> Нет запланированных оплат\n"
                
                message_text += f"\n👋 <b>Выберите действие:</b>"
            else:
                message_text = '👋 Главное меню. Выберите действие:'
        except Exception as e:
            logging.error(f"Ошибка при получении еженедельной сводки: {e}")
            message_text = '👋 Главное меню. Выберите действие:'
        
        keyboard = [
            [InlineKeyboardButton("📄 Абонементы", callback_data="menu_subscriptions")],
            [InlineKeyboardButton("📊 Прогноз бюджета", callback_data="menu_forecast")],
            [InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="menu_settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return MAIN_MENU
        
    except Exception as e:
        logging.error(f"❌ Ошибка при запуске обновления данных: {e}")
        
        # При ошибке показываем простое главное меню
        keyboard = [
            [InlineKeyboardButton("📄 Абонементы", callback_data="menu_subscriptions")],
            [InlineKeyboardButton("📊 Прогноз бюджета", callback_data="menu_forecast")],
            [InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")],
            [InlineKeyboardButton("⚙️ Настройки", callback_data="menu_settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text('👋 Главное меню. Выберите действие:', reply_markup=reply_markup)
        return MAIN_MENU

async def update_data_in_background():
    """Обновляет данные в фоновом режиме без блокировки интерфейса."""
    try:
        logging.info("🔄 Начинаю фоновое обновление всех данных...")
        
        # 1. Обновляем статистику абонементов
        logging.info("📊 Обновляю статистику абонементов...")
        calendar_count, calendar_errors = sheets_service.update_subscriptions_statistics()
        logging.info(f"✅ Обновлено абонементов: {calendar_count}")
        
        # 2. Обновляем прогноз бюджета
        await asyncio.sleep(2)  # Задержка для снижения нагрузки на API
        logging.info("💰 Обновляю прогноз бюджета...")
        forecast_count, forecast_errors = sheets_service.update_full_forecast()
        logging.info(f"✅ Создано прогнозов: {forecast_count}")
        
        # 3. Синхронизируем с Google Calendar (фоновая синхронизация)
        await asyncio.sleep(3)  # Задержка для снижения нагрузки на API
        logging.info("🔄 Синхронизирую с Google Calendar...")
        try:
            calendar_result = sheets_service.sync_calendar_with_google_calendar()
            logging.info(f"✅ Синхронизация календаря: {calendar_result[:100]}...")
        except Exception as e:
            logging.error(f"❌ Ошибка при синхронизации календаря: {e}")
        
        # 4. Синхронизируем прогноз с Google Calendar (фоновая синхронизация)
        await asyncio.sleep(2)  # Задержка для снижения нагрузки на API
        logging.info("💰 Синхронизирую прогноз с Google Calendar...")
        try:
            forecast_result = sheets_service.sync_forecast_with_google_calendar()
            logging.info(f"✅ Синхронизация прогноза: {forecast_result[:100]}...")
        except Exception as e:
            logging.error(f"❌ Ошибка при синхронизации прогноза: {e}")
        
        # 5. Очищаем дубли в Google Calendar (фоновая очистка)
        await asyncio.sleep(2)  # Задержка для снижения нагрузки на API
        logging.info("🧹 Очищаю дубли в Google Calendar...")
        try:
            clean_result = sheets_service.clean_duplicate_events()
            logging.info(f"✅ Очистка дублей: {clean_result[:100]}...")
        except AttributeError as attr_error:
            logging.warning(f"⚠️ Функция очистки дублей не найдена: {attr_error}")
            logging.info("ℹ️ Пропускаю очистку дублей - функция не реализована")
        except Exception as e:
            logging.error(f"❌ Ошибка при очистке дублей: {e}")
        
        logging.info("🎉 Фоновое обновление всех данных завершено!")
        
    except Exception as e:
        logging.error(f"❌ Ошибка при фоновом обновлении данных: {e}")

async def update_stats_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик кнопки 'Обновить статистику'."""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("🔄 Обновляю статистику и прогноз...")
    
    try:
        # Обновляем календари занятий для всех абонементов
        calendar_count, calendar_errors = sheets_service.update_all_calendars()
        
        # Обновляем полный прогноз
        forecast_count, skipped_forecasts = sheets_service.update_full_forecast()
        
        # Формируем сообщение с результатами
        message_text = "✅ <b>Статистика обновлена!</b>\n\n"
        message_text += f"📅 Обновлено календарей: <b>{calendar_count}</b>\n"
        message_text += f"📊 Создано записей прогноза: <b>{forecast_count}</b>\n"
        
        # Показываем ошибки календарей
        if calendar_errors:
            message_text += f"\n⚠️ <b>Ошибки календарей:</b> {len(calendar_errors)}\n"
            for i, error in enumerate(calendar_errors[:2]):
                message_text += f"• {error}\n"
            if len(calendar_errors) > 2:
                message_text += f"• ... и еще {len(calendar_errors) - 2}\n"
        
        # Показываем ошибки прогноза
        if skipped_forecasts:
            message_text += f"\n⚠️ <b>Пропущено абонементов в прогнозе:</b> {len(skipped_forecasts)}\n"
            # Показываем только первые 2 ошибки, чтобы не перегружать сообщение
            for i, error in enumerate(skipped_forecasts[:2]):
                message_text += f"• {error}\n"
            if len(skipped_forecasts) > 2:
                message_text += f"• ... и еще {len(skipped_forecasts) - 2}\n"
        
        message_text += "\n📋 <b>Что было обновлено:</b>\n"
        message_text += "• Проверены все абонементы\n"
        message_text += "• Пересоздан календарь занятий\n"
        message_text += "• Пересчитан прогноз оплат на 2 месяца\n"
        
    except Exception as e:
        message_text = f"❌ <b>Ошибка при обновлении статистики:</b>\n\n{e}"
    
    keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

async def update_subscriptions_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик кнопки 'Обновить абонементы'."""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("🔄 Обновляю статистику абонементов...")
    
    try:
        # Вызываем функцию обновления статистики абонементов
        updated_count, errors = sheets_service.update_subscriptions_statistics()
        
        # Формируем сообщение с результатами
        message_text = "✅ <b>Статистика абонементов обновлена!</b>\n\n"
        message_text += f"📋 Обработано абонементов: <b>{updated_count}</b>\n"
        
        if errors:
            message_text += f"\n⚠️ <b>Ошибки:</b> {len(errors)}\n"
            # Показываем только первые 3 ошибки
            for i, error in enumerate(errors[:3]):
                message_text += f"• {error}\n"
            if len(errors) > 3:
                message_text += f"• ... и еще {len(errors) - 3}\n"
        
        # Синхронизация с Google Calendar удалена
        calendar_status = "➖"
        
        message_text += "\n📋 <b>Что было выполнено:</b>\n"
        message_text += "• Пересчитана статистика всех абонементов\n"
        message_text += "• Обновлены поля: Прошло/Осталось/Пропущено\n"
        message_text += "• Перестроено будущее расписание\n"
        message_text += "• Обработаны переносы для абонементов 'С переносами'\n"
        message_text += "• Обновлены статусы и даты окончания\n"
        message_text += f"• Синхронизация с Google Calendar: отключена\n"
        
    except Exception as e:
        message_text = f"❌ <b>Ошибка при обновлении абонементов:</b>\n\n{e}"
    
    keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU


async def google_calendar_sync_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Информирует об отключенной синхронизации с Google Календарем."""
    query = update.callback_query
    await query.answer()
    
    try:
        # Показываем сообщение об отключенной функции
        message_text = "📅 <b>Google Календарь</b>\n\n"
        message_text += "⚠️ Синхронизация с Google Календарем временно отключена.\n\n"
        message_text += "Эта функция будет доступна в следующих обновлениях."
        
    except Exception as e:
        message_text = f"❌ <b>Ошибка при запуске синхронизации:</b>\n\n{e}"
    
    keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

async def fix_duplicate_ids_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Исправляет дублированные ID занятий в календаре."""
    query = update.callback_query
    await query.answer()
    
    # Показываем сообщение о начале исправления
    await query.edit_message_text("🔧 Исправляю дублированные ID занятий...\n\nЭто может занять несколько секунд.")
    
    try:
        # Запускаем исправление дублей
        result = sheets_service.fix_duplicate_lesson_ids()
        
        if result:
            message_text = "✅ <b>Дублированные ID успешно исправлены!</b>\n\n"
            message_text += "🔧 Все занятия в календаре теперь имеют уникальные ID\n"
            message_text += "📊 ID присвоены последовательно начиная с 1\n"
            message_text += "✅ Теперь можно ставить отметки посещения без ошибок\n\n"
            message_text += "Рекомендуется также запустить синхронизацию с Google Календарем для обновления событий."
        else:
            message_text = "❌ <b>Ошибка при исправлении дублированных ID</b>\n\n"
            message_text += "Проверьте логи бота для получения подробной информации об ошибке."
        
    except Exception as e:
        message_text = f"❌ <b>Критическая ошибка при исправлении ID:</b>\n\n{e}"
    
    keyboard = [
        [InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU
    
async def forecast_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает запланированные оплаты с кнопками для управления."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🔄 Загружаю запланированные оплаты...")

    logging.info("🔍 Запуск forecast_menu_handler")
    planned_payments = sheets_service.get_planned_payments()
    logging.info(f"📊 Получено запланированных оплат: {len(planned_payments)}")
    
    if not planned_payments:
        message_text = "📊 <b>Запланированные оплаты</b>\n\n"
        message_text += "Нет запланированных оплат со статусом 'Оплата запланирована'."
        keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
    else:
        # Группируем оплаты по абонементам (ребенок + кружок)
        grouped_payments = {}
        total_sum = 0
        
        logging.info(f"📋 Начинаю группировку {len(planned_payments)} оплат")
        for payment in planned_payments:
            key = payment['key']  # child_name|circle_name
            logging.info(f"  Обрабатываю оплату с ключом: '{key}'")
            if key not in grouped_payments:
                grouped_payments[key] = {
                    'child_name': payment['child_name'],
                    'circle_name': payment['circle_name'],
                    'payments': []
                }
            grouped_payments[key]['payments'].append(payment)
            
            # Подсчитываем общую сумму
            try:
                budget = float(payment['budget'])
                total_sum += budget
                logging.info(f"  Добавлен бюджет: {budget}")
            except Exception as e:
                logging.warning(f"  Ошибка при парсинге бюджета '{payment['budget']}': {e}")
        
        logging.info(f"📊 Создано групп: {len(grouped_payments)}, общая сумма: {total_sum}")
        
        message_text = "📊 <b>Запланированные оплаты</b>\n\n"
        message_text += f"Найдено <b>{len(planned_payments)}</b> запланированных оплат\n"
        message_text += f"Общая сумма: <b>{total_sum:.0f} руб.</b>\n\n"
        message_text += "Выберите абонемент для управления оплатами:"
        
        # Создаем кнопки для каждого абонемента
        keyboard = []
        logging.info(f"🔘 Создаю кнопки для {len(grouped_payments)} групп")
        for key, group in grouped_payments.items():
            child_name = group['child_name']
            circle_name = group['circle_name']
            payment_count = len(group['payments'])
            
            button_text = f"{child_name} - {circle_name} ({payment_count})"
            callback_data = f"forecast_sub_{key}"
            logging.info(f"  Создаю кнопку: '{button_text}' -> '{callback_data}'")
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")])
        logging.info(f"✅ Создано {len(keyboard)} кнопок (включая кнопку 'Назад')")
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

async def forecast_subscription_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает запланированные оплаты для конкретного абонемента."""
    query = update.callback_query
    logging.info(f"🔍 forecast_subscription_handler ВЫЗВАН! callback_data = '{query.data}'")
    logging.info(f"🔍 User ID: {query.from_user.id}, Chat ID: {query.message.chat.id}")
    await query.answer("✅ Обрабатываю запрос...")
    
    try:
        # Извлекаем ключ абонемента из callback_data
        subscription_key = query.data.replace("forecast_sub_", "")
        logging.info(f"🔍 forecast_subscription_handler: subscription_key = '{subscription_key}'")
        
        child_name, circle_name = subscription_key.split("|")
        logging.info(f"📋 Ребенок: {child_name}, Кружок: {circle_name}")
        
        await query.edit_message_text("🔄 Загружаю оплаты для абонемента...")
        
        # Получаем все запланированные оплаты
        logging.info("📊 Получаю запланированные оплаты...")
        planned_payments = sheets_service.get_planned_payments()
        logging.info(f"📊 Получено {len(planned_payments)} запланированных оплат")
        
        # Фильтруем оплаты для данного абонемента
        subscription_payments = [
            payment for payment in planned_payments 
            if payment['key'] == subscription_key
        ]
        logging.info(f"📊 Отфильтровано {len(subscription_payments)} оплат для абонемента")
    except Exception as e:
        logging.error(f"❌ Ошибка в forecast_subscription_handler: {e}")
        import traceback
        logging.error(traceback.format_exc())
        await query.edit_message_text(
            f"❌ Произошла ошибка: {e}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_forecast")]])
        )
        return MAIN_MENU
    
    if not subscription_payments:
        message_text = f"📊 <b>{child_name} - {circle_name}</b>\n\n"
        message_text += "Нет запланированных оплат для этого абонемента."
        keyboard = [
            [InlineKeyboardButton("⏪ Назад к прогнозу", callback_data="menu_forecast")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
        ]
    else:
        message_text = f"📊 <b>{child_name} - {circle_name}</b>\n\n"
        message_text += f"Найдено <b>{len(subscription_payments)}</b> запланированных оплат:\n\n"
        
        total_sum = 0
        for i, payment in enumerate(subscription_payments, 1):
            budget = payment.get('budget', 0)
            try:
                budget_float = float(budget)
                total_sum += budget_float
            except:
                budget_float = 0
            
            message_text += f"{i}. 📅 <b>{payment['payment_date']}</b> - {budget} руб.\n"
        
        message_text += f"\n💰 <b>Общая сумма:</b> {total_sum:.0f} руб.\n\n"
        message_text += "Выберите оплату для отметки как оплаченной:"
        
        # Создаем кнопки только для продления абонемента (убираем кнопки оплат)
        keyboard = []
        
        # Добавляем кнопки для быстрого продления абонемента
        keyboard.append([InlineKeyboardButton("🔄 Продлить абонемент", callback_data=f"renew_subscription_{subscription_key}")])
        
        keyboard.append([InlineKeyboardButton("⏪ Назад к прогнозу", callback_data="menu_forecast")])
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

async def renewal_subscription_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает начало процесса продления абонемента."""
    query = update.callback_query
    await query.answer("🔄 Подготавливаю продление абонемента...")
    
    try:
        # Извлекаем ключ абонемента из callback_data
        subscription_key = query.data.replace("renew_subscription_", "")
        child_name, circle_name = subscription_key.split("|")
        
        # Сохраняем данные в контексте
        context.user_data['renewal_subscription_key'] = subscription_key
        context.user_data['renewal_child_name'] = child_name
        context.user_data['renewal_circle_name'] = circle_name
        
        # Получаем информацию о текущем абонементе для копирования
        current_sub = sheets_service.get_current_subscription_by_child_circle(child_name, circle_name)
        if not current_sub:
            await query.edit_message_text(
                f"❌ Не найден активный абонемент для {child_name} - {circle_name}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_forecast")]])
            )
            return MAIN_MENU
        
        context.user_data['current_subscription'] = current_sub
        
        # Получаем все прогнозные даты оплаты для этого абонемента
        planned_payments = sheets_service.get_planned_payments()
        subscription_payments = [p for p in planned_payments if p['key'] == subscription_key]
        
        # Сортируем по дате
        if subscription_payments:
            subscription_payments.sort(key=lambda x: x['payment_date'])
        
        message_text = f"🔄 <b>Продление абонемента</b>\n\n"
        message_text += f"👤 <b>Ребенок:</b> {child_name}\n"
        message_text += f"🎨 <b>Кружок:</b> {circle_name}\n\n"
        message_text += f"📋 <b>Текущий абонемент:</b>\n"
        message_text += f"• Тип: {current_sub.get('Тип абонемента', '')}\n"
        message_text += f"• Стоимость: {current_sub.get('Стоимость', '')} руб.\n"
        message_text += f"• Количество занятий: {current_sub.get('К-во занятий', '')}\n\n"
        
        if subscription_payments:
            message_text += f"💰 <b>Прогнозные даты оплат:</b>\n"
            for payment in subscription_payments:
                message_text += f"💳 {payment['payment_date']} - {payment['budget']} руб.\n"
            message_text += "\n"
        
        message_text += "Выберите дату начала нового абонемента:"
        
        # Создаем кнопки для каждой прогнозной даты
        keyboard = []
        for payment in subscription_payments:
            button_text = f"📅 Продлить с {payment['payment_date']}"
            callback_data = f"renewal_use_date_{payment['payment_date']}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        # Добавляем кнопку "Выбрать другую дату"
        keyboard.append([InlineKeyboardButton("📆 Выбрать другую дату", callback_data="renewal_select_custom_date")])
        keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data=f"forecast_sub_{subscription_key}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return RENEWAL_SELECT_DATE_TYPE
        
    except Exception as e:
        logging.error(f"Ошибка в renewal_subscription_handler: {e}")
        await query.edit_message_text(
            f"❌ Произошла ошибка: {e}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_forecast")]])
        )
        return MAIN_MENU

async def renewal_date_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор типа даты для продления."""
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith("renewal_use_date_"):
        # Используем конкретную прогнозную дату
        selected_date = query.data.replace("renewal_use_date_", "")
        context.user_data['renewal_start_date'] = selected_date
        return await renewal_confirm_handler(update, context)
    
    elif query.data == "renewal_select_custom_date":
        # Показываем календарь для выбора даты
        from datetime import datetime
        current_date = datetime.now()
        
        message_text = f"🔄 <b>Продление абонемента</b>\n\n"
        message_text += f"👤 <b>Ребенок:</b> {context.user_data['renewal_child_name']}\n"
        message_text += f"🎨 <b>Кружок:</b> {context.user_data['renewal_circle_name']}\n\n"
        message_text += "📅 Выберите дату начала нового абонемента:"
        
        calendar_keyboard = create_calendar_keyboard(current_date.year, current_date.month)
        await query.edit_message_text(message_text, reply_markup=calendar_keyboard, parse_mode='HTML')
        return RENEWAL_SELECT_CUSTOM_DATE
    
    else:
        # Возврат назад
        subscription_key = context.user_data.get('renewal_subscription_key', '')
        query.data = f"forecast_sub_{subscription_key}"
        return await forecast_subscription_handler(update, context)

async def renewal_custom_date_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор пользовательской даты из календаря."""
    query = update.callback_query
    await query.answer()
    
    if query.data.startswith("cal_day_"):
        # Пользователь выбрал день
        day = int(query.data.replace("cal_day_", ""))
        
        # Получаем текущий месяц и год из контекста или используем текущие
        from datetime import datetime
        current_date = datetime.now()
        selected_date = datetime(current_date.year, current_date.month, day)
        
        context.user_data['renewal_start_date'] = selected_date.strftime('%d.%m.%Y')
        return await renewal_confirm_handler(update, context)
    
    elif query.data.startswith("cal_month_"):
        # Навигация по месяцам
        parts = query.data.split("_")
        year = int(parts[2])
        month = int(parts[3])
        
        message_text = f"🔄 <b>Продление абонемента</b>\n\n"
        message_text += f"👤 <b>Ребенок:</b> {context.user_data['renewal_child_name']}\n"
        message_text += f"🎨 <b>Кружок:</b> {context.user_data['renewal_circle_name']}\n\n"
        message_text += "📅 Выберите дату начала нового абонемента:"
        
        calendar_keyboard = create_calendar_keyboard(year, month)
        await query.edit_message_text(message_text, reply_markup=calendar_keyboard, parse_mode='HTML')
        return RENEWAL_SELECT_CUSTOM_DATE
    
    else:
        # Отмена - возврат к выбору типа даты
        query.data = "renewal_select_custom_date"
        return await renewal_date_type_handler(update, context)

async def renewal_confirm_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает подтверждение создания нового абонемента."""
    query = update.callback_query
    
    try:
        current_sub = context.user_data['current_subscription']
        start_date = context.user_data['renewal_start_date']
        child_name = context.user_data['renewal_child_name']
        circle_name = context.user_data['renewal_circle_name']
        
        message_text = f"✅ <b>Подтверждение продления абонемента</b>\n\n"
        message_text += f"👤 <b>Ребенок:</b> {child_name}\n"
        message_text += f"🎨 <b>Кружок:</b> {circle_name}\n\n"
        message_text += f"📋 <b>Параметры нового абонемента:</b>\n"
        message_text += f"• Тип: {current_sub.get('Тип абонемента', '')}\n"
        message_text += f"• Тип оплаты: {current_sub.get('Тип оплаты', '')}\n"
        message_text += f"• Стоимость: {current_sub.get('Стоимость', '')} руб.\n"
        message_text += f"• Количество занятий: {current_sub.get('К-во занятий', '')}\n"
        message_text += f"• Дата начала: <b>{start_date}</b>\n\n"
        message_text += "⚠️ <b>Внимание:</b> Старые прогнозные даты оплат будут удалены и созданы новые.\n\n"
        message_text += "Подтвердите создание нового абонемента:"
        
        keyboard = [
            [InlineKeyboardButton("✅ Создать абонемент", callback_data="renewal_confirm_create")],
            [InlineKeyboardButton("📅 Изменить дату", callback_data="renewal_select_custom_date")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"forecast_sub_{context.user_data['renewal_subscription_key']}")]
        ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return RENEWAL_CONFIRM
        
    except Exception as e:
        logging.error(f"Ошибка в renewal_confirm_handler: {e}")
        await query.edit_message_text(
            f"❌ Произошла ошибка: {e}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_forecast")]])
        )
        return MAIN_MENU

async def renewal_create_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Создает новый абонемент на основе текущего."""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("🔄 Создаю новый абонемент...")
    
    try:
        current_sub = context.user_data['current_subscription']
        start_date_str = context.user_data['renewal_start_date']
        child_name = context.user_data['renewal_child_name']
        circle_name = context.user_data['renewal_circle_name']
        subscription_key = context.user_data['renewal_subscription_key']
        
        # Подготавливаем данные для нового абонемента
        from datetime import datetime
        start_date = datetime.strptime(start_date_str, '%d.%m.%Y')
        
        # Получаем расписание текущего абонемента
        current_sub_id = current_sub.get('ID абонемента', '')
        logging.info(f"🔍 Получен ID текущего абонемента: '{current_sub_id}'")
        logging.info(f"🔍 Тип ID: {type(current_sub_id)}")
        logging.info(f"🔍 Полные данные абонемента: {current_sub}")
        logging.info(f"🔑 Доступные ключи в current_sub: {list(current_sub.keys()) if current_sub else 'None'}")
        
        schedule_data = sheets_service.get_subscription_schedule(current_sub_id)
        
        if not schedule_data:
            await query.edit_message_text(
                f"❌ Не найдено расписание для текущего абонемента {current_sub_id}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_forecast")]])
            )
            return MAIN_MENU
        
        # Генерируем ID для нового абонемента
        ru_months = ['янв', 'фев', 'мар', 'апр', 'май', 'июн', 'июл', 'авг', 'сен', 'окт', 'ноя', 'дек']
        date_part = f"{start_date.day}{ru_months[start_date.month - 1]}"
        new_sub_id = f"{date_part}.{child_name}{circle_name}-{start_date.year % 100}"
        
        # Шаблон расписания будет создан автоматически в create_full_subscription
        
        # Преобразуем schedule_data в нужный формат
        formatted_schedule = []
        for item in schedule_data:
            # Конвертируем Python день недели (0-6) в формат таблицы (1-7)
            day_num = item['day'] + 1
            formatted_schedule.append({
                'day_num': day_num,
                'start_time': item['start_time'],
                'end_time': item['end_time']
            })
        
        # Логируем данные текущего абонемента для отладки
        logging.info(f"🔍 Данные текущего абонемента для переноса:")
        logging.info(f"  📋 Тип абонемента: '{current_sub.get('Тип абонемента', '')}'")
        logging.info(f"  💳 Тип оплаты: '{current_sub.get('Тип оплаты', '')}'")
        logging.info(f"🔑 ВСЕ ключи: {list(current_sub.keys())}")
        logging.info(f"  💰 Стоимость: '{current_sub.get('Стоимость', '')}'")
        
        # Формируем данные нового абонемента
        new_sub_data = {
            'child_name': child_name,
            'circle_name': circle_name,
            'sub_type': current_sub.get('Тип абонемента', ''),
            'payment_type': current_sub.get('Оплата', '') or current_sub.get('Тип оплаты', ''),
            'cost': current_sub.get('Стоимость', ''),
            'total_classes': current_sub.get('К-во занятий', ''),
            'remaining_classes': current_sub.get('К-во занятий', ''),  # Новый абонемент - все занятия доступны
            'start_date': start_date,
            'schedule': formatted_schedule
        }
        
        # Удаляем старые прогнозные даты оплат для этого абонемента
        await query.edit_message_text("💰 Переношу прогнозные оплаты в Оплачено...")
        transfer_result = sheets_service.transfer_forecast_to_paid(subscription_key, start_date_str)
        
        # Создаем новый абонемент
        await query.edit_message_text("✨ Создаю новый абонемент...")
        
        # Логируем данные для отладки
        logging.info(f"🔍 Передаю в create_full_subscription:")
        logging.info(f"  📋 formatted_schedule: {formatted_schedule}")
        logging.info(f"  📋 new_sub_data['schedule']: {new_sub_data['schedule']}")
        
        result = sheets_service.create_full_subscription(new_sub_data)
        
        if "✅" in result:
            # Запускаем фоновые обновления
            asyncio.create_task(update_after_subscription_creation())
            
            success_message = f"🎉 <b>Абонемент успешно продлен!</b>\n\n"
            success_message += f"👤 <b>Ребенок:</b> {child_name}\n"
            success_message += f"🎨 <b>Кружок:</b> {circle_name}\n"
            success_message += f"📅 <b>Дата начала:</b> {start_date_str}\n\n"
            success_message += f"📋 <b>Результат:</b>\n{result}\n\n"
            success_message += f"🗑️ <b>Удаление старых прогнозов:</b> {transfer_result}\n\n"
            success_message += "🔄 <b>Прогноз бюджета и календарь обновляются в фоне.</b>"
            
            keyboard = [
                [InlineKeyboardButton("📊 Прогноз бюджета", callback_data="menu_forecast")],
                [InlineKeyboardButton("📄 Абонементы", callback_data="menu_subscriptions")],
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
            ]
        else:
            success_message = f"❌ <b>Ошибка при создании абонемента:</b>\n\n{result}"
            keyboard = [
                [InlineKeyboardButton("🔄 Попробовать снова", callback_data="renewal_confirm_create")],
                [InlineKeyboardButton("⏪ Назад к прогнозу", callback_data="menu_forecast")]
            ]
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(success_message, reply_markup=reply_markup, parse_mode='HTML')
        
        # Очищаем данные продления из контекста
        renewal_keys = [k for k in context.user_data.keys() if k.startswith('renewal_')]
        for key in renewal_keys:
            context.user_data.pop(key, None)
        
        return MAIN_MENU
        
    except Exception as e:
        logging.error(f"Ошибка в renewal_create_handler: {e}")
        await query.edit_message_text(
            f"❌ Произошла ошибка при создании абонемента: {e}",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_forecast")]])
        )
        return MAIN_MENU

async def mark_payment_paid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмечает конкретную оплату как оплаченную и перемещает в лист 'Оплачено'."""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем номер строки из callback_data
    row_index = int(query.data.replace("mark_payment_", ""))
    
    await query.edit_message_text("🔄 Перемещаю оплату в лист 'Оплачено'...")
    
    # Перемещаем оплату в лист "Оплачено"
    success, message = sheets_service.move_payment_to_paid(row_index)
    
    if success:
        message_text = f"✅ <b>Оплата отмечена как оплаченная!</b>\n\n{message}"
    else:
        message_text = f"❌ <b>Ошибка!</b>\n\n{message}"
    
    keyboard = [
        [InlineKeyboardButton("⏪ Назад к прогнозу", callback_data="menu_forecast")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

async def mark_all_payments_paid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмечает все оплаты абонемента как оплаченные."""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем ключ абонемента из callback_data
    subscription_key = query.data.replace("mark_paid_all_", "")
    child_name, circle_name = subscription_key.split("|")
    
    await query.edit_message_text(f"🔄 Отмечаю все оплаты для {child_name} - {circle_name} как оплаченные...")
    
    # Отмечаем оплаты как оплаченные
    success, message = sheets_service.mark_payments_as_paid(subscription_key)
    
    if success:
        message_text = f"✅ <b>Успешно!</b>\n\n"
        message_text += f"<b>{child_name} - {circle_name}</b>\n\n"
        message_text += message
    else:
        message_text = f"❌ <b>Ошибка!</b>\n\n"
        message_text += f"<b>{child_name} - {circle_name}</b>\n\n"
        message_text += message
    
    keyboard = [
        [InlineKeyboardButton("⏪ Назад к прогнозу", callback_data="menu_forecast")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

async def manage_individual_payments_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает отдельные оплаты для управления."""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем ключ абонемента из callback_data
    subscription_key = query.data.replace("manage_payments_", "")
    child_name, circle_name = subscription_key.split("|")
    
    await query.edit_message_text("🔄 Загружаю отдельные оплаты...")
    
    # Получаем все запланированные оплаты для данного абонемента
    planned_payments = sheets_service.get_planned_payments()
    subscription_payments = [
        payment for payment in planned_payments 
        if payment['key'] == subscription_key
    ]
    
    if not subscription_payments:
        message_text = f"📊 <b>{child_name} - {circle_name}</b>\n\n"
        message_text += "Нет запланированных оплат для управления."
        keyboard = [
            [InlineKeyboardButton("⏪ Назад к прогнозу", callback_data="menu_forecast")],
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
        ]
    else:
        message_text = f"📊 <b>{child_name} - {circle_name}</b>\n\n"
        message_text += "Выберите оплату для отметки как оплаченной:\n\n"
        
        keyboard = []
        for i, payment in enumerate(subscription_payments):
            try:
                budget = float(payment['budget'])
                button_text = f"{payment['payment_date']} - {budget:.0f} руб."
            except:
                button_text = f"{payment['payment_date']} - {payment['budget']} руб."
            
            callback_data = f"mark_single_paid_{payment['row_index']}"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("⏪ Назад к абонементу", callback_data=f"forecast_sub_{subscription_key}")])
        keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")])
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

async def mark_single_payment_paid_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмечает одну оплату как оплаченную."""
    query = update.callback_query
    await query.answer()
    
    # Извлекаем номер строки из callback_data
    row_index = int(query.data.replace("mark_single_paid_", ""))
    
    await query.edit_message_text("🔄 Отмечаю оплату как оплаченную...")
    
    # Отмечаем оплату как оплаченную
    success, message = sheets_service.mark_single_payment_as_paid(row_index)
    
    if success:
        message_text = f"✅ <b>Успешно!</b>\n\n{message}"
    else:
        message_text = f"❌ <b>Ошибка!</b>\n\n{message}"
    
    keyboard = [
        [InlineKeyboardButton("⏪ Назад к прогнозу", callback_data="menu_forecast")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

# === Меню Календаря ===
def generate_calendar_keyboard(year, month, lessons_by_date):
    """Генерирует клавиатуру календаря с отметками занятий."""
    import calendar
    from datetime import datetime, date
    
    # Получаем календарь для месяца
    cal = calendar.monthcalendar(year, month)
    
    # Названия месяцев
    month_names = [
        '', 'Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
        'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'
    ]
    
    keyboard = []
    
    # Заголовок с месяцем и годом
    keyboard.append([InlineKeyboardButton(f"📅 {month_names[month]} {year}", callback_data="ignore")])
    
    # Дни недели
    keyboard.append([
        InlineKeyboardButton("Пн", callback_data="ignore"),
        InlineKeyboardButton("Вт", callback_data="ignore"),
        InlineKeyboardButton("Ср", callback_data="ignore"),
        InlineKeyboardButton("Чт", callback_data="ignore"),
        InlineKeyboardButton("Пт", callback_data="ignore"),
        InlineKeyboardButton("Сб", callback_data="ignore"),
        InlineKeyboardButton("Вс", callback_data="ignore"),
    ])
    
    # Дни месяца
    for week in cal:
        row = []
        for day in week:
            if day == 0:
                # Пустая ячейка
                row.append(InlineKeyboardButton(" ", callback_data="ignore"))
            else:
                # Проверяем, есть ли ЗАПЛАНИРОВАННЫЕ занятия в этот день
                date_str = f"{day:02d}.{month:02d}.{year}"
                lessons_on_date = lessons_by_date.get(date_str, [])
                
                # Считаем только занятия со статусом "Запланировано"
                planned_lessons_count = 0
                for lesson in lessons_on_date:
                    status = lesson.get('Статус посещения', '').lower()
                    if status == 'запланировано':
                        planned_lessons_count += 1
                
                if planned_lessons_count > 0:
                    # Есть запланированные занятия - добавляем индикатор
                    button_text = f"{day}🔸"
                    callback_data = f"calendar_date_{date_str}"
                else:
                    # Нет запланированных занятий (но могут быть завершенные/пропущенные)
                    button_text = str(day)
                    callback_data = f"calendar_date_{date_str}"
                
                row.append(InlineKeyboardButton(button_text, callback_data=callback_data))
        keyboard.append(row)
    
    # Навигация по месяцам
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    
    keyboard.append([
        InlineKeyboardButton("⬅️", callback_data=f"calendar_nav_{prev_year}_{prev_month}"),
        InlineKeyboardButton("Сегодня", callback_data="calendar_today"),
        InlineKeyboardButton("➡️", callback_data=f"calendar_nav_{next_year}_{next_month}"),
    ])
    
    # Кнопка назад
    keyboard.append([InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")])
    
    return keyboard

async def calendar_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает интерактивный календарь занятий."""
    query = update.callback_query
    await query.answer()
    
    try:
        from datetime import datetime
        import logging
        
        logging.info("🔄 Загрузка календаря занятий...")
        
        # Получаем все занятия из календаря с обработкой ошибок
        try:
            lessons = sheets_service.get_calendar_lessons()
            logging.info(f"✅ Загружено занятий из календаря: {len(lessons) if lessons else 0}")
        except Exception as e:
            logging.error(f"❌ Ошибка при загрузке календаря: {e}")
            error_text = "❌ Ошибка при загрузке календаря"
            if "429" in str(e):
                error_text += "\n\n⚠️ Превышена квота Google Sheets API.\nПодождите 1-2 минуты и попробуйте снова."
            else:
                error_text += f"\n\n{e}"
            
            keyboard = [
                [InlineKeyboardButton("🔄 Попробовать снова", callback_data="menu_calendar")],
                [InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(error_text, reply_markup=reply_markup)
            return MAIN_MENU
        
        if not lessons:
            keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("📅 Календарь занятий пуст.\n\nСначала создайте абонементы.", reply_markup=reply_markup)
            return MAIN_MENU
        
        # Группируем занятия по датам, показываем ВСЕ занятия
        lessons_by_date = {}
        valid_lessons_count = 0
        
        for lesson in lessons:
            date_str = lesson.get('Дата занятия', '')
            
            # Показываем все занятия с валидными датами
            if date_str and date_str.strip():
                if date_str not in lessons_by_date:
                    lessons_by_date[date_str] = []
                lessons_by_date[date_str].append(lesson)
                valid_lessons_count += 1
        
        # Логирование для отладки
        logging.info(f"📊 Валидных занятий с датами: {valid_lessons_count}")
        logging.info(f"📅 Сгруппировано по датам: {len(lessons_by_date)} дат")
        
        if not lessons_by_date:
            keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("📅 Календарь занятий пуст или данные некорректны.\n\nПопробуйте создать абонементы или обновить данные.", reply_markup=reply_markup)
            return MAIN_MENU
        
        if lessons_by_date:
            logging.info(f"Первые 5 дат: {list(lessons_by_date.keys())[:5]}")
            first_date = list(lessons_by_date.keys())[0]
            logging.info(f"Занятий на {first_date}: {len(lessons_by_date[first_date])}")
        
        # Определяем текущий месяц и год
        today = datetime.now()
        year = context.user_data.get('calendar_year', today.year)
        month = context.user_data.get('calendar_month', today.month)
        
        # Сохраняем данные в контексте
        context.user_data['lessons_by_date'] = lessons_by_date
        context.user_data['calendar_year'] = year
        context.user_data['calendar_month'] = month
        
        # Генерируем клавиатуру календаря
        keyboard = generate_calendar_keyboard(year, month, lessons_by_date)
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = "📅 *Интерактивный календарь занятий*\n\n"
        message_text += "• Дни с занятиями отмечены символом 🔸\n"
        message_text += "• Показываются только запланированные занятия\n"
        message_text += "• Нажмите на дату для просмотра занятий"
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        return INTERACTIVE_CALENDAR
        
    except Exception as e:
        keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"❌ Ошибка при загрузке календаря: {e}", reply_markup=reply_markup)
        return MAIN_MENU

async def calendar_navigation_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает навигацию по календарю."""
    query = update.callback_query
    
    try:
        from datetime import datetime
        
        if query.data == "calendar_today":
            # Переход к текущему месяцу
            today = datetime.now()
            current_year = context.user_data.get('calendar_year')
            current_month = context.user_data.get('calendar_month')
            
            # Проверяем, не показываем ли мы уже текущий месяц
            if current_year == today.year and current_month == today.month:
                # Уже показываем текущий месяц, просто отвечаем пользователю
                await query.answer("📅 Уже показан текущий месяц")
                return INTERACTIVE_CALENDAR
            
            await query.answer()
            context.user_data['calendar_year'] = today.year
            context.user_data['calendar_month'] = today.month
        elif query.data.startswith("calendar_nav_"):
            # Навигация по месяцам
            await query.answer()
            parts = query.data.split("_")
            year = int(parts[2])
            month = int(parts[3])
            context.user_data['calendar_year'] = year
            context.user_data['calendar_month'] = month
        
        # Перегенерируем календарь
        lessons_by_date = context.user_data.get('lessons_by_date', {})
        year = context.user_data['calendar_year']
        month = context.user_data['calendar_month']
        
        keyboard = generate_calendar_keyboard(year, month, lessons_by_date)
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = "📅 *Интерактивный календарь занятий*\n\n"
        message_text += "• Дни с занятиями отмечены символом 🔸\n"
        message_text += "• Показываются только запланированные занятия\n"
        message_text += "• Нажмите на дату для просмотра занятий"
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        return INTERACTIVE_CALENDAR
        
    except Exception as e:
        keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"❌ Ошибка навигации: {e}", reply_markup=reply_markup)
        return MAIN_MENU

async def select_calendar_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает занятия на выбранную дату."""
    query = update.callback_query
    await query.answer()
    
    try:
        # Извлекаем дату из callback_data
        date_str = query.data.replace("calendar_date_", "")
        context.user_data['selected_date'] = date_str
        
        # Получаем занятия на эту дату
        lessons_by_date = context.user_data.get('lessons_by_date', {})
        lessons_on_date = lessons_by_date.get(date_str, [])
        
        # Логирование для отладки
        import logging
        logging.info(f"Выбрана дата: {date_str}")
        logging.info(f"Всего дат в lessons_by_date: {len(lessons_by_date)}")
        logging.info(f"Занятий на дату {date_str}: {len(lessons_on_date)}")
        if lessons_by_date:
            logging.info(f"Доступные даты: {list(lessons_by_date.keys())[:5]}")  # Первые 5 дат
        
        if not lessons_on_date:
            keyboard = [[InlineKeyboardButton("⏪ Назад к календарю", callback_data="menu_calendar")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(f"📅 На {date_str} занятий не запланировано.", reply_markup=reply_markup)
            return INTERACTIVE_CALENDAR
        
        # Создаем детальную информацию и кнопки для каждого занятия
        keyboard = []
        message_text = f"📅 *Занятия на {date_str}*\n\n"
        
        # Показываем детальную информацию о каждом занятии
        for i, lesson in enumerate(lessons_on_date):
            # Используем номер строки как ID занятия, если столбец № пустой
            lesson_id = lesson.get('№', '')
            if not lesson_id:
                # Если ID пустой, используем индекс + дату + имя ребенка для уникальности
                lesson_id = f"{date_str}_{i}_{lesson.get('Ребенок', '')}"
            
            subscription_id = lesson.get('ID абонемента', '')
            child_name = lesson.get('Ребенок', '')
            start_time = lesson.get('Время начала', '')
            end_time = lesson.get('Время завершения', '')
            status = lesson.get('Статус посещения', '')
            mark = lesson.get('Отметка', '')
            
            # Логирование для отладки
            import logging
            logging.info(f"Занятие {i+1}: lesson_id='{lesson_id}', child_name='{child_name}', subscription_id='{subscription_id}'")
            
            # Получаем детальную информацию об абонементе
            sub_details = sheets_service.get_subscription_details(subscription_id)
            circle_name = sub_details.get('circle_name', 'Неизвестно') if sub_details else 'Неизвестно'
            
            # Формируем детальный текст занятия
            lesson_text = f"*{child_name}* - {circle_name}"
            
            if start_time and end_time:
                lesson_text += f"\n🕐 {start_time} - {end_time}"
            if status:
                lesson_text += f"\n📊 {status}"
            if mark:
                lesson_text += f"\n✅ {mark}"
            
            # Добавляем информацию об абонементе
            if sub_details:
                lesson_text += f"\n\n📋 *Информация об абонементе:*"
                lesson_text += f"\n🆔 ID: {subscription_id}"
                if sub_details.get('start_date'):
                    lesson_text += f"\n📅 Дата начала: {sub_details['start_date']}"
                if sub_details.get('end_date_forecast'):
                    lesson_text += f"\n🔮 Дата окончания прогноз: {sub_details['end_date_forecast']}"
                if sub_details.get('total_classes'):
                    lesson_text += f"\n📊 К-во занятий: {sub_details['total_classes']}"
                if sub_details.get('attended_classes'):
                    lesson_text += f"\n✅ Прошло занятий: {sub_details['attended_classes']}"
                if sub_details.get('remaining_classes'):
                    lesson_text += f"\n⏳ Осталось занятий: {sub_details['remaining_classes']}"
                if sub_details.get('missed_classes'):
                    lesson_text += f"\n❌ Пропущено: {sub_details['missed_classes']}"
                if sub_details.get('cost'):
                    lesson_text += f"\n💰 Стоимость: {sub_details['cost']} руб."
                
                # Получаем прогнозные даты оплат
                payment_dates = sheets_service.get_forecast_payment_dates(child_name, circle_name)
                if payment_dates:
                    lesson_text += f"\n\n💰 *Прогнозные даты оплат:*"
                    for date in payment_dates:  # Показываем все даты
                        lesson_text += f"\n💳 {date}"
            
            message_text += f"{i+1}. {lesson_text}\n\n"
            
            # Создаем кнопку с именем ребенка и кружком
            button_text = f"{child_name} - {circle_name}"
            if mark:
                button_text += f" ✅"
            
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"lesson_select_{lesson_id}")])
        
        keyboard.append([InlineKeyboardButton("⏪ Назад к календарю", callback_data="menu_calendar")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        return SELECT_LESSON_FROM_DATE
        
    except Exception as e:
        keyboard = [[InlineKeyboardButton("⏪ Назад к календарю", callback_data="menu_calendar")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"❌ Ошибка при загрузке занятий: {e}", reply_markup=reply_markup)
        return INTERACTIVE_CALENDAR

async def select_lesson_from_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает варианты отметок для выбранного занятия."""
    query = update.callback_query
    await query.answer()
    
    try:
        # Логирование для отладки
        import logging
        logging.info(f"select_lesson_from_date: callback_data = {query.data}")
        
        # Извлекаем ID занятия
        lesson_id = query.data.replace("lesson_select_", "")
        context.user_data['selected_lesson_id'] = lesson_id
        
        logging.info(f"Выбрано занятие с ID: {lesson_id}")
        logging.info(f"Сохранено в контексте: {context.user_data.get('selected_lesson_id')}")
        logging.info(f"Полный контекст: {context.user_data}")
        
        # Получаем информацию о занятии для использования в кнопках
        lessons_by_date = context.user_data.get('lessons_by_date', {})
        selected_date = context.user_data.get('selected_date', '')
        lessons_on_date = lessons_by_date.get(selected_date, [])
        
        # Находим текущее занятие
        current_lesson = None
        for lesson in lessons_on_date:
            if str(lesson.get('№', '')) == str(lesson_id):
                current_lesson = lesson
                break
        
        # Получаем статусы посещения из Справочника
        attendance_statuses = sheets_service.get_handbook_items("Статусы посещения")
        
        if not attendance_statuses:
            keyboard = [[InlineKeyboardButton("⏪ Назад к занятиям", callback_data="menu_calendar")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text("❌ Не найдены статусы посещения в Справочнике.", reply_markup=reply_markup)
            return SELECT_LESSON_FROM_DATE
        
        # Создаем кнопки для каждого статуса с эмодзи
        keyboard = []
        
        # Словарь соответствия статусов и эмодзи
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
                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"attendance_mark_{lesson_id}|||{status}")])
        
        keyboard.append([InlineKeyboardButton("⏪ Назад к занятиям", callback_data=f"calendar_date_{context.user_data.get('selected_date', '')}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = f"✅ *Выберите отметку посещения*\n\n"
        
        if current_lesson:
            child_name = current_lesson.get('Ребенок', '')
            subscription_id = current_lesson.get('ID абонемента', '')
            start_time = current_lesson.get('Время начала', '')
            end_time = current_lesson.get('Время завершения', '')
            
            # Получаем название кружка
            sub_details = sheets_service.get_subscription_details(subscription_id)
            circle_name = sub_details.get('circle_name', 'Неизвестно') if sub_details else 'Неизвестно'
            
            message_text += f"👤 *Ребенок:* {child_name}\n"
            message_text += f"🎨 *Кружок:* {circle_name}\n"
            message_text += f"🆔 *ID абонемента:* {subscription_id}\n"
            message_text += f"📅 *Дата занятия:* {selected_date}\n"
            if start_time and end_time:
                message_text += f"🕐 *Время:* {start_time} - {end_time}\n"
        else:
            message_text += f"Занятие ID: {lesson_id}\n"
            message_text += f"Дата: {selected_date}\n"
        
        message_text += "\nВыберите статус посещения:"
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='Markdown')
        return SELECT_ATTENDANCE_MARK
        
    except Exception as e:
        keyboard = [[InlineKeyboardButton("⏪ Назад к календарю", callback_data="menu_calendar")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"❌ Ошибка при загрузке статусов: {e}", reply_markup=reply_markup)
        return INTERACTIVE_CALENDAR

async def save_attendance_mark(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Сохраняет отметку посещения и запускает обновления."""
    query = update.callback_query
    
    try:
        # ПОДРОБНЫЕ ЛОГИ ДЛЯ ОТЛАДКИ УВЕДОМЛЕНИЙ
        import logging
        logging.info("=" * 80)
        logging.info("🔍 НАЧАЛО ОБРАБОТКИ ОТМЕТКИ ПОСЕЩЕНИЯ")
        logging.info(f"📱 Chat ID: {query.message.chat_id}")
        logging.info(f"📨 Message ID: {query.message.message_id}")
        logging.info(f"👤 User ID: {query.from_user.id}")
        logging.info(f"📋 Полный callback_data: '{query.data}'")
        logging.info(f"🔄 Текущее состояние: {context.user_data}")
        
        # Извлекаем ID занятия и отметку из callback_data
        callback_parts = query.data.replace("attendance_mark_", "").split("|||", 1)
        logging.info(f"🔧 Части callback_data после обработки: {callback_parts}")
        
        if len(callback_parts) == 2:
            lesson_id, attendance_mark = callback_parts
            logging.info(f"✅ Успешно извлечены данные:")
            logging.info(f"   📝 lesson_id: '{lesson_id}'")
            logging.info(f"   ✏️ attendance_mark: '{attendance_mark}'")
        else:
            lesson_id = None
            attendance_mark = query.data.replace("attendance_mark_", "")
            logging.error(f"❌ Ошибка парсинга callback_data:")
            logging.error(f"   📊 Количество частей: {len(callback_parts)}")
            logging.error(f"   📋 Части: {callback_parts}")
            logging.error(f"   📝 attendance_mark (fallback): '{attendance_mark}'")

        logging.info(f"🎯 Финальные значения:")
        logging.info(f"   📝 lesson_id: '{lesson_id}'")
        logging.info(f"   ✏️ attendance_mark: '{attendance_mark}'")
        
        if not lesson_id:
            logging.error("❌ ID занятия не найден в callback_data")
            await query.answer("❌ Ошибка: не выбрано занятие")
            
            # Для уведомлений показываем простое сообщение об ошибке
            error_text = "❌ <b>Ошибка обработки уведомления</b>\n\n"
            error_text += "Не удалось определить ID занятия.\n"
            error_text += "Попробуйте отметить посещение через календарь занятий."
            
            keyboard = [[InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(error_text, reply_markup=reply_markup, parse_mode='HTML')
            logging.info("🔚 ЗАВЕРШЕНИЕ: Возврат к MAIN_MENU")
            return MAIN_MENU
        
        # Сразу показываем процесс обновления БЕЗ query.answer() чтобы избежать timeout
        logging.info("🔄 Начинаю обработку отметки без answer для избежания timeout")
        
        processing_message = f"🔄 <b>Обновление данных...</b>\n\n"
        processing_message += f"✅ <b>Отметка:</b> {attendance_mark}\n"
        processing_message += f"📊 <b>Статус:</b> Сохранение в Google Sheets...\n\n"
        processing_message += "⏳ Пожалуйста, подождите..."
        
        try:
            await query.edit_message_text(processing_message, parse_mode='HTML')
        except Exception as edit_error:
            logging.warning(f"⚠️ Не удалось обновить сообщение: {edit_error}")
            # Попробуем отправить новое сообщение
            try:
                await query.message.reply_text(processing_message, parse_mode='HTML')
            except Exception as reply_error:
                logging.error(f"❌ Не удалось отправить сообщение: {reply_error}")
        
        # 1. Сохраняем отметку в Google Sheets
        logging.info(f"📝 Сохранение отметки '{attendance_mark}' для занятия {lesson_id}")
        success = sheets_service.update_lesson_mark(lesson_id, attendance_mark)
        
        if not success:
            error_message = f"❌ <b>Ошибка при сохранении отметки</b>\n\n"
            error_message += f"Не удалось сохранить отметку '{attendance_mark}' для занятия {lesson_id}.\n"
            error_message += "Попробуйте еще раз или обратитесь к администратору."
            
            keyboard = [
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
                [InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(error_message, reply_markup=reply_markup, parse_mode='HTML')
            return MAIN_MENU
        
        # 2. Запускаем полное фоновое обновление (как в календаре)
        logging.info("🔄 Запуск полного фонового обновления данных...")
        
        # Обновляем сообщение о процессе
        processing_message = f"🔄 <b>Обновление данных...</b>\n\n"
        processing_message += f"✅ <b>Отметка сохранена:</b> {attendance_mark}\n"
        processing_message += f"📊 <b>Статус:</b> Обновление статистики и прогнозов...\n\n"
        processing_message += "⏳ Почти готово..."
        
        await query.edit_message_text(processing_message, parse_mode='HTML')
        
        # Запускаем фоновое обновление и ждем его завершения
        try:
            await update_data_in_background()
            logging.info("✅ Фоновое обновление завершено успешно")
            
            # 3. Показываем PUSH-уведомление об успехе на 3 секунды
            success_message = f"🎉 <b>УСПЕШНО!</b>\n\n"
            success_message += f"✅ <b>Отметка сохранена:</b> {attendance_mark}\n"
            success_message += f"📊 <b>Все данные обновлены:</b>\n"
            success_message += "• Статистика абонементов\n"
            success_message += "• Прогноз бюджета\n"
            success_message += "• Google Calendar синхронизирован\n"
            success_message += "• Дубли очищены\n\n"
            success_message += "🚀 <b>Готово!</b>"
            
            await query.edit_message_text(success_message, parse_mode='HTML')
            
            # Ждем 3 секунды
            await asyncio.sleep(3)
            
            # 4. Показываем уведомление об успехе и возвращаемся к календарю
            logging.info("🔄 Возвращаюсь к интерактивному календарю после сохранения отметки")
            
            # Показываем краткое уведомление об успехе
            success_text = f"✅ <b>Отметка сохранена!</b>\n\n"
            success_text += f"📝 <b>Отметка:</b> {attendance_mark}\n"
            success_text += f"📊 <b>Статус:</b> Все данные обновлены\n\n"
            success_text += "🔄 Возвращаюсь к календарю..."
            
            await query.edit_message_text(success_text, parse_mode='HTML')
            
            # Небольшая пауза для показа уведомления
            await asyncio.sleep(1.5)
            
            # Возвращаемся к интерактивному календарю
            try:
                # Вызываем функцию календаря для показа интерактивного календаря
                return await calendar_menu(update, context)
                
            except Exception as calendar_error:
                logging.error(f"❌ Ошибка при возврате к календарю: {calendar_error}")
                # Fallback - показываем кнопку перехода к календарю
                fallback_text = f"✅ <b>Отметка '{attendance_mark}' сохранена!</b>\n\n"
                fallback_text += "📊 Все данные обновлены.\n"
                fallback_text += "Нажмите кнопку для возврата к календарю:"
                
                keyboard = [[InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await query.edit_message_text(fallback_text, reply_markup=reply_markup, parse_mode='HTML')
                return MAIN_MENU
            
        except Exception as update_error:
            logging.error(f"❌ Ошибка при фоновом обновлении: {update_error}")
            
            # Показываем сообщение об ошибке обновления
            error_message = f"⚠️ <b>Отметка сохранена, но есть проблемы</b>\n\n"
            error_message += f"✅ <b>Отметка:</b> {attendance_mark} - сохранена\n"
            error_message += f"❌ <b>Обновление данных:</b> Ошибка\n\n"
            error_message += f"📝 <b>Детали:</b> {str(update_error)}\n\n"
            error_message += "Попробуйте обновить данные вручную через настройки."
            
            keyboard = [
                [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
                [InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(error_message, reply_markup=reply_markup, parse_mode='HTML')
        
        logging.info("✅ УСПЕШНОЕ ЗАВЕРШЕНИЕ ОБРАБОТКИ ОТМЕТКИ")
        logging.info(f"🔚 Возврат к состоянию: INTERACTIVE_CALENDAR")
        logging.info("=" * 80)
        
        return INTERACTIVE_CALENDAR
        
    except Exception as e:
        logging.error("=" * 80)
        logging.error("❌ КРИТИЧЕСКАЯ ОШИБКА В ОБРАБОТКЕ ОТМЕТКИ")
        logging.error(f"🔥 Тип ошибки: {type(e).__name__}")
        logging.error(f"📝 Сообщение ошибки: {str(e)}")
        logging.error(f"📋 Callback data: {query.data}")
        logging.error(f"🔄 Контекст: {context.user_data}")
        import traceback
        logging.error(f"📊 Полный traceback:\n{traceback.format_exc()}")
        logging.error("=" * 80)
        
        try:
            keyboard = [[InlineKeyboardButton("⏪ Назад к календарю", callback_data="menu_calendar")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(f"❌ Критическая ошибка: {e}", reply_markup=reply_markup)
        except Exception as edit_error:
            logging.error(f"❌ Ошибка при редактировании сообщения: {edit_error}")
            try:
                await query.answer(f"❌ Ошибка: {e}", show_alert=True)
            except Exception as answer_error:
                logging.error(f"❌ Ошибка при отправке ответа: {answer_error}")
        
        return MAIN_MENU

# Старая функция удалена - теперь используем update_data_in_background() для полного обновления

async def generate_attendance_report(lesson_id: str, attendance_mark: str) -> str:
    """Генерирует детальный отчет после сохранения отметки посещения."""
    try:
        import logging
        logging.info(f"Генерирую отчет для занятия {lesson_id} с отметкой '{attendance_mark}'")
        
        # Получаем информацию о занятии
        lesson_info = sheets_service.get_lesson_info_by_id(lesson_id)
        if not lesson_info:
            return f"✅ Отметка '*{attendance_mark}*' сохранена!\n\n🔄 Данные обновляются в фоне."
        
        subscription_id = lesson_info.get('ID абонемента', '')
        child_name = lesson_info.get('Ребенок', '')
        lesson_date = lesson_info.get('Дата занятия', '')
        
        # Получаем детальную информацию об абонементе
        sub_details = sheets_service.get_subscription_details(subscription_id)
        circle_name = sub_details.get('circle_name', 'Неизвестно') if sub_details else 'Неизвестно'
        
        # Получаем все занятия по этому абонементу с отметками
        all_lessons = sheets_service.get_lessons_by_subscription_with_marks(subscription_id)
        
        # Получаем прогнозные даты оплат
        payment_dates = sheets_service.get_forecast_payment_dates(child_name, circle_name)
        
        # Получаем прогнозируемый бюджет
        forecast_budget = sheets_service.get_forecast_budget_for_child_circle(child_name, circle_name)
        
        # Формируем отчет
        message = f"✅ *Отметка '{attendance_mark}' сохранена!*\n\n"
        message += f"🎨 *Кружок:* {circle_name}\n"
        message += f"👤 *Ребенок:* {child_name}\n"
        message += f"📅 *Дата занятия:* {lesson_date}\n\n"
        
        # Статистика по абонементу
        if sub_details:
            message += f"📊 *Статистика абонемента:*\n"
            message += f"🆔 ID: {subscription_id}\n"
            message += f"📚 Всего занятий: {sub_details.get('total_classes', 0)}\n"
            message += f"✅ Прошло: {sub_details.get('attended_classes', 0)}\n"
            message += f"⏳ Осталось: {sub_details.get('remaining_classes', 0)}\n"
            message += f"❌ Пропущено: {sub_details.get('missed_classes', 0)}\n"
            if sub_details.get('cost'):
                message += f"💰 Стоимость: {sub_details['cost']} руб.\n\n"
        
        # История отметок
        if all_lessons:
            message += f"📋 *История занятий:*\n"
            for lesson in all_lessons[-5:]:  # Последние 5 занятий
                date = lesson.get('Дата занятия', '')
                mark = lesson.get('Отметка', '')
                status = lesson.get('Статус посещения', '')
                if mark:
                    message += f"• {date}: {mark}\n"
                else:
                    message += f"• {date}: {status}\n"
            message += "\n"
        
        # Прогнозные даты оплат
        if payment_dates:
            message += f"💳 *Прогнозные даты оплат:*\n"
            for date in payment_dates[:3]:  # Первые 3 даты
                message += f"• {date}\n"
            message += "\n"
        
        # Прогнозируемый бюджет
        if forecast_budget:
            message += f"💰 *Прогнозируемый бюджет:* {forecast_budget} руб.\n\n"
        
        message += "🔄 *Данные обновляются в фоне.*"
        
        return message
        
    except Exception as e:
        logging.error(f"Ошибка при генерации отчета: {e}")
        return f"✅ Отметка '*{attendance_mark}*' сохранена!\n\n🔄 Данные обновляются в фоне."

async def update_after_subscription_creation():
    """Асинхронно обновляет статистику и календари после создания нового абонемента."""
    try:
        import logging
        logging.info("=== НАЧАЛО ФОНОВЫХ ОБНОВЛЕНИЙ ПОСЛЕ СОЗДАНИЯ АБОНЕМЕНТА ===")
        
        # 1. Обновить прогноз бюджета (создание прогнозных дат оплат)
        logging.info("1. Запуск обновления прогноза бюджета...")
        sheets_service.update_full_forecast()
        logging.info("Прогноз бюджета обновлен")
        
        # 2. Синхронизация с Google Calendar (фоновая синхронизация)
        logging.info("2. Синхронизация с Google Calendar...")
        try:
            calendar_result = sheets_service.sync_calendar_with_google_calendar()
            logging.info(f"Синхронизация календаря завершена: {calendar_result[:100]}...")
        except Exception as e:
            logging.error(f"Ошибка при синхронизации календаря: {e}")
        
        # 3. Синхронизация прогноза с Google Calendar (фоновая синхронизация)
        logging.info("3. Синхронизация прогноза с Google Calendar...")
        try:
            forecast_result = sheets_service.sync_forecast_with_google_calendar()
            logging.info(f"Синхронизация прогноза завершена: {forecast_result[:100]}...")
        except Exception as e:
            logging.error(f"Ошибка при синхронизации прогноза: {e}")
        
        # 4. Очистка дублей в Google Calendar (фоновая очистка)
        logging.info("4. Очистка дублей в Google Calendar...")
        try:
            clean_result = sheets_service.clean_duplicate_events()
            logging.info(f"Очистка дублей завершена: {clean_result[:100]}...")
        except Exception as e:
            logging.error(f"Ошибка при очистке дублей: {e}")
        
        # ПРИМЕЧАНИЕ: update_subscriptions_statistics() НЕ вызывается при создании абонемента,
        # потому что календарь уже создан правильно, а статистика пока не нужна
        
        logging.info("=== ЗАВЕРШЕНИЕ ФОНОВЫХ ОБНОВЛЕНИЙ ПОСЛЕ СОЗДАНИЯ АБОНЕМЕНТА ===")
        
    except Exception as e:
        logging.error(f"Ошибка при фоновых обновлениях после создания абонемента: {e}")

# Функция синхронизации с Google Calendar удалена

async def select_calendar_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает занятия для выбранного абонемента."""
    query = update.callback_query
    await query.answer()
    
    subscription_id = query.data.replace("calendar_sub_", "")
    context.user_data['selected_subscription_id'] = subscription_id
    
    try:
        # Получаем занятия для данного абонемента
        lessons = sheets_service.get_lessons_by_subscription(subscription_id)
        
        if not lessons:
            keyboard = [[InlineKeyboardButton("⏪ Назад к списку абонементов", callback_data="menu_calendar")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.edit_message_text(f"📅 Занятия для абонемента {subscription_id} не найдены.", reply_markup=reply_markup)
            return SELECT_CALENDAR_SUBSCRIPTION
        
        # Создаем кнопки для каждого занятия
        keyboard = []
        for i, lesson in enumerate(lessons):
            lesson_date = lesson.get('Дата занятия', '')
            lesson_time = lesson.get('Время начала', '')
            status = lesson.get('Статус посещения', '')
            mark = lesson.get('Отметка', '')
            
            # Форматируем отображение
            status_emoji = {
                'Запланировано': '⏳',
                'Завершен': '✅',
                'Пропуск': '❌'
            }.get(status, '❓')
            
            mark_text = f" ({mark})" if mark else ""
            button_text = f"{status_emoji} {lesson_date} {lesson_time}{mark_text}"
            
            # Сохраняем номер строки в callback_data (i+2 потому что строка 1 - заголовки)
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"lesson_{i+2}")])
        
        keyboard.append([InlineKeyboardButton("⏪ Назад к списку абонементов", callback_data="menu_calendar")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(f"📅 Занятия для абонемента {subscription_id}:\n\nВыберите занятие для отметки:", reply_markup=reply_markup)
        return SELECT_LESSON
        
    except Exception as e:
        keyboard = [[InlineKeyboardButton("⏪ Назад к списку абонементов", callback_data="menu_calendar")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"❌ Ошибка при загрузке занятий: {e}", reply_markup=reply_markup)
        return SELECT_CALENDAR_SUBSCRIPTION

async def select_lesson(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает варианты отметок для выбранного занятия."""
    query = update.callback_query
    await query.answer()
    
    lesson_row = int(query.data.replace("lesson_", ""))
    context.user_data['selected_lesson_row'] = lesson_row
    
    subscription_id = context.user_data.get('selected_subscription_id', '')
    
    keyboard = [
        [InlineKeyboardButton("✅ Посещение", callback_data="mark_посещение")],
        [InlineKeyboardButton("❌ Пропуск (по вине)", callback_data="mark_пропуск (по вине)")],
        [InlineKeyboardButton("🤒 Отмена (болезнь)", callback_data="mark_отмена (болезнь)")],
        [InlineKeyboardButton("📅 Перенос", callback_data="mark_перенос")],
        [InlineKeyboardButton("⏪ Назад к занятиям", callback_data=f"calendar_sub_{subscription_id}")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text("Выберите отметку для занятия:", reply_markup=reply_markup)
    return SELECT_ATTENDANCE_MARK

async def select_attendance_mark(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор отметки посещения."""
    query = update.callback_query
    await query.answer()
    
    mark = query.data.replace("mark_", "")
    lesson_row = context.user_data.get('selected_lesson_row')
    subscription_id = context.user_data.get('selected_subscription_id', '')
    
    try:
        # Обновляем отметку в Google Sheets
        success = sheets_service.update_lesson_mark(lesson_row, mark, subscription_id)
        
        if success:
            # Обновляем статистику абонементов
            sheets_service.update_subscription_stats(subscription_id)
            
            await query.edit_message_text(
                f"✅ Отметка '{mark}' успешно сохранена!\n\nСтатистика абонементов обновлена.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("⏪ Назад к занятиям", callback_data=f"calendar_sub_{subscription_id}"),
                    InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")
                ]])
            )
            return CALENDAR_LESSONS
        else:
            await query.edit_message_text(
                f"❌ Ошибка при сохранении отметки.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔄 Попробовать снова", callback_data=f"lesson_{lesson_row}"),
                    InlineKeyboardButton("⏪ Назад к занятиям", callback_data=f"calendar_sub_{subscription_id}")
                ]])
            )
        
        return SELECT_LESSON
        
    except Exception as e:
        await query.edit_message_text(
            f"❌ Ошибка при обновлении отметки: {e}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("⏪ Назад к занятиям", callback_data=f"calendar_sub_{subscription_id}")
            ]])
        )
        return SELECT_LESSON

# === Меню "Настройки" ===
async def settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает главное меню настроек."""
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("🎨 Управлять занятиями", callback_data="settings_cat_Название кружка")],
        [InlineKeyboardButton("👤 Управлять детьми", callback_data="settings_cat_Ребенок")],
        [InlineKeyboardButton("💳 Управлять оплатой", callback_data="settings_cat_Оплата")],
        [InlineKeyboardButton("🔔 Настройка уведомлений", callback_data="notification_settings")],
        [InlineKeyboardButton("🧪 Тест уведомлений", callback_data="test_notifications")],
        [InlineKeyboardButton("🔄 Обновить Google календарь", callback_data="sync_google_calendar")],
        [InlineKeyboardButton("💰 Google прогноз", callback_data="sync_google_forecast")],
        [InlineKeyboardButton("🧹 Очистить дубли", callback_data="clean_duplicates")],
        [InlineKeyboardButton("🔄 Обновить статистику", callback_data="menu_update_stats")],
        [InlineKeyboardButton("🔄 Обновить абонементы", callback_data="menu_update_subscriptions")],
        [InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("⚙️ <b>Настройки</b>\n\nВыберите категорию для управления:", reply_markup=reply_markup, parse_mode='HTML')
    return SETTINGS_MENU

async def settings_show_category_items(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отображает список элементов выбранной категории в виде кнопок."""
    query = update.callback_query
    await query.answer()

    category_header = query.data.split('_cat_')[1]
    context.user_data['settings_category_header'] = category_header
    
    category_titles = {'Название кружка': 'занятиями', 'Ребенок': 'детьми', 'Оплата': 'оплатой'}
    context.user_data['settings_category_title'] = category_titles.get(category_header, 'элементами')

    await query.edit_message_text(f"🔄 Загружаю список: {context.user_data['settings_category_title']}...")
    
    return await _display_settings_category_list(update, context, query.edit_message_text)


async def show_category_items_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает нажатия в меню списка элементов."""
    query = update.callback_query
    await query.answer()
    
    if query.data == 'settings_add':
        header = context.user_data['settings_category_header']
        sent_message = await query.edit_message_text(f"Введите новое значение для категории '{header}':")
        context.user_data['prompt_message_id'] = sent_message.message_id
        return ADD_ITEM
    
    elif query.data.startswith('settings_select_item_'):
        item_name = query.data.split('settings_select_item_')[1]
        context.user_data['settings_selected_item'] = item_name
        
        keyboard = [
            [InlineKeyboardButton("✏️ Редактировать", callback_data="settings_edit_item")],
            [InlineKeyboardButton("🗑️ Удалить", callback_data="settings_delete_item")],
            [InlineKeyboardButton("⏪ Назад к списку", callback_data=f"settings_cat_{context.user_data['settings_category_header']}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(f"Выбран элемент: <b>{item_name}</b>\n\nВыберите действие:", reply_markup=reply_markup, parse_mode='HTML')
        return MANAGE_SINGLE_ITEM

async def manage_single_item_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает действия 'Редактировать' или 'Удалить'."""
    query = update.callback_query
    await query.answer()

    action = query.data
    item_name = context.user_data['settings_selected_item']

    if action == "settings_edit_item":
        sent_message = await query.edit_message_text(f"Введите новое значение для '{item_name}':")
        context.user_data['prompt_message_id'] = sent_message.message_id
        return GET_NEW_VALUE_FOR_EDIT
    
    elif action == "settings_delete_item":
        keyboard = [
            [InlineKeyboardButton("❗️ Да, удалить", callback_data="settings_confirm_delete")],
            [InlineKeyboardButton("⏪ Нет, отмена", callback_data=f"settings_select_item_{item_name}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(f"Вы уверены, что хотите удалить '{item_name}'?", reply_markup=reply_markup)
        return CONFIRM_DELETE_ITEM

async def add_item_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Добавляет новый элемент и возвращается к списку."""
    await update.message.delete()
    if 'prompt_message_id' in context.user_data:
        try: await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=context.user_data.pop('prompt_message_id'))
        except Exception as e: logging.warning(f"Could not delete prompt message: {e}")

    new_value = update.message.text.strip()
    header = context.user_data['settings_category_header']
    sheets_service.add_handbook_item(header, new_value)
    
    return await _display_settings_category_list(update, context, context.bot.send_message)

async def get_new_value_for_edit_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Получает новое значение, редактирует элемент и возвращается к списку."""
    await update.message.delete()
    if 'prompt_message_id' in context.user_data:
        try: await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=context.user_data.pop('prompt_message_id'))
        except Exception as e: logging.warning(f"Could not delete prompt message: {e}")

    new_value = update.message.text.strip()
    old_value = context.user_data['settings_selected_item']
    header = context.user_data['settings_category_header']
    sheets_service.edit_handbook_item(header, old_value, new_value)
    return await _display_settings_category_list(update, context, context.bot.send_message)

async def confirm_delete_item_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Подтверждает и удаляет элемент, затем возвращается к списку."""
    query = update.callback_query
    item_to_delete = context.user_data['settings_selected_item']
    header = context.user_data['settings_category_header']
    success, message = sheets_service.delete_handbook_item(header, item_to_delete)
    await query.answer(text=message, show_alert=True)
    return await _display_settings_category_list(update, context, query.edit_message_text)


# === Настройка уведомлений ===
async def notification_settings_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает настройки уведомлений."""
    query = update.callback_query
    await query.answer()
    
    try:
        # Получаем текущее время уведомлений
        current_time = sheets_service.get_notification_time()
        
        message_text = "🔔 <b>Настройка уведомлений</b>\n\n"
        message_text += "📋 Система будет отправлять ежедневные уведомления о занятиях с возможностью быстрой отметки посещения.\n\n"
        
        if current_time:
            message_text += f"⏰ <b>Текущее время уведомлений:</b> {current_time}\n\n"
        else:
            message_text += "⏰ <b>Время уведомлений не настроено</b>\n\n"
            
        message_text += "🕘 Выберите время для ежедневных уведомлений:"
        
        # Создаем кнопки с временными интервалами (с 9:00 до 21:00 с шагом 30 минут)
        keyboard = []
        times = []
        
        for hour in range(9, 22):  # с 9 до 21
            for minute in [0, 30]:
                time_str = f"{hour:02d}:{minute:02d}"
                times.append(time_str)
        
        # Группируем по 3 кнопки в ряд
        for i in range(0, len(times), 3):
            row = []
            for j in range(3):
                if i + j < len(times):
                    time_str = times[i + j]
                    # Выделяем текущее время
                    if current_time == time_str:
                        button_text = f"✅ {time_str}"
                    else:
                        button_text = time_str
                    row.append(InlineKeyboardButton(button_text, callback_data=f"set_notification_time_{time_str}"))
            keyboard.append(row)
        
        # Добавляем кнопку отключения уведомлений
        if current_time:
            keyboard.append([InlineKeyboardButton("🔕 Отключить уведомления", callback_data="disable_notifications")])
        
        keyboard.append([InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        
        return NOTIFICATION_TIME_SETTINGS
        
    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке настроек уведомлений: {e}")
        message_text = f"❌ <b>Ошибка при загрузке настроек:</b>\n\n{e}"
        keyboard = [[InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return SETTINGS_MENU

async def set_notification_time_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Устанавливает время уведомлений."""
    query = update.callback_query
    await query.answer()
    
    try:
        # Извлекаем время из callback_data
        time_str = query.data.replace("set_notification_time_", "")
        
        # Сохраняем время в Справочник (ячейка N2)
        success = sheets_service.set_notification_time(time_str)
        
        if success:
            # Также сохраняем chat_id пользователя
            chat_id = query.message.chat_id
            success_chat = sheets_service.set_notification_chat_id(str(chat_id))
            
            message_text = f"✅ <b>Время уведомлений установлено!</b>\n\n"
            message_text += f"⏰ <b>Время:</b> {time_str}\n\n"
            message_text += "📬 Теперь каждый день в это время вы будете получать уведомления о занятиях с возможностью быстрой отметки посещения.\n\n"
            message_text += "🔔 <b>Что будет происходить:</b>\n"
            message_text += "• Система найдет все занятия на текущий день\n"
            message_text += "• Отправит уведомление по каждому занятию\n"
            message_text += "• Вы сможете быстро отметить посещение одним нажатием\n"
            message_text += "• Все данные автоматически обновятся в фоне"
            
            # Инициализируем планировщик если он еще не запущен
            global notification_scheduler
            if notification_scheduler is None:
                from notification_scheduler import get_notification_scheduler
                notification_scheduler = get_notification_scheduler(context.bot)
                
            # Устанавливаем chat_id в планировщик
            if notification_scheduler:
                notification_scheduler.set_chat_id(chat_id)
                
            if notification_scheduler and not notification_scheduler.is_running:
                await notification_scheduler.start_scheduler()
                message_text += "\n\n🚀 <b>Планировщик уведомлений запущен!</b>"
        else:
            message_text = "❌ <b>Ошибка при сохранении времени уведомлений</b>\n\nПопробуйте еще раз."
        
        keyboard = [
            [InlineKeyboardButton("🔔 Изменить время", callback_data="notification_settings")],
            [InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return NOTIFICATION_TIME_SETTINGS  # Остаемся в том же состоянии для корректной навигации
        
    except Exception as e:
        logging.error(f"❌ Ошибка при установке времени уведомлений: {e}")
        message_text = f"❌ <b>Ошибка:</b>\n\n{e}"
        keyboard = [[InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return SETTINGS_MENU

async def disable_notifications_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отключает уведомления."""
    query = update.callback_query
    await query.answer()
    
    try:
        # Очищаем время в Справочнике
        success = sheets_service.set_notification_time("")
        
        if success:
            message_text = "🔕 <b>Уведомления отключены</b>\n\n"
            message_text += "Ежедневные уведомления о занятиях больше не будут отправляться.\n\n"
            message_text += "Вы можете включить их снова в любое время через настройки."
            
            # Останавливаем планировщик
            global notification_scheduler
            if notification_scheduler and notification_scheduler.is_running:
                await notification_scheduler.stop_scheduler()
                message_text += "\n\n⏹️ <b>Планировщик уведомлений остановлен</b>"
        else:
            message_text = "❌ <b>Ошибка при отключении уведомлений</b>\n\nПопробуйте еще раз."
        
        keyboard = [
            [InlineKeyboardButton("🔔 Настроить уведомления", callback_data="notification_settings")],
            [InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return SETTINGS_MENU
        
    except Exception as e:
        logging.error(f"❌ Ошибка при отключении уведомлений: {e}")
        message_text = f"❌ <b>Ошибка:</b>\n\n{e}"
        keyboard = [[InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return SETTINGS_MENU

async def test_notifications_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Тестирует систему уведомлений."""
    query = update.callback_query
    await query.answer()
    
    try:
        await query.edit_message_text("🧪 <b>Тестирование системы уведомлений</b>\n\n⏳ Ищу занятия на сегодня...", parse_mode='HTML')
        
        # Импортируем планировщик
        from notification_scheduler import get_notification_scheduler
        
        # Получаем планировщик
        notification_scheduler = get_notification_scheduler(context.bot)
        
        if notification_scheduler is None:
            notification_scheduler = get_notification_scheduler(context.bot)
        
        # Устанавливаем текущий chat_id для тестирования
        current_chat_id = query.message.chat_id
        notification_scheduler.set_chat_id(current_chat_id)
        
        # Получаем занятия на сегодня
        today_lessons = notification_scheduler._get_today_lessons()
        
        if not today_lessons:
            message_text = "🧪 <b>Результат тестирования</b>\n\n"
            message_text += "📅 На сегодня занятий не найдено или все уже отмечены.\n\n"
            message_text += "💡 <b>Для тестирования:</b>\n"
            message_text += "• Добавьте занятие на сегодня в 'Календарь занятий'\n"
            message_text += "• Убедитесь, что колонка 'Отметка' пустая\n"
            message_text += "• Попробуйте тест еще раз"
        else:
            message_text = f"🧪 <b>Результат тестирования</b>\n\n"
            message_text += f"✅ Найдено занятий на сегодня: {len(today_lessons)}\n\n"
            message_text += "📬 <b>Отправляю тестовые уведомления...</b>\n\n"
            
            # Отправляем уведомления
            for i, lesson in enumerate(today_lessons, 1):
                try:
                    await notification_scheduler._send_lesson_notification(lesson)
                    message_text += f"• Занятие {i}: {lesson['child_name']} - {lesson['circle_name']} ✅\n"
                except Exception as e:
                    message_text += f"• Занятие {i}: {lesson['child_name']} - {lesson['circle_name']} ❌ (ошибка)\n"
                    logging.error(f"Ошибка при отправке тестового уведомления: {e}")
                await asyncio.sleep(2)  # Увеличиваем задержку между уведомлениями
            
            message_text += f"\n🎉 <b>Отправлено {len(today_lessons)} уведомлений!</b>\n\n"
            message_text += "📱 Проверьте чат - должны прийти уведомления с кнопками отметок."
        
        keyboard = [
            [InlineKeyboardButton("🔄 Повторить тест", callback_data="test_notifications")],
            [InlineKeyboardButton("🔔 Настройки уведомлений", callback_data="notification_settings")],
            [InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return NOTIFICATION_TIME_SETTINGS  # Остаемся в том же состоянии для корректной навигации
        
    except Exception as e:
        logging.error(f"❌ Ошибка при тестировании уведомлений: {e}")
        message_text = f"❌ <b>Ошибка при тестировании:</b>\n\n{e}\n\n"
        message_text += "🔧 <b>Возможные причины:</b>\n"
        message_text += "• Не настроен TELEGRAM_CHAT_ID в .env\n"
        message_text += "• Нет доступа к Google Sheets\n"
        message_text += "• Ошибка в данных календаря"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Повторить тест", callback_data="test_notifications")],
            [InlineKeyboardButton("⏪ Назад к настройкам", callback_data="menu_settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return SETTINGS_MENU

# === Общий обработчик для отладки всех callback'ов ===
async def debug_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Отладочный обработчик для всех callback'ов."""
    query = update.callback_query
    if query:
        logging.info("🔍 ПОЛУЧЕН CALLBACK:")
        logging.info(f"   📋 callback_data: '{query.data}'")
        logging.info(f"   📱 chat_id: {query.message.chat_id}")
        logging.info(f"   📨 message_id: {query.message.message_id}")
        logging.info(f"   👤 user_id: {query.from_user.id}")
        
        # Проверяем, является ли это callback от уведомления
        if query.data.startswith('attendance_mark_'):
            logging.info("🎯 ЭТО CALLBACK ОТ УВЕДОМЛЕНИЯ!")
            logging.info("   🔄 Должен обрабатываться функцией save_attendance_mark")

# === Уведомления теперь используют существующий обработчик save_attendance_mark ===


# === Меню "Абонементы" ===
async def subscriptions_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает аналитику активных абонементов с подробной информацией."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🔄 Загружаю аналитику абонементов...")
    
    try:
        # Получаем активные абонементы
        active_subs = sheets_service.get_active_subscriptions()
        
        # Получаем прогнозы оплат для определения ближайших дат
        forecast_data = sheets_service.get_planned_payments()
        
        keyboard = []
        
        if not active_subs:
            message_text = "📄 <b>АБОНЕМЕНТЫ</b>\n\n❌ Активных абонементов не найдено."
        else:
            message_text = "📄 <b>АНАЛИТИКА АБОНЕМЕНТОВ</b>\n\n"
            
            total_active = len(active_subs)
            total_lessons_remaining = 0
            total_lessons_attended = 0
            total_lessons_missed = 0
            
            for i, sub in enumerate(active_subs, 1):
                # Основная информация об абонементе
                child_name = sub.get('Ребенок', 'Неизвестно')
                circle_name = sub.get('Кружок', 'Неизвестно')
                
                # Получаем все ключи для доступа по индексам
                available_keys = list(sub.keys())
                
                # Преобразуем строки в числа с обработкой ошибок
                # Столбец E - Количество занятий (5-й столбец = индекс 4)
                try:
                    if len(available_keys) > 4:  # Столбец E = индекс 4
                        col_e_name = available_keys[4]
                        total_lessons_value = sub.get(col_e_name, 0)
                    else:
                        total_lessons_value = 0
                    total_lessons = int(total_lessons_value) if total_lessons_value else 0
                except (ValueError, TypeError):
                    total_lessons = 0
                
                # Столбец I - Осталось занятий (9-й столбец = индекс 8)
                try:
                    if len(available_keys) > 8:  # Столбец I = индекс 8
                        col_i_name = available_keys[8]
                        remaining_value = sub.get(col_i_name, 0)
                    else:
                        remaining_value = 0
                    remaining_lessons = int(remaining_value) if remaining_value else 0
                except (ValueError, TypeError):
                    remaining_lessons = 0
                    
                try:
                    cost_value = (sub.get('Стоимость') or 
                                sub.get('Цена') or 
                                sub.get('Сумма') or 0)
                    cost = float(cost_value) if cost_value else 0
                except (ValueError, TypeError):
                    cost = 0
                
                # Статистика посещений
                # Столбец H - Посещено (8-й столбец = индекс 7)
                try:
                    if len(available_keys) > 7:  # Столбец H = индекс 7
                        col_h_name = available_keys[7]
                        attended_value = sub.get(col_h_name, 0)
                    else:
                        attended_value = 0
                    attended = int(attended_value) if attended_value else 0
                except (ValueError, TypeError):
                    attended = 0
                    
                # Столбец M - Пропущено (13-й столбец = индекс 12)
                try:
                    if len(available_keys) > 12:  # Столбец M = индекс 12
                        col_m_name = available_keys[12]
                        missed_value = sub.get(col_m_name, 0)
                    else:
                        missed_value = 0
                    missed = int(missed_value) if missed_value else 0
                except (ValueError, TypeError):
                    missed = 0
                
                total_lessons_remaining += remaining_lessons
                total_lessons_attended += attended
                total_lessons_missed += missed
                
                # Ищем ближайшую дату оплаты в прогнозе
                next_payment_date = "Не найдена"
                
                if forecast_data:
                    # Ищем по ребенку и кружку
                    for payment in forecast_data:
                        if (payment.get('child_name') == child_name and 
                            payment.get('circle_name') == circle_name):
                            next_payment_date = payment.get('payment_date', 'Не найдена')
                            break
                
                # Статус абонемента
                status = str(sub.get('Статус', '')).strip().lower()
                status_emoji = '✅' if status == 'активен' else '⏳' if status == 'ожидает' else '❓'
                
                # Формируем информацию об абонементе
                message_text += f"{status_emoji} <b>{child_name} - {circle_name}</b>\n"
                message_text += f"📊 Занятий: {total_lessons} | Осталось: {remaining_lessons}\n"
                message_text += f"✅ Посещено: {attended} | ❌ Пропущено: {missed}\n"
                message_text += f"💰 Стоимость: {cost:.0f} руб.\n"
                message_text += f"📅 Ближайшая оплата: {next_payment_date}\n"
                
                if i < len(active_subs):
                    message_text += "\n" + "─" * 25 + "\n\n"
            
            # Общая статистика
            message_text += f"\n📈 <b>ОБЩАЯ СТАТИСТИКА:</b>\n"
            message_text += f"📄 Активных абонементов: {total_active}\n"
            message_text += f"📚 Всего осталось занятий: {total_lessons_remaining}\n"
            message_text += f"✅ Всего посещено: {total_lessons_attended}\n"
            message_text += f"❌ Всего пропущено: {total_lessons_missed}\n"
            
            # Добавляем кнопки для каждого абонемента
            message_text += f"\n🔧 <b>Выберите абонемент для управления:</b>"
            
            for sub in active_subs:
                child_name = sub.get('Ребенок', '')
                circle_name = sub.get('Кружок', '')
                remaining = sub.get('Осталось занятий', 0)
                
                button_text = f"📋 {child_name} - {circle_name} ({remaining} зан.)"
                callback_data = f"select_sub_{sub.get('ID абонемента')}"
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
        
        keyboard.append([InlineKeyboardButton("➕ Создать новый", callback_data="sub_create")])
        keyboard.append([InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return SELECT_SUBSCRIPTION
        
    except Exception as e:
        logging.error(f"Ошибка при загрузке аналитики абонементов: {e}")
        message_text = f"❌ <b>Ошибка при загрузке аналитики:</b>\n\n{e}"
        keyboard = [[InlineKeyboardButton("⏪ Назад в главное меню", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        return SELECT_SUBSCRIPTION

# === Управление выбранным абонементом ===
async def select_subscription_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор конкретного абонемента из списка."""
    query = update.callback_query
    await query.answer()
    
    sub_id = query.data.replace("select_sub_", "")
    context.user_data['selected_sub_id'] = sub_id
    
    all_subs = sheets_service.get_active_subscriptions()
    selected_sub_info = next((sub for sub in all_subs if str(sub.get('ID абонемента')) == str(sub_id)), None)
    
    if not selected_sub_info:
        await query.edit_message_text("Не удалось найти информацию. Возможно, он был изменен.")
        query.data = 'menu_subscriptions'
        return await subscriptions_menu(update, context)
        
    context.user_data['selected_sub_info'] = selected_sub_info
    
    # Получаем полную статистику абонемента
    stats = sheets_service.get_subscription_full_stats(sub_id)
    
    if not stats:
        message_text = "❌ Не удалось загрузить статистику абонемента"
    else:
        sub_info = stats['subscription']
        schedule = stats['schedule_template']
        lessons = stats['calendar_lessons']
        forecasts = stats['forecast_payments']
        
        # Формируем детальную информацию
        message_text = f"📊 <b>ПОЛНАЯ СТАТИСТИКА АБОНЕМЕНТА</b>\n\n"
        
        # Основная информация
        message_text += f"🆔 <b>ID:</b> <code>{sub_id}</code>\n"
        message_text += f"👤 <b>Ребенок:</b> {sub_info.get('Ребенок', 'N/A')}\n"
        message_text += f"🎨 <b>Кружок:</b> {sub_info.get('Кружок', 'N/A')}\n"
        # Определяем дату окончания: сравниваем столбец G и L
        logging.info(f"🔍 Доступные ключи в sub_info: {list(sub_info.keys())}")
        end_date_g = sub_info.get('Дата окончания', '')  # Столбец G
        end_date_l = sub_info.get('Дата последнего занятия', '')  # Столбец L
        logging.info(f"🔍 Дата окончания G: '{end_date_g}', Дата последнего занятия L: '{end_date_l}'")
        
        # Логика выбора даты окончания
        if end_date_g and end_date_l:
            try:
                from datetime import datetime
                date_g = datetime.strptime(end_date_g, '%d.%m.%Y')
                date_l = datetime.strptime(end_date_l, '%d.%m.%Y')
                end_date = end_date_l if date_l >= date_g else end_date_g
            except ValueError:
                end_date = end_date_g or end_date_l or 'N/A'
        else:
            end_date = end_date_g or end_date_l or 'N/A'
        
        message_text += f"📅 <b>Период:</b> {sub_info.get('Дата начала', 'N/A')} - {end_date}\n"
        message_text += f"📊 <b>Статус:</b> {sub_info.get('Статус', 'N/A')}\n"
        message_text += f"💰 <b>Стоимость:</b> {sub_info.get('Стоимость', 'N/A')} руб.\n"
        message_text += f"📚 <b>Всего занятий:</b> {sub_info.get('К-во занятий', 'N/A')}\n"
        message_text += f"📉 <b>Осталось:</b> {sub_info.get('Осталось занятий', 'N/A')}\n"
        message_text += f"💳 <b>Тип оплаты:</b> {sub_info.get('Оплата', 'N/A')}\n\n"
        
        # Расписание
        message_text += f"🕐 <b>РАСПИСАНИЕ ({len(schedule)} дней):</b>\n"
        if schedule:
            days_map = {1: 'Пн', 2: 'Вт', 3: 'Ср', 4: 'Чт', 5: 'Пт', 6: 'Сб', 7: 'Вс'}
            for sch in schedule:
                day_name = days_map.get(sch.get('День недели', 0), 'N/A')
                message_text += f"• {day_name}: {sch.get('Время начала', 'N/A')}-{sch.get('Время звершения', 'N/A')}\n"
        else:
            message_text += "• Расписание не найдено\n"
        
        # Статистика занятий
        message_text += f"\n📋 <b>ЗАНЯТИЯ ({len(lessons)} записей):</b>\n"
        if lessons:
            attended = sum(1 for l in lessons if l.get('Отметка', '') in ['✔️', 'Присутствовал'])
            missed = sum(1 for l in lessons if l.get('Отметка', '') in ['✖️', 'Отсутствовал'])
            planned = sum(1 for l in lessons if l.get('Отметка', '') == '')
            
            message_text += f"• ✅ Посещено: {attended}\n"
            message_text += f"• ❌ Пропущено: {missed}\n"
            message_text += f"• 📅 Запланировано: {planned}\n"
            
            if attended + missed > 0:
                attendance_rate = round((attended / (attended + missed)) * 100, 1)
                message_text += f"• 📊 Посещаемость: {attendance_rate}%\n"
        else:
            message_text += "• Занятия не найдены\n"
        
        # Прогноз оплат
        message_text += f"\n💰 <b>ПРОГНОЗ ОПЛАТ ({len(forecasts)} записей):</b>\n"
        if forecasts:
            total_forecast = sum(float(f.get('Бюджет', 0) or 0) for f in forecasts)
            message_text += f"• Общая сумма: {total_forecast} руб.\n"
            for forecast in forecasts[:3]:  # Показываем первые 3
                message_text += f"• {forecast.get('Дата оплаты', 'N/A')}: {forecast.get('Бюджет', 'N/A')} руб.\n"
            if len(forecasts) > 3:
                message_text += f"• ... и еще {len(forecasts) - 3} платежей\n"
        else:
            message_text += "• Прогнозные оплаты не найдены\n"
    
    keyboard = [
        [InlineKeyboardButton("🔄 Обновить статистику", callback_data="update_stats_sub")],
        [InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")],
        [InlineKeyboardButton("🗑️ Удалить", callback_data="delete_sub")],
        [InlineKeyboardButton("⏪ Назад к списку", callback_data="menu_subscriptions")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MANAGE_SUBSCRIPTION

async def manage_subscription_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает действия с выбранным абонементом."""
    query = update.callback_query
    await query.answer()

    sub_id = context.user_data.get('selected_sub_id')
    if not sub_id:
        await query.edit_message_text("Ошибка: ID абонемента не найден. Попробуйте снова.")
        query.data = 'menu_subscriptions'
        return await subscriptions_menu(update, context)

    if query.data == 'update_stats_sub':
        await query.edit_message_text(f"🔄 Обновляю статистику для абонемента `{sub_id}`...")
        result_message = sheets_service.update_subscription_stats(sub_id)
        
        await query.edit_message_text(
            result_message,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад к списку", callback_data="menu_subscriptions")]])
        )
        return SELECT_SUBSCRIPTION

    elif query.data == 'delete_sub':
        keyboard = [
            [InlineKeyboardButton("❗️ Да, удалить", callback_data="confirm_delete_yes")],
            [InlineKeyboardButton("⏪ Нет, назад", callback_data=f"select_sub_{sub_id}")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text(
            f"Вы уверены, что хотите удалить абонемент `{sub_id}`?\n\n"
            "Это действие необратимо и также удалит все связанные с ним занятия из календаря.",
            reply_markup=reply_markup
        )
        return CONFIRM_DELETE_SUBSCRIPTION

    elif query.data in ['edit_sub', 'duplicate_sub']:
        action_text = "Редактирование" if query.data == 'edit_sub' else "Дублирование"
        await query.edit_message_text(
            f"{action_text} абонемента находится в разработке.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⏪ Назад к абонементу", callback_data=f"select_sub_{sub_id}")]
            ])
        )
        return MANAGE_SUBSCRIPTION

async def confirm_delete_subscription_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает подтверждение удаления абонемента с полной очисткой всех данных."""
    query = update.callback_query
    
    sub_id = context.user_data.get('selected_sub_id')
    if not sub_id:
        try:
            await query.answer()
            await query.edit_message_text("Ошибка: ID абонемента не найден.")
        except Exception:
            # Если callback истек, отправляем новое сообщение
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="Ошибка: ID абонемента не найден."
            )
        return await subscriptions_menu(update, context)
    
    if query.data == 'confirm_delete_yes':
        try:
            # Сначала отвечаем на callback чтобы избежать timeout
            await query.answer("Начинаю удаление...")
            
            # Показываем процесс удаления
            try:
                await query.edit_message_text(f"🗑️ Удаляю абонемент `{sub_id}`...\n\n⏳ Удаление из Google Sheets...")
            except Exception:
                # Если не можем редактировать, отправляем новое сообщение
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"🗑️ Удаляю абонемент `{sub_id}`...\n\n⏳ Удаление из Google Sheets..."
                )
            
            # 1. Удаляем из Google Sheets (все листы)
            deletion_result = sheets_service.delete_subscription(sub_id)
            
            if not deletion_result['success']:
                try:
                    await query.edit_message_text(f"❌ Ошибка при удалении:\n{deletion_result['message']}")
                except Exception:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f"❌ Ошибка при удалении:\n{deletion_result['message']}"
                    )
                return await subscriptions_menu(update, context)
            
            # Обновляем сообщение о прогрессе
            progress_message = f"🗑️ Удаляю абонемент `{sub_id}`...\n\n"
            progress_message += f"✅ Google Sheets: {sum(deletion_result['deleted_counts'].values())} записей\n"
            progress_message += "⏳ Удаление из Google Calendar..."
            try:
                await query.edit_message_text(progress_message)
            except Exception:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=progress_message
                )
            
            # 2. Удаляем события из Google Calendar
            calendar_result = {'deleted_count': 0, 'message': 'Calendar API недоступен'}
            if deletion_result['child_name'] and deletion_result['circle_name']:
                try:
                    from google_calendar_service import GoogleCalendarService
                    calendar_service = GoogleCalendarService()
                    calendar_result = calendar_service.delete_subscription_events(
                        deletion_result['child_name'],
                        deletion_result['circle_name'], 
                        sub_id
                    )
                except Exception as e:
                    logging.warning(f"⚠️ Не удалось удалить события из календаря: {e}")
            
            # Обновляем сообщение о прогрессе
            progress_message = f"🗑️ Удаляю абонемент `{sub_id}`...\n\n"
            progress_message += f"✅ Google Sheets: {sum(deletion_result['deleted_counts'].values())} записей\n"
            progress_message += f"✅ Google Calendar: {calendar_result['deleted_count']} событий\n"
            progress_message += "⏳ Запуск фоновых обновлений..."
            try:
                await query.edit_message_text(progress_message)
            except Exception:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=progress_message
                )
            
            # 3. Запускаем фоновые обновления
            try:
                # Запускаем полное фоновое обновление
                asyncio.create_task(update_data_in_background())
                logging.info("🔄 Запущены фоновые обновления после удаления абонемента")
            except Exception as e:
                logging.error(f"❌ Ошибка при запуске фоновых обновлений: {e}")
            
            # 4. Формируем итоговое сообщение
            final_message = f"🎉 Абонемент `{sub_id}` полностью удален!\n\n"
            
            # Детали удаления из Google Sheets
            if deletion_result['deleted_counts']:
                final_message += "📊 Удалено из Google Sheets:\n"
                for sheet_name, count in deletion_result['deleted_counts'].items():
                    if count > 0:
                        final_message += f"• {sheet_name}: {count} записей\n"
            
            # Детали удаления из Google Calendar
            if calendar_result['deleted_count'] > 0:
                final_message += f"\n📅 Google Calendar: {calendar_result['deleted_count']} событий\n"
            elif 'недоступен' not in calendar_result['message']:
                final_message += f"\n📅 Google Calendar: события не найдены\n"
            
            final_message += "\n🔄 Фоновые обновления запущены"
            final_message += "\n✅ Все данные синхронизированы"
            
            # Показываем итоговое сообщение
            try:
                await query.edit_message_text(final_message)
            except Exception:
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=final_message
                )
            
            # Показываем alert с кратким результатом (если callback еще активен)
            try:
                alert_message = f"✅ Абонемент {sub_id} удален!\n"
                alert_message += f"📊 Sheets: {sum(deletion_result['deleted_counts'].values())} записей\n"
                alert_message += f"📅 Calendar: {calendar_result['deleted_count']} событий"
                await query.answer(alert_message, show_alert=True)
            except Exception:
                # Если callback истек, просто пропускаем alert
                pass
            
            # Очищаем контекст и возвращаемся к меню
            context.user_data.clear()
            
            # Небольшая задержка перед возвратом в меню
            await asyncio.sleep(2)
            return await subscriptions_menu(update, context)
            
        except Exception as e:
            logging.error(f"❌ Критическая ошибка при удалении абонемента {sub_id}: {e}")
            try:
                await query.edit_message_text(f"❌ Произошла критическая ошибка при удалении:\n{str(e)}")
                await query.answer("❌ Ошибка удаления", show_alert=True)
            except Exception:
                # Если callback истек, отправляем новое сообщение
                await context.bot.send_message(
                    chat_id=update.effective_chat.id,
                    text=f"❌ Произошла критическая ошибка при удалении:\n{str(e)}"
                )
            return await subscriptions_menu(update, context)

# === Логика создания абонемента ===
async def create_sub_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Начало процесса создания абонемента."""
    query = update.callback_query
    await query.answer()
    
    try:
        context.user_data['new_sub'] = {'schedule': []}
        
        children = sheets_service.get_children_list()
        keyboard = [[InlineKeyboardButton(name, callback_data=f"create_sub_child_{name}")] for name in children]
        keyboard.append([InlineKeyboardButton("➕ Добавить нового ребенка", callback_data="create_sub_add_child")])
        keyboard.append([InlineKeyboardButton("⏪ Назад к списку", callback_data="menu_subscriptions")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await query.edit_message_text("Шаг 1/9: Выберите ребенка или добавьте нового.", reply_markup=reply_markup, parse_mode='HTML')
        return CREATE_SUB_CHILD
    except Exception as e:
        logging.error(f"Ошибка в create_sub_start: {e}", exc_info=True)
        await query.edit_message_text(f"❌ Ошибка при загрузке списка детей: {e}", 
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_subscriptions")]]))
        return SELECT_SUBSCRIPTION

async def create_sub_child_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    if query.data == "create_sub_add_child":
        sent_message = await query.edit_message_text("Введите имя и фамилию нового ребенка:")
        context.user_data['prompt_message_id'] = sent_message.message_id
        return CREATE_SUB_GET_CHILD_NAME
    else:
        child_name = query.data.replace("create_sub_child_", "", 1)
        context.user_data['new_sub']['child_name'] = child_name
        return await create_sub_ask_for_circle(update, context)

async def create_sub_get_child_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.delete()
    if 'prompt_message_id' in context.user_data:
        try: await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=context.user_data.pop('prompt_message_id'))
        except Exception: pass
        
    child_name = update.message.text.strip()
    if not child_name:
        sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text="Имя не может быть пустым. Попробуйте еще раз.")
        context.user_data['prompt_message_id'] = sent_message.message_id
        return CREATE_SUB_GET_CHILD_NAME
    
    sheets_service.add_handbook_item("Ребенок", child_name)
    context.user_data['new_sub']['child_name'] = child_name
    return await create_sub_ask_for_circle(update, context)

async def create_sub_ask_for_circle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    try:
        circles = sheets_service.get_circles_list()
        keyboard = [[InlineKeyboardButton(name, callback_data=f"create_sub_circle_{name}")] for name in circles]
        keyboard.append([InlineKeyboardButton("➕ Добавить новый кружок", callback_data="create_sub_add_circle")])
        keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data="sub_create")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n\nШаг 2/9: Выберите кружок."
        
        if update.callback_query:
            await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
            
        return CREATE_SUB_CIRCLE
        
    except Exception as e:
        logging.error(f"Ошибка в create_sub_ask_for_circle: {e}", exc_info=True)
        error_message = f"❌ Ошибка при загрузке списка кружков: {e}"
        
        if update.callback_query:
            await update.callback_query.edit_message_text(error_message, 
                                                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_subscriptions")]]))
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=error_message)
        
        return SELECT_SUBSCRIPTION

async def create_sub_circle_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    try:
        if query.data == "create_sub_add_circle":
            sent_message = await query.edit_message_text("Введите название нового кружка:")
            context.user_data['prompt_message_id'] = sent_message.message_id
            return CREATE_SUB_GET_CIRCLE_NAME
        else:
            circle_name = query.data.replace("create_sub_circle_", "", 1)
            context.user_data['new_sub']['circle_name'] = circle_name
            return await create_sub_ask_for_type(update, context)
    except Exception as e:
        logging.error(f"Ошибка в create_sub_circle_handler: {e}", exc_info=True)
        await query.edit_message_text(f"❌ Ошибка: {e}", 
                                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="menu_subscriptions")]]))
        return SELECT_SUBSCRIPTION

async def create_sub_get_circle_name_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.delete()
    if 'prompt_message_id' in context.user_data:
        try: await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=context.user_data.pop('prompt_message_id'))
        except Exception: pass
        
    circle_name = update.message.text.strip()
    if not circle_name:
        sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text="Название не может быть пустым. Попробуйте еще раз.")
        context.user_data['prompt_message_id'] = sent_message.message_id
        return CREATE_SUB_GET_CIRCLE_NAME
    
    sheets_service.add_handbook_item("Название кружка", circle_name)
    context.user_data['new_sub']['circle_name'] = circle_name
    return await create_sub_ask_for_type(update, context)

async def create_sub_ask_for_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    sub_types = sheets_service.get_subscription_types()
    keyboard = [[InlineKeyboardButton(stype, callback_data=f"create_sub_type_{stype}")] for stype in sub_types]
    keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data=f"create_sub_child_{context.user_data['new_sub']['child_name']}")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n\n"
                    "Шаг 3/9: Выберите тип абонемента.")
    
    sender = update.callback_query.edit_message_text if update.callback_query else context.bot.send_message
    try:
        if update.callback_query:
            await sender(message_text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            await sender(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
    except telegram.error.BadRequest as e:
        if "Message is not modified" not in str(e):
             logging.warning(f"Ignored 'Message is not modified' error.")
        else:
            raise e

    return CREATE_SUB_TYPE

async def create_sub_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    sub_type = query.data.replace("create_sub_type_", "", 1)
    context.user_data['new_sub']['sub_type'] = sub_type
    
    return await create_sub_ask_for_payment_type(update, context)

async def create_sub_ask_for_payment_type(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает список типов оплаты для выбора."""
    try:
        payment_types = sheets_service.get_payment_types()
        logging.info(f"Получены типы оплаты: {payment_types}")
        
        if not payment_types:
            # Если нет данных, показываем ошибку и возвращаемся назад
            message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                            f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                            f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n\n"
                            "❌ Ошибка: Не найдены типы оплаты в Справочнике.\n"
                            "Проверьте, что в листе 'Справочник' есть столбец 'Тип оплаты' с данными.")
            
            keyboard = [[InlineKeyboardButton("⏪ Назад к типу абонемента", callback_data="back_to_sub_type")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            if update.callback_query:
                await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
            else:
                await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
            return CREATE_SUB_PAYMENT_TYPE
        
        keyboard = [[InlineKeyboardButton(ptype, callback_data=f"create_sub_payment_{ptype}")] for ptype in payment_types]
        keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data=f"create_sub_circle_{context.user_data['new_sub']['circle_name']}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
    except Exception as e:
        logging.error(f"Ошибка при получении типов оплаты: {e}")
        # Показываем ошибку пользователю
        message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                        f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                        f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n\n"
                        f"❌ Ошибка при загрузке типов оплаты: {e}")
        
        keyboard = [[InlineKeyboardButton("⏪ Назад к типу абонемента", callback_data="back_to_sub_type")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if update.callback_query:
            await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
        else:
            await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
        return CREATE_SUB_PAYMENT_TYPE

    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n\n"
                    "Шаг 4/9: Выберите тип оплаты.")

    if update.callback_query:
        await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
        
    return CREATE_SUB_PAYMENT_TYPE

async def create_sub_payment_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    payment_type = query.data.replace("create_sub_payment_", "", 1)
    context.user_data['new_sub']['payment_type'] = payment_type
    
    return await create_sub_ask_for_cost(update, context)

async def create_sub_ask_for_cost(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает варианты стоимости для выбора."""
    # Создаем кнопки с популярными стоимостями
    cost_options = [1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 6000, 7000, 8000]
    keyboard = []
    
    # Размещаем по 3 кнопки в ряд
    for i in range(0, len(cost_options), 3):
        row = []
        for j in range(3):
            if i + j < len(cost_options):
                cost = cost_options[i + j]
                row.append(InlineKeyboardButton(f"{cost} ₽", callback_data=f"create_sub_cost_{cost}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("💰 Другая сумма", callback_data="create_sub_cost_custom")])
    keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data="back_to_payment_type")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                    f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n\n"
                    "Шаг 5/9: Выберите стоимость абонемента.")

    await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_COST

async def create_sub_cost_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    if query.data == "create_sub_cost_custom":
        # Переход к ручному вводу стоимости
        message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                        f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                        f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                        f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n\n"
                        "Введите стоимость абонемента (только цифры):")
        
        sent_message = await query.edit_message_text(message_text, parse_mode='HTML')
        context.user_data['prompt_message_id'] = sent_message.message_id
        context.user_data['awaiting_custom_cost'] = True
        return CREATE_SUB_COST
    
    # Обработка выбора готовой стоимости
    cost = int(query.data.replace("create_sub_cost_", ""))
    context.user_data['new_sub']['cost'] = cost
    
    return await create_sub_ask_for_total_classes(update, context)

async def create_sub_cost_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает ручной ввод стоимости."""
    await update.message.delete()
    if 'prompt_message_id' in context.user_data:
        try: await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=context.user_data.pop('prompt_message_id'))
        except Exception: pass

    cost = update.message.text.strip()
    if not cost.isdigit() or int(cost) <= 0:
        sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text="❗️Пожалуйста, введите корректное число.")
        context.user_data['prompt_message_id'] = sent_message.message_id
        return CREATE_SUB_COST

    context.user_data['new_sub']['cost'] = int(cost)
    context.user_data.pop('awaiting_custom_cost', None)
    
    return await create_sub_ask_for_total_classes(update, context)

async def create_sub_ask_for_total_classes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает кнопки для выбора общего количества занятий."""
    keyboard = []
    
    # Создаем кнопки от 1 до 15 по 5 в ряд
    for i in range(1, 16, 5):
        row = []
        for j in range(5):
            if i + j <= 15:
                num = i + j
                row.append(InlineKeyboardButton(str(num), callback_data=f"create_sub_total_{num}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data="back_to_cost")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                    f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n"
                    f"💰 Стоимость: <b>{context.user_data['new_sub']['cost']} ₽</b>\n\n"
                    "Шаг 6/9: Выберите общее количество занятий.")

    if hasattr(update, 'callback_query') and update.callback_query:
        await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
    
    return CREATE_SUB_TOTAL_CLASSES

async def create_sub_total_classes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    total_classes = int(query.data.replace("create_sub_total_", ""))
    context.user_data['new_sub']['total_classes'] = total_classes
    
    return await create_sub_ask_for_remaining_classes(update, context)

async def create_sub_ask_for_remaining_classes(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает кнопки для выбора количества оставшихся занятий."""
    total_classes = context.user_data['new_sub']['total_classes']
    keyboard = []
    
    # Создаем кнопки от 1 до total_classes по 5 в ряд
    for i in range(1, total_classes + 1, 5):
        row = []
        for j in range(5):
            if i + j <= total_classes:
                num = i + j
                row.append(InlineKeyboardButton(str(num), callback_data=f"create_sub_remaining_{num}"))
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data="back_to_total_classes")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                    f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n"
                    f"💰 Стоимость: <b>{context.user_data['new_sub']['cost']} ₽</b>\n"
                    f"📚 Всего занятий: <b>{total_classes}</b>\n\n"
                    "Шаг 7/9: Выберите количество оставшихся занятий.")

    await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_REMAINING_CLASSES

async def create_sub_remaining_classes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    
    remaining_classes = int(query.data.replace("create_sub_remaining_", ""))
    context.user_data['new_sub']['remaining_classes'] = remaining_classes
    
    return await create_sub_ask_for_start_date(update, context)

async def create_sub_ask_for_start_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает календарь для выбора даты начала."""
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                    f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n"
                    f"💰 Стоимость: <b>{context.user_data['new_sub']['cost']} ₽</b>\n"
                    f"📚 Всего занятий: <b>{context.user_data['new_sub']['total_classes']}</b>\n"
                    f"📉 Осталось: <b>{context.user_data['new_sub']['remaining_classes']}</b>\n\n"
                    "Шаг 8/9: Выберите дату начала абонемента.")

    # Создаем календарь
    today = datetime.now()
    calendar_keyboard = []
    
    # Заголовок месяца
    month_names = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
                   'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь']
    calendar_keyboard.append([InlineKeyboardButton(f"{month_names[today.month-1]} {today.year}", callback_data="ignore")])
    
    # Дни недели
    calendar_keyboard.append([
        InlineKeyboardButton("Пн", callback_data="ignore"),
        InlineKeyboardButton("Вт", callback_data="ignore"),
        InlineKeyboardButton("Ср", callback_data="ignore"),
        InlineKeyboardButton("Чт", callback_data="ignore"),
        InlineKeyboardButton("Пт", callback_data="ignore"),
        InlineKeyboardButton("Сб", callback_data="ignore"),
        InlineKeyboardButton("Вс", callback_data="ignore")
    ])
    
    # Получаем первый день месяца и количество дней
    first_day = today.replace(day=1)
    start_weekday = first_day.weekday()  # 0 = понедельник
    days_in_month = (today.replace(month=today.month % 12 + 1, day=1) - timedelta(days=1)).day if today.month < 12 else 31
    
    # Создаем строки календаря
    week = []
    
    # Пустые ячейки в начале
    for _ in range(start_weekday):
        week.append(InlineKeyboardButton(" ", callback_data="ignore"))
    
    # Дни месяца
    for day in range(1, days_in_month + 1):
        if day >= today.day:  # Только будущие даты
            week.append(InlineKeyboardButton(str(day), callback_data=f"cal_{today.year}_{today.month}_{day}"))
        else:
            week.append(InlineKeyboardButton(" ", callback_data="ignore"))
        
        if len(week) == 7:
            calendar_keyboard.append(week)
            week = []
    
    # Добавляем оставшиеся дни
    if week:
        while len(week) < 7:
            week.append(InlineKeyboardButton(" ", callback_data="ignore"))
        calendar_keyboard.append(week)
    
    calendar_keyboard.append([InlineKeyboardButton("⏪ Назад", callback_data="back_to_remaining_classes")])
    reply_markup = InlineKeyboardMarkup(calendar_keyboard)
    
    await update.callback_query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_START_DATE_MONTH

async def create_sub_total_classes_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает ручной ввод общего количества занятий (если нужно)."""
    await update.message.delete()
    if 'prompt_message_id' in context.user_data:
        try: await context.bot.delete_message(chat_id=update.effective_chat.id, message_id=context.user_data.pop('prompt_message_id'))
        except Exception: pass

    total_classes = update.message.text.strip()
    if not total_classes.isdigit() or int(total_classes) <= 0:
        sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text="❗️Пожалуйста, введите корректное число.")
        context.user_data['prompt_message_id'] = sent_message.message_id
        return CREATE_SUB_TOTAL_CLASSES

    context.user_data['new_sub']['total_classes'] = int(total_classes)
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                    f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n"
                    f"💰 Стоимость: <b>{context.user_data['new_sub']['cost']} ₽</b>\n"
                    f"📚 Всего занятий: <b>{total_classes}</b>\n\n"
                    "Шаг 7/9: Введите количество оставшихся занятий.")
    
    sent_message = await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text)
    context.user_data['prompt_message_id'] = sent_message.message_id
    return CREATE_SUB_REMAINING_CLASSES


async def create_sub_calendar_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор даты в календаре."""
    query = update.callback_query
    await query.answer()
    
    callback_data = query.data
    
    if callback_data.startswith('cal_month_'):
        # Переключение месяца
        parts = callback_data.split('_')
        year = int(parts[2])
        month = int(parts[3])
        
        # Сохраняем текущий год и месяц
        context.user_data['calendar_year'] = year
        context.user_data['calendar_month'] = month
        
        calendar_keyboard = create_calendar_keyboard(year, month)
        
        message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                        f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                        f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                        f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n"
                        f"💰 Стоимость: <b>{context.user_data['new_sub']['cost']} ₽</b>\n"
                        f"📚 Всего занятий: <b>{context.user_data['new_sub']['total_classes']}</b>\n"
                        f"📉 Осталось: <b>{context.user_data['new_sub']['remaining_classes']}</b>\n\n"
                        "Шаг 8/9: Выберите дату начала абонемента:")
        
        await query.edit_message_text(message_text, reply_markup=calendar_keyboard, parse_mode='HTML')
        return CREATE_SUB_START_DATE_MONTH
        
    elif callback_data.startswith('cal_') and len(callback_data.split('_')) == 4:
        # Выбор дня (формат: cal_year_month_day)
        parts = callback_data.split('_')
        year = int(parts[1])
        month = int(parts[2])
        day = int(parts[3])
        
        try:
            selected_date = datetime(year, month, day)
            context.user_data['new_sub']['start_date'] = selected_date
            
            # Переходим к настройке расписания
            context.user_data['new_sub']['schedule'] = []  # Инициализируем список расписания
            
            message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                            f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                            f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                            f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n"
                            f"💰 Стоимость: <b>{context.user_data['new_sub']['cost']} ₽</b>\n"
                            f"📚 Всего занятий: <b>{context.user_data['new_sub']['total_classes']}</b>\n"
                            f"📉 Осталось: <b>{context.user_data['new_sub']['remaining_classes']}</b>\n"
                            f"📅 Дата начала: <b>{selected_date.strftime('%d.%m.%Y')}</b>\n\n"
                            "Шаг 9/9: Выберите дни недели для занятий:")
            
            # Создаем клавиатуру с днями недели
            days_keyboard = [
                [InlineKeyboardButton("Понедельник", callback_data="schedule_day_1")],
                [InlineKeyboardButton("Вторник", callback_data="schedule_day_2")],
                [InlineKeyboardButton("Среда", callback_data="schedule_day_3")],
                [InlineKeyboardButton("Четверг", callback_data="schedule_day_4")],
                [InlineKeyboardButton("Пятница", callback_data="schedule_day_5")],
                [InlineKeyboardButton("Суббота", callback_data="schedule_day_6")],
                [InlineKeyboardButton("Воскресенье", callback_data="schedule_day_7")],
                [InlineKeyboardButton("⏪ Назад к календарю", callback_data="create_sub_back_to_calendar")]
            ]
            reply_markup = InlineKeyboardMarkup(days_keyboard)
            
            await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
            return CREATE_SUB_SCHEDULE_DAY
            
        except ValueError:
            await query.answer("Некорректная дата", show_alert=True)
            return CREATE_SUB_START_DATE_MONTH
    
    elif callback_data == 'ignore':
        # Игнорируем нажатия на заголовки
        return CREATE_SUB_START_DATE_MONTH
    
    return CREATE_SUB_START_DATE_MONTH

async def create_sub_back_to_calendar_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к календарю выбора даты."""
    query = update.callback_query
    await query.answer()
    
    from datetime import datetime
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    calendar_keyboard = create_calendar_keyboard(current_year, current_month)
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"⚜️ Тип: <b>{context.user_data['new_sub']['sub_type']}</b>\n"
                    f"💳 Оплата: <b>{context.user_data['new_sub']['payment_type']}</b>\n"
                    f"💰 Стоимость: <b>{context.user_data['new_sub']['cost']} ₽</b>\n"
                    f"📚 Всего занятий: <b>{context.user_data['new_sub']['total_classes']}</b>\n"
                    f"📉 Осталось: <b>{context.user_data['new_sub']['remaining_classes']}</b>\n\n"
                    "Шаг 8/9: Выберите дату начала абонемента:")
    
    await query.edit_message_text(message_text, reply_markup=calendar_keyboard, parse_mode='HTML')
    return CREATE_SUB_START_DATE_MONTH

async def create_sub_schedule_day_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор дня недели для расписания."""
    query = update.callback_query
    await query.answer()
    
    day_num = int(query.data.split('_')[2])
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    
    context.user_data['current_schedule_day'] = day_num
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n\n"
                    "Выберите час начала занятия:")
    
    # Создаем клавиатуру с часами (с 8:00 до 20:00)
    time_keyboard = []
    for hour in range(8, 21):
        time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:xx", callback_data=f"start_hour_{hour}")])
    
    time_keyboard.append([InlineKeyboardButton("⏪ Назад к выбору дня", callback_data="back_to_day_selection")])
    reply_markup = InlineKeyboardMarkup(time_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_START_HOUR

async def create_sub_start_hour_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор часа начала занятия."""
    query = update.callback_query
    await query.answer()
    
    start_hour = int(query.data.split('_')[2])
    context.user_data['current_start_hour'] = start_hour
    
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    day_num = context.user_data['current_schedule_day']
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n"
                    f"⏰ Час начала: <b>{start_hour:02d}:xx</b>\n\n"
                    "Выберите минуты начала занятия:")
    
    # Создаем клавиатуру с минутами (каждые 5 минут)
    minute_keyboard = []
    for minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
        minute_keyboard.append([InlineKeyboardButton(f"{start_hour:02d}:{minute:02d}", callback_data=f"start_minute_{minute}")])
    
    minute_keyboard.append([InlineKeyboardButton("⏪ Назад к выбору часа", callback_data="back_to_start_hour_selection")])
    reply_markup = InlineKeyboardMarkup(minute_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_START_MINUTE

async def create_sub_start_minute_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор минут начала занятия."""
    query = update.callback_query
    await query.answer()
    
    start_minute = int(query.data.split('_')[2])
    context.user_data['current_start_minute'] = start_minute
    
    start_hour = context.user_data['current_start_hour']
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    day_num = context.user_data['current_schedule_day']
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n"
                    f"⏰ Время начала: <b>{start_hour:02d}:{start_minute:02d}</b>\n\n"
                    "Выберите время окончания занятия:")
    
    # Создаем клавиатуру с временем окончания (минимум +30 минут от времени начала)
    time_keyboard = []
    
    # Вычисляем минимальное время окончания (время начала + 30 минут)
    min_end_hour = start_hour
    min_end_minute = start_minute + 30
    if min_end_minute >= 60:
        min_end_hour += 1
        min_end_minute -= 60
    
    # Добавляем варианты времени с шагом 30 минут
    for hour in range(min_end_hour, 23):
        if hour == min_end_hour:
            # Для первого часа начинаем с минимальной минуты
            if min_end_minute == 0:
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:00", callback_data=f"end_time_{hour}_0")])
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
            elif min_end_minute == 30:
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
        else:
            # Для остальных часов добавляем оба варианта (00 и 30)
            time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:00", callback_data=f"end_time_{hour}_0")])
            time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
    
    time_keyboard.append([InlineKeyboardButton("⏪ Назад к минутам начала", callback_data="back_to_start_minute")])
    reply_markup = InlineKeyboardMarkup(time_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_END_HOUR

async def create_sub_end_hour_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор времени окончания занятия."""
    query = update.callback_query
    await query.answer()
    
    # Парсим время окончания из callback_data (формат: end_time_hour_minute)
    data_parts = query.data.split('_')
    end_hour = int(data_parts[2])
    end_minute = int(data_parts[3])
    
    context.user_data['current_end_hour'] = end_hour
    context.user_data['current_end_minute'] = end_minute
    
    start_hour = context.user_data['current_start_hour']
    start_minute = context.user_data['current_start_minute']
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    day_num = context.user_data['current_schedule_day']
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n"
                    f"⏰ Время начала: <b>{start_hour:02d}:{start_minute:02d}</b>\n"
                    f"⏰ Время окончания: <b>{end_hour:02d}:{end_minute:02d}</b>\n\n"
                    "✅ Время занятия настроено!")
    
    # Создаем клавиатуру для подтверждения или изменения
    keyboard = [
        [InlineKeyboardButton("✅ Подтвердить время", callback_data="confirm_schedule_time")],
        [InlineKeyboardButton("🔄 Изменить время начала", callback_data="back_to_start_hour")],
        [InlineKeyboardButton("🔄 Изменить время окончания", callback_data="back_to_end_time_selection")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_CONFIRM

async def create_sub_end_minute_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает выбор минут окончания занятия."""
    query = update.callback_query
    await query.answer()
    
    end_minute = int(query.data.split('_')[2])
    
    start_hour = context.user_data['current_start_hour']
    start_minute = context.user_data['current_start_minute']
    end_hour = context.user_data['current_end_hour']
    day_num = context.user_data['current_schedule_day']
    
    # Добавляем расписание в список
    schedule_item = {
        'day_num': day_num,
        'start_time': f"{start_hour:02d}:{start_minute:02d}",
        'end_time': f"{end_hour:02d}:{end_minute:02d}"
    }
    context.user_data['new_sub']['schedule'].append(schedule_item)
    
    # Показываем сводку расписания
    return await create_sub_show_schedule_summary(update, context)

async def create_sub_show_schedule_summary(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Показывает сводку расписания и предлагает добавить еще дни или завершить."""
    query = update.callback_query
    if query:
        await query.answer()
    
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    
    schedule_text = ""
    for item in context.user_data['new_sub']['schedule']:
        day_name = day_names[item['day_num']]
        schedule_text += f"• {day_name}: {item['start_time']} - {item['end_time']}\n"
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"📋 <b>Текущее расписание:</b>\n{schedule_text}\n"
                    "Что дальше?")
    
    keyboard = [
        [InlineKeyboardButton("➕ Добавить еще день", callback_data="add_another_day")],
        [InlineKeyboardButton("✅ Создать абонемент", callback_data="create_sub_finish")],
        [InlineKeyboardButton("⏪ Назад к календарю", callback_data="create_sub_back_to_calendar")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if query:
        await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    else:
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text, reply_markup=reply_markup, parse_mode='HTML')
    
    return CREATE_SUB_SCHEDULE_DAY

async def create_sub_back_to_day_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору дня недели."""
    query = update.callback_query
    await query.answer()
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    "Шаг 9/9: Выберите дни недели для занятий:")
    
    # Создаем клавиатуру с днями недели
    days_keyboard = [
        [InlineKeyboardButton("Понедельник", callback_data="schedule_day_1")],
        [InlineKeyboardButton("Вторник", callback_data="schedule_day_2")],
        [InlineKeyboardButton("Среда", callback_data="schedule_day_3")],
        [InlineKeyboardButton("Четверг", callback_data="schedule_day_4")],
        [InlineKeyboardButton("Пятница", callback_data="schedule_day_5")],
        [InlineKeyboardButton("Суббота", callback_data="schedule_day_6")],
        [InlineKeyboardButton("Воскресенье", callback_data="schedule_day_7")],
        [InlineKeyboardButton("⏪ Назад к календарю", callback_data="create_sub_back_to_calendar")]
    ]
    reply_markup = InlineKeyboardMarkup(days_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_DAY

async def create_sub_back_to_start_hour(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору времени начала."""
    query = update.callback_query
    await query.answer()
    
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    day_num = context.user_data['current_schedule_day']
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n\n"
                    "Выберите время начала занятия:")
    
    # Создаем клавиатуру с часами (с 8:00 до 20:00)
    time_keyboard = []
    for hour in range(8, 21):
        time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:xx", callback_data=f"start_hour_{hour}")])
    
    time_keyboard.append([InlineKeyboardButton("⏪ Назад к выбору дня", callback_data="back_to_day_selection")])
    reply_markup = InlineKeyboardMarkup(time_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_START_HOUR

# Добавляем недостающие обработчики кнопок "Назад"
async def back_to_payment_type_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору типа оплаты."""
    return await create_sub_ask_for_payment_type(update, context)

async def back_to_cost_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору стоимости."""
    return await create_sub_ask_for_cost(update, context)

async def back_to_total_classes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору общего количества занятий."""
    return await create_sub_ask_for_total_classes(update, context)

async def back_to_remaining_classes_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору оставшихся занятий."""
    return await create_sub_ask_for_remaining_classes(update, context)

async def back_to_day_selection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору дня недели."""
    return await create_sub_back_to_day_selection(update, context)

async def back_to_start_hour_selection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору часа начала."""
    return await create_sub_back_to_start_hour(update, context)

async def back_to_start_minute_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору минут начала."""
    query = update.callback_query
    await query.answer()
    
    start_hour = context.user_data['current_start_hour']
    context.user_data['current_start_minute'] = 0  # Сбрасываем минуты
    
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    day_num = context.user_data['current_schedule_day']
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n"
                    f"⏰ Час начала: <b>{start_hour:02d}:xx</b>\n\n"
                    "Выберите минуты начала занятия:")
    
    # Создаем клавиатуру с минутами (каждые 5 минут)
    minute_keyboard = []
    for minute in [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]:
        minute_keyboard.append([InlineKeyboardButton(f"{start_hour:02d}:{minute:02d}", callback_data=f"start_minute_{minute}")])
    
    minute_keyboard.append([InlineKeyboardButton("⏪ Назад к выбору часа", callback_data="back_to_start_hour_selection")])
    reply_markup = InlineKeyboardMarkup(minute_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_START_MINUTE

async def back_to_end_hour_selection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору часа окончания."""
    query = update.callback_query
    await query.answer()
    
    start_hour = context.user_data['current_start_hour']
    start_minute = context.user_data['current_start_minute']
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    day_num = context.user_data['current_schedule_day']
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n"
                    f"⏰ Время начала: <b>{start_hour:02d}:{start_minute:02d}</b>\n\n"
                    "Выберите час окончания занятия:")
    
    # Создаем клавиатуру с временем окончания (минимум +30 минут от времени начала)
    time_keyboard = []
    
    # Вычисляем минимальное время окончания (время начала + 30 минут)
    min_end_hour = start_hour
    min_end_minute = start_minute + 30
    if min_end_minute >= 60:
        min_end_hour += 1
        min_end_minute -= 60
    
    # Добавляем варианты времени с шагом 30 минут
    for hour in range(min_end_hour, 23):
        if hour == min_end_hour:
            # Для первого часа начинаем с минимальной минуты
            if min_end_minute == 0:
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:00", callback_data=f"end_time_{hour}_0")])
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
            elif min_end_minute == 30:
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
        else:
            # Для остальных часов добавляем оба варианта (00 и 30)
            time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:00", callback_data=f"end_time_{hour}_0")])
            time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
    
    time_keyboard.append([InlineKeyboardButton("⏪ Назад к минутам начала", callback_data="back_to_start_minute")])
    reply_markup = InlineKeyboardMarkup(time_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_END_HOUR

async def back_to_end_time_selection_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору времени окончания."""
    query = update.callback_query
    await query.answer()
    
    start_hour = context.user_data['current_start_hour']
    start_minute = context.user_data['current_start_minute']
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    day_num = context.user_data['current_schedule_day']
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"Настройка расписания для <b>{day_names[day_num]}</b>\n"
                    f"⏰ Время начала: <b>{start_hour:02d}:{start_minute:02d}</b>\n\n"
                    "Выберите время окончания занятия:")
    
    # Создаем клавиатуру с временем окончания (минимум +30 минут от времени начала)
    time_keyboard = []
    
    # Вычисляем минимальное время окончания (время начала + 30 минут)
    min_end_hour = start_hour
    min_end_minute = start_minute + 30
    if min_end_minute >= 60:
        min_end_hour += 1
        min_end_minute -= 60
    
    # Добавляем варианты времени с шагом 30 минут
    for hour in range(min_end_hour, 23):
        if hour == min_end_hour:
            # Для первого часа начинаем с минимальной минуты
            if min_end_minute == 0:
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:00", callback_data=f"end_time_{hour}_0")])
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
            elif min_end_minute == 30:
                time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
        else:
            # Для остальных часов добавляем оба варианта (00 и 30)
            time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:00", callback_data=f"end_time_{hour}_0")])
            time_keyboard.append([InlineKeyboardButton(f"{hour:02d}:30", callback_data=f"end_time_{hour}_30")])
    
    time_keyboard.append([InlineKeyboardButton("⏪ Назад к минутам начала", callback_data="back_to_start_minute")])
    reply_markup = InlineKeyboardMarkup(time_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_END_HOUR

async def create_sub_confirm_schedule_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обрабатывает подтверждение времени занятия."""
    query = update.callback_query
    await query.answer()
    
    # Сохраняем расписание для текущего дня
    start_hour = context.user_data['current_start_hour']
    start_minute = context.user_data['current_start_minute']
    end_hour = context.user_data['current_end_hour']
    end_minute = context.user_data['current_end_minute']
    day_num = context.user_data['current_schedule_day']
    
    # Добавляем день в расписание
    if 'schedule' not in context.user_data['new_sub']:
        context.user_data['new_sub']['schedule'] = []
    
    context.user_data['new_sub']['schedule'].append({
        'day_num': day_num,
        'start_time': f"{start_hour:02d}:{start_minute:02d}",
        'end_time': f"{end_hour:02d}:{end_minute:02d}"
    })
    
    # Показываем сводку и варианты действий
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    
    schedule_text = ""
    for item in context.user_data['new_sub']['schedule']:
        day_name = day_names[item['day_num']]
        schedule_text += f"📅 {day_name}: {item['start_time']} - {item['end_time']}\n"
    
    message_text = (f"👤 Ребенок: <b>{context.user_data['new_sub']['child_name']}</b>\n"
                    f"🎨 Кружок: <b>{context.user_data['new_sub']['circle_name']}</b>\n"
                    f"📅 Дата начала: <b>{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}</b>\n\n"
                    f"📋 Расписание:\n{schedule_text}\n"
                    "Что дальше?")
    
    keyboard = [
        [InlineKeyboardButton("➕ Добавить еще день", callback_data="add_another_day")],
        [InlineKeyboardButton("✅ Создать абонемент", callback_data="create_sub_finish")],
        [InlineKeyboardButton("⏪ Назад к календарю", callback_data="create_sub_back_to_calendar")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_DAY

async def create_sub_add_another_day(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Возврат к выбору дня недели для добавления еще одного дня."""
    query = update.callback_query
    await query.answer()
    
    # Получаем уже выбранные дни
    selected_days = {item['day_num'] for item in context.user_data['new_sub']['schedule']}
    
    message_text = (f"👤 Ребенок: *{context.user_data['new_sub']['child_name']}*\n"
                    f"🎨 Кружок: *{context.user_data['new_sub']['circle_name']}*\n"
                    f"📅 Дата начала: *{context.user_data['new_sub']['start_date'].strftime('%d.%m.%Y')}*\n\n"
                    "Выберите еще один день недели для занятий:")
    
    # Создаем клавиатуру с днями недели (исключаем уже выбранные)
    day_names = {1: "Понедельник", 2: "Вторник", 3: "Среда", 4: "Четверг", 
                 5: "Пятница", 6: "Суббота", 7: "Воскресенье"}
    
    days_keyboard = []
    for day_num, day_name in day_names.items():
        if day_num not in selected_days:
            days_keyboard.append([InlineKeyboardButton(day_name, callback_data=f"schedule_day_{day_num}")])
    
    days_keyboard.append([InlineKeyboardButton("⏪ Назад к сводке", callback_data="show_schedule_summary")])
    reply_markup = InlineKeyboardMarkup(days_keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return CREATE_SUB_SCHEDULE_DAY

async def create_sub_finish_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершает создание абонемента."""
    query = update.callback_query
    await query.answer()
    
    await query.edit_message_text("🔄 Создаю абонемент...")
    
    try:
        # Подготавливаем данные для создания абонемента
        sub_data = context.user_data['new_sub']
        
        # Проверяем, что расписание создано
        if not sub_data.get('schedule') or len(sub_data['schedule']) == 0:
            await query.edit_message_text(
                "❌ Ошибка: Не создано расписание. Вернитесь и добавьте хотя бы один день занятий.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⏪ Назад", callback_data="show_schedule_summary")]])
            )
            return CREATE_SUB_SCHEDULE_DAY
        
        # Создаем абонемент через сервис
        result_message = sheets_service.create_full_subscription(sub_data)
        
        # Запускаем фоновые обновления асинхронно
        asyncio.create_task(update_after_subscription_creation())
        
        # Очищаем данные пользователя
        context.user_data.clear()
        
        # Показываем результат с информацией о фоновых обновлениях
        result_message += "\n\n🔄 Обновление статистики и календарей запущено в фоне."
        keyboard = [
            [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")],
            [InlineKeyboardButton("📅 Календарь занятий", callback_data="menu_calendar")],
            [InlineKeyboardButton("📄 Список абонементов", callback_data="menu_subscriptions")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(result_message, reply_markup=reply_markup)
        return MAIN_MENU
        
    except Exception as e:
        error_message = f"❌ Произошла ошибка при создании абонемента: {e}"
        keyboard = [
            [InlineKeyboardButton("🔄 Попробовать снова", callback_data="create_sub_finish")],
            [InlineKeyboardButton("⏪ Назад к списку", callback_data="menu_subscriptions")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.edit_message_text(error_message, reply_markup=reply_markup)
        return CREATE_SUB_SCHEDULE_DAY

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data.clear()
    message_text = 'Действие отменено.'
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(message_text)
    else:
        if update.message: await update.message.delete()
        await context.bot.send_message(chat_id=update.effective_chat.id, text=message_text)
        
    start_update = type('obj', (object,), {'callback_query': None, 'message': update.effective_message, 'effective_chat': update.effective_chat})
    await start(start_update, context)
    return ConversationHandler.END

# === Обработчик отмены уведомлений ===
async def cancel_notification_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик кнопки 'Отмена' в уведомлениях - удаляет сообщение с эффектом расщепления"""
    query = update.callback_query
    
    try:
        # Получаем lesson_id из callback_data
        lesson_id = query.data.replace("cancel_notification_", "")
        
        logging.info(f"❌ Отмена уведомления для занятия ID: {lesson_id}")
        
        # Удаляем сообщение с эффектом расщепления
        await query.message.delete()
        
        logging.info(f"🗑️ Уведомление для занятия {lesson_id} удалено с эффектом расщепления")
        
        # Возвращаем текущее состояние (не меняем состояние ConversationHandler)
        return NOTIFICATION_TIME_SETTINGS
        
    except Exception as e:
        logging.error(f"❌ Ошибка при отмене уведомления: {e}")
        
        # Если не удалось удалить, показываем сообщение об ошибке
        try:
            await query.answer("❌ Ошибка при удалении уведомления", show_alert=True)
        except:
            pass
        
        return NOTIFICATION_TIME_SETTINGS

# === Обработчики команд удалены - остается только /start ===

# === Собираем всю логику в ConversationHandler ===
def create_conversation_handler() -> ConversationHandler:
    logging.info("🔧 Создаю ConversationHandler с обработчиками...")
    logging.info("📋 MAIN_MENU обработчики:")
    logging.info("  - CallbackQueryHandler(main_menu_handler, pattern='^menu_')")
    logging.info("  - CallbackQueryHandler(forecast_subscription_handler, pattern='^forecast_sub_')")
    logging.info("  - CallbackQueryHandler(mark_payment_paid_handler, pattern='^mark_payment_')")
    logging.info("  - CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_') ← ДЛЯ УВЕДОМЛЕНИЙ")
    
    # Добавляем отладочный обработчик для всех callback'ов
    from telegram.ext import CallbackQueryHandler as CQH
    
    return ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(main_menu_handler, pattern='^menu_'),
                CallbackQueryHandler(sync_google_calendar_handler, pattern='^sync_google_calendar$'),
                CallbackQueryHandler(sync_google_forecast_handler, pattern='^sync_google_forecast$'),
                CallbackQueryHandler(clean_duplicates_handler, pattern='^clean_duplicates$'),
                CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_'),
                CallbackQueryHandler(cancel_notification_handler, pattern='^cancel_notification_'),  # Для отмены уведомлений
                CallbackQueryHandler(forecast_subscription_handler, pattern='^forecast_sub_'),
                CallbackQueryHandler(mark_payment_paid_handler, pattern='^mark_payment_'),
                CallbackQueryHandler(renewal_subscription_handler, pattern='^renew_subscription_'),
                CallbackQueryHandler(force_refresh_all_data, pattern='^force_refresh_data$'),
                CallbackQueryHandler(go_back_to_main_menu, pattern='^refresh_main_menu$'),
                CallbackQueryHandler(go_back_to_main_menu, pattern='^main_menu$'),
                # Отладочный обработчик для всех остальных callback'ов в MAIN_MENU
                CallbackQueryHandler(debug_callback_handler),
            ],
            # --- State machine for Settings ---
            SETTINGS_MENU: [
                CallbackQueryHandler(settings_show_category_items, pattern='^settings_cat_'),
                CallbackQueryHandler(notification_settings_handler, pattern='^notification_settings$'),
                CallbackQueryHandler(test_notifications_handler, pattern='^test_notifications$'),
                CallbackQueryHandler(sync_google_calendar_handler, pattern='^sync_google_calendar$'),
                CallbackQueryHandler(sync_google_forecast_handler, pattern='^sync_google_forecast$'),
                CallbackQueryHandler(clean_duplicates_handler, pattern='^clean_duplicates$'),
                CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_'),  # Для уведомлений
                CallbackQueryHandler(cancel_notification_handler, pattern='^cancel_notification_'),  # Для отмены уведомлений
                CallbackQueryHandler(update_stats_menu_handler, pattern='^menu_update_stats$'),
                CallbackQueryHandler(update_subscriptions_menu_handler, pattern='^menu_update_subscriptions$'),
                CallbackQueryHandler(go_back_to_main_menu, pattern='^main_menu$'),
            ],
            NOTIFICATION_TIME_SETTINGS: [
                CallbackQueryHandler(set_notification_time_handler, pattern='^set_notification_time_'),
                CallbackQueryHandler(disable_notifications_handler, pattern='^disable_notifications$'),
                CallbackQueryHandler(test_notifications_handler, pattern='^test_notifications$'),  # Для кнопки "Повторить тест"
                CallbackQueryHandler(notification_settings_handler, pattern='^notification_settings$'),  # Для кнопки "Настройки уведомлений"
                CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_'),  # Для уведомлений
                CallbackQueryHandler(cancel_notification_handler, pattern='^cancel_notification_'),  # Для отмены уведомлений
                CallbackQueryHandler(settings_menu, pattern='^menu_settings$'),
            ],
            SHOW_CATEGORY_ITEMS: [
                CallbackQueryHandler(show_category_items_handler, pattern='^settings_'),
                CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_'),  # Для уведомлений
                CallbackQueryHandler(cancel_notification_handler, pattern='^cancel_notification_'),  # Для отмены уведомлений
                CallbackQueryHandler(settings_menu, pattern='^menu_settings$'),
            ],
            ADD_ITEM: [MessageHandler(filters.TEXT & ~filters.COMMAND, add_item_handler)],
            MANAGE_SINGLE_ITEM: [
                CallbackQueryHandler(manage_single_item_handler, pattern='^settings_'),
                CallbackQueryHandler(settings_show_category_items, pattern='^settings_cat_')
            ],
            GET_NEW_VALUE_FOR_EDIT: [MessageHandler(filters.TEXT & ~filters.COMMAND, get_new_value_for_edit_handler)],
            CONFIRM_DELETE_ITEM: [
                CallbackQueryHandler(confirm_delete_item_handler, pattern='^settings_confirm_delete$'),
                CallbackQueryHandler(show_category_items_handler, pattern='^settings_select_item_') 
            ],

            # --- State machine for Subscriptions ---
            SELECT_SUBSCRIPTION: [
                CallbackQueryHandler(select_subscription_handler, pattern='^select_sub_'),
                CallbackQueryHandler(create_sub_start, pattern='^sub_create$'),
                CallbackQueryHandler(go_back_to_main_menu, pattern='^main_menu$'),
                CallbackQueryHandler(subscriptions_menu, pattern='^menu_subscriptions$') 
            ],
            MANAGE_SUBSCRIPTION: [
                CallbackQueryHandler(manage_subscription_handler, pattern='^(edit_sub|duplicate_sub|update_stats_sub|delete_sub)$'),
                CallbackQueryHandler(calendar_menu, pattern='^menu_calendar$'),
                CallbackQueryHandler(subscriptions_menu, pattern='^menu_subscriptions$'),
                CallbackQueryHandler(select_subscription_handler, pattern='^select_sub_')
            ],
            CONFIRM_DELETE_SUBSCRIPTION: [
                CallbackQueryHandler(confirm_delete_subscription_handler, pattern='^confirm_delete_yes$'),
                CallbackQueryHandler(select_subscription_handler, pattern='^select_sub_')
            ],
            # --- Subscription Creation States ---
            CREATE_SUB_CHILD: [
                CallbackQueryHandler(create_sub_child_handler, pattern='^create_sub_'),
                CallbackQueryHandler(subscriptions_menu, pattern='^menu_subscriptions$'),
            ],
            CREATE_SUB_GET_CHILD_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, create_sub_get_child_name_handler)],
            CREATE_SUB_CIRCLE: [
                CallbackQueryHandler(create_sub_circle_handler, pattern='^create_sub_'),
                CallbackQueryHandler(create_sub_start, pattern='^sub_create$'), 
            ],
            CREATE_SUB_GET_CIRCLE_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, create_sub_get_circle_name_handler)],
            CREATE_SUB_TYPE: [
                CallbackQueryHandler(create_sub_type_handler, pattern='^create_sub_type_'),
                CallbackQueryHandler(create_sub_circle_handler, pattern='^create_sub_child_')
            ],
            CREATE_SUB_PAYMENT_TYPE: [
                CallbackQueryHandler(create_sub_payment_type_handler, pattern='^create_sub_payment_'),
                CallbackQueryHandler(create_sub_ask_for_type, pattern='^back_to_sub_type$')
            ],
            CREATE_SUB_COST: [
                CallbackQueryHandler(create_sub_cost_handler, pattern='^create_sub_cost_'),
                CallbackQueryHandler(back_to_payment_type_handler, pattern='^back_to_payment_type$'),
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_sub_cost_text_handler)
            ],
            CREATE_SUB_TOTAL_CLASSES: [
                CallbackQueryHandler(create_sub_total_classes_handler, pattern='^create_sub_total_'),
                CallbackQueryHandler(back_to_cost_handler, pattern='^back_to_cost$')
            ],
            CREATE_SUB_REMAINING_CLASSES: [
                CallbackQueryHandler(create_sub_remaining_classes_handler, pattern='^create_sub_remaining_'),
                CallbackQueryHandler(back_to_total_classes_handler, pattern='^back_to_total_classes$')
            ],
            CREATE_SUB_START_DATE_MONTH: [
                CallbackQueryHandler(create_sub_calendar_handler, pattern='^cal_'),
                CallbackQueryHandler(back_to_remaining_classes_handler, pattern='^back_to_remaining_classes$'),
                CallbackQueryHandler(subscriptions_menu, pattern='^menu_subscriptions$')
            ],
            CREATE_SUB_SCHEDULE_DAY: [
                CallbackQueryHandler(create_sub_schedule_day_handler, pattern='^schedule_day_'),
                CallbackQueryHandler(create_sub_back_to_calendar_handler, pattern='^create_sub_back_to_calendar$'),
                CallbackQueryHandler(create_sub_show_schedule_summary, pattern='^show_schedule_summary$'),
                CallbackQueryHandler(create_sub_add_another_day, pattern='^add_another_day$'),
                CallbackQueryHandler(create_sub_finish_handler, pattern='^create_sub_finish$'),
                CallbackQueryHandler(subscriptions_menu, pattern='^menu_subscriptions$')
            ],
            CREATE_SUB_SCHEDULE_START_HOUR: [
                CallbackQueryHandler(create_sub_start_hour_handler, pattern='^start_hour_'),
                CallbackQueryHandler(back_to_day_selection_handler, pattern='^back_to_day_selection$')
            ],
            CREATE_SUB_SCHEDULE_START_MINUTE: [
                CallbackQueryHandler(create_sub_start_minute_handler, pattern='^start_minute_'),
                CallbackQueryHandler(back_to_start_hour_selection_handler, pattern='^back_to_start_hour_selection$')
            ],
            CREATE_SUB_SCHEDULE_END_HOUR: [
                CallbackQueryHandler(create_sub_end_hour_handler, pattern='^end_time_'),
                CallbackQueryHandler(back_to_start_minute_handler, pattern='^back_to_start_minute$')
            ],
            CREATE_SUB_SCHEDULE_END_MINUTE: [
                CallbackQueryHandler(create_sub_end_minute_handler, pattern='^end_minute_'),
                CallbackQueryHandler(back_to_end_hour_selection_handler, pattern='^back_to_end_hour_selection$')
            ],
            CREATE_SUB_SCHEDULE_CONFIRM: [
                CallbackQueryHandler(create_sub_confirm_schedule_handler, pattern='^confirm_schedule_time$'),
                CallbackQueryHandler(create_sub_start_hour_handler, pattern='^back_to_start_hour$'),
                CallbackQueryHandler(back_to_end_time_selection_handler, pattern='^back_to_end_time_selection$')
            ],
            
            # Calendar states
            INTERACTIVE_CALENDAR: [
                CallbackQueryHandler(calendar_navigation_handler, pattern='^(calendar_nav_|calendar_today)'),
                CallbackQueryHandler(select_calendar_date, pattern='^calendar_date_'),
                CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_'),  # Для уведомлений
                CallbackQueryHandler(cancel_notification_handler, pattern='^cancel_notification_'),  # Для отмены уведомлений
                CallbackQueryHandler(start, pattern='^(start|main_menu)$'),
                CallbackQueryHandler(calendar_menu, pattern='^menu_calendar$')
            ],
            SELECT_LESSON_FROM_DATE: [
                CallbackQueryHandler(select_lesson_from_date, pattern='^lesson_select_'),
                CallbackQueryHandler(select_calendar_date, pattern='^calendar_date_'),
                CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_'),  # Для уведомлений
                CallbackQueryHandler(cancel_notification_handler, pattern='^cancel_notification_'),  # Для отмены уведомлений
                CallbackQueryHandler(calendar_menu, pattern='^menu_calendar$')
            ],
            SELECT_ATTENDANCE_MARK: [
                CallbackQueryHandler(save_attendance_mark, pattern='^attendance_mark_'),
                CallbackQueryHandler(cancel_notification_handler, pattern='^cancel_notification_'),  # Для отмены уведомлений
                CallbackQueryHandler(select_calendar_date, pattern='^calendar_date_'),
                CallbackQueryHandler(calendar_menu, pattern='^menu_calendar$')
            ],
            
            # Subscription Renewal states
            RENEWAL_SELECT_DATE_TYPE: [
                CallbackQueryHandler(renewal_date_type_handler, pattern='^(renewal_use_date_|renewal_select_custom_date)'),
                CallbackQueryHandler(forecast_subscription_handler, pattern='^forecast_sub_')
            ],
            RENEWAL_SELECT_CUSTOM_DATE: [
                CallbackQueryHandler(renewal_custom_date_handler, pattern='^(cal_day_|cal_month_)'),
                CallbackQueryHandler(renewal_date_type_handler, pattern='^renewal_select_custom_date$')
            ],
            RENEWAL_CONFIRM: [
                CallbackQueryHandler(renewal_create_handler, pattern='^renewal_confirm_create$'),
                CallbackQueryHandler(renewal_date_type_handler, pattern='^renewal_select_custom_date$'),
                CallbackQueryHandler(forecast_subscription_handler, pattern='^forecast_sub_')
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel), 
            CommandHandler('start', start),
        ],
        allow_reentry=True,
    )

async def dashboard_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик кнопки дашборда"""
    query = update.callback_query
    await query.answer()
    
    message_text = """📊 <b>Дашборд</b>

🌐 <b>Локальный доступ:</b>
Откройте в браузере: http://127.0.0.1:5000

📱 <b>Для Telegram Mini App:</b>
Требуется HTTPS сервер для работы в Telegram.

🔧 <b>Что доступно в дашборде:</b>
• 📊 Метрики посещаемости
• 💰 Финансовые показатели  
• 📈 Прогресс по абонементам
• 📅 Интерактивный календарь
• 🎯 Фильтры по ученикам

💡 <b>Совет:</b> Скопируйте ссылку и откройте в браузере для полного доступа к дашборду."""

    keyboard = [
        [InlineKeyboardButton("🌐 Открыть в браузере", url="http://127.0.0.1:5000")],
        [InlineKeyboardButton("🏠 Главное меню", callback_data="main_menu")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(message_text, reply_markup=reply_markup, parse_mode='HTML')
    return MAIN_MENU

