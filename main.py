import logging
from telegram.ext import Application
from telegram import BotCommand
import config
from bot_handlers import create_conversation_handler
from google_sheets_service import sheets_service

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

async def setup_bot_commands(application):
    """Устанавливает команды меню бота."""
    commands = [
        BotCommand("start", "🏠 Главное меню"),
    ]
    
    await application.bot.set_my_commands(commands)
    logger.info("✅ Команда меню установлена")

def main() -> None:
    """Основная функция для запуска бота."""
    
    # 1. Проверяем, что сервис Google Sheets работает
    if not sheets_service:
        logging.critical("Не удалось запустить: ошибка подключения к Google Таблицам.")
        return

    # 2. Создаем и настраиваем приложение бота
    logger.info("Создаю приложение бота...")
    application = Application.builder().token(config.TELEGRAM_TOKEN).build()
    logger.info("Приложение бота создано успешно.")
    
    logger.info("Бот запускается...")

    # 3. Регистрация ConversationHandler
    logger.info("Регистрирую обработчики...")
    conv_handler = create_conversation_handler()
    application.add_handler(conv_handler)
    logger.info("Обработчики зарегистрированы.")
    
    # 4. Инициализируем планировщик уведомлений
    logger.info("🔔 Инициализирую планировщик уведомлений...")
    try:
        from notification_scheduler import get_notification_scheduler
        notification_scheduler = get_notification_scheduler(application.bot)
        
        # Проверяем, настроено ли время уведомлений
        notification_time = sheets_service.get_notification_time()
        notification_chat_id = sheets_service.get_notification_chat_id()
        
        logger.info(f"📊 СТАТУС НАСТРОЕК УВЕДОМЛЕНИЙ:")
        logger.info(f"   ⏰ notification_time: '{notification_time}'")
        logger.info(f"   📱 notification_chat_id: '{notification_chat_id}'")
        
        if notification_time and notification_chat_id:
            logger.info(f"✅ Найдено настроенное время уведомлений: {notification_time}")
            logger.info(f"✅ Найден chat_id для уведомлений: {notification_chat_id}")
            
            # Устанавливаем chat_id в планировщик
            notification_scheduler.set_chat_id(notification_chat_id)
            
            # Планировщик будет запущен после старта event loop
            application.post_init = lambda app: asyncio.create_task(notification_scheduler.start_scheduler())
            logger.info("🚀 Планировщик уведомлений будет запущен после старта бота")
        else:
            if not notification_time:
                logger.info("⚠️ Время уведомлений не настроено")
            if not notification_chat_id:
                logger.info("⚠️ Chat ID для уведомлений не настроен")
            logger.info("❌ Планировщик не запущен")
    except Exception as e:
        logger.error(f"❌ Ошибка при инициализации планировщика уведомлений: {e}")
        import traceback
        logger.error(f"📊 Traceback: {traceback.format_exc()}")
    
    # 5. Устанавливаем команды меню при запуске
    logger.info("🔧 Команды меню будут установлены при запуске бота...")
    application.post_init = setup_bot_commands
    
    # 6. Запускаем бота
    logger.info("Запускаю polling...")
    try:
        application.run_polling()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки (Ctrl+C)")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
    finally:
        logger.info("Бот остановлен.")

if __name__ == '__main__':
    main()

