import logging
import asyncio
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

async def clear_webhook_and_setup(application):
    """Очищает webhook и устанавливает команды меню бота."""
    try:
        # Очищаем webhook
        logger.info("🔧 Очищаю webhook...")
        await application.bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook очищен")
        
        # Небольшая задержка
        await asyncio.sleep(2)
        
        # Устанавливаем команды
        commands = [
            BotCommand("start", "🏠 Главное меню"),
        ]
        
        await application.bot.set_my_commands(commands)
        logger.info("✅ Команда меню установлена")
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке webhook: {e}")

async def post_init_handler(application):
    """Обработчик инициализации после запуска бота."""
    # Сначала очищаем webhook и устанавливаем команды
    await clear_webhook_and_setup(application)
    
    # Затем запускаем планировщик если настроен
    try:
        if sheets_service:
            from notification_scheduler import get_notification_scheduler
            notification_scheduler = get_notification_scheduler(application.bot)
            
            notification_time = sheets_service.get_notification_time()
            notification_chat_id = sheets_service.get_notification_chat_id()
            
            if notification_time and notification_chat_id:
                logger.info("🚀 Запускаю планировщик уведомлений...")
                notification_scheduler.set_chat_id(notification_chat_id)
                await asyncio.create_task(notification_scheduler.start_scheduler())
            else:
                logger.info("⚠️ Планировщик уведомлений не настроен")
        else:
            logger.info("⚠️ Планировщик пропущен - Google Sheets недоступен")
    except Exception as e:
        logger.error(f"❌ Ошибка при запуске планировщика: {e}")

def main() -> None:
    """Основная функция для запуска бота."""
    
    # 0. ПРИНУДИТЕЛЬНАЯ ОЧИСТКА WEBHOOK ПЕРЕД ЗАПУСКОМ
    logger.info("🔧 ПРИНУДИТЕЛЬНАЯ очистка webhook перед запуском...")
    try:
        import requests
        import config
        url = f"https://api.telegram.org/bot{config.TELEGRAM_TOKEN}/deleteWebhook?drop_pending_updates=true"
        logger.info(f"📡 Отправляю запрос: {url}")
        response = requests.post(url)
        logger.info(f"✅ Webhook очищен принудительно: {response.json()}")
    except Exception as e:
        logger.error(f"❌ Ошибка принудительной очистки webhook: {e}")
    
    logger.info("🔄 Продолжаю инициализацию...")
    
    # 1. Проверяем, что сервис Google Sheets работает
    if not sheets_service:
        logging.warning("⚠️ Google Sheets недоступен, но бот запустится без него")
        # ВРЕМЕННО: Полностью игнорируем Google Sheets для исправления webhook
        pass

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
    
    # 4. Устанавливаем обработчик инициализации
    logger.info("🔧 Настраиваю обработчик инициализации...")
    application.post_init = post_init_handler
    
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

