#!/usr/bin/env python3
"""
ТОЛЬКО TELEGRAM БОТ для Railway Worker процесса
Этот файл запускает ИСКЛЮЧИТЕЛЬНО бота, без dashboard
"""

import os
import sys
import logging

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

def main():
    """Запускает ТОЛЬКО Telegram бота"""
    try:
        logger.info("🤖 WORKER: Запуск ТОЛЬКО Telegram бота...")
        
        # Проверяем обязательные переменные
        required_vars = ['TELEGRAM_TOKEN', 'GOOGLE_CREDENTIALS_JSON', 'GOOGLE_SHEET_NAME']
        missing_vars = []
        
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
            else:
                logger.info(f"✅ {var} найден")
        
        if missing_vars:
            logger.error(f"❌ Отсутствуют переменные: {missing_vars}")
            sys.exit(1)
        
        # Принудительно устанавливаем режим бота
        os.environ['SERVICE_MODE'] = 'bot'
        logger.info("✅ SERVICE_MODE принудительно установлен в 'bot'")
        
        # Импортируем и запускаем ТОЛЬКО бота
        logger.info("📥 Импорт bot_main...")
        from bot_main import main as bot_main
        
        logger.info("🚀 Запуск Telegram бота (worker.py)...")
        bot_main()
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в worker.py: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
