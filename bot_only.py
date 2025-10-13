#!/usr/bin/env python3
"""
Отдельный файл только для запуска Telegram бота на Railway
Используется если не получается настроить worker процесс
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
    try:
        logger.info("🚀 Railway Telegram Bot - запуск...")
        
        # Устанавливаем режим бота
        os.environ['SERVICE_MODE'] = 'bot'
        logger.info("✅ SERVICE_MODE установлен в 'bot'")
        
        # Проверяем основные переменные
        required_vars = ['TELEGRAM_TOKEN', 'GOOGLE_CREDENTIALS_JSON', 'GOOGLE_SHEET_NAME']
        for var in required_vars:
            if not os.getenv(var):
                logger.error(f"❌ Отсутствует переменная окружения: {var}")
                return
            else:
                logger.info(f"✅ {var} найден")
        
        # Импортируем и запускаем основной файл
        logger.info("📥 Импорт bot_main...")
        from bot_main import main as bot_main
        
        logger.info("🤖 Запуск Telegram бота...")
        bot_main()
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске бота: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
