#!/usr/bin/env python3
"""
Отдельный файл только для запуска Telegram бота на Railway
Используется если не получается настроить worker процесс
"""

import os
import sys

# Устанавливаем режим бота
os.environ['SERVICE_MODE'] = 'bot'

# Импортируем и запускаем основной файл
from bot_main import main

if __name__ == "__main__":
    print("🤖 Запуск только Telegram бота...")
    main()
