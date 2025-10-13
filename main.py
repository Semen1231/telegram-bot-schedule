#!/usr/bin/env python3
"""
Главный файл запуска для Railway деплоя
Запускает и Dashboard, и Telegram бота одновременно
"""

import os
import sys
import threading
import asyncio
import time

def start_dashboard():
    """Запускает Dashboard сервер в отдельном потоке"""
    try:
        print("📊 Запуск Dashboard Server...")
        from dashboard_server import app
        port = int(os.getenv('PORT', 5001))
        print(f"🌐 Dashboard запускается на порту {port}")
        app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
    except Exception as e:
        print(f"❌ Ошибка запуска Dashboard: {e}")

def start_telegram_bot():
    """Запускает Telegram бота в отдельном потоке"""
    try:
        print("🤖 Запуск Telegram Bot...")
        # Небольшая задержка, чтобы дашборд успел запуститься
        time.sleep(5)
        from bot_main import main as bot_main
        bot_main()
    except Exception as e:
        print(f"❌ Ошибка запуска Telegram Bot: {e}")

def main():
    """Запускает оба сервиса одновременно"""
    
    service_mode = os.getenv('SERVICE_MODE', 'both')
    print(f"🚀 Railway деплой - режим: {service_mode}")
    
    if service_mode == 'dashboard':
        # Только дашборд
        start_dashboard()
        
    elif service_mode == 'bot':
        # Только бот
        print("🤖 Запуск только Telegram Bot...")
        try:
            from bot_main import main as bot_main
            bot_main()
        except Exception as e:
            print(f"❌ Ошибка запуска Telegram Bot: {e}")
            sys.exit(1)
        
    elif service_mode == 'both' or service_mode == '':
        # Оба сервиса (по умолчанию для Railway)
        print("🔄 Запуск обоих сервисов...")
        
        # Запускаем дашборд в отдельном потоке
        dashboard_thread = threading.Thread(target=start_dashboard, daemon=True)
        dashboard_thread.start()
        
        # Запускаем бота в основном потоке
        start_telegram_bot()
        
    else:
        print(f"❌ Неизвестный режим: {service_mode}")
        print("💡 Доступные режимы: 'bot', 'dashboard', 'both'")
        sys.exit(1)

if __name__ == "__main__":
    main()
