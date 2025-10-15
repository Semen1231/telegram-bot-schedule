#!/usr/bin/env python3
"""
Главный файл запуска для Railway деплоя
Определяет, что запускать: Telegram бота или Dashboard сервер
"""

import os
import sys

def main():
    """Определяет режим запуска на основе переменных окружения."""
    
    # По умолчанию запускаем dashboard (web процесс)
    service_mode = os.getenv('SERVICE_MODE', 'dashboard')
    
    print(f"🚀 Railway деплой - режим: {service_mode}")
    
    if service_mode == 'dashboard':
        print("📊 Запуск Dashboard Server...")
        # Импортируем и запускаем dashboard
        try:
            from dashboard_server import app
            port = int(os.getenv('PORT', 5001))
            print(f"🌐 Dashboard запускается на порту {port}")
            app.run(host='0.0.0.0', port=port, debug=False)
        except Exception as e:
            print(f"❌ Ошибка запуска Dashboard: {e}")
            sys.exit(1)
        
    elif service_mode == 'bot':
        print("🤖 Запуск Telegram Bot...")
        # Импортируем и запускаем бота
        try:
            from bot_main import main as bot_main
            bot_main()
        except Exception as e:
            print(f"❌ Ошибка запуска Telegram Bot: {e}")
            sys.exit(1)
        
    else:
        print(f"❌ Неизвестный режим: {service_mode}")
        print("💡 Доступные режимы: 'bot', 'dashboard'")
        sys.exit(1)

if __name__ == "__main__":
    main()
