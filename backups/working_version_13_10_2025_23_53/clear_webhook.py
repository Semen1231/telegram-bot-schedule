#!/usr/bin/env python3
"""
Скрипт для принудительной очистки webhook Telegram бота
"""
import asyncio
import os
from telegram import Bot
from telegram.error import TelegramError

async def clear_webhook():
    """Принудительно очищает webhook и pending updates"""
    
    # Получаем токен из переменной окружения
    token = os.getenv('TELEGRAM_BOT_TOKEN')
    if not token:
        print("❌ TELEGRAM_BOT_TOKEN не найден в переменных окружения")
        print("Добавьте токен: export TELEGRAM_BOT_TOKEN='ваш_токен'")
        return False
    
    try:
        bot = Bot(token=token)
        
        print("🔧 Очищаю webhook...")
        
        # Удаляем webhook и все pending updates
        await bot.delete_webhook(drop_pending_updates=True)
        print("✅ Webhook успешно очищен")
        
        # Проверяем статус webhook
        webhook_info = await bot.get_webhook_info()
        print(f"📋 Статус webhook: {webhook_info.url if webhook_info.url else 'Не установлен'}")
        print(f"📋 Pending updates: {webhook_info.pending_update_count}")
        
        return True
        
    except TelegramError as e:
        print(f"❌ Ошибка Telegram API: {e}")
        return False
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Запуск очистки webhook...")
    success = asyncio.run(clear_webhook())
    
    if success:
        print("✅ Webhook очищен! Теперь можно запускать бота на Railway.")
    else:
        print("❌ Не удалось очистить webhook. Проверьте токен и интернет-соединение.")
