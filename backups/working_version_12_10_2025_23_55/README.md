# 🤖 Telegram Bot для управления расписанием занятий

## 📋 Описание
Telegram бот для автоматизации управления расписанием детских занятий с интеграцией Google Sheets и Google Calendar.

## ⚡ Основные функции
- 📄 **Управление абонементами** - создание, продление, аналитика
- 📊 **Прогноз бюджета** - планирование оплат и расходов  
- 📅 **Календарь занятий** - отметка посещений, интерактивный календарь
- 🔔 **Уведомления** - автоматические напоминания о занятиях
- ⚙️ **Настройки** - управление данными и синхронизация

## 🚀 Запуск

### 1. Установка зависимостей
```bash
pip install -r requirements.txt
```

### 2. Настройка окружения
```bash
cp .env.example .env
# Заполните переменные в .env файле
```

### 3. Запуск бота
```bash
python main.py
```

## 📁 Структура проекта
```
├── main.py                    # Точка входа
├── bot_handlers.py            # Основная логика бота
├── google_sheets_service.py   # Интеграция с Google Sheets
├── google_calendar_service.py # Интеграция с Google Calendar  
├── notification_scheduler.py  # Система уведомлений
├── config.py                  # Конфигурация
├── requirements.txt           # Зависимости Python
├── service_account.json       # Ключи Google API
├── .env                       # Переменные окружения
└── .env.example              # Пример конфигурации
```

## 🔧 Требования
- Python 3.11+
- Google Sheets API
- Google Calendar API (опционально)
- Telegram Bot Token

## 📊 Статус
✅ **Готов к продуктивному использованию**

Дата: Октябрь 2025  
Версия: Production Ready
