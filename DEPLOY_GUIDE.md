# 🚀 ГАЙД ПО ДЕПЛОЮ НА СЕРВЕР

## 📋 ВЫБОР ПЛАТФОРМЫ

### 🎯 Рекомендуется: **Railway.app**
- ✅ Бесплатный tier: $5 в месяц кредитов (хватит на бота)
- ✅ Простая интеграция с GitHub
- ✅ Автоматический деплой при пуше в GitHub
- ✅ Поддержка нескольких сервисов (бот + дашборд)
- ✅ Легкая настройка переменных окружения

### 🔄 Альтернатива: **Render.com**
- ✅ Полностью бесплатный tier
- ⚠️ Ограничение: сервис засыпает после 15 минут бездействия
- ✅ Хорошо подходит для дашборда

---

## 📦 ШАГ 1: ПОДГОТОВКА ПРОЕКТА

### 1.1 Файлы уже созданы:
- ✅ `Procfile` - команды запуска
- ✅ `railway.toml` - конфигурация Railway
- ✅ `runtime.txt` - версия Python
- ✅ `.gitignore` - исключения для Git
- ✅ `requirements.txt` - зависимости

### 1.2 Проверьте `.env` файл (НЕ КОММИТИТЬ!)
Убедитесь что в `.env` есть все необходимые переменные:
```env
TELEGRAM_BOT_TOKEN=your_bot_token
GOOGLE_SPREADSHEET_ID=your_spreadsheet_id
GOOGLE_CALENDAR_ID=disabled
GOOGLE_CREDENTIALS_PATH=service_account.json
```

---

## 🔐 ШАГ 2: ПОДГОТОВКА СЕКРЕТНЫХ ДАННЫХ

### 2.1 Скопируйте содержимое service_account.json
```bash
cat service_account.json
```
Скопируйте весь JSON (понадобится для Railway)

### 2.2 Запомните свой TELEGRAM_BOT_TOKEN
Это токен от @BotFather

---

## 📤 ШАГ 3: GITHUB ИНТЕГРАЦИЯ (WINDSURF)

### 3.1 Инициализация Git репозитория
```bash
cd /Users/semenmareev/Desktop/telegram-bot-app-Рабочий-08.10.2025

# Инициализируем Git (если еще не сделано)
git init

# Добавляем все файлы
git add .

# Первый коммит
git commit -m "Initial commit: Working version with dashboard"
```

### 3.2 Создайте репозиторий на GitHub

1. Откройте https://github.com
2. Нажмите "+" → "New repository"
3. Название: `telegram-bot-schedule` (или любое другое)
4. **Важно:** НЕ добавляйте README, .gitignore, license
5. Нажмите "Create repository"

### 3.3 Подключите локальный репозиторий к GitHub

GitHub покажет команды, выполните их:
```bash
git remote add origin https://github.com/ваш-username/telegram-bot-schedule.git
git branch -M main
git push -u origin main
```

### 3.4 Автоматизация через Windsurf (VS Code)

В Windsurf/VS Code:
1. Откройте Source Control (Ctrl+Shift+G)
2. Введите commit message
3. Нажмите ✓ (Commit)
4. Нажмите "Sync Changes" или "Push"

---

## 🚂 ШАГ 4: ДЕПЛОЙ НА RAILWAY

### 4.1 Регистрация на Railway
1. Откройте https://railway.app
2. Нажмите "Login" → "Login with GitHub"
3. Авторизуйтесь через GitHub

### 4.2 Создание проекта
1. Нажмите "New Project"
2. Выберите "Deploy from GitHub repo"
3. Выберите ваш репозиторий `telegram-bot-schedule`
4. Railway автоматически обнаружит Python проект

### 4.3 Настройка переменных окружения

В Railway проекте:
1. Нажмите на ваш сервис
2. Перейдите в "Variables"
3. Добавьте переменные:

```
TELEGRAM_BOT_TOKEN = ваш_токен_бота
GOOGLE_SPREADSHEET_ID = ваш_id_таблицы
GOOGLE_CALENDAR_ID = disabled
GOOGLE_CREDENTIALS_PATH = service_account.json
PORT = 5001
```

### 4.4 Добавление service_account.json

**Вариант 1: Через переменную окружения (рекомендуется)**
```
GOOGLE_SERVICE_ACCOUNT_JSON = {"type":"service_account","project_id":"..."}
```
Вставьте весь содержимое service_account.json как одну строку.

Затем измените `config.py`:
```python
import json
import os

# Если есть переменная окружения, используем её
if os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON'):
    credentials_dict = json.loads(os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON'))
    # Сохраняем во временный файл
    with open('service_account.json', 'w') as f:
        json.dump(credentials_dict, f)
```

**Вариант 2: Через Railway Volume (сложнее)**

### 4.5 Деплой
1. Railway автоматически начнет деплой
2. Дождитесь завершения (2-5 минут)
3. Проверьте логи: "View Logs"

### 4.6 Получение URL дашборда
1. В Railway перейдите в Settings
2. Нажмите "Generate Domain"
3. Получите URL вида: `https://your-app.railway.app`

---

## 🔄 ШАГ 5: АВТОМАТИЧЕСКИЙ ДЕПЛОЙ

Теперь при каждом push в GitHub:
1. Railway автоматически обнаружит изменения
2. Пересоберет проект
3. Задеплоит новую версию

### Workflow:
```bash
# 1. Вносите изменения в коде
# 2. Коммит и push
git add .
git commit -m "Update: добавил новую функцию"
git push

# 3. Railway автоматически задеплоит изменения
```

---

## 🎯 ШАГ 6: НАСТРОЙКА ДВУХ СЕРВИСОВ

Railway позволяет запустить несколько процессов:

### Вариант 1: Один сервис (текущий)
```toml
# railway.toml
[deploy]
startCommand = "python main.py & python dashboard_server.py"
```

### Вариант 2: Два отдельных сервиса
1. **Сервис 1: Bot**
   - Start Command: `python main.py`
   
2. **Сервис 2: Dashboard**
   - Start Command: `python dashboard_server.py`
   - Generate Domain для веб-доступа

---

## 🆓 АЛЬТЕРНАТИВА: RENDER.COM

### Если хотите полностью бесплатно:

1. Откройте https://render.com
2. "New +" → "Web Service"
3. Connect GitHub repository
4. Настройки:
   - **Name**: telegram-bot-dashboard
   - **Environment**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python dashboard_server.py`
   - **Plan**: Free

5. Добавьте переменные окружения (как в Railway)

6. Для бота создайте **Background Worker**:
   - Start Command: `python main.py`

⚠️ **Важно**: Free tier Render засыпает после 15 минут бездействия.

---

## 🔍 ПРОВЕРКА ДЕПЛОЯ

### Проверьте логи:
```
Railway: View Logs → Real-time logs
Render: Logs → View logs
```

### Должны увидеть:
```
✅ Успешное подключение к Google Таблицам
✅ Telegram бот запущен
✅ Dashboard Server на порту 5001
```

### Проверьте дашборд:
Откройте URL: `https://your-app.railway.app` или `https://your-app.onrender.com`

---

## 🐛 ЧАСТЫЕ ПРОБЛЕМЫ

### 1. "service_account.json not found"
**Решение**: Добавьте `GOOGLE_SERVICE_ACCOUNT_JSON` в переменные окружения

### 2. "Module not found"
**Решение**: Проверьте `requirements.txt` - все зависимости указаны?

### 3. "Port already in use"
**Решение**: В Railway/Render используйте переменную `PORT`
```python
PORT = int(os.getenv('PORT', 5001))
app.run(host='0.0.0.0', port=PORT)
```

### 4. Бот не отвечает
**Решение**: Проверьте логи, возможно проблема с `TELEGRAM_BOT_TOKEN`

---

## 📊 МОНИТОРИНГ

### Railway:
- Metrics → CPU, Memory, Network usage
- Logs → Real-time logs

### Render:
- Metrics → Request stats
- Logs → Application logs

---

## 💰 СТОИМОСТЬ

### Railway (Рекомендуется):
- **Бесплатно**: $5 кредитов/месяц
- **Хватит на**: ~500 часов работы
- **Для постоянной работы**: $5-10/месяц

### Render:
- **Полностью бесплатно** для спящих сервисов
- **Платно**: $7/месяц для постоянной работы

---

## 🎉 ГОТОВО!

Теперь ваш бот работает в облаке 24/7!

### Что дальше:
1. ✅ Бот запущен и работает
2. ✅ Дашборд доступен по URL
3. ✅ Автоматический деплой при push в GitHub
4. ✅ Логи и мониторинг доступны

### Обновление кода:
```bash
# Внесите изменения
git add .
git commit -m "Update: описание изменений"
git push

# Railway автоматически задеплоит новую версию!
```

---

## 📞 ПОЛЕЗНЫЕ ССЫЛКИ

- Railway: https://railway.app
- Render: https://render.com
- Railway Docs: https://docs.railway.app
- Render Docs: https://render.com/docs

🚀 **Успешного деплоя!**
