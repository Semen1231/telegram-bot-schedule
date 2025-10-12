# ⚡ БЫСТРЫЙ ДЕПЛОЙ (5 МИНУТ)

## 📋 ЧЕКЛИСТ

### ☑️ ШАГ 1: GitHub (2 минуты)

```bash
# В терминале (внутри проекта):
git init
git add .
git commit -m "Ready for deploy"
```

Затем на GitHub.com:
1. New repository → `telegram-bot-schedule`
2. Скопируйте команды подключения:

```bash
git remote add origin https://github.com/ВАШ-USERNAME/telegram-bot-schedule.git
git branch -M main
git push -u origin main
```

✅ Репозиторий создан!

---

### ☑️ ШАГ 2: Railway.app (3 минуты)

1. Откройте: https://railway.app
2. **Login with GitHub**
3. **New Project** → **Deploy from GitHub repo**
4. Выберите: `telegram-bot-schedule`

✅ Проект создан!

---

### ☑️ ШАГ 3: Переменные окружения (2 минуты)

В Railway → Variables → Add переменные:

```env
TELEGRAM_BOT_TOKEN=ваш_токен_от_BotFather
GOOGLE_SHEET_NAME=ваш_id_таблицы
GOOGLE_CALENDAR_ID=disabled
```

**Важно! Добавьте service_account.json:**

```env
GOOGLE_SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"...весь JSON..."}
```

Как получить:
```bash
cat service_account.json
```
Скопируйте весь вывод и вставьте в переменную.

✅ Переменные добавлены!

---

### ☑️ ШАГ 4: Деплой (автоматически)

Railway автоматически:
- ✅ Установит зависимости
- ✅ Запустит бота
- ✅ Запустит дашборд

Проверьте логи: **View Logs**

Должны увидеть:
```
✅ Google Service Account создан
✅ Успешное подключение к Google Таблицам
✅ Telegram бот запущен
```

---

### ☑️ ШАГ 5: URL дашборда

1. Settings → **Generate Domain**
2. Получите URL: `https://your-app.railway.app`
3. Откройте в браузере

✅ Дашборд работает!

---

## 🎉 ГОТОВО!

Теперь:
- ✅ Бот работает 24/7
- ✅ Дашборд доступен по URL
- ✅ Автодеплой при push в GitHub

### Обновление кода:

```bash
# Внесите изменения
git add .
git commit -m "Update"
git push

# Railway задеплоит автоматически!
```

---

## 🐛 Если что-то не работает:

1. **Проверьте логи**: Railway → View Logs
2. **Проверьте переменные**: Railway → Variables
3. **Проверьте service_account.json**: Вставлен полностью?

---

## 💡 АЛЬТЕРНАТИВА: Render.com (100% бесплатно)

Если Railway не подходит:

1. https://render.com
2. New → Web Service
3. Connect GitHub
4. Start Command: `python dashboard_server.py`
5. Add Background Worker → `python main.py`

⚠️ Free tier засыпает после 15 мин бездействия.

---

📖 **Полная инструкция**: `DEPLOY_GUIDE.md`
