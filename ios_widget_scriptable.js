/**
 * 📱 iOS SCRIPTABLE WIDGET - ПРОГРЕСС ПО АКТИВНЫМ АБОНЕМЕНТАМ
 * 
 * Виджет для отображения данных активных абонементов на рабочем столе iOS
 * Использует API вашего дашборда для получения актуальных данных
 * 
 * Инструкция по установке:
 * 1. Скачайте приложение Scriptable из App Store
 * 2. Создайте новый скрипт и вставьте этот код
 * 3. Измените API_BASE_URL на ваш реальный адрес сервера
 * 4. Добавьте виджет Scriptable на рабочий стол
 * 5. Выберите этот скрипт в настройках виджета
 */

// ========== НАСТРОЙКИ ==========
const API_BASE_URL = "https://disabled-temporarily.local"; // ОТКЛЮЧЕНО: Ваш Railway URL
const REFRESH_INTERVAL = 15; // Интервал обновления в минутах

// ========== ОСНОВНАЯ ФУНКЦИЯ ==========
async function createWidget() {
    const widget = new ListWidget();
    
    try {
        // Получаем данные из API
        const data = await fetchDashboardData();
        
        if (data && data.subscriptions && data.subscriptions.length > 0) {
            await setupSuccessWidget(widget, data);
        } else {
            setupEmptyWidget(widget);
        }
        
    } catch (error) {
        console.error("Ошибка получения данных:", error);
        setupErrorWidget(widget, error.message);
    }
    
    // Настройки виджета
    widget.refreshAfterDate = new Date(Date.now() + REFRESH_INTERVAL * 60 * 1000);
    widget.backgroundColor = new Color("#151634"); // Темно-синий фон как в дашборде
    
    return widget;
}

// ========== ПОЛУЧЕНИЕ ДАННЫХ ==========
async function fetchDashboardData() {
    console.log("Получение данных из API...");
    console.log("API Base URL:", API_BASE_URL);
    
    // Получаем активные абонементы
    const subscriptionsUrl = `${API_BASE_URL}/api/subscriptions?student=Все`;
    console.log("Subscriptions URL:", subscriptionsUrl);
    
    const subscriptionsRequest = new Request(subscriptionsUrl);
    subscriptionsRequest.timeoutInterval = 10;
    
    console.log("Отправляю запрос на получение абонементов...");
    const subscriptionsResponse = await subscriptionsRequest.loadJSON();
    console.log("Ответ абонементов:", subscriptionsResponse);
    
    if (!subscriptionsResponse.success) {
        throw new Error("Ошибка получения данных абонементов");
    }
    
    // Получаем метрики
    const metricsUrl = `${API_BASE_URL}/api/metrics?student=Все`;
    console.log("Metrics URL:", metricsUrl);
    
    const metricsRequest = new Request(metricsUrl);
    metricsRequest.timeoutInterval = 10;
    
    console.log("Отправляю запрос на получение метрик...");
    const metricsResponse = await metricsRequest.loadJSON();
    console.log("Ответ метрик:", metricsResponse);
    
    return {
        subscriptions: subscriptionsResponse.subscriptions || [],
        metrics: metricsResponse || {}
    };
}

// ========== УСПЕШНЫЙ ВИДЖЕТ ==========
async function setupSuccessWidget(widget, data) {
    const subscriptions = data.subscriptions;
    const metrics = data.metrics;
    
    // Заголовок
    const titleStack = widget.addStack();
    titleStack.layoutHorizontally();
    
    const titleText = titleStack.addText("📊 Активные абонементы");
    titleText.font = Font.boldSystemFont(14);
    titleText.textColor = Color.white();
    
    const spacer = titleStack.addSpacer();
    
    // Время обновления
    const timeText = titleStack.addText(formatTime(new Date()));
    timeText.font = Font.systemFont(10);
    timeText.textColor = new Color("#94A3B8");
    
    widget.addSpacer(8);
    
    // Общие метрики (если есть)
    if (metrics.planned !== undefined) {
        const metricsStack = widget.addStack();
        metricsStack.layoutHorizontally();
        metricsStack.spacing = 12;
        
        // Запланировано
        const plannedStack = metricsStack.addStack();
        plannedStack.layoutVertically();
        const plannedLabel = plannedStack.addText("Запланировано");
        plannedLabel.font = Font.systemFont(9);
        plannedLabel.textColor = new Color("#94A3B8");
        const plannedValue = plannedStack.addText(`${metrics.planned || 0}`);
        plannedValue.font = Font.boldSystemFont(12);
        plannedValue.textColor = Color.white();
        
        // Посещено
        const attendedStack = metricsStack.addStack();
        attendedStack.layoutVertically();
        const attendedLabel = attendedStack.addText("Посещено");
        attendedLabel.font = Font.systemFont(9);
        attendedLabel.textColor = new Color("#94A3B8");
        const attendedValue = attendedStack.addText(`${metrics.attended || 0}`);
        attendedValue.font = Font.boldSystemFont(12);
        attendedValue.textColor = new Color("#00C1FF");
        
        // Посещаемость
        const rateStack = metricsStack.addStack();
        rateStack.layoutVertically();
        const rateLabel = rateStack.addText("Посещаемость");
        rateLabel.font = Font.systemFont(9);
        rateLabel.textColor = new Color("#94A3B8");
        const rateValue = rateStack.addText(`${Math.round(metrics.attendance_rate || 0)}%`);
        rateValue.font = Font.boldSystemFont(12);
        rateValue.textColor = new Color("#FDDD00");
        
        widget.addSpacer(10);
    }
    
    // Абонементы (показываем до 3-4 штук в зависимости от размера виджета)
    const maxSubscriptions = config.widgetFamily === "large" ? 4 : 3;
    const displaySubscriptions = subscriptions.slice(0, maxSubscriptions);
    
    for (let i = 0; i < displaySubscriptions.length; i++) {
        const sub = displaySubscriptions[i];
        await addSubscriptionRow(widget, sub);
        
        if (i < displaySubscriptions.length - 1) {
            widget.addSpacer(6);
        }
    }
    
    // Показываем количество оставшихся абонементов
    if (subscriptions.length > maxSubscriptions) {
        widget.addSpacer(4);
        const moreText = widget.addText(`+${subscriptions.length - maxSubscriptions} еще...`);
        moreText.font = Font.systemFont(10);
        moreText.textColor = new Color("#94A3B8");
        moreText.centerAlignText();
    }
}

// ========== СТРОКА АБОНЕМЕНТА ==========
async function addSubscriptionRow(widget, subscription) {
    const subStack = widget.addStack();
    subStack.layoutVertically();
    subStack.backgroundColor = new Color("#2A344A", 0.5);
    subStack.cornerRadius = 8;
    subStack.setPadding(8, 10, 8, 10);
    
    // Название абонемента
    const nameStack = subStack.addStack();
    nameStack.layoutHorizontally();
    
    const nameText = nameStack.addText(subscription.name || "Неизвестный абонемент");
    nameText.font = Font.boldSystemFont(11);
    nameText.textColor = Color.white();
    nameText.lineLimit = 1;
    
    const nameSpacer = nameStack.addSpacer();
    
    // Прогресс в процентах
    const progressPercent = Math.round(subscription.progress_percent || 0);
    const percentText = nameStack.addText(`${progressPercent}%`);
    percentText.font = Font.boldSystemFont(11);
    percentText.textColor = new Color("#FDDD00");
    
    subStack.addSpacer(4);
    
    // Прогресс бар
    const progressStack = subStack.addStack();
    progressStack.layoutHorizontally();
    progressStack.spacing = 2;
    
    const totalLessons = subscription.total_lessons || 0;
    const completedLessons = subscription.completed_lessons || 0;
    
    // Создаем сегменты прогресс-бара
    const maxSegments = 10; // Максимум сегментов для отображения
    const segmentsToShow = Math.min(totalLessons, maxSegments);
    
    if (segmentsToShow > 0) {
        const segmentWidth = Math.floor(segmentsToShow > 0 ? segmentsToShow : 1);
        
        for (let i = 0; i < segmentsToShow; i++) {
            const segment = progressStack.addStack();
            segment.size = new Size(segmentWidth * 2, 4);
            segment.cornerRadius = 2;
            
            // Определяем цвет сегмента
            if (i < completedLessons) {
                // Посещенные занятия - градиент от голубого к фиолетовому
                const ratio = segmentsToShow > 1 ? i / (segmentsToShow - 1) : 1;
                const r = Math.round(0 * (1 - ratio) + 106 * ratio);
                const g = Math.round(193 * (1 - ratio) + 0 * ratio);
                const b = Math.round(255 * (1 - ratio) + 255 * ratio);
                segment.backgroundColor = new Color(`rgb(${r}, ${g}, ${b})`);
            } else {
                // Незавершенные занятия
                segment.backgroundColor = new Color("#4B5563");
            }
        }
    }
    
    subStack.addSpacer(3);
    
    // Детали
    const detailsStack = subStack.addStack();
    detailsStack.layoutHorizontally();
    detailsStack.spacing = 12;
    
    // Прошло/Всего
    const completedText = detailsStack.addText(`Прошло: ${completedLessons}/${totalLessons}`);
    completedText.font = Font.systemFont(9);
    completedText.textColor = new Color("#94A3B8");
    
    const detailsSpacer = detailsStack.addSpacer();
    
    // Пропущено в этом месяце
    const missedThisMonth = subscription.missed_this_month || 0;
    if (missedThisMonth > 0) {
        const missedText = detailsStack.addText(`Пропущено: ${missedThisMonth}`);
        missedText.font = Font.systemFont(9);
        missedText.textColor = new Color("#FD7000");
    }
}

// ========== ПУСТОЙ ВИДЖЕТ ==========
function setupEmptyWidget(widget) {
    widget.addSpacer();
    
    const titleText = widget.addText("📊 Активные абонементы");
    titleText.font = Font.boldSystemFont(16);
    titleText.textColor = Color.white();
    titleText.centerAlignText();
    
    widget.addSpacer(8);
    
    const emptyText = widget.addText("Нет активных абонементов");
    emptyText.font = Font.systemFont(14);
    emptyText.textColor = new Color("#94A3B8");
    emptyText.centerAlignText();
    
    widget.addSpacer();
}

// ========== ВИДЖЕТ ОШИБКИ ==========
function setupErrorWidget(widget, errorMessage) {
    widget.addSpacer();
    
    const titleText = widget.addText("❌ Ошибка");
    titleText.font = Font.boldSystemFont(16);
    titleText.textColor = new Color("#FF6B6B");
    titleText.centerAlignText();
    
    widget.addSpacer(8);
    
    const errorText = widget.addText(errorMessage || "Не удалось загрузить данные");
    errorText.font = Font.systemFont(12);
    errorText.textColor = new Color("#94A3B8");
    errorText.centerAlignText();
    
    widget.addSpacer(4);
    
    const retryText = widget.addText("Попробуйте позже");
    retryText.font = Font.systemFont(10);
    retryText.textColor = new Color("#6B7280");
    retryText.centerAlignText();
    
    widget.addSpacer();
}

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
function formatTime(date) {
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    return `${hours}:${minutes}`;
}

// ========== ЗАПУСК ==========
// Проверяем, запущен ли скрипт как виджет или в приложении
if (config.runsInWidget) {
    // Запуск как виджет
    const widget = await createWidget();
    Script.setWidget(widget);
} else {
    // Запуск в приложении для предварительного просмотра
    const widget = await createWidget();
    
    if (config.widgetFamily === "medium") {
        widget.presentMedium();
    } else if (config.widgetFamily === "large") {
        widget.presentLarge();
    } else {
        widget.presentSmall();
    }
}

Script.complete();
