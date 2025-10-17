/**
 * 📱 iOS SCRIPTABLE WIDGET - УПРОЩЕННАЯ ВЕРСИЯ
 * 
 * Компактный виджет для отображения основных метрик
 * Подходит для маленького размера виджета
 */

// ========== НАСТРОЙКИ ==========
const API_BASE_URL = "https://web-production-547b.up.railway.app"; // Ваш Railway URL

// ========== ОСНОВНАЯ ФУНКЦИЯ ==========
async function createWidget() {
    const widget = new ListWidget();
    widget.backgroundColor = new Color("#151634");
    
    try {
        const data = await fetchMetrics();
        await setupMetricsWidget(widget, data);
    } catch (error) {
        setupErrorWidget(widget);
    }
    
    return widget;
}

// ========== ПОЛУЧЕНИЕ МЕТРИК ==========
async function fetchMetrics() {
    const url = `${API_BASE_URL}/api/metrics?student=Все`;
    const request = new Request(url);
    request.timeoutInterval = 8;
    
    const response = await request.loadJSON();
    return response;
}

// ========== ВИДЖЕТ МЕТРИК ==========
async function setupMetricsWidget(widget, metrics) {
    // Заголовок
    const titleText = widget.addText("📊 Студия");
    titleText.font = Font.boldSystemFont(14);
    titleText.textColor = Color.white();
    titleText.centerAlignText();
    
    widget.addSpacer(8);
    
    // Основные метрики в сетке 2x2
    const topStack = widget.addStack();
    topStack.layoutHorizontally();
    topStack.spacing = 12;
    
    // Запланировано
    const plannedStack = topStack.addStack();
    plannedStack.layoutVertically();
    const plannedLabel = plannedStack.addText("Запланировано");
    plannedLabel.font = Font.systemFont(9);
    plannedLabel.textColor = new Color("#94A3B8");
    plannedLabel.centerAlignText();
    const plannedValue = plannedStack.addText(`${metrics.planned || 0}`);
    plannedValue.font = Font.boldSystemFont(18);
    plannedValue.textColor = Color.white();
    plannedValue.centerAlignText();
    
    // Посещено
    const attendedStack = topStack.addStack();
    attendedStack.layoutVertically();
    const attendedLabel = attendedStack.addText("Посещено");
    attendedLabel.font = Font.systemFont(9);
    attendedLabel.textColor = new Color("#94A3B8");
    attendedLabel.centerAlignText();
    const attendedValue = attendedStack.addText(`${metrics.attended || 0}`);
    attendedValue.font = Font.boldSystemFont(18);
    attendedValue.textColor = new Color("#00C1FF");
    attendedValue.centerAlignText();
    
    widget.addSpacer(8);
    
    const bottomStack = widget.addStack();
    bottomStack.layoutHorizontally();
    bottomStack.spacing = 12;
    
    // Пропущено
    const missedStack = bottomStack.addStack();
    missedStack.layoutVertically();
    const missedLabel = missedStack.addText("Пропущено");
    missedLabel.font = Font.systemFont(9);
    missedLabel.textColor = new Color("#94A3B8");
    missedLabel.centerAlignText();
    const missedValue = missedStack.addText(`${metrics.missed || 0}`);
    missedValue.font = Font.boldSystemFont(18);
    missedValue.textColor = new Color("#FD7000");
    missedValue.centerAlignText();
    
    // Посещаемость
    const rateStack = bottomStack.addStack();
    rateStack.layoutVertically();
    const rateLabel = rateStack.addText("Посещаемость");
    rateLabel.font = Font.systemFont(9);
    rateLabel.textColor = new Color("#94A3B8");
    rateLabel.centerAlignText();
    const rateValue = rateStack.addText(`${Math.round(metrics.attendance_rate || 0)}%`);
    rateValue.font = Font.boldSystemFont(18);
    rateValue.textColor = new Color("#FDDD00");
    rateValue.centerAlignText();
    
    widget.addSpacer(4);
    
    // Время обновления
    const timeText = widget.addText(formatTime(new Date()));
    timeText.font = Font.systemFont(8);
    timeText.textColor = new Color("#6B7280");
    timeText.centerAlignText();
}

// ========== ВИДЖЕТ ОШИБКИ ==========
function setupErrorWidget(widget) {
    widget.addSpacer();
    
    const errorText = widget.addText("❌");
    errorText.font = Font.systemFont(32);
    errorText.centerAlignText();
    
    const messageText = widget.addText("Ошибка загрузки");
    messageText.font = Font.systemFont(12);
    messageText.textColor = new Color("#94A3B8");
    messageText.centerAlignText();
    
    widget.addSpacer();
}

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
function formatTime(date) {
    const hours = date.getHours().toString().padStart(2, '0');
    const minutes = date.getMinutes().toString().padStart(2, '0');
    return `${hours}:${minutes}`;
}

// ========== ЗАПУСК ==========
if (config.runsInWidget) {
    const widget = await createWidget();
    Script.setWidget(widget);
} else {
    const widget = await createWidget();
    widget.presentSmall();
}

Script.complete();
