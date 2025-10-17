/**
 * 📱 iOS SCRIPTABLE WIDGET - ФИНАНСОВЫЕ МЕТРИКИ
 * 
 * Виджет для отображения бюджетных показателей студии
 * Показывает доходы, планы и статистику по оплатам
 */

// ========== НАСТРОЙКИ ==========
const API_BASE_URL = "https://web-production-547b.up.railway.app"; // Ваш Railway URL

// ========== ОСНОВНАЯ ФУНКЦИЯ ==========
async function createWidget() {
    const widget = new ListWidget();
    widget.backgroundColor = new Color("#151634");
    
    try {
        const metrics = await fetchMetrics();
        await setupFinancialWidget(widget, metrics);
    } catch (error) {
        setupErrorWidget(widget, error.message);
    }
    
    return widget;
}

// ========== ПОЛУЧЕНИЕ МЕТРИК ==========
async function fetchMetrics() {
    const url = `${API_BASE_URL}/api/metrics?student=Все`;
    const request = new Request(url);
    request.timeoutInterval = 10;
    
    const response = await request.loadJSON();
    return response;
}

// ========== ФИНАНСОВЫЙ ВИДЖЕТ ==========
async function setupFinancialWidget(widget, metrics) {
    // Заголовок
    const headerStack = widget.addStack();
    headerStack.layoutHorizontally();
    
    const titleText = headerStack.addText("💰 Финансы");
    titleText.font = Font.boldSystemFont(14);
    titleText.textColor = Color.white();
    
    const spacer = headerStack.addSpacer();
    
    const timeText = headerStack.addText(formatTime(new Date()));
    timeText.font = Font.systemFont(10);
    timeText.textColor = new Color("#94A3B8");
    
    widget.addSpacer(10);
    
    // Месячные показатели
    const monthlyStack = widget.addStack();
    monthlyStack.layoutVertically();
    monthlyStack.backgroundColor = new Color("#2A344A", 0.3);
    monthlyStack.cornerRadius = 8;
    monthlyStack.setPadding(8, 10, 8, 10);
    
    const monthlyTitle = monthlyStack.addText("📅 Месяц");
    monthlyTitle.font = Font.boldSystemFont(12);
    monthlyTitle.textColor = new Color("#FDDD00");
    
    monthlyStack.addSpacer(4);
    
    const monthlyMetricsStack = monthlyStack.addStack();
    monthlyMetricsStack.layoutHorizontally();
    monthlyMetricsStack.spacing = 15;
    
    // Бюджет месяца
    const budgetMonthStack = monthlyMetricsStack.addStack();
    budgetMonthStack.layoutVertically();
    const budgetMonthLabel = budgetMonthStack.addText("Бюджет");
    budgetMonthLabel.font = Font.systemFont(9);
    budgetMonthLabel.textColor = new Color("#94A3B8");
    const budgetMonthValue = budgetMonthStack.addText(formatCurrency(metrics.budget_month || 0));
    budgetMonthValue.font = Font.boldSystemFont(13);
    budgetMonthValue.textColor = Color.white();
    
    // Оплачено месяца
    const paidMonthStack = monthlyMetricsStack.addStack();
    paidMonthStack.layoutVertically();
    const paidMonthLabel = paidMonthStack.addText("Оплачено");
    paidMonthLabel.font = Font.systemFont(9);
    paidMonthLabel.textColor = new Color("#94A3B8");
    const paidMonthValue = paidMonthStack.addText(formatCurrency(metrics.paid_month || 0));
    paidMonthValue.font = Font.boldSystemFont(13);
    paidMonthValue.textColor = new Color("#00C1FF");
    
    widget.addSpacer(8);
    
    // Недельные показатели
    const weeklyStack = widget.addStack();
    weeklyStack.layoutVertically();
    weeklyStack.backgroundColor = new Color("#2A344A", 0.3);
    weeklyStack.cornerRadius = 8;
    weeklyStack.setPadding(8, 10, 8, 10);
    
    const weeklyTitle = weeklyStack.addText("📊 Неделя");
    weeklyTitle.font = Font.boldSystemFont(12);
    weeklyTitle.textColor = new Color("#00C1FF");
    
    weeklyStack.addSpacer(4);
    
    const weeklyMetricsStack = weeklyStack.addStack();
    weeklyMetricsStack.layoutHorizontally();
    weeklyMetricsStack.spacing = 15;
    
    // Бюджет недели
    const budgetWeekStack = weeklyMetricsStack.addStack();
    budgetWeekStack.layoutVertically();
    const budgetWeekLabel = budgetWeekStack.addText("Бюджет");
    budgetWeekLabel.font = Font.systemFont(9);
    budgetWeekLabel.textColor = new Color("#94A3B8");
    const budgetWeekValue = budgetWeekStack.addText(formatCurrency(metrics.budget_week || 0));
    budgetWeekValue.font = Font.boldSystemFont(13);
    budgetWeekValue.textColor = Color.white();
    
    // Оплачено недели
    const paidWeekStack = weeklyMetricsStack.addStack();
    paidWeekStack.layoutVertically();
    const paidWeekLabel = paidWeekStack.addText("Оплачено");
    paidWeekLabel.font = Font.systemFont(9);
    paidWeekLabel.textColor = new Color("#94A3B8");
    const paidWeekValue = paidWeekStack.addText(formatCurrency(metrics.paid_week || 0));
    paidWeekValue.font = Font.boldSystemFont(13);
    paidWeekValue.textColor = new Color("#00C1FF");
    
    widget.addSpacer(8);
    
    // Процент выполнения бюджета
    if (metrics.budget_month > 0) {
        const completionRate = Math.round((metrics.paid_month / metrics.budget_month) * 100);
        
        const completionStack = widget.addStack();
        completionStack.layoutHorizontally();
        completionStack.spacing = 8;
        
        const completionLabel = completionStack.addText("Выполнение бюджета:");
        completionLabel.font = Font.systemFont(11);
        completionLabel.textColor = new Color("#94A3B8");
        
        const completionSpacer = completionStack.addSpacer();
        
        const completionValue = completionStack.addText(`${completionRate}%`);
        completionValue.font = Font.boldSystemFont(11);
        
        // Цвет в зависимости от процента выполнения
        if (completionRate >= 90) {
            completionValue.textColor = new Color("#10B981"); // Зеленый
        } else if (completionRate >= 70) {
            completionValue.textColor = new Color("#FDDD00"); // Желтый
        } else {
            completionValue.textColor = new Color("#FD7000"); // Оранжевый
        }
        
        widget.addSpacer(4);
        
        // Прогресс-бар выполнения бюджета
        const progressStack = widget.addStack();
        progressStack.layoutHorizontally();
        progressStack.spacing = 1;
        
        const totalSegments = 20;
        const filledSegments = Math.round((completionRate / 100) * totalSegments);
        
        for (let i = 0; i < totalSegments; i++) {
            const segment = progressStack.addStack();
            segment.size = new Size(3, 4);
            segment.cornerRadius = 1;
            
            if (i < filledSegments) {
                if (completionRate >= 90) {
                    segment.backgroundColor = new Color("#10B981");
                } else if (completionRate >= 70) {
                    segment.backgroundColor = new Color("#FDDD00");
                } else {
                    segment.backgroundColor = new Color("#FD7000");
                }
            } else {
                segment.backgroundColor = new Color("#4B5563");
            }
        }
    }
    
    // Дополнительные метрики посещаемости
    if (config.widgetFamily === "large") {
        widget.addSpacer(8);
        
        const attendanceStack = widget.addStack();
        attendanceStack.layoutHorizontally();
        attendanceStack.spacing = 15;
        
        // Посещаемость
        const attendanceRateStack = attendanceStack.addStack();
        attendanceRateStack.layoutVertically();
        const attendanceLabel = attendanceRateStack.addText("Посещаемость");
        attendanceLabel.font = Font.systemFont(9);
        attendanceLabel.textColor = new Color("#94A3B8");
        attendanceLabel.centerAlignText();
        const attendanceValue = attendanceRateStack.addText(`${Math.round(metrics.attendance_rate || 0)}%`);
        attendanceValue.font = Font.boldSystemFont(16);
        attendanceValue.textColor = new Color("#FDDD00");
        attendanceValue.centerAlignText();
        
        // Запланировано занятий
        const plannedStack = attendanceStack.addStack();
        plannedStack.layoutVertically();
        const plannedLabel = plannedStack.addText("Запланировано");
        plannedLabel.font = Font.systemFont(9);
        plannedLabel.textColor = new Color("#94A3B8");
        plannedLabel.centerAlignText();
        const plannedValue = plannedStack.addText(`${metrics.planned || 0}`);
        plannedValue.font = Font.boldSystemFont(16);
        plannedValue.textColor = Color.white();
        plannedValue.centerAlignText();
    }
}

// ========== ВИДЖЕТ ОШИБКИ ==========
function setupErrorWidget(widget, errorMessage) {
    widget.addSpacer();
    
    const titleText = widget.addText("💰 Финансы");
    titleText.font = Font.boldSystemFont(16);
    titleText.textColor = Color.white();
    titleText.centerAlignText();
    
    widget.addSpacer(8);
    
    const errorText = widget.addText("❌ Ошибка загрузки");
    errorText.font = Font.systemFont(12);
    errorText.textColor = new Color("#FF6B6B");
    errorText.centerAlignText();
    
    widget.addSpacer();
}

// ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
function formatCurrency(value) {
    if (value === 0) return "0 ₽";
    
    // Форматируем число с разделителями тысяч
    const formatted = Math.round(value).toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
    return `${formatted} ₽`;
}

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
    
    if (config.widgetFamily === "medium") {
        widget.presentMedium();
    } else if (config.widgetFamily === "large") {
        widget.presentLarge();
    } else {
        widget.presentSmall();
    }
}

Script.complete();
