/**
 * 📊 INTEGRATED DASHBOARD JAVASCRIPT
 * Интегрированный дашборд с данными из Google Sheets API
 */

class Dashboard {
    constructor() {
        this.apiBaseUrl = window.location.origin;
        this.refreshInterval = null;
        this.isLoading = false;
        this.currentDisplayDate = new Date();
        this.dashboardData = null;
        this.demoEvents = [];
        this.demoSubscriptions = [];
        
        this.init();
    }
    
    init() {
        console.log('🚀 Инициализация дашборда');
        
        // Инициализация Telegram Mini App
        this.initTelegramWebApp();
        
        // Привязка событий
        this.bindEvents();
        
        // Первоначальная загрузка данных
        this.loadDashboardData();
    }
    
    initTelegramWebApp() {
        if (window.Telegram && window.Telegram.WebApp) {
            const tg = window.Telegram.WebApp;
            
            // Настройка темы
            document.documentElement.style.setProperty('--tg-theme-bg-color', tg.themeParams.bg_color || '#151634');
            document.documentElement.style.setProperty('--tg-theme-text-color', tg.themeParams.text_color || '#ffffff');
            
            // Уведомляем Telegram что приложение готово
            tg.ready();
            
            // Расширяем приложение на весь экран
            tg.expand();
            
            console.log('✅ Telegram Mini App инициализирован');
        } else {
            console.log('ℹ️ Запуск вне Telegram Mini App');
        }
    }
    
    bindEvents() {
        // Кнопка обновления убрана - данные загружаются только при открытии

        // Навигация календаря
        const prevBtn = document.getElementById('prev-btn');
        const nextBtn = document.getElementById('next-btn');
        if (prevBtn) {
            prevBtn.addEventListener('click', () => {
                this.currentDisplayDate.setDate(this.currentDisplayDate.getDate() - 7);
                this.updateDashboard();
            });
        }
        if (nextBtn) {
            nextBtn.addEventListener('click', () => {
                this.currentDisplayDate.setDate(this.currentDisplayDate.getDate() + 7);
                this.updateDashboard();
            });
        }

        // Фильтр студентов будет настроен в populateStudentFilter()

        // Модальное окно
        const modal = document.getElementById('event-modal');
        const modalClose = document.getElementById('modal-close');
        if (modalClose) {
            modalClose.addEventListener('click', () => modal.classList.add('hidden'));
        }
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    modal.classList.add('hidden');
                }
            });
        }
        
        // Обработка видимости страницы
        document.addEventListener('visibilitychange', () => {
            if (!document.hidden) {
                console.log('📱 Страница стала видимой, обновляем данные');
                this.loadDashboardData();
            }
        });
    }

    async loadDashboardData() {
        if (this.isLoading) {
            console.log('⏳ Загрузка уже в процессе');
            return;
        }
        
        this.isLoading = true;
        this.showPreloader();
        
        try {
            console.log('📊 Загрузка данных дашборда...');
            
            // Получаем текущий фильтр (по умолчанию "Все")
            const currentFilter = 'Все';
            
            // Загружаем все данные параллельно
            const [filtersResponse, metricsResponse, subscriptionsResponse, calendarResponse] = await Promise.all([
                fetch(`${this.apiBaseUrl}/api/filters`),
                fetch(`${this.apiBaseUrl}/api/metrics?student=${encodeURIComponent(currentFilter)}`),
                fetch(`${this.apiBaseUrl}/api/subscriptions?student=${encodeURIComponent(currentFilter)}`),
                fetch(`${this.apiBaseUrl}/api/calendar?student=${encodeURIComponent(currentFilter)}`)
            ]);
            
            // Проверяем ответы
            if (!filtersResponse.ok || !metricsResponse.ok || !subscriptionsResponse.ok || !calendarResponse.ok) {
                throw new Error('Ошибка загрузки данных с сервера');
            }
            
            // Парсим JSON
            const filters = await filtersResponse.json();
            const metrics = await metricsResponse.json();
            const subscriptions = await subscriptionsResponse.json();
            const calendar = await calendarResponse.json();
            
            console.log('✅ Данные загружены успешно');
            console.log('📊 Metrics:', metrics);
            console.log('📈 Subscriptions:', subscriptions);
            
            // Сохраняем данные
            this.dashboardData = {
                filters: filters.filters || ['Все'],
                metrics: metrics,
                subscriptions: subscriptions.subscriptions || [],
                calendar: calendar.events || []
            };
            
            console.log('💾 Сохранено в dashboardData:', this.dashboardData);
            
            // Преобразуем данные для интерфейса
            this.convertApiDataToEvents();
            
            // ВАЖНО: Обновляем метрики сразу после загрузки данных
            console.log('🔄 Вызов updateApiMetrics() напрямую');
            this.updateApiMetrics();
            
            // Обновляем остальные части интерфейса
            console.log('🔄 Вызов updateDashboard()');
            this.updateDashboard();
            
            console.log('✅ Дашборд полностью обновлен');
            
        } catch (error) {
            console.error('❌ Ошибка загрузки данных:', error);
            // Используем демо-данные при ошибке
            this.loadDemoData();
        } finally {
            this.isLoading = false;
            this.hidePreloader();
        }
    }

    getCurrentStudentFilter() {
        const filterSelect = document.getElementById('studentFilter');
        return filterSelect ? filterSelect.value : 'Все';
    }

    convertApiDataToEvents() {
        // Преобразуем данные из API в формат событий для календаря
        this.demoEvents = [];
        this.demoSubscriptions = [];
        
        if (!this.dashboardData) return;
        
        // Преобразуем события календаря
        if (this.dashboardData.calendar && Array.isArray(this.dashboardData.calendar)) {
            this.demoEvents = this.dashboardData.calendar.map(event => ({
                date: event.date,
                time: event.time || '09:00',
                duration: 1.5, // По умолчанию 1.5 часа
                title: event.title || `${event.circle || 'Занятие'} - ${event.child || 'Ученик'}`,
                status: this.mapStatus(event.status || event.attendance)
            }));
        }
        
        // Преобразуем данные абонементов
        if (this.dashboardData.subscriptions && Array.isArray(this.dashboardData.subscriptions)) {
            this.demoSubscriptions = this.dashboardData.subscriptions.map(sub => ({
                id: sub.id || 'unknown',
                title: sub.name || 'Неизвестный абонемент',
                totalLessons: sub.total_lessons || 0,
                completedLessons: sub.completed_lessons || 0,
                remainingLessons: sub.remaining_lessons || 0,
                progressPercent: sub.progress_percent || 0,
                missedThisMonth: sub.missed_this_month || 0,
                lessons: sub.lessons || []
            }));
        }
    }

    loadDemoData() {
        // Демо-данные как fallback
        this.demoEvents = [
            { date: '2025-10-15', time: '10:00', duration: 1.5, title: 'Футбол - Марк', status: 'Посещение' },
            { date: '2025-10-16', time: '11:00', duration: 1, title: 'Логопед - Алиса', status: 'Посещение' },
            { date: '2025-10-17', time: '12:30', duration: 2, title: 'Рисование - Майя', status: 'Запланировано' },
        ];
        
        this.demoSubscriptions = [
            { 
                id: 'demo1',
                title: 'Футбол - Марк', 
                lessons: [
                    { date: '2025-10-15', startTime: '10:00', endTime: '11:30', status: 'Посещение'},
                    { date: '2025-10-17', startTime: '10:00', endTime: '11:30', status: 'Запланировано'},
                ]
            }
        ];
        
        // Фильтр убран
        this.updateDashboard();
    }

    calculateDuration(startTime, endTime) {
        if (!startTime || !endTime) return 1;
        
        const [startHour, startMin] = startTime.split(':').map(Number);
        const [endHour, endMin] = endTime.split(':').map(Number);
        
        const startMinutes = startHour * 60 + startMin;
        const endMinutes = endHour * 60 + endMin;
        
        return (endMinutes - startMinutes) / 60;
    }

    mapStatus(status) {
        const statusMap = {
            'Присутствовал': 'Посещение',
            'Отсутствовал': 'Пропуск',
            'Запланировано': 'Запланировано',
            '✔️': 'Посещение',
            '✖️': 'Пропуск'
        };
        return statusMap[status] || 'Запланировано';
    }
    
    updateDashboard() {
        // Календарь убран - используется Google Calendar
        // Обновляем только прогресс абонементов
        this.renderSubscriptionsProgress();
        // Метрики обновляются через updateApiMetrics() после загрузки данных
    }

    updateApiMetrics() {
        // Обновляем метрики из API данных
        if (!this.dashboardData || !this.dashboardData.metrics) {
            console.log('⚠️ Нет данных метрик');
            return;
        }
        
        const metrics = this.dashboardData.metrics;
        console.log('📊 Обновление метрик:', metrics);
        
        // Обновляем основные метрики (правильные ID из HTML)
        this.updateElement('kpi-scheduled', metrics.planned || 0);
        this.updateElement('kpi-attended', metrics.attended || 0);
        this.updateElement('kpi-skipped', metrics.missed || 0);
        this.updateElement('kpi-rate', `${Math.round(metrics.attendance_rate || 0)}%`);
        
        // Форматируем валюту
        const formatCurrency = (value) => new Intl.NumberFormat('ru-RU').format(value || 0);
        
        // Обновляем бюджетные метрики (правильные ID из HTML)
        this.updateElement('kpi-budget-month', formatCurrency(metrics.budget_month));
        this.updateElement('kpi-paid-month', formatCurrency(metrics.paid_month));
        this.updateElement('kpi-budget-week', formatCurrency(metrics.budget_week));
        this.updateElement('kpi-paid-week', formatCurrency(metrics.paid_week));
        
        console.log('✅ Все метрики обновлены успешно');
    }
    
    updateElement(id, value) {
        const element = document.getElementById(id);
        if (element) {
            element.textContent = value;
            console.log(`✅ Обновлен элемент #${id} = ${value}`);
        } else {
            console.error(`❌ Элемент #${id} не найден!`);
        }
    }

    populateStudentFilter() {
        const studentFilter = document.getElementById('student-filter');
        if (!studentFilter) return;
        
        studentFilter.innerHTML = '';
        
        // Используем данные из API
        const filters = this.dashboardData?.filters || ['Все'];
        
        filters.forEach(filterOption => {
            const option = document.createElement('option');
            option.value = filterOption;
            option.textContent = filterOption;
            studentFilter.appendChild(option);
        });
        
        // Добавляем обработчик изменения фильтра
        studentFilter.addEventListener('change', () => {
            console.log('🔄 Изменен фильтр студента:', studentFilter.value);
            this.loadDashboardData(); // Перезагружаем данные с новым фильтром
        });
    }

    renderCalendar() {
        const calendarBody = document.getElementById('calendar-body');
        if (!calendarBody) return;
        
        calendarBody.innerHTML = '';
        this.renderWeekView(calendarBody);
    }

    renderWeekView(container) {
        const hourHeight = 60;
        container.innerHTML = `
            <div class="flex text-xs text-center text-gray-400 mb-2 py-2 border-y border-gray-700">
                <div class="w-14 flex-shrink-0"></div>
                <div id="week-header-grid" class="flex-grow grid grid-cols-7"></div>
            </div>
            <div class="overflow-x-auto">
                <div style="min-width: 800px;">
                    <div class="week-container">
                        <div class="timeline"></div>
                        <div class="week-grid"></div>
                    </div>
                </div>
            </div>
        `;
        
        const weekGrid = container.querySelector('.week-grid');
        const timeline = container.querySelector('.timeline');
        const weekHeaderGrid = document.getElementById('week-header-grid');

        // Создаем временную шкалу
        for (let i = 9; i <= 21; i++) {
            const hourMarker = document.createElement('div');
            hourMarker.classList.add('hour-marker');
            hourMarker.textContent = `${String(i).padStart(2, '0')}:00`;
            timeline.appendChild(hourMarker);
        }

        // Вычисляем неделю
        const dayOfWeek = (this.currentDisplayDate.getDay() + 6) % 7;
        const startDate = new Date(this.currentDisplayDate.getFullYear(), this.currentDisplayDate.getMonth(), this.currentDisplayDate.getDate() - dayOfWeek);
        const endDate = new Date(startDate);
        endDate.setDate(startDate.getDate() + 6);
        
        this.updateWeeklyKPIs(startDate, endDate);
        
        const options = { month: 'long', day: 'numeric' };
        const calendarTitle = document.getElementById('calendar-title');
        if (calendarTitle) {
            calendarTitle.textContent = `${startDate.toLocaleDateString('ru', options)} - ${endDate.toLocaleDateString('ru', options)}`;
        }

        // Создаем дни недели
        for (let i = 0; i < 7; i++) {
            const dayDate = new Date(startDate);
            dayDate.setDate(startDate.getDate() + i);
            
            const dayHeader = document.createElement('div');
            dayHeader.innerHTML = `<span class="font-bold">${dayDate.toLocaleDateString('ru', { weekday: 'short' })}</span> <span class="day-number ${this.isToday(dayDate) ? 'today' : ''}">${dayDate.getDate()}</span>`;
            weekHeaderGrid.appendChild(dayHeader);

            const dayColumn = document.createElement('div');
            dayColumn.classList.add('calendar-day');
            
            const dateString = `${dayDate.getFullYear()}-${String(dayDate.getMonth() + 1).padStart(2, '0')}-${String(dayDate.getDate()).padStart(2, '0')}`;
            
            const eventsForDay = this.demoEvents.filter(e => {
                if (e.date !== dateString) return false;
                const [h] = e.time.split(':');
                const hour = parseInt(h);
                return hour >= 9 && hour < 22;
            });
            
            eventsForDay.forEach(event => {
                const eventEl = document.createElement('div');
                eventEl.classList.add('event-pill');
                const [hours, minutes] = event.time.split(':');
                
                const top = ((parseInt(hours) - 9) + parseInt(minutes)/60) * hourHeight;
                const height = event.duration * hourHeight;

                eventEl.style.top = `${top}px`;
                eventEl.style.height = `${height - 4}px`;
                 
                if (event.status === 'Посещение') eventEl.classList.add('event-attended');
                else if (event.status === 'Пропуск') eventEl.classList.add('event-skipped');
                else eventEl.classList.add('event-scheduled');
                
                eventEl.innerHTML = `<div class="font-bold">${event.time}</div><div>${event.title}</div>`;
                
                eventEl.addEventListener('click', () => {
                    this.showEventModal(event);
                });

                dayColumn.appendChild(eventEl);
            });

            weekGrid.appendChild(dayColumn);
        }
    }

    renderSubscriptionsProgress() {
        const container = document.getElementById('subscriptions-progress-container');
        const tooltip = document.getElementById('tooltip');
        if (!container) return;
        
        container.innerHTML = '';

        // Используем данные абонементов из API
        const subscriptions = this.dashboardData?.subscriptions || this.demoSubscriptions;

        subscriptions.forEach(sub => {
            const total = sub.totalLessons || sub.lessons?.length || 0;
            const completed = sub.completedLessons || 0;
            const remaining = sub.remainingLessons || 0;
            const progressPercentage = sub.progressPercent || (total > 0 ? (completed / total) * 100 : 0);
            const missed = sub.missedThisMonth || 0;
            
            let progressBarHTML = '';
            const lessons = sub.lessons || [];
            
            // Создаем сегменты для всех запланированных занятий
            for (let i = 0; i < total; i++) {
                const lesson = lessons[i];
                let segmentClass = 'bg-gray-700';
                let segmentStyle = '';
                
                if (lesson) {
                    if (lesson.status === 'Посещение' || lesson.attendance === 'Посещение') {
                        const startColor = [0, 193, 255];
                        const endColor = [106, 0, 255];
                        const ratio = total > 1 ? i / (total - 1) : 1;
                        const r = Math.round(startColor[0] * (1 - ratio) + endColor[0] * ratio);
                        const g = Math.round(startColor[1] * (1 - ratio) + endColor[1] * ratio);
                        const b = Math.round(startColor[2] * (1 - ratio) + endColor[2] * ratio);
                        segmentStyle = `background-color: rgb(${r}, ${g}, ${b});`;
                    } else if (lesson.status === 'Пропуск' || lesson.attendance === 'Пропуск') {
                        segmentClass = 'bg-orange-500';
                    }
                }
                
                progressBarHTML += `<div class="progress-segment ${segmentClass}" style="${segmentStyle}"
                    data-id="${sub.id}"
                    data-date="${lesson?.date || ''}"
                    data-status="${lesson?.status || lesson?.attendance || 'Запланировано'}"
                    data-start="${lesson?.time || lesson?.start_time || ''}"
                    data-end="${lesson?.end_time || ''}"
                ></div>`;
            }

            const cardHTML = `
                <div class="bg-black/20 p-4 rounded-lg border border-gray-700">
                    <div class="flex justify-between items-center mb-2">
                        <span class="font-bold text-white text-sm">${sub.title || sub.name}</span>
                        <div>
                            <span class="text-sm text-gray-400">Осталось: ${remaining} / ${total}</span>
                            <span class="text-sm font-bold text-white ml-2">${Math.round(progressPercentage)}%</span>
                        </div>
                    </div>
                    <div class="progress-bar-container">
                       ${progressBarHTML}
                    </div>
                    ${missed > 0 ? `<div class="text-xs mt-2"><span class="gradient-text-orange font-bold">Пропущено: ${missed}</span></div>` : ''}
                </div>
            `;
            container.innerHTML += cardHTML;
        });
        
        // Добавляем обработчики для tooltip
        if (tooltip) {
            container.querySelectorAll('.progress-segment').forEach(segment => {
                segment.addEventListener('mousemove', (e) => {
                    const data = e.target.dataset;
                    tooltip.style.display = 'block';
                    tooltip.style.left = `${e.pageX + 10}px`;
                    tooltip.style.top = `${e.pageY + 10}px`;
                    const timeRange = data.start && data.end ? `${data.start} - ${data.end}` : data.start || 'Время не указано';
                    tooltip.innerHTML = `
                        <div class="font-bold text-white">${data.date}</div>
                        <div class="text-gray-400">${timeRange}</div>
                        <div class="text-gray-400">Статус: ${data.status}</div>
                        <div class="text-gray-400">ID: ${data.id}</div>
                    `;
                });
                segment.addEventListener('mouseout', () => {
                    tooltip.style.display = 'none';
                });
            });
        }
    }

    updateGlobalKPIs(subscriptions) {
        let totalAttended = 0;
        let totalSkipped = 0;
        let totalScheduled = 0;

        subscriptions.forEach(sub => {
            sub.lessons.forEach(lesson => {
                if(lesson.status === 'Посещение') totalAttended++;
                else if (lesson.status === 'Пропуск') totalSkipped++;
                else if (lesson.status === 'Запланировано') totalScheduled++;
            });
        });
        
        const totalPast = totalAttended + totalSkipped;
        const rate = totalPast > 0 ? Math.round((totalAttended / totalPast) * 100) : 0;

        this.updateElement('kpi-attended', totalAttended);
        this.updateElement('kpi-skipped', totalSkipped);
        this.updateElement('kpi-scheduled', totalScheduled);
        this.updateElement('kpi-rate', `${rate}%`);
    }

    updateFinancialKPIs() {
        // Используем данные из API или демо-данные
        let budgetMonth = 0;
        let paidMonth = 0;
        let budgetWeek = 0;
        let paidWeek = 0;

        if (this.dashboardData && this.dashboardData.forecast) {
            budgetMonth = this.dashboardData.forecast.total_forecast || 0;
            budgetWeek = this.dashboardData.forecast.next_7_days || 0;
        } else {
            // Демо-данные
            budgetMonth = 148500;
            paidMonth = 120000;
            budgetWeek = 35000;
            paidWeek = 25000;
        }
        
        const formatCurrency = (value) => new Intl.NumberFormat('ru-RU').format(value) + ' ₽';

        this.updateElement('kpi-budget-month', formatCurrency(budgetMonth));
        this.updateElement('kpi-paid-month', formatCurrency(paidMonth));
        this.updateElement('kpi-budget-week', formatCurrency(budgetWeek));
        this.updateElement('kpi-paid-week', formatCurrency(paidWeek));
    }

    updateWeeklyKPIs(startDate, endDate) {
        // Эта функция может быть использована для дополнительной логики в будущем
    }

    showEventModal(event) {
        const modal = document.getElementById('event-modal');
        if (!modal) return;
        
        const [circle, child] = event.title.split(' - ');
        
        const modalTitle = document.getElementById('modal-title');
        const modalChild = document.getElementById('modal-child');
        const modalTime = document.getElementById('modal-time');
        const modalStatus = document.getElementById('modal-status');
        
        if (modalTitle) modalTitle.textContent = circle || 'Название не указано';
        if (modalChild) modalChild.textContent = `Ученик: ${child || 'Не указан'}`;
        if (modalTime) modalTime.textContent = `Время: ${event.time}, ${event.duration} ч.`;
        if (modalStatus) modalStatus.textContent = `Статус: ${event.status}`;
        
        modal.classList.remove('hidden');
        modal.classList.add('flex');
    }

    isToday(date) {
        const today = new Date();
        return date.getDate() === today.getDate() &&
               date.getMonth() === today.getMonth() &&
               date.getFullYear() === today.getFullYear();
    }

    updateElement(id, value) {
        const element = document.getElementById(id);
        if (element) {
            element.style.opacity = '0.5';
            setTimeout(() => {
                element.textContent = value;
                element.style.opacity = '1';
            }, 150);
        }
    }

    // Функция обновления удалена - данные загружаются только при открытии

    showPreloader() {
        const preloader = document.getElementById('preloader');
        const content = document.getElementById('dashboard-content');
        
        if (preloader) {
            preloader.classList.remove('hidden');
        }
        
        if (content) {
            content.classList.add('hidden');
        }
        
        // Анимация прогресс-бара
        const progressFill = document.getElementById('progress-fill');
        if (progressFill) {
            progressFill.style.width = '0%';
            setTimeout(() => {
                progressFill.style.width = '100%';
            }, 100);
        }
    }
    
    hidePreloader() {
        setTimeout(() => {
            const preloader = document.getElementById('preloader');
            const content = document.getElementById('dashboard-content');
            
            if (preloader) {
                preloader.classList.add('hidden');
            }
            
            if (content) {
                content.classList.remove('hidden');
            }
        }, 1000);
    }

    showError(message) {
        console.error('Dashboard Error:', message);
        // Можно добавить показ ошибки в UI
    }
}

// Инициализация дашборда при загрузке страницы
document.addEventListener('DOMContentLoaded', () => {
    console.log('📱 DOM загружен, инициализируем дашборд');
    window.dashboard = new Dashboard();
});

// Обработка ошибок JavaScript
window.addEventListener('error', (event) => {
    console.error('❌ JavaScript ошибка:', event.error);
});

// Обработка необработанных промисов
window.addEventListener('unhandledrejection', (event) => {
    console.error('❌ Необработанная ошибка промиса:', event.reason);
});

// CSS анимация для кнопки обновления
const additionalStyles = `
@keyframes spin {
    from { transform: rotate(0deg); }
    to { transform: rotate(360deg); }
}
`;

const styleSheet = document.createElement('style');
styleSheet.textContent = additionalStyles;
document.head.appendChild(styleSheet);
