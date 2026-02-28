/**
 * Multimarks Analytics - Main JavaScript
 * HTMX + Alpine.js setup and utilities
 */

// =============================================================================
// HTMX CONFIGURATION
// =============================================================================

document.body.addEventListener('htmx:configRequest', (event) => {
    // Add loading state
    event.detail.headers['X-Requested-With'] = 'htmx';
});

document.body.addEventListener('htmx:beforeRequest', (event) => {
    // Show loading indicator
    const target = event.detail.target;
    if (target && !target.classList.contains('no-loading')) {
        target.classList.add('loading-state');
    }
});

document.body.addEventListener('htmx:afterRequest', (event) => {
    // Hide loading indicator
    const target = event.detail.target;
    if (target) {
        target.classList.remove('loading-state');
    }
});

// =============================================================================
// TOAST NOTIFICATIONS
// =============================================================================

window.showToast = function(message, type = 'info') {
    const container = document.getElementById('toast-container') || createToastContainer();

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            ${getToastIcon(type)}
        </svg>
        <span>${message}</span>
    `;

    container.appendChild(toast);

    // Auto remove after 5 seconds
    setTimeout(() => {
        toast.style.animation = 'slideOut 200ms ease forwards';
        setTimeout(() => toast.remove(), 200);
    }, 5000);
};

function createToastContainer() {
    const container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'toast-container';
    document.body.appendChild(container);
    return container;
}

function getToastIcon(type) {
    switch (type) {
        case 'success':
            return '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><path d="m9 11 3 3L22 4"/>';
        case 'error':
            return '<circle cx="12" cy="12" r="10"/><path d="m15 9-6 6"/><path d="m9 9 6 6"/>';
        case 'warning':
            return '<path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3Z"/><path d="M12 9v4"/><path d="M12 17h.01"/>';
        default:
            return '<circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/>';
    }
}

// =============================================================================
// FILE UPLOAD
// =============================================================================

window.initUploadZone = function(elementId) {
    const zone = document.getElementById(elementId);
    if (!zone) return;

    const input = zone.querySelector('input[type="file"]');

    // Click to upload
    zone.addEventListener('click', () => input.click());

    // Drag and drop
    zone.addEventListener('dragover', (e) => {
        e.preventDefault();
        zone.classList.add('dragover');
    });

    zone.addEventListener('dragleave', () => {
        zone.classList.remove('dragover');
    });

    zone.addEventListener('drop', (e) => {
        e.preventDefault();
        zone.classList.remove('dragover');

        const files = e.dataTransfer.files;
        if (files.length > 0) {
            input.files = files;
            input.dispatchEvent(new Event('change'));
        }
    });
};

// =============================================================================
// FORMATTING UTILITIES
// =============================================================================

window.formatCurrency = function(value) {
    if (value == null) return 'R$ 0,00';
    return new Intl.NumberFormat('pt-BR', {
        style: 'currency',
        currency: 'BRL'
    }).format(value);
};

window.formatNumber = function(value, decimals = 0) {
    if (value == null) return '0';
    return new Intl.NumberFormat('pt-BR', {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals
    }).format(value);
};

window.formatPercent = function(value, decimals = 0) {
    if (value == null) return '0%';
    return `${formatNumber(value, decimals)}%`;
};

// =============================================================================
// ALPINE.JS STORES
// =============================================================================

document.addEventListener('alpine:init', () => {
    // Global store for filters
    Alpine.store('filters', {
        ciclos: [],
        setores: [],
        marcas: [],
        gerencias: [],
        selectedCiclos: [],
        selectedSetores: [],
        selectedGerencias: [],

        async load() {
            try {
                const response = await fetch('/api/filtros');
                const data = await response.json();
                this.ciclos = data.ciclos || [];
                this.setores = data.setores || [];
                this.marcas = data.marcas || [];
                this.gerencias = data.gerencias || [];
            } catch (error) {
                console.error('Failed to load filters:', error);
            }
        },

        getQueryString() {
            const params = new URLSearchParams();
            if (this.selectedCiclos.length > 0) {
                params.set('ciclos', this.selectedCiclos.join(','));
            }
            if (this.selectedSetores.length > 0) {
                params.set('setores', this.selectedSetores.join(','));
            }
            if (this.selectedGerencias.length > 0) {
                params.set('gerencias', this.selectedGerencias.join(','));
            }
            return params.toString();
        }
    });

    // Global store for app state
    Alpine.store('app', {
        hasData: false,
        loading: false,

        setHasData(value) {
            this.hasData = value;
        }
    });
});

// =============================================================================
// CHART UTILITIES - INTERACTIVE CHARTS
// =============================================================================

window.chartColors = {
    'oBoticario': '#00a86b',        // Green
    'Eudora': '#9333ea',            // Purple
    'Quem Disse Berenice': '#ec4899', // Pink
    'O.U.I': '#ef4444',             // Red
    'AuAmigos': '#fbbf24',          // Yellow
    'DESCONHECIDA': '#525252',
    'Multimarcas': '#8b5cf6',
    'Monomarca': '#64748b',
    'default': [
        '#3b82f6',
        '#22c55e',
        '#f59e0b',
        '#ef4444',
        '#8b5cf6',
        '#06b6d4',
        '#ec4899'
    ]
};

// Store for drill-down data
window.chartDrilldownData = {};

// Enhanced tooltip configuration
const enhancedTooltip = {
    enabled: true,
    backgroundColor: 'rgba(20, 20, 30, 0.95)',
    titleColor: '#f8fafc',
    bodyColor: '#e2e8f0',
    borderColor: 'rgba(139, 92, 246, 0.5)',
    borderWidth: 1,
    cornerRadius: 8,
    padding: 12,
    titleFont: { size: 14, weight: 'bold' },
    bodyFont: { size: 13 },
    displayColors: true,
    boxPadding: 6
};

// Create interactive pie/doughnut chart with drill-down
window.createPieChart = function(elementId, data, labelKey, valueKey, options = {}) {
    const ctx = document.getElementById(elementId);
    if (!ctx) return;

    const labels = data.map(d => d[labelKey]);
    const values = data.map(d => d[valueKey]);
    const total = values.reduce((a, b) => a + b, 0);
    const colors = labels.map((label, i) =>
        chartColors[label] || chartColors.default[i % chartColors.default.length]
    );

    // Store original data for drill-down
    window.chartDrilldownData[elementId] = data;

    const chart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderWidth: 2,
                borderColor: 'rgba(20, 20, 30, 0.8)',
                hoverBorderColor: '#fff',
                hoverBorderWidth: 3,
                hoverOffset: 8
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '60%',
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#a1a1a1',
                        font: { size: 12 },
                        padding: 12,
                        usePointStyle: true,
                        pointStyle: 'circle'
                    }
                },
                tooltip: {
                    ...enhancedTooltip,
                    callbacks: {
                        title: function(context) {
                            return context[0].label;
                        },
                        label: function(context) {
                            const value = context.raw;
                            const percentage = ((value / total) * 100).toFixed(1);
                            return [
                                `Valor: ${formatCurrency(value)}`,
                                `Percentual: ${percentage}%`
                            ];
                        },
                        afterLabel: function(context) {
                            const item = data[context.dataIndex];
                            if (item.itens) {
                                return `Itens: ${formatNumber(item.itens)}`;
                            }
                            if (item.vendas) {
                                return `Vendas: ${formatNumber(item.vendas)}`;
                            }
                            return '';
                        }
                    }
                }
            },
            onClick: function(event, elements) {
                if (elements.length > 0 && options.onDrilldown) {
                    const index = elements[0].index;
                    const item = data[index];
                    options.onDrilldown(item, labels[index], values[index]);
                }
            },
            onHover: function(event, elements) {
                event.native.target.style.cursor = elements.length > 0 && options.onDrilldown ? 'pointer' : 'default';
            }
        }
    });

    return chart;
};

// Create interactive bar chart with drill-down and zoom
window.createBarChart = function(elementId, data, labelKey, valueKey, options = {}) {
    const ctx = document.getElementById(elementId);
    if (!ctx) return;

    const labels = data.map(d => d[labelKey]);
    const values = data.map(d => d[valueKey]);
    const colors = labels.map((label, i) =>
        chartColors[label] || chartColors.default[i % chartColors.default.length]
    );

    // Store original data for drill-down
    window.chartDrilldownData[elementId] = data;

    const chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderRadius: 6,
                borderSkipped: false,
                hoverBackgroundColor: colors.map(c => c + 'dd'),
                barThickness: options.horizontal ? 24 : 'flex'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: options.horizontal ? 'y' : 'x',
            plugins: {
                legend: { display: false },
                tooltip: {
                    ...enhancedTooltip,
                    callbacks: {
                        title: function(context) {
                            return context[0].label;
                        },
                        label: function(context) {
                            const value = context.raw;
                            const item = data[context.dataIndex];
                            const lines = [`Valor: ${formatCurrency(value)}`];

                            if (item.itens) lines.push(`Itens: ${formatNumber(item.itens)}`);
                            if (item.vendas) lines.push(`Vendas: ${formatNumber(item.vendas)}`);
                            if (item.clientes) lines.push(`Clientes: ${formatNumber(item.clientes)}`);
                            if (item.percent) lines.push(`Percentual: ${item.percent.toFixed(1)}%`);

                            return lines;
                        }
                    }
                },
                zoom: {
                    pan: {
                        enabled: true,
                        mode: 'x'
                    },
                    zoom: {
                        wheel: { enabled: true },
                        pinch: { enabled: true },
                        mode: 'x',
                        onZoomComplete: function({chart}) {
                            // Show reset button when zoomed
                            showZoomResetButton(elementId, chart);
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: { color: '#a1a1a1', font: { size: 11 } }
                },
                y: {
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: {
                        color: '#a1a1a1',
                        font: { size: 11 },
                        callback: function(value) {
                            if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
                            if (value >= 1000) return (value / 1000).toFixed(0) + 'K';
                            return value;
                        }
                    }
                }
            },
            onClick: function(event, elements) {
                if (elements.length > 0 && options.onDrilldown) {
                    const index = elements[0].index;
                    const item = data[index];
                    options.onDrilldown(item, labels[index], values[index]);
                }
            },
            onHover: function(event, elements) {
                event.native.target.style.cursor = elements.length > 0 && options.onDrilldown ? 'pointer' : 'default';
            }
        }
    });

    return chart;
};

// Create interactive line chart with zoom
window.createLineChart = function(elementId, data, labelKey, valueKey, options = {}) {
    const ctx = document.getElementById(elementId);
    if (!ctx) return;

    const labels = data.map(d => d[labelKey]);
    const values = data.map(d => d[valueKey]);

    // Calculate trend (up/down)
    const firstValue = values[0] || 0;
    const lastValue = values[values.length - 1] || 0;
    const trend = lastValue >= firstValue ? 'up' : 'down';
    const trendColor = trend === 'up' ? '#22c55e' : '#ef4444';

    // Store original data for drill-down
    window.chartDrilldownData[elementId] = data;

    const chart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                borderColor: options.color || '#3b82f6',
                backgroundColor: (options.color || '#3b82f6') + '20',
                fill: true,
                tension: 0.4,
                pointRadius: 4,
                pointHoverRadius: 8,
                pointBackgroundColor: options.color || '#3b82f6',
                pointBorderColor: '#fff',
                pointBorderWidth: 2,
                pointHoverBackgroundColor: '#fff',
                pointHoverBorderColor: options.color || '#3b82f6',
                pointHoverBorderWidth: 3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    ...enhancedTooltip,
                    callbacks: {
                        title: function(context) {
                            return `Ciclo: ${context[0].label}`;
                        },
                        label: function(context) {
                            const value = context.raw;
                            const item = data[context.dataIndex];
                            const lines = [`Valor: ${formatCurrency(value)}`];

                            if (item.clientes) lines.push(`Clientes: ${formatNumber(item.clientes)}`);
                            if (item.multimarcas) lines.push(`Multimarcas: ${formatNumber(item.multimarcas)}`);
                            if (item.percent) lines.push(`% Multi: ${item.percent.toFixed(1)}%`);

                            return lines;
                        },
                        afterBody: function(context) {
                            const index = context[0].dataIndex;
                            if (index > 0) {
                                const current = values[index];
                                const previous = values[index - 1];
                                if (previous > 0) {
                                    const variation = ((current - previous) / previous * 100).toFixed(1);
                                    const sign = variation >= 0 ? '+' : '';
                                    return [`Variacao: ${sign}${variation}%`];
                                }
                            }
                            return [];
                        }
                    }
                },
                zoom: {
                    pan: {
                        enabled: true,
                        mode: 'x'
                    },
                    zoom: {
                        wheel: { enabled: true },
                        pinch: { enabled: true },
                        mode: 'x',
                        onZoomComplete: function({chart}) {
                            showZoomResetButton(elementId, chart);
                        }
                    }
                }
            },
            scales: {
                x: {
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: { color: '#a1a1a1', font: { size: 11 } }
                },
                y: {
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    ticks: {
                        color: '#a1a1a1',
                        font: { size: 11 },
                        callback: function(value) {
                            if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
                            if (value >= 1000) return (value / 1000).toFixed(0) + 'K';
                            return value;
                        }
                    }
                }
            },
            onClick: function(event, elements) {
                if (elements.length > 0 && options.onDrilldown) {
                    const index = elements[0].index;
                    const item = data[index];
                    options.onDrilldown(item, labels[index], values[index]);
                }
            },
            onHover: function(event, elements) {
                event.native.target.style.cursor = elements.length > 0 && options.onDrilldown ? 'pointer' : 'default';
            }
        }
    });

    return chart;
};

// Show zoom reset button
function showZoomResetButton(elementId, chart) {
    const canvas = document.getElementById(elementId);
    if (!canvas) return;

    // Remove existing button
    const existingBtn = document.getElementById(`${elementId}-reset-zoom`);
    if (existingBtn) existingBtn.remove();

    // Create reset button
    const btn = document.createElement('button');
    btn.id = `${elementId}-reset-zoom`;
    btn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg> Reset Zoom';
    btn.style.cssText = `
        position: absolute;
        top: 8px;
        right: 8px;
        padding: 6px 12px;
        background: rgba(139, 92, 246, 0.2);
        border: 1px solid rgba(139, 92, 246, 0.4);
        border-radius: 6px;
        color: #a78bfa;
        font-size: 12px;
        cursor: pointer;
        display: flex;
        align-items: center;
        gap: 6px;
        z-index: 10;
    `;

    btn.addEventListener('click', function() {
        chart.resetZoom();
        btn.remove();
    });

    // Add button to canvas container
    const container = canvas.parentElement;
    container.style.position = 'relative';
    container.appendChild(btn);
}

// Drill-down modal helper
window.showDrilldownModal = function(title, content) {
    // Remove existing modal
    const existingModal = document.getElementById('drilldown-modal');
    if (existingModal) existingModal.remove();

    // Create modal
    const modal = document.createElement('div');
    modal.id = 'drilldown-modal';
    modal.innerHTML = `
        <div class="drilldown-overlay" onclick="closeDrilldownModal()">
            <div class="drilldown-content" onclick="event.stopPropagation()">
                <div class="drilldown-header">
                    <h3>${title}</h3>
                    <button onclick="closeDrilldownModal()" class="drilldown-close">&times;</button>
                </div>
                <div class="drilldown-body">
                    ${content}
                </div>
            </div>
        </div>
    `;

    document.body.appendChild(modal);

    // Add styles if not exists
    if (!document.getElementById('drilldown-styles')) {
        const styles = document.createElement('style');
        styles.id = 'drilldown-styles';
        styles.textContent = `
            .drilldown-overlay {
                position: fixed;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                background: rgba(0, 0, 0, 0.8);
                display: flex;
                align-items: center;
                justify-content: center;
                z-index: 9999;
                animation: fadeIn 0.2s ease;
            }
            .drilldown-content {
                background: linear-gradient(145deg, #1a1a2e, #16162a);
                border: 1px solid rgba(139, 92, 246, 0.3);
                border-radius: 16px;
                max-width: 800px;
                width: 90%;
                max-height: 80vh;
                overflow: hidden;
                animation: slideUp 0.3s ease;
            }
            .drilldown-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 20px 24px;
                border-bottom: 1px solid rgba(139, 92, 246, 0.2);
            }
            .drilldown-header h3 {
                margin: 0;
                color: #f8fafc;
                font-size: 1.25rem;
            }
            .drilldown-close {
                background: none;
                border: none;
                color: #94a3b8;
                font-size: 1.5rem;
                cursor: pointer;
                padding: 0;
                line-height: 1;
            }
            .drilldown-close:hover { color: #f8fafc; }
            .drilldown-body {
                padding: 24px;
                max-height: 60vh;
                overflow-y: auto;
            }
            .drilldown-table {
                width: 100%;
                border-collapse: collapse;
            }
            .drilldown-table th {
                text-align: left;
                padding: 12px;
                color: #a78bfa;
                font-size: 0.75rem;
                text-transform: uppercase;
                border-bottom: 1px solid rgba(139, 92, 246, 0.2);
            }
            .drilldown-table td {
                padding: 12px;
                color: #e2e8f0;
                border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            }
            .drilldown-table tr:hover {
                background: rgba(139, 92, 246, 0.08);
            }
            @keyframes fadeIn {
                from { opacity: 0; }
                to { opacity: 1; }
            }
            @keyframes slideUp {
                from { transform: translateY(20px); opacity: 0; }
                to { transform: translateY(0); opacity: 1; }
            }
        `;
        document.head.appendChild(styles);
    }
};

window.closeDrilldownModal = function() {
    const modal = document.getElementById('drilldown-modal');
    if (modal) {
        modal.querySelector('.drilldown-overlay').style.animation = 'fadeIn 0.2s ease reverse';
        setTimeout(() => modal.remove(), 200);
    }
};

// =============================================================================
// INITIALIZATION
// =============================================================================

document.addEventListener('DOMContentLoaded', () => {
    // Initialize Lucide icons
    if (window.lucide) {
        lucide.createIcons();
    }

    // Initialize upload zones
    const uploadZone = document.getElementById('upload-zone');
    if (uploadZone) {
        initUploadZone('upload-zone');
    }
});
