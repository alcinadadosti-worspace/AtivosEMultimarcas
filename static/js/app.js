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
// CHART UTILITIES
// =============================================================================

window.chartColors = {
    'oBoticario': '#00a86b',        // Green
    'Eudora': '#9333ea',            // Purple
    'Quem Disse Berenice': '#ec4899', // Pink
    'O.U.I': '#ef4444',             // Red
    'AuAmigos': '#fbbf24',          // Yellow
    'DESCONHECIDA': '#525252',
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

window.createPieChart = function(elementId, data, labelKey, valueKey) {
    const ctx = document.getElementById(elementId);
    if (!ctx) return;

    const labels = data.map(d => d[labelKey]);
    const values = data.map(d => d[valueKey]);
    const colors = labels.map((label, i) =>
        chartColors[label] || chartColors.default[i % chartColors.default.length]
    );

    return new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        color: '#a1a1a1',
                        font: { size: 12 },
                        padding: 12
                    }
                }
            }
        }
    });
};

window.createBarChart = function(elementId, data, labelKey, valueKey) {
    const ctx = document.getElementById(elementId);
    if (!ctx) return;

    const labels = data.map(d => d[labelKey]);
    const values = data.map(d => d[valueKey]);

    return new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                backgroundColor: '#3b82f6',
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { color: '#1f1f1f' },
                    ticks: { color: '#a1a1a1' }
                },
                y: {
                    grid: { display: false },
                    ticks: { color: '#a1a1a1' }
                }
            }
        }
    });
};

window.createLineChart = function(elementId, data, labelKey, valueKey) {
    const ctx = document.getElementById(elementId);
    if (!ctx) return;

    const labels = data.map(d => d[labelKey]);
    const values = data.map(d => d[valueKey]);

    return new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                data: values,
                borderColor: '#3b82f6',
                backgroundColor: 'rgba(59, 130, 246, 0.1)',
                fill: true,
                tension: 0.3
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    grid: { color: '#1f1f1f' },
                    ticks: { color: '#a1a1a1' }
                },
                y: {
                    grid: { color: '#1f1f1f' },
                    ticks: { color: '#a1a1a1' }
                }
            }
        }
    });
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
