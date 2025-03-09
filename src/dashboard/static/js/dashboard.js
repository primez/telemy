// Dashboard functionality
document.addEventListener('DOMContentLoaded', function() {
    // Initialize all widgets
    const widgets = document.querySelectorAll('.widget');
    widgets.forEach(initWidget);

    // Setup refresh buttons
    const refreshButtons = document.querySelectorAll('.refresh-btn');
    refreshButtons.forEach(button => {
        button.addEventListener('click', function() {
            const widget = this.closest('.widget');
            refreshWidget(widget);
        });
    });

    // Setup WebSocket connection for real-time updates
    setupWebSocket();
});

// Initialize a widget based on its type
function initWidget(widget) {
    const widgetId = widget.dataset.id;
    const widgetType = widget.dataset.type;
    const dataSource = widget.dataset.source;

    // Load initial data
    loadWidgetData(widget);

    // Set up automatic refresh based on the widget's refresh interval
    const refreshInterval = getWidgetRefreshInterval(widgetId);
    if (refreshInterval) {
        setInterval(() => {
            refreshWidget(widget);
        }, refreshInterval);
    }
}

// Load data for a widget
function loadWidgetData(widget) {
    const widgetId = widget.dataset.id;
    const widgetType = widget.dataset.type;
    const dataSource = widget.dataset.source;

    // Show loading state
    if (widgetType === 'table') {
        const tbody = widget.querySelector('tbody');
        tbody.innerHTML = '<tr><td colspan="2">Loading...</td></tr>';
    }

    // Fetch data from the API
    fetch(`/api/widgets/${encodeURIComponent(widgetId)}/data`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not ok');
            }
            return response.json();
        })
        .then(data => {
            updateWidget(widget, data);
        })
        .catch(error => {
            console.error('Error fetching widget data:', error);
            showWidgetError(widget, error);
        });
}

// Refresh a widget
function refreshWidget(widget) {
    // Add a visual indication that the widget is refreshing
    widget.classList.add('refreshing');
    
    // Load new data
    loadWidgetData(widget);
    
    // Remove the refreshing indication after a short delay
    setTimeout(() => {
        widget.classList.remove('refreshing');
    }, 500);
}

// Update a widget with new data
function updateWidget(widget, data) {
    const widgetType = widget.dataset.type;
    const dataSource = widget.dataset.source;
    
    if (widgetType === 'graph') {
        updateChart(widget, data);
    } else if (widgetType === 'table') {
        updateTable(widget, data);
    }
}

// Update a chart widget
function updateChart(widget, data) {
    const canvas = widget.querySelector('.chart');
    let chart = Chart.getChart(canvas);
    
    // Process data based on the data source
    let chartData = {
        labels: [],
        datasets: [{
            label: widget.querySelector('h3').textContent,
            data: [],
            borderColor: '#3498db',
            backgroundColor: 'rgba(52, 152, 219, 0.1)',
            borderWidth: 2,
            tension: 0.1
        }]
    };
    
    if (data.data && data.data.result) {
        // Process metrics data
        const series = data.data.result[0];
        if (series && series.values) {
            series.values.forEach(point => {
                const timestamp = new Date(point[0] * 1000);
                chartData.labels.push(formatTime(timestamp));
                chartData.datasets[0].data.push(point[1]);
            });
        }
    }
    
    // Create or update the chart
    if (chart) {
        chart.data = chartData;
        chart.update();
    } else {
        chart = new Chart(canvas, {
            type: 'line',
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    y: {
                        beginAtZero: true
                    }
                }
            }
        });
    }
}

// Update a table widget
function updateTable(widget, data) {
    const tbody = widget.querySelector('tbody');
    tbody.innerHTML = '';
    
    if (data.data) {
        if (data.data.logs && data.data.logs.length > 0) {
            // Process logs data
            data.data.logs.forEach(log => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${formatTime(new Date(log.timestamp))}</td>
                    <td>${escapeHTML(log.message)}</td>
                `;
                tbody.appendChild(row);
            });
        } else if (data.data.traces && data.data.traces.length > 0) {
            // Process traces data
            data.data.traces.forEach(trace => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${formatTime(new Date(trace.startTime))}</td>
                    <td>${escapeHTML(trace.name)} (${trace.duration.toFixed(2)}ms)</td>
                `;
                tbody.appendChild(row);
            });
        } else {
            // No data
            tbody.innerHTML = '<tr><td colspan="2">No data available</td></tr>';
        }
    } else {
        // Invalid data format
        tbody.innerHTML = '<tr><td colspan="2">Invalid data format</td></tr>';
    }
}

// Show an error message in a widget
function showWidgetError(widget, error) {
    const widgetType = widget.dataset.type;
    
    if (widgetType === 'graph') {
        const canvas = widget.querySelector('.chart');
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.font = '14px Arial';
        ctx.fillStyle = '#e74c3c';
        ctx.textAlign = 'center';
        ctx.fillText('Error loading data', canvas.width / 2, canvas.height / 2);
    } else if (widgetType === 'table') {
        const tbody = widget.querySelector('tbody');
        tbody.innerHTML = `<tr><td colspan="2">Error: ${error.message}</td></tr>`;
    }
}

// Get the refresh interval for a widget
function getWidgetRefreshInterval(widgetId) {
    // This would typically come from the widget configuration
    // For now, we'll use a default of 30 seconds
    return 30000; // 30 seconds
}

// Format a timestamp
function formatTime(date) {
    return date.toLocaleTimeString();
}

// Escape HTML to prevent XSS
function escapeHTML(str) {
    return str
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#039;');
}

// Setup WebSocket connection for real-time updates
function setupWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/api/ws`;
    
    const socket = new WebSocket(wsUrl);
    
    socket.onopen = function(event) {
        console.log('WebSocket connection established');
    };
    
    socket.onmessage = function(event) {
        try {
            const data = JSON.parse(event.data);
            if (data.type === 'widget_update' && data.widgetId) {
                const widget = document.querySelector(`.widget[data-id="${data.widgetId}"]`);
                if (widget) {
                    updateWidget(widget, data.data);
                }
            } else if (data.type === 'alert') {
                showAlert(data.alert);
            }
        } catch (error) {
            console.error('Error processing WebSocket message:', error);
        }
    };
    
    socket.onclose = function(event) {
        console.log('WebSocket connection closed');
        // Attempt to reconnect after a delay
        setTimeout(setupWebSocket, 5000);
    };
    
    socket.onerror = function(error) {
        console.error('WebSocket error:', error);
    };
}

// Show an alert notification
function showAlert(alert) {
    // Create alert element
    const alertElement = document.createElement('div');
    alertElement.className = `alert alert-${alert.severity.toLowerCase()}`;
    alertElement.innerHTML = `
        <strong>${alert.name}</strong>: ${alert.details}
        <button class="close-btn">&times;</button>
    `;
    
    // Add to the DOM
    document.body.appendChild(alertElement);
    
    // Add close button functionality
    const closeButton = alertElement.querySelector('.close-btn');
    closeButton.addEventListener('click', function() {
        alertElement.remove();
    });
    
    // Auto-remove after 10 seconds
    setTimeout(() => {
        if (document.body.contains(alertElement)) {
            alertElement.remove();
        }
    }, 10000);
} 