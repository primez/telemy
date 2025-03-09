package dashboard

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"telemy/config"
	"telemy/query"
	"telemy/storage"
)

//go:embed templates static
var content embed.FS

// Manager manages the dashboard functionality
type Manager struct {
	config         config.DashboardConfig
	storageManager *storage.Manager
	queryEngine    *query.Engine
	server         *http.Server
	router         *mux.Router
	clients        map[*websocket.Conn]bool
	clientsMutex   sync.Mutex
	wg             sync.WaitGroup
	mu             sync.RWMutex
	running        bool
}

// NewManager creates a new dashboard manager
func NewManager(cfg config.DashboardConfig, storageManager *storage.Manager) (*Manager, error) {
	// Create query engine
	queryEngine, err := query.NewEngine(storageManager)
	if err != nil {
		return nil, fmt.Errorf("failed to create query engine: %w", err)
	}

	// Create manager
	manager := &Manager{
		config:         cfg,
		storageManager: storageManager,
		queryEngine:    queryEngine,
		router:         mux.NewRouter(),
		clients:        make(map[*websocket.Conn]bool),
	}

	// Setup routes
	manager.setupRoutes()

	return manager, nil
}

// Start starts the dashboard server
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	// Create server
	m.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", 8080), // TODO: use configured port
		Handler: m.router,
	}

	// Start server in a goroutine
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Dashboard server error: %v", err)
		}
	}()

	m.running = true
	log.Printf("Dashboard server started on %s", m.server.Addr)
	return nil
}

// Stop stops the dashboard server
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	// Close all WebSocket connections
	m.clientsMutex.Lock()
	for client := range m.clients {
		client.Close()
		delete(m.clients, client)
	}
	m.clientsMutex.Unlock()

	// Shutdown server with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("error shutting down dashboard server: %w", err)
	}

	// Wait for all goroutines to finish
	m.wg.Wait()

	m.running = false
	log.Println("Dashboard server stopped")
	return nil
}

// Close closes the dashboard manager
func (m *Manager) Close() error {
	return m.Stop()
}

// setupRoutes sets up the HTTP routes for the dashboard
func (m *Manager) setupRoutes() {
	// Static files
	m.router.PathPrefix("/static/").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Extract the file path from the request URL
		filePath := strings.TrimPrefix(r.URL.Path, "/")

		// Open the file from embed
		file, err := content.Open(filePath)
		if err != nil {
			http.Error(w, fmt.Sprintf("File not found: %s", filePath), http.StatusNotFound)
			return
		}
		defer file.Close()

		// Set the correct MIME type based on file extension
		switch {
		case strings.HasSuffix(filePath, ".css"):
			w.Header().Set("Content-Type", "text/css")
		case strings.HasSuffix(filePath, ".js"):
			w.Header().Set("Content-Type", "application/javascript")
		case strings.HasSuffix(filePath, ".html"):
			w.Header().Set("Content-Type", "text/html")
		case strings.HasSuffix(filePath, ".json"):
			w.Header().Set("Content-Type", "application/json")
		case strings.HasSuffix(filePath, ".png"):
			w.Header().Set("Content-Type", "image/png")
		case strings.HasSuffix(filePath, ".jpg"), strings.HasSuffix(filePath, ".jpeg"):
			w.Header().Set("Content-Type", "image/jpeg")
		case strings.HasSuffix(filePath, ".svg"):
			w.Header().Set("Content-Type", "image/svg+xml")
		default:
			// Default to octet-stream for unknown types
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		// Copy the file content to the response
		io.Copy(w, file)
	}))

	// Main dashboard page
	m.router.HandleFunc("/", m.handleIndex)
	m.router.HandleFunc("/dashboard/{view}", m.handleDashboard)

	// API endpoints
	api := m.router.PathPrefix("/api").Subrouter()
	api.HandleFunc("/metrics", m.handleMetricsQuery).Methods("GET")
	api.HandleFunc("/logs", m.handleLogsQuery).Methods("GET")
	api.HandleFunc("/traces", m.handleTracesQuery).Methods("GET")
	api.HandleFunc("/ws", m.handleWebSocket).Methods("GET")

	// Widgets API
	api.HandleFunc("/widgets", m.handleGetWidgets).Methods("GET")
	api.HandleFunc("/widgets/{id}", m.handleGetWidget).Methods("GET")
	api.HandleFunc("/widgets/{id}/data", m.handleGetWidgetData).Methods("GET")
}

// handleIndex handles the index page
func (m *Manager) handleIndex(w http.ResponseWriter, r *http.Request) {
	// Redirect to default dashboard view
	http.Redirect(w, r, fmt.Sprintf("/dashboard/%s", m.config.DefaultView), http.StatusFound)
}

// handleDashboard handles the dashboard page
func (m *Manager) handleDashboard(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	view := vars["view"]

	// Create a function map with our custom functions
	funcMap := template.FuncMap{
		"split": strings.Split,
	}

	// Parse the dashboard template with the function map
	tmpl, err := template.New("dashboard.html").Funcs(funcMap).ParseFS(content, "templates/dashboard.html", "templates/partials/*.html")
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing template: %v", err), http.StatusInternalServerError)
		return
	}

	// Prepare template data
	data := map[string]interface{}{
		"Title":   "Telemy Dashboard",
		"View":    view,
		"Widgets": m.config.Widgets,
		"NavLinks": []map[string]string{
			{"name": "Overview", "url": "/dashboard/overview"},
			{"name": "Metrics", "url": "/dashboard/metrics"},
			{"name": "Logs", "url": "/dashboard/logs"},
			{"name": "Traces", "url": "/dashboard/traces"},
		},
	}

	// Execute the template
	if err := tmpl.Execute(w, data); err != nil {
		http.Error(w, fmt.Sprintf("Error executing template: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleMetricsQuery handles metrics queries
func (m *Manager) handleMetricsQuery(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := r.URL.Query().Get("query")
	startTime := r.URL.Query().Get("start")
	endTime := r.URL.Query().Get("end")
	step := r.URL.Query().Get("step")

	// Execute query
	result, err := m.queryEngine.QueryMetrics(query, startTime, endTime, step)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the result as JSON
	if err := writeJSON(w, result); err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleLogsQuery handles logs queries
func (m *Manager) handleLogsQuery(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := r.URL.Query().Get("query")
	startTime := r.URL.Query().Get("start")
	endTime := r.URL.Query().Get("end")
	limit := r.URL.Query().Get("limit")

	// Execute query
	result, err := m.queryEngine.QueryLogs(query, startTime, endTime, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the result as JSON
	if err := writeJSON(w, result); err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleTracesQuery handles traces queries
func (m *Manager) handleTracesQuery(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	query := r.URL.Query().Get("query")
	startTime := r.URL.Query().Get("start")
	endTime := r.URL.Query().Get("end")
	limit := r.URL.Query().Get("limit")

	// Execute query
	result, err := m.queryEngine.QueryTraces(query, startTime, endTime, limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the result as JSON
	if err := writeJSON(w, result); err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetWidgets handles requests for all widgets
func (m *Manager) handleGetWidgets(w http.ResponseWriter, r *http.Request) {
	// Return the widgets as JSON
	if err := writeJSON(w, m.config.Widgets); err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetWidget handles requests for a specific widget
func (m *Manager) handleGetWidget(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// Find the widget
	var widget *config.WidgetConfig
	for i, w := range m.config.Widgets {
		if w.Title == id || fmt.Sprintf("%d", i) == id {
			widget = &m.config.Widgets[i]
			break
		}
	}

	if widget == nil {
		http.Error(w, fmt.Sprintf("Widget not found: %s", id), http.StatusNotFound)
		return
	}

	// Return the widget as JSON
	if err := writeJSON(w, widget); err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

// handleGetWidgetData handles requests for widget data
func (m *Manager) handleGetWidgetData(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	// Find the widget
	var widget *config.WidgetConfig
	for i, w := range m.config.Widgets {
		if w.Title == id || fmt.Sprintf("%d", i) == id {
			widget = &m.config.Widgets[i]
			break
		}
	}

	if widget == nil {
		http.Error(w, fmt.Sprintf("Widget not found: %s", id), http.StatusNotFound)
		return
	}

	// Get the current time
	now := time.Now()
	// Calculate the start time based on the refresh interval
	refreshInterval, err := time.ParseDuration(widget.RefreshInterval)
	if err != nil {
		refreshInterval = 30 * time.Second
	}
	start := now.Add(-refreshInterval * 10) // Show last 10 refresh intervals

	// Execute the query based on the data source
	var result interface{}
	switch widget.DataSource {
	case "metrics":
		result, err = m.queryEngine.QueryMetrics(widget.Query, start.Format(time.RFC3339), now.Format(time.RFC3339), "")
	case "logs":
		result, err = m.queryEngine.QueryLogs(widget.Query, start.Format(time.RFC3339), now.Format(time.RFC3339), fmt.Sprintf("%d", widget.MaxRows))
	case "traces":
		result, err = m.queryEngine.QueryTraces(widget.Query, start.Format(time.RFC3339), now.Format(time.RFC3339), fmt.Sprintf("%d", widget.MaxRows))
	default:
		http.Error(w, fmt.Sprintf("Unsupported data source: %s", widget.DataSource), http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, fmt.Sprintf("Error executing query: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the result as JSON
	if err := writeJSON(w, result); err != nil {
		http.Error(w, fmt.Sprintf("Error writing response: %v", err), http.StatusInternalServerError)
		return
	}
}

// Upgrader for WebSocket connections
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for now
	},
}

// handleWebSocket handles WebSocket connections
func (m *Manager) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Upgrade the HTTP connection to a WebSocket connection
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Error upgrading to WebSocket: %v", err)
		return
	}

	// Register the client
	m.clientsMutex.Lock()
	m.clients[conn] = true
	m.clientsMutex.Unlock()

	// Start a goroutine to handle messages from this client
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer func() {
			// Unregister the client when the connection is closed
			m.clientsMutex.Lock()
			delete(m.clients, conn)
			m.clientsMutex.Unlock()
			conn.Close()
		}()

		for {
			// Read message from the client
			_, message, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Error reading WebSocket message: %v", err)
				break
			}

			// Process the message (e.g., execute a query)
			log.Printf("Received WebSocket message: %s", message)
		}
	}()
}

// writeJSON writes the given value as JSON to the response writer
func writeJSON(w http.ResponseWriter, v interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	return getEncoder(w).Encode(v)
}

// getEncoder creates a JSON encoder for the response writer
func getEncoder(w http.ResponseWriter) *json.Encoder {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc
}
