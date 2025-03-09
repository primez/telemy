package ingestion

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"telemy/config"
	"telemy/storage"
)

// Manager manages OTLP data ingestion
type Manager struct {
	config         config.IngestionConfig
	storageManager *storage.Manager
	httpServer     *http.Server
	grpcServer     *grpc.Server
	wg             sync.WaitGroup
	mu             sync.RWMutex
	running        bool
	metricsHandler *MetricsHandler
	logsHandler    *LogsHandler
	tracesHandler  *TracesHandler
}

// NewManager creates a new ingestion manager
func NewManager(cfg config.IngestionConfig, storageManager *storage.Manager) (*Manager, error) {
	manager := &Manager{
		config:         cfg,
		storageManager: storageManager,
	}

	// Initialize handlers
	manager.metricsHandler = NewMetricsHandler(storageManager.MetricsStore())
	manager.logsHandler = NewLogsHandler(storageManager.LogsStore())
	manager.tracesHandler = NewTracesHandler(storageManager.TracesStore())

	return manager, nil
}

// Start starts the ingestion manager
func (m *Manager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return nil
	}

	// Start HTTP server if configured
	if m.config.OTLP.HTTPEndpoint != "" {
		if err := m.startHTTPServer(); err != nil {
			return fmt.Errorf("failed to start HTTP server: %w", err)
		}
	}

	// Start gRPC server if configured
	if m.config.OTLP.GRPCEndpoint != "" {
		if err := m.startGRPCServer(); err != nil {
			// Shutdown HTTP server if it was started
			if m.httpServer != nil {
				m.httpServer.Shutdown(context.Background())
			}
			return fmt.Errorf("failed to start gRPC server: %w", err)
		}
	}

	m.running = true
	return nil
}

// Stop stops the ingestion manager
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	// Shutdown HTTP server
	if m.httpServer != nil {
		// Use a context with timeout for graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := m.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown HTTP server: %w", err)
		}
	}

	// Shutdown gRPC server
	if m.grpcServer != nil {
		m.grpcServer.GracefulStop()
	}

	// Wait for all goroutines to finish
	m.wg.Wait()

	m.running = false
	return nil
}

// Close closes the ingestion manager
func (m *Manager) Close() error {
	return m.Stop()
}

// startHTTPServer starts the HTTP server
func (m *Manager) startHTTPServer() error {
	// Create router
	router := mux.NewRouter()

	// Register OTLP HTTP endpoints
	router.HandleFunc("/v1/metrics", m.metricsHandler.HandleHTTP).Methods("POST")
	router.HandleFunc("/v1/logs", m.logsHandler.HandleHTTP).Methods("POST")
	router.HandleFunc("/v1/traces", m.tracesHandler.HandleHTTP).Methods("POST")

	// Create server
	m.httpServer = &http.Server{
		Addr:    m.config.OTLP.HTTPEndpoint,
		Handler: router,
	}

	// Start server in a goroutine
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	fmt.Printf("HTTP server listening on %s\n", m.config.OTLP.HTTPEndpoint)
	return nil
}

// startGRPCServer starts the gRPC server
func (m *Manager) startGRPCServer() error {
	// Create listener
	lis, err := net.Listen("tcp", m.config.OTLP.GRPCEndpoint)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// Create server
	m.grpcServer = grpc.NewServer()

	// Register OTLP gRPC services
	m.metricsHandler.RegisterGRPC(m.grpcServer)
	m.logsHandler.RegisterGRPC(m.grpcServer)
	m.tracesHandler.RegisterGRPC(m.grpcServer)

	// Enable reflection for debugging
	reflection.Register(m.grpcServer)

	// Start server in a goroutine
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		if err := m.grpcServer.Serve(lis); err != nil {
			fmt.Printf("gRPC server error: %v\n", err)
		}
	}()

	fmt.Printf("gRPC server listening on %s\n", m.config.OTLP.GRPCEndpoint)
	return nil
}
