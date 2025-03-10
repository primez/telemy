package storage

import (
	"fmt"
	"os"
	"sync"
	"time"

	"telemy/config"
)

// Manager manages storage for metrics, logs, and traces
type Manager struct {
	config       config.StorageConfig
	metricsStore *PromTSDBStore
	logsStore    *BadgerStore
	tracesStore  *PromTSDBStore
	mu           sync.RWMutex
}

// NewManager creates a new storage manager
func NewManager(cfg config.StorageConfig) (*Manager, error) {
	// Ensure data directories exist
	if err := ensureDir(cfg.Metrics.DataPath); err != nil {
		return nil, fmt.Errorf("failed to create metrics data directory: %w", err)
	}
	if err := ensureDir(cfg.Logs.DataPath); err != nil {
		return nil, fmt.Errorf("failed to create logs data directory: %w", err)
	}
	if err := ensureDir(cfg.Traces.DataPath); err != nil {
		return nil, fmt.Errorf("failed to create traces data directory: %w", err)
	}

	// Create the manager
	manager := &Manager{
		config: cfg,
	}

	// Initialize metrics storage
	metricsRetention, err := parseDuration(cfg.Metrics.RetentionPeriod)
	if err != nil {
		return nil, fmt.Errorf("invalid metrics retention period: %w", err)
	}
	metricsBlockSize, err := parseDuration(cfg.Metrics.IndexConfig.BlockSize)
	if err != nil {
		return nil, fmt.Errorf("invalid metrics block size: %w", err)
	}
	metricsStore, err := NewPromTSDBStore(cfg.Metrics.DataPath, metricsRetention, metricsBlockSize, cfg.Metrics.IndexConfig.Compaction)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize metrics storage: %w", err)
	}
	manager.metricsStore = metricsStore

	// Initialize logs storage
	logsStore, err := NewBadgerStore(cfg.Logs.DataPath, cfg.Logs.Indexing, cfg.Logs.MaxFileSizeMB)
	if err != nil {
		metricsStore.Close()
		return nil, fmt.Errorf("failed to initialize logs storage: %w", err)
	}
	manager.logsStore = logsStore

	// Initialize traces storage
	tracesRetention, err := parseDuration(cfg.Traces.RetentionPeriod)
	if err != nil {
		metricsStore.Close()
		logsStore.Close()
		return nil, fmt.Errorf("invalid traces retention period: %w", err)
	}
	// Assume 2h block size for traces, similar to metrics
	tracesStore, err := NewPromTSDBStore(cfg.Traces.DataPath, tracesRetention, 2*time.Hour, true)
	if err != nil {
		metricsStore.Close()
		logsStore.Close()
		return nil, fmt.Errorf("failed to initialize traces storage: %w", err)
	}
	manager.tracesStore = tracesStore

	return manager, nil
}

// Close closes all storage instances
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error

	if m.metricsStore != nil {
		if err := m.metricsStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close metrics store: %w", err))
		}
	}

	if m.logsStore != nil {
		if err := m.logsStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close logs store: %w", err))
		}
	}

	if m.tracesStore != nil {
		if err := m.tracesStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close traces store: %w", err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors occurred while closing storage: %v", errs)
	}
	return nil
}

// MetricsStore returns the metrics store
func (m *Manager) MetricsStore() *PromTSDBStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metricsStore
}

// LogsStore returns the logs store
func (m *Manager) LogsStore() *BadgerStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.logsStore
}

// TracesStore returns the traces store
func (m *Manager) TracesStore() *PromTSDBStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tracesStore
}

// ensureDir ensures that the specified directory exists
func ensureDir(path string) error {
	return os.MkdirAll(path, 0755)
}

// parseDuration parses a duration string (e.g., "30d", "2h")
func parseDuration(s string) (time.Duration, error) {
	// Custom parsing for days
	if len(s) > 0 && s[len(s)-1] == 'd' {
		var days int
		_, err := fmt.Sscanf(s, "%dd", &days)
		if err != nil {
			return 0, err
		}
		return time.Hour * 24 * time.Duration(days), nil
	}

	// Use Go's time.ParseDuration for standard duration formats
	return time.ParseDuration(s)
}
