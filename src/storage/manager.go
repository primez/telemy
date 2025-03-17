package storage

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"telemy/config"
)

// Manager manages storage for metrics, logs, and traces
type Manager struct {
	config       config.StorageConfig
	metricsStore MetricsStore
	logsStore    LogsStore
	tracesStore  TracesStore
	mu           sync.RWMutex
}

// NewManager creates a new storage manager
func NewManager(cfg config.StorageConfig) (*Manager, error) {
	// Create a new manager
	manager := &Manager{
		config: cfg,
	}

	// Ensure data directories exist
	log.Println("Ensuring data directories exist...")
	if err := ensureDir(cfg.Metrics.DataPath); err != nil {
		return nil, fmt.Errorf("failed to create metrics data directory: %w", err)
	}
	if err := ensureDir(cfg.Logs.DataPath); err != nil {
		return nil, fmt.Errorf("failed to create logs data directory: %w", err)
	}
	if err := ensureDir(cfg.Traces.DataPath); err != nil {
		return nil, fmt.Errorf("failed to create traces data directory: %w", err)
	}

	// Log the data directories once
	log.Printf("Metrics data path: %s", resolvePath(cfg.Metrics.DataPath))
	log.Printf("Logs data path: %s", resolvePath(cfg.Logs.DataPath))
	log.Printf("Traces data path: %s", resolvePath(cfg.Traces.DataPath))

	// Check if using FrostDB or the original storage engines
	if cfg.Metrics.Engine == "frostdb" {
		// Use FrostDB for all storage types
		log.Println("Using FrostDB for all storage types")

		// Initialize FrostDB storage
		frostdbPath := resolvePath(filepath.Join(filepath.Dir(cfg.Metrics.DataPath), "frostdb"))
		if err := ensureDir(frostdbPath); err != nil {
			return nil, fmt.Errorf("failed to create FrostDB data directory: %w", err)
		}

		// Create shared FrostDB instance
		retention, err := parseDuration(cfg.Metrics.RetentionPeriod)
		if err != nil {
			return nil, fmt.Errorf("invalid retention period: %w", err)
		}

		frostStore, err := NewFrostDBStore(frostdbPath, retention)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize FrostDB storage: %w", err)
		}

		// Use the same store for all three types
		manager.metricsStore = frostStore
		manager.logsStore = frostStore
		manager.tracesStore = frostStore
	} else {
		// Use original storage engines (TSDB and BadgerDB)
		log.Println("Using original storage engines (TSDB and BadgerDB)")

		// Initialize metrics storage
		metricsRetention, err := parseDuration(cfg.Metrics.RetentionPeriod)
		if err != nil {
			return nil, fmt.Errorf("invalid metrics retention period: %w", err)
		}
		metricsBlockSize, err := parseDuration(cfg.Metrics.IndexConfig.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("invalid metrics block size: %w", err)
		}
		metricsPath := resolvePath(cfg.Metrics.DataPath)
		metricsStore, err := NewPromTSDBStore(metricsPath, metricsRetention, metricsBlockSize, cfg.Metrics.IndexConfig.Compaction)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize metrics storage: %w", err)
		}
		manager.metricsStore = metricsStore

		// Initialize logs storage
		logsPath := resolvePath(cfg.Logs.DataPath)
		logsStore, err := NewBadgerStore(logsPath, cfg.Logs.Indexing, cfg.Logs.MaxFileSizeMB)
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
		tracesPath := resolvePath(cfg.Traces.DataPath)
		tracesStore, err := NewPromTSDBStore(tracesPath, tracesRetention, 2*time.Hour, true)
		if err != nil {
			metricsStore.Close()
			logsStore.Close()
			return nil, fmt.Errorf("failed to initialize traces storage: %w", err)
		}
		manager.tracesStore = tracesStore
	}

	return manager, nil
}

// Close closes all storage engines
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Println("Closing storage managers...")
	var errs []error

	if m.metricsStore != nil {
		if err := m.metricsStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing metrics store: %w", err))
		}
	}

	if m.logsStore != nil {
		if err := m.logsStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing logs store: %w", err))
		}
	}

	if m.tracesStore != nil {
		if err := m.tracesStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("error closing traces store: %w", err))
		}
	}

	if len(errs) > 0 {
		errMsgs := []string{}
		for _, err := range errs {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("errors closing storage: %s", strings.Join(errMsgs, "; "))
	}

	log.Println("All storage managers closed successfully")
	return nil
}

// MetricsStore returns the metrics store
func (m *Manager) MetricsStore() MetricsStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.metricsStore
}

// LogsStore returns the logs store
func (m *Manager) LogsStore() LogsStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.logsStore
}

// TracesStore returns the traces store
func (m *Manager) TracesStore() TracesStore {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tracesStore
}

// ensureDir ensures that the specified directory exists
func ensureDir(path string) error {
	resolvedPath := resolvePath(path)
	return os.MkdirAll(resolvedPath, 0755)
}

// resolvePath resolves a path relative to the application root directory
func resolvePath(path string) string {
	// Already absolute, return as is
	if filepath.IsAbs(path) {
		return path
	}

	// Get the executable directory to find the app root
	execPath, err := os.Executable()
	if err != nil {
		// If we can't get the executable path, just use the path as is
		log.Printf("Warning: failed to get executable path: %v", err)
		return path
	}

	// The executable is at the app root, so resolve relative to that directory
	appRoot := filepath.Dir(execPath)

	// If path starts with "./", remove it for clarity
	originalPath := path
	path = strings.TrimPrefix(path, "./")

	// If path starts with "../", it's already trying to go to root from config dir
	// so replace it with direct path from app root
	path = strings.TrimPrefix(path, "../")

	// Combine the app root with the relative path
	resolvedPath := filepath.Join(appRoot, path)

	// Only log when converting relative paths
	if originalPath != resolvedPath {
		log.Printf("Resolved path: %s -> %s", originalPath, resolvedPath)
	}

	return resolvedPath
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
