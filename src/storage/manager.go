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
	if cfg.Metrics.Engine != nil && cfg.Metrics.Engine.Type == "frostdb" {
		// Use FrostDB for all storage types
		log.Println("Using FrostDB for all storage types")

		// Initialize FrostDB storage
		frostdbPath := resolvePath(filepath.Join(filepath.Dir(cfg.Metrics.DataPath), "frostdb"))
		if err := ensureDir(frostdbPath); err != nil {
			return nil, fmt.Errorf("failed to create FrostDB data directory: %w", err)
		}

		// Get FrostDB config and determine which one to use as the primary
		var primaryConfig *config.FrostDBConfig
		var primarySource string
		var retention time.Duration
		var retentionSource string

		// First check if logs or traces have UseSettingsFrom set
		var logsUseSettingsFrom string
		var tracesUseSettingsFrom string

		if cfg.Logs.Engine != nil && cfg.Logs.Engine.Type == "frostdb" &&
			cfg.Logs.Engine.FrostDBConfig != nil {
			logsUseSettingsFrom = cfg.Logs.Engine.FrostDBConfig.UseSettingsFrom
		}

		if cfg.Traces.Engine != nil && cfg.Traces.Engine.Type == "frostdb" &&
			cfg.Traces.Engine.FrostDBConfig != nil {
			tracesUseSettingsFrom = cfg.Traces.Engine.FrostDBConfig.UseSettingsFrom
		}

		// Determine which config to use as primary
		if cfg.Metrics.Engine.FrostDBConfig != nil {
			primaryConfig = cfg.Metrics.Engine.FrostDBConfig
			primarySource = "metrics"

			// Get retention from metrics
			if primaryConfig.RetentionPeriod != "" {
				var err error
				retention, err = parseDuration(primaryConfig.RetentionPeriod)
				if err != nil {
					return nil, fmt.Errorf("invalid metrics retention period: %w", err)
				}
				retentionSource = "metrics"
			}
		}

		// Check if logs should be primary
		if logsUseSettingsFrom == "" && tracesUseSettingsFrom == "logs" {
			if cfg.Logs.Engine != nil && cfg.Logs.Engine.Type == "frostdb" &&
				cfg.Logs.Engine.FrostDBConfig != nil {
				primaryConfig = cfg.Logs.Engine.FrostDBConfig
				primarySource = "logs"
			}
		}

		// Check if traces should be primary
		if tracesUseSettingsFrom == "" &&
			(logsUseSettingsFrom == "traces" || primarySource == "") {
			if cfg.Traces.Engine != nil && cfg.Traces.Engine.Type == "frostdb" &&
				cfg.Traces.Engine.FrostDBConfig != nil {
				primaryConfig = cfg.Traces.Engine.FrostDBConfig
				primarySource = "traces"

				// Get retention from traces
				if primaryConfig.RetentionPeriod != "" && retentionSource == "" {
					var err error
					retention, err = parseDuration(primaryConfig.RetentionPeriod)
					if err != nil {
						return nil, fmt.Errorf("invalid traces retention period: %w", err)
					}
					retentionSource = "traces"
				}
			}
		}

		// If no primary config was found, use default settings
		if primaryConfig == nil {
			log.Println("No FrostDB configuration found, using defaults")
			primaryConfig = &config.FrostDBConfig{}
		} else {
			log.Printf("Using FrostDB configuration from %s section", primarySource)
		}

		// Default retention if not specified
		if retention == 0 {
			retention = 30 * 24 * time.Hour // 30 days default
			log.Printf("No retention period specified, using default: %v", retention)
		} else {
			log.Printf("Using retention period from %s: %v", retentionSource, retention)
		}

		// Configure FrostDB options
		frostdbOptions := DefaultFrostDBOptions()

		// Apply custom configuration from the primary config
		if primaryConfig.BatchSize > 0 {
			frostdbOptions.BatchSize = primaryConfig.BatchSize
			log.Printf("Using FrostDB batch size: %d", frostdbOptions.BatchSize)
		}

		if primaryConfig.FlushInterval != "" {
			flushInterval, err := parseDuration(primaryConfig.FlushInterval)
			if err != nil {
				return nil, fmt.Errorf("invalid FrostDB flush interval: %w", err)
			}
			frostdbOptions.FlushInterval = flushInterval
			log.Printf("Using FrostDB flush interval: %s", frostdbOptions.FlushInterval)
		}

		if primaryConfig.ActiveMemoryMB > 0 {
			frostdbOptions.ActiveMemorySize = int64(primaryConfig.ActiveMemoryMB) * 1024 * 1024 // Convert to bytes
			log.Printf("Using FrostDB active memory size: %d MB", primaryConfig.ActiveMemoryMB)
		}

		// Only apply WAL config if explicitly set (maintain backward compatibility)
		if primaryConfig.WALEnabled != frostdbOptions.WALEnabled {
			frostdbOptions.WALEnabled = primaryConfig.WALEnabled
			log.Printf("FrostDB WAL enabled: %v", frostdbOptions.WALEnabled)
		}

		frostStore, err := NewFrostDBStore(frostdbPath, retention, frostdbOptions)
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
		metricsRetention := time.Hour * 24 * 30 // Default 30 days
		var metricsBlockSize time.Duration
		var metricsCompaction bool
		var err error

		// Get TSDB-specific configuration if available
		if cfg.Metrics.Engine != nil && cfg.Metrics.Engine.Type == "tsdb" && cfg.Metrics.Engine.TSDBConfig != nil {
			if cfg.Metrics.Engine.TSDBConfig.RetentionPeriod != "" {
				metricsRetention, err = parseDuration(cfg.Metrics.Engine.TSDBConfig.RetentionPeriod)
				if err != nil {
					return nil, fmt.Errorf("invalid metrics retention period: %w", err)
				}
			}

			if cfg.Metrics.Engine.TSDBConfig.BlockSize != "" {
				metricsBlockSize, err = parseDuration(cfg.Metrics.Engine.TSDBConfig.BlockSize)
				if err != nil {
					return nil, fmt.Errorf("invalid metrics block size: %w", err)
				}
			} else {
				// Default block size if not specified
				metricsBlockSize, _ = parseDuration("2h")
			}
			metricsCompaction = cfg.Metrics.Engine.TSDBConfig.Compaction
		} else {
			// Default values if not using TSDB config
			metricsBlockSize, _ = parseDuration("2h")
			metricsCompaction = true
		}

		metricsPath := resolvePath(cfg.Metrics.DataPath)
		metricsStore, err := NewPromTSDBStore(metricsPath, metricsRetention, metricsBlockSize, metricsCompaction)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize metrics storage: %w", err)
		}
		manager.metricsStore = metricsStore

		// Initialize logs storage
		logsPath := resolvePath(cfg.Logs.DataPath)
		var logsIndexing bool
		var logsMaxFileSizeMB int

		if cfg.Logs.Engine != nil && cfg.Logs.Engine.Type == "badger" && cfg.Logs.Engine.BadgerConfig != nil {
			logsMaxFileSizeMB = cfg.Logs.Engine.BadgerConfig.MaxFileSizeMB
			logsIndexing = cfg.Logs.Engine.BadgerConfig.Indexing
		} else {
			// Default values if not specified
			logsMaxFileSizeMB = 100
			logsIndexing = true
		}

		logsStore, err := NewBadgerStore(logsPath, logsIndexing, logsMaxFileSizeMB)
		if err != nil {
			metricsStore.Close()
			return nil, fmt.Errorf("failed to initialize logs storage: %w", err)
		}
		manager.logsStore = logsStore

		// Initialize traces storage
		tracesPath := resolvePath(cfg.Traces.DataPath)
		var tracesRetention time.Duration

		if cfg.Traces.Engine != nil && cfg.Traces.Engine.Type == "tsdb" && cfg.Traces.Engine.TSDBConfig != nil &&
			cfg.Traces.Engine.TSDBConfig.RetentionPeriod != "" {
			tracesRetention, err = parseDuration(cfg.Traces.Engine.TSDBConfig.RetentionPeriod)
			if err != nil {
				return nil, fmt.Errorf("invalid traces retention period: %w", err)
			}
		} else {
			// Default traces retention
			tracesRetention = 7 * 24 * time.Hour // 7 days
		}

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
