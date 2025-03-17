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
		// Using FrostDB, but with separate instances for each telemetry type
		log.Println("Using FrostDB with separate instances for each telemetry type")

		// Process metrics storage
		if cfg.Metrics.Engine != nil && cfg.Metrics.Engine.Type == "frostdb" &&
			cfg.Metrics.Engine.FrostDBConfig != nil {
			metricsPath := resolvePath(cfg.Metrics.DataPath)
			if err := ensureDir(metricsPath); err != nil {
				return nil, fmt.Errorf("failed to create metrics data directory: %w", err)
			}

			// Configure metrics FrostDB options
			metricsOptions := DefaultFrostDBOptions()

			// Apply metrics configuration
			if cfg.Metrics.Engine.FrostDBConfig.BatchSize > 0 {
				metricsOptions.BatchSize = cfg.Metrics.Engine.FrostDBConfig.BatchSize
				log.Printf("Metrics FrostDB batch size: %d", metricsOptions.BatchSize)
			}

			if cfg.Metrics.Engine.FrostDBConfig.FlushInterval != "" {
				flushInterval, err := parseDuration(cfg.Metrics.Engine.FrostDBConfig.FlushInterval)
				if err != nil {
					return nil, fmt.Errorf("invalid metrics FrostDB flush interval: %w", err)
				}
				metricsOptions.FlushInterval = flushInterval
				log.Printf("Metrics FrostDB flush interval: %s", metricsOptions.FlushInterval)
			}

			if cfg.Metrics.Engine.FrostDBConfig.ActiveMemoryMB > 0 {
				metricsOptions.ActiveMemorySize = int64(cfg.Metrics.Engine.FrostDBConfig.ActiveMemoryMB) * 1024 * 1024
				log.Printf("Metrics FrostDB active memory size: %d MB", cfg.Metrics.Engine.FrostDBConfig.ActiveMemoryMB)
			}

			if cfg.Metrics.Engine.FrostDBConfig.WALEnabled != metricsOptions.WALEnabled {
				metricsOptions.WALEnabled = cfg.Metrics.Engine.FrostDBConfig.WALEnabled
				log.Printf("Metrics FrostDB WAL enabled: %v", metricsOptions.WALEnabled)
			}

			// Get metrics retention period
			var metricsRetention time.Duration
			if cfg.Metrics.Engine.FrostDBConfig.RetentionPeriod != "" {
				var err error
				metricsRetention, err = parseDuration(cfg.Metrics.Engine.FrostDBConfig.RetentionPeriod)
				if err != nil {
					return nil, fmt.Errorf("invalid metrics retention period: %w", err)
				}
			} else {
				// Default retention if not specified
				metricsRetention = 30 * 24 * time.Hour // 30 days
			}
			log.Printf("Metrics retention period: %v", metricsRetention)

			// Set metrics-specific options
			metricsOptions.TableName = "metrics"

			// Create metrics FrostDB store
			metricsStore, err := NewFrostDBStore(metricsPath, metricsRetention, metricsOptions)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize metrics FrostDB storage: %w", err)
			}
			manager.metricsStore = metricsStore
		}

		// Process logs storage
		if cfg.Logs.Engine != nil && cfg.Logs.Engine.Type == "frostdb" &&
			cfg.Logs.Engine.FrostDBConfig != nil {
			logsPath := resolvePath(cfg.Logs.DataPath)
			if err := ensureDir(logsPath); err != nil {
				return nil, fmt.Errorf("failed to create logs data directory: %w", err)
			}

			// Configure logs FrostDB options
			logsOptions := DefaultFrostDBOptions()

			// Apply logs configuration
			if cfg.Logs.Engine.FrostDBConfig.BatchSize > 0 {
				logsOptions.BatchSize = cfg.Logs.Engine.FrostDBConfig.BatchSize
				log.Printf("Logs FrostDB batch size: %d", logsOptions.BatchSize)
			}

			if cfg.Logs.Engine.FrostDBConfig.FlushInterval != "" {
				flushInterval, err := parseDuration(cfg.Logs.Engine.FrostDBConfig.FlushInterval)
				if err != nil {
					return nil, fmt.Errorf("invalid logs FrostDB flush interval: %w", err)
				}
				logsOptions.FlushInterval = flushInterval
				log.Printf("Logs FrostDB flush interval: %s", logsOptions.FlushInterval)
			}

			if cfg.Logs.Engine.FrostDBConfig.ActiveMemoryMB > 0 {
				logsOptions.ActiveMemorySize = int64(cfg.Logs.Engine.FrostDBConfig.ActiveMemoryMB) * 1024 * 1024
				log.Printf("Logs FrostDB active memory size: %d MB", cfg.Logs.Engine.FrostDBConfig.ActiveMemoryMB)
			}

			if cfg.Logs.Engine.FrostDBConfig.WALEnabled != logsOptions.WALEnabled {
				logsOptions.WALEnabled = cfg.Logs.Engine.FrostDBConfig.WALEnabled
				log.Printf("Logs FrostDB WAL enabled: %v", logsOptions.WALEnabled)
			}

			// Get logs retention period
			var logsRetention time.Duration
			if cfg.Logs.Engine.FrostDBConfig.RetentionPeriod != "" {
				var err error
				logsRetention, err = parseDuration(cfg.Logs.Engine.FrostDBConfig.RetentionPeriod)
				if err != nil {
					return nil, fmt.Errorf("invalid logs retention period: %w", err)
				}
			} else {
				// Default retention if not specified
				logsRetention = 14 * 24 * time.Hour // 14 days
			}
			log.Printf("Logs retention period: %v", logsRetention)

			// Set logs-specific options
			logsOptions.TableName = "logs"

			// Create logs FrostDB store
			logsStore, err := NewFrostDBStore(logsPath, logsRetention, logsOptions)
			if err != nil {
				if manager.metricsStore != nil {
					manager.metricsStore.Close()
				}
				return nil, fmt.Errorf("failed to initialize logs FrostDB storage: %w", err)
			}
			manager.logsStore = logsStore
		}

		// Process traces storage
		if cfg.Traces.Engine != nil && cfg.Traces.Engine.Type == "frostdb" &&
			cfg.Traces.Engine.FrostDBConfig != nil {
			tracesPath := resolvePath(cfg.Traces.DataPath)
			if err := ensureDir(tracesPath); err != nil {
				return nil, fmt.Errorf("failed to create traces data directory: %w", err)
			}

			// Configure traces FrostDB options
			tracesOptions := DefaultFrostDBOptions()

			// Apply traces configuration
			if cfg.Traces.Engine.FrostDBConfig.BatchSize > 0 {
				tracesOptions.BatchSize = cfg.Traces.Engine.FrostDBConfig.BatchSize
				log.Printf("Traces FrostDB batch size: %d", tracesOptions.BatchSize)
			}

			if cfg.Traces.Engine.FrostDBConfig.FlushInterval != "" {
				flushInterval, err := parseDuration(cfg.Traces.Engine.FrostDBConfig.FlushInterval)
				if err != nil {
					return nil, fmt.Errorf("invalid traces FrostDB flush interval: %w", err)
				}
				tracesOptions.FlushInterval = flushInterval
				log.Printf("Traces FrostDB flush interval: %s", tracesOptions.FlushInterval)
			}

			if cfg.Traces.Engine.FrostDBConfig.ActiveMemoryMB > 0 {
				tracesOptions.ActiveMemorySize = int64(cfg.Traces.Engine.FrostDBConfig.ActiveMemoryMB) * 1024 * 1024
				log.Printf("Traces FrostDB active memory size: %d MB", cfg.Traces.Engine.FrostDBConfig.ActiveMemoryMB)
			}

			if cfg.Traces.Engine.FrostDBConfig.WALEnabled != tracesOptions.WALEnabled {
				tracesOptions.WALEnabled = cfg.Traces.Engine.FrostDBConfig.WALEnabled
				log.Printf("Traces FrostDB WAL enabled: %v", tracesOptions.WALEnabled)
			}

			// Get traces retention period
			var tracesRetention time.Duration
			if cfg.Traces.Engine.FrostDBConfig.RetentionPeriod != "" {
				var err error
				tracesRetention, err = parseDuration(cfg.Traces.Engine.FrostDBConfig.RetentionPeriod)
				if err != nil {
					return nil, fmt.Errorf("invalid traces retention period: %w", err)
				}
			} else {
				// Default retention if not specified
				tracesRetention = 7 * 24 * time.Hour // 7 days
			}
			log.Printf("Traces retention period: %v", tracesRetention)

			// Set traces-specific options
			tracesOptions.TableName = "traces"

			// Create traces FrostDB store
			tracesStore, err := NewFrostDBStore(tracesPath, tracesRetention, tracesOptions)
			if err != nil {
				if manager.metricsStore != nil {
					manager.metricsStore.Close()
				}
				if manager.logsStore != nil {
					manager.logsStore.Close()
				}
				return nil, fmt.Errorf("failed to initialize traces FrostDB storage: %w", err)
			}
			manager.tracesStore = tracesStore
		}
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
