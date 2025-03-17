package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Config represents the top-level configuration structure
type Config struct {
	Service     ServiceConfig     `json:"service"`
	Ingestion   IngestionConfig   `json:"ingestion"`
	Storage     StorageConfig     `json:"storage"`
	Dashboard   DashboardConfig   `json:"dashboard"`
	Aggregation AggregationConfig `json:"aggregation"`
	Alerts      AlertsConfig      `json:"alerts"`
}

// ServiceConfig represents the service configuration section
type ServiceConfig struct {
	Name      string   `json:"name"`
	Port      int      `json:"port"`
	Protocols []string `json:"protocols"`
	LogLevel  string   `json:"logLevel"`
}

// IngestionConfig represents the ingestion configuration section
type IngestionConfig struct {
	OTLP OTLPConfig `json:"otlp"`
}

// OTLPConfig represents the OTLP configuration
type OTLPConfig struct {
	HTTPEndpoint string `json:"httpEndpoint"`
	GRPCEndpoint string `json:"grpcEndpoint"`
}

// StorageConfig represents the storage configuration section
type StorageConfig struct {
	Metrics MetricsStorageConfig `json:"metrics"`
	Logs    LogsStorageConfig    `json:"logs"`
	Traces  TracesStorageConfig  `json:"traces"`
}

// EngineConfig is a generic interface for engine configurations
type EngineConfig struct {
	Type string `json:"type"`
	// Engine-specific configuration is handled by the specific engine types
	// and unmarshaled from the same JSON object
	TSDBConfig    *TSDBConfig    `json:"-"`
	BadgerConfig  *BadgerConfig  `json:"-"`
	FrostDBConfig *FrostDBConfig `json:"-"`
}

// UnmarshalJSON implements the json.Unmarshaler interface to handle
// engine configuration objects with different structures based on type
func (ec *EngineConfig) UnmarshalJSON(data []byte) error {
	// First, parse the type field
	type engineType struct {
		Type string `json:"type"`
	}
	var et engineType
	if err := json.Unmarshal(data, &et); err != nil {
		return err
	}
	ec.Type = et.Type

	// Based on the type, unmarshal to the appropriate config struct
	switch ec.Type {
	case "tsdb":
		var conf TSDBConfig
		if err := json.Unmarshal(data, &conf); err != nil {
			return err
		}
		ec.TSDBConfig = &conf
	case "badger":
		var conf BadgerConfig
		if err := json.Unmarshal(data, &conf); err != nil {
			return err
		}
		ec.BadgerConfig = &conf
	case "frostdb":
		var conf FrostDBConfig
		if err := json.Unmarshal(data, &conf); err != nil {
			return err
		}
		ec.FrostDBConfig = &conf
	default:
		// For simple string-based engines with no config, do nothing
	}
	return nil
}

// MarshalJSON implements the json.Marshaler interface to convert the engine
// configuration back to JSON
func (ec *EngineConfig) MarshalJSON() ([]byte, error) {
	// Create a map to hold all the fields
	m := make(map[string]interface{})
	m["type"] = ec.Type

	// Add the specific engine configuration fields
	var additionalFields map[string]interface{}

	switch ec.Type {
	case "tsdb":
		if ec.TSDBConfig != nil {
			tsdbJSON, err := json.Marshal(ec.TSDBConfig)
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(tsdbJSON, &additionalFields); err != nil {
				return nil, err
			}
		}
	case "badger":
		if ec.BadgerConfig != nil {
			badgerJSON, err := json.Marshal(ec.BadgerConfig)
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(badgerJSON, &additionalFields); err != nil {
				return nil, err
			}
		}
	case "frostdb":
		if ec.FrostDBConfig != nil {
			frostdbJSON, err := json.Marshal(ec.FrostDBConfig)
			if err != nil {
				return nil, err
			}
			if err := json.Unmarshal(frostdbJSON, &additionalFields); err != nil {
				return nil, err
			}
		}
	}

	// Merge the additional fields
	for k, v := range additionalFields {
		if k != "type" { // Avoid overwriting the type
			m[k] = v
		}
	}

	return json.Marshal(m)
}

// TSDBConfig represents TSDB-specific configuration
type TSDBConfig struct {
	BlockSize       string `json:"blockSize,omitempty"`
	Compaction      bool   `json:"compaction,omitempty"`
	RetentionPeriod string `json:"retentionPeriod,omitempty"`
}

// BadgerConfig represents BadgerDB-specific configuration
type BadgerConfig struct {
	MaxFileSizeMB int  `json:"maxFileSizeMB,omitempty"`
	Indexing      bool `json:"indexing,omitempty"`
}

// FrostDBConfig represents FrostDB-specific configuration
type FrostDBConfig struct {
	BatchSize       int    `json:"batchSize,omitempty"`
	FlushInterval   string `json:"flushInterval,omitempty"`
	ActiveMemoryMB  int    `json:"activeMemoryMB,omitempty"`
	WALEnabled      bool   `json:"walEnabled,omitempty"`
	UseSettingsFrom string `json:"useSettingsFrom,omitempty"`
	RetentionPeriod string `json:"retentionPeriod,omitempty"`
	Indexing        bool   `json:"indexing,omitempty"`
}

// MetricsStorageConfig represents the metrics storage configuration
type MetricsStorageConfig struct {
	Engine   *EngineConfig `json:"engine"`
	DataPath string        `json:"dataPath"`
}

// LogsStorageConfig represents the logs storage configuration
type LogsStorageConfig struct {
	Engine   *EngineConfig `json:"engine"`
	DataPath string        `json:"dataPath"`
}

// TracesStorageConfig represents the traces storage configuration
type TracesStorageConfig struct {
	Engine   *EngineConfig `json:"engine"`
	DataPath string        `json:"dataPath"`
}

// DashboardConfig represents the dashboard configuration section
type DashboardConfig struct {
	DefaultView string         `json:"defaultView"`
	Widgets     []WidgetConfig `json:"widgets"`
}

// WidgetConfig represents a dashboard widget configuration
type WidgetConfig struct {
	Type            string `json:"type"`
	Title           string `json:"title"`
	DataSource      string `json:"dataSource"`
	Query           string `json:"query"`
	RefreshInterval string `json:"refreshInterval"`
	MaxRows         int    `json:"maxRows,omitempty"`
}

// AggregationConfig represents the aggregation configuration section
type AggregationConfig struct {
	Functions          []string                  `json:"functions"`
	CustomAggregations []CustomAggregationConfig `json:"customAggregations"`
}

// CustomAggregationConfig represents a custom aggregation configuration
type CustomAggregationConfig struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
}

// AlertsConfig represents the alerts configuration section
type AlertsConfig struct {
	Email EmailConfig `json:"email"`
	Rules []AlertRule `json:"rules"`
}

// EmailConfig represents the email configuration for alerts
type EmailConfig struct {
	Enabled     bool              `json:"enabled"`
	SMTPServer  string            `json:"smtpServer"`
	SMTPPort    int               `json:"smtpPort"`
	Username    string            `json:"username"`
	Password    string            `json:"password"`
	FromAddress string            `json:"fromAddress"`
	ToAddresses []string          `json:"toAddresses"`
	Templates   map[string]string `json:"templates"`
}

// AlertRule represents an alert rule configuration
type AlertRule struct {
	Name       string `json:"name"`
	DataSource string `json:"dataSource"`
	Query      string `json:"query"`
	Duration   string `json:"duration"`
	Severity   string `json:"severity"`
}

// LoadConfig loads and parses the configuration file
func LoadConfig(configPath string) (*Config, error) {
	// Read the configuration file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %w", err)
	}

	// Parse the JSON configuration
	var config Config
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("error parsing config file: %w", err)
	}

	// Validate the configuration
	err = validateConfig(&config)
	if err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &config, nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	// Validate service configuration
	if config.Service.Name == "" {
		return fmt.Errorf("service name is required")
	}
	if config.Service.Port <= 0 || config.Service.Port > 65535 {
		return fmt.Errorf("invalid service port: %d", config.Service.Port)
	}

	// Validate storage paths
	if config.Storage.Metrics.DataPath == "" {
		return fmt.Errorf("metrics data path is required")
	}
	if config.Storage.Logs.DataPath == "" {
		return fmt.Errorf("logs data path is required")
	}
	if config.Storage.Traces.DataPath == "" {
		return fmt.Errorf("traces data path is required")
	}

	// Validate retention periods if specified
	if config.Storage.Metrics.Engine != nil {
		var retentionPeriod string

		// Check retention period based on engine type
		switch config.Storage.Metrics.Engine.Type {
		case "tsdb":
			if config.Storage.Metrics.Engine.TSDBConfig != nil {
				retentionPeriod = config.Storage.Metrics.Engine.TSDBConfig.RetentionPeriod
			}
		case "frostdb":
			if config.Storage.Metrics.Engine.FrostDBConfig != nil {
				retentionPeriod = config.Storage.Metrics.Engine.FrostDBConfig.RetentionPeriod
			}
		}

		if retentionPeriod != "" {
			if _, err := parseDuration(retentionPeriod); err != nil {
				return fmt.Errorf("invalid metrics retention period: %w", err)
			}
		}
	}

	if config.Storage.Traces.Engine != nil {
		var retentionPeriod string

		// Check retention period based on engine type
		switch config.Storage.Traces.Engine.Type {
		case "tsdb":
			if config.Storage.Traces.Engine.TSDBConfig != nil {
				retentionPeriod = config.Storage.Traces.Engine.TSDBConfig.RetentionPeriod
			}
		case "frostdb":
			if config.Storage.Traces.Engine.FrostDBConfig != nil {
				retentionPeriod = config.Storage.Traces.Engine.FrostDBConfig.RetentionPeriod
			}
		}

		if retentionPeriod != "" {
			if _, err := parseDuration(retentionPeriod); err != nil {
				return fmt.Errorf("invalid traces retention period: %w", err)
			}
		}
	}

	// Validate ingestion endpoints
	if config.Ingestion.OTLP.HTTPEndpoint == "" && config.Ingestion.OTLP.GRPCEndpoint == "" {
		return fmt.Errorf("at least one OTLP endpoint (HTTP or GRPC) must be configured")
	}

	// Validate alert configuration if email alerts are enabled
	if config.Alerts.Email.Enabled {
		if config.Alerts.Email.SMTPServer == "" {
			return fmt.Errorf("SMTP server is required when email alerts are enabled")
		}
		if config.Alerts.Email.SMTPPort <= 0 || config.Alerts.Email.SMTPPort > 65535 {
			return fmt.Errorf("invalid SMTP port: %d", config.Alerts.Email.SMTPPort)
		}
		if config.Alerts.Email.FromAddress == "" {
			return fmt.Errorf("from address is required when email alerts are enabled")
		}
		if len(config.Alerts.Email.ToAddresses) == 0 {
			return fmt.Errorf("at least one recipient address is required when email alerts are enabled")
		}
	}

	return nil
}

// parseDuration parses a duration string (e.g., "30d", "2h")
func parseDuration(s string) (time.Duration, error) {
	// Custom parsing for days
	if len(s) > 0 && s[len(s)-1] == 'd' {
		days, err := parseInt(s[:len(s)-1])
		if err != nil {
			return 0, err
		}
		return time.Hour * 24 * time.Duration(days), nil
	}

	// Use Go's time.ParseDuration for standard duration formats
	return time.ParseDuration(s)
}

// parseInt parses an integer string
func parseInt(s string) (int, error) {
	var result int
	_, err := fmt.Sscanf(s, "%d", &result)
	return result, err
}
