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

// MetricsStorageConfig represents the metrics storage configuration
type MetricsStorageConfig struct {
	Engine          string      `json:"engine"`
	DataPath        string      `json:"dataPath"`
	RetentionPeriod string      `json:"retentionPeriod"`
	IndexConfig     IndexConfig `json:"indexConfig"`
}

// LogsStorageConfig represents the logs storage configuration
type LogsStorageConfig struct {
	Engine        string `json:"engine"`
	DataPath      string `json:"dataPath"`
	Indexing      bool   `json:"indexing"`
	MaxFileSizeMB int    `json:"maxFileSizeMB"`
}

// TracesStorageConfig represents the traces storage configuration
type TracesStorageConfig struct {
	Engine          string `json:"engine"`
	DataPath        string `json:"dataPath"`
	RetentionPeriod string `json:"retentionPeriod"`
}

// IndexConfig represents the index configuration
type IndexConfig struct {
	BlockSize  string `json:"blockSize"`
	Compaction bool   `json:"compaction"`
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

	// Validate retention periods
	if _, err := parseDuration(config.Storage.Metrics.RetentionPeriod); err != nil {
		return fmt.Errorf("invalid metrics retention period: %w", err)
	}
	if _, err := parseDuration(config.Storage.Traces.RetentionPeriod); err != nil {
		return fmt.Errorf("invalid traces retention period: %w", err)
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
