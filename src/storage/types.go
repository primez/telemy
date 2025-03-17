package storage

import (
	"time"
)

// DataPoint represents a single data point in the TSDB
type DataPoint struct {
	Timestamp time.Time         `json:"timestamp"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// MetricQuery defines the parameters for querying metrics
type MetricQuery struct {
	StartTime   time.Time
	EndTime     time.Time
	LabelFilter func(map[string]string) bool
	Limit       int
}

// MetricsStore interface for storing and querying metrics
type MetricsStore interface {
	StoreMetric(point *DataPoint) error
	QueryMetrics(query *MetricQuery) ([]*DataPoint, error)
	Close() error
}

// LogsStore interface for storing and querying logs
type LogsStore interface {
	StoreLog(entry *LogEntry) error
	QueryLogs(query *LogQuery) ([]*LogEntry, error)
	Close() error
}

// TracesStore interface for storing and querying traces
// Currently using the same data structure as metrics
type TracesStore interface {
	StoreTrace(point *DataPoint) error
	QueryTraces(query *MetricQuery) ([]*DataPoint, error)
	Close() error
}
