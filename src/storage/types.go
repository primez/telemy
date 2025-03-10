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
