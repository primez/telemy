package ingestion

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"google.golang.org/grpc"

	"telemy/storage"
)

// MetricsHandler handles OTLP metrics ingestion
type MetricsHandler struct {
	store storage.MetricsStore
}

// NewMetricsHandler creates a new metrics handler
func NewMetricsHandler(store storage.MetricsStore) *MetricsHandler {
	return &MetricsHandler{
		store: store,
	}
}

// HandleHTTP handles OTLP HTTP metrics ingestion
func (h *MetricsHandler) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	// Read request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading request body: %v", err), http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Parse metrics
	var metrics OTLPMetricsRequest
	if err := json.Unmarshal(body, &metrics); err != nil {
		http.Error(w, fmt.Sprintf("Error parsing metrics: %v", err), http.StatusBadRequest)
		return
	}

	// Process metrics
	err = h.processMetrics(&metrics)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error processing metrics: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"success"}`))
}

// RegisterGRPC registers the gRPC service
func (h *MetricsHandler) RegisterGRPC(server *grpc.Server) {
	// For now, we'll just stub this out
	// We would need to implement the OTLP gRPC metrics service here
	// This would require generating the OTLP protobuf code
}

// processMetrics processes OTLP metrics and stores them in the TSDB
func (h *MetricsHandler) processMetrics(metrics *OTLPMetricsRequest) error {
	// Process each resource metrics
	for _, rm := range metrics.ResourceMetrics {
		// Extract resource attributes as labels
		resourceLabels := make(map[string]string)
		for _, attr := range rm.Resource.Attributes {
			resourceLabels[attr.Key] = attr.Value.StringValue
		}

		// Process each instrumentation library metrics
		for _, ilm := range rm.ScopeMetrics {
			// Process each metric
			for i := range ilm.Metrics {
				metric := &ilm.Metrics[i]
				// Process based on data type
				switch {
				case metric.Gauge != nil:
					err := h.processGauge(metric, resourceLabels)
					if err != nil {
						return err
					}
				case metric.Sum != nil:
					err := h.processSum(metric, resourceLabels)
					if err != nil {
						return err
					}
				case metric.Histogram != nil:
					err := h.processHistogram(metric, resourceLabels)
					if err != nil {
						return err
					}
				case metric.Summary != nil:
					err := h.processSummary(metric, resourceLabels)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

// processGauge processes gauge metrics
func (h *MetricsHandler) processGauge(metric *Metric, resourceLabels map[string]string) error {
	// Process each data point
	for _, dp := range metric.Gauge.DataPoints {
		// Combine resource labels with data point labels
		labels := copyLabels(resourceLabels)
		for _, attr := range dp.Attributes {
			labels[attr.Key] = attr.Value.StringValue
		}

		// Add metric name as a label
		labels["__name__"] = metric.Name

		// Create a data point
		point := &storage.DataPoint{
			Timestamp: time.Unix(0, dp.TimeUnixNano),
			Value:     dp.AsDouble,
			Labels:    labels,
		}

		// Store the data point
		if err := h.store.StoreMetric(point); err != nil {
			return fmt.Errorf("error storing gauge metric: %w", err)
		}
	}

	return nil
}

// processSum processes sum metrics
func (h *MetricsHandler) processSum(metric *Metric, resourceLabels map[string]string) error {
	// Process each data point
	for _, dp := range metric.Sum.DataPoints {
		// Combine resource labels with data point labels
		labels := copyLabels(resourceLabels)
		for _, attr := range dp.Attributes {
			labels[attr.Key] = attr.Value.StringValue
		}

		// Add metric name as a label
		labels["__name__"] = metric.Name

		// Create a data point
		point := &storage.DataPoint{
			Timestamp: time.Unix(0, dp.TimeUnixNano),
			Value:     dp.AsDouble,
			Labels:    labels,
		}

		// Store the data point
		if err := h.store.StoreMetric(point); err != nil {
			return fmt.Errorf("error storing sum metric: %w", err)
		}
	}

	return nil
}

// processHistogram processes histogram metrics
func (h *MetricsHandler) processHistogram(metric *Metric, resourceLabels map[string]string) error {
	// Process each data point
	for _, dp := range metric.Histogram.DataPoints {
		// Combine resource labels with data point labels
		labels := copyLabels(resourceLabels)
		for _, attr := range dp.Attributes {
			labels[attr.Key] = attr.Value.StringValue
		}

		// Add metric name as a label
		labels["__name__"] = metric.Name

		// Store count
		countLabels := copyLabels(labels)
		countLabels["__metric_type__"] = "count"
		countPoint := &storage.DataPoint{
			Timestamp: time.Unix(0, dp.TimeUnixNano),
			Value:     float64(dp.Count),
			Labels:    countLabels,
		}
		if err := h.store.StoreMetric(countPoint); err != nil {
			return fmt.Errorf("error storing histogram count: %w", err)
		}

		// Store sum
		sumLabels := copyLabels(labels)
		sumLabels["__metric_type__"] = "sum"
		sumPoint := &storage.DataPoint{
			Timestamp: time.Unix(0, dp.TimeUnixNano),
			Value:     dp.Sum,
			Labels:    sumLabels,
		}
		if err := h.store.StoreMetric(sumPoint); err != nil {
			return fmt.Errorf("error storing histogram sum: %w", err)
		}

		// Store each bucket
		for i, bound := range dp.ExplicitBounds {
			bucketLabels := copyLabels(labels)
			bucketLabels["__metric_type__"] = "bucket"
			bucketLabels["le"] = fmt.Sprintf("%g", bound)
			bucketPoint := &storage.DataPoint{
				Timestamp: time.Unix(0, dp.TimeUnixNano),
				Value:     float64(dp.BucketCounts[i]),
				Labels:    bucketLabels,
			}
			if err := h.store.StoreMetric(bucketPoint); err != nil {
				return fmt.Errorf("error storing histogram bucket: %w", err)
			}
		}
	}

	return nil
}

// processSummary processes summary metrics
func (h *MetricsHandler) processSummary(metric *Metric, resourceLabels map[string]string) error {
	// Process each data point
	for _, dp := range metric.Summary.DataPoints {
		// Combine resource labels with data point labels
		labels := copyLabels(resourceLabels)
		for _, attr := range dp.Attributes {
			labels[attr.Key] = attr.Value.StringValue
		}

		// Add metric name as a label
		labels["__name__"] = metric.Name

		// Store count
		countLabels := copyLabels(labels)
		countLabels["__metric_type__"] = "count"
		countPoint := &storage.DataPoint{
			Timestamp: time.Unix(0, dp.TimeUnixNano),
			Value:     float64(dp.Count),
			Labels:    countLabels,
		}
		if err := h.store.StoreMetric(countPoint); err != nil {
			return fmt.Errorf("error storing summary count: %w", err)
		}

		// Store sum
		sumLabels := copyLabels(labels)
		sumLabels["__metric_type__"] = "sum"
		sumPoint := &storage.DataPoint{
			Timestamp: time.Unix(0, dp.TimeUnixNano),
			Value:     dp.Sum,
			Labels:    sumLabels,
		}
		if err := h.store.StoreMetric(sumPoint); err != nil {
			return fmt.Errorf("error storing summary sum: %w", err)
		}

		// Store each quantile
		for _, quantile := range dp.QuantileValues {
			quantileLabels := copyLabels(labels)
			quantileLabels["__metric_type__"] = "quantile"
			quantileLabels["quantile"] = fmt.Sprintf("%g", quantile.Quantile)
			quantilePoint := &storage.DataPoint{
				Timestamp: time.Unix(0, dp.TimeUnixNano),
				Value:     quantile.Value,
				Labels:    quantileLabels,
			}
			if err := h.store.StoreMetric(quantilePoint); err != nil {
				return fmt.Errorf("error storing summary quantile: %w", err)
			}
		}
	}

	return nil
}

// copyLabels creates a copy of the labels map
func copyLabels(labels map[string]string) map[string]string {
	copy := make(map[string]string, len(labels))
	for k, v := range labels {
		copy[k] = v
	}
	return copy
}

// OTLPMetricsRequest represents an OTLP metrics request
type OTLPMetricsRequest struct {
	ResourceMetrics []ResourceMetrics `json:"resourceMetrics"`
}

// ResourceMetrics represents a resource metrics
type ResourceMetrics struct {
	Resource     Resource       `json:"resource"`
	ScopeMetrics []ScopeMetrics `json:"scopeMetrics"`
}

// Resource represents a resource
type Resource struct {
	Attributes []Attribute `json:"attributes"`
}

// ScopeMetrics represents instrumentation library metrics
type ScopeMetrics struct {
	Scope   Scope    `json:"scope"`
	Metrics []Metric `json:"metrics"`
}

// Scope represents an instrumentation scope
type Scope struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Metric represents a metric
type Metric struct {
	Name        string     `json:"name"`
	Description string     `json:"description"`
	Unit        string     `json:"unit"`
	Gauge       *Gauge     `json:"gauge,omitempty"`
	Sum         *Sum       `json:"sum,omitempty"`
	Histogram   *Histogram `json:"histogram,omitempty"`
	Summary     *Summary   `json:"summary,omitempty"`
}

// Gauge represents a gauge metric
type Gauge struct {
	DataPoints []NumberDataPoint `json:"dataPoints"`
}

// Sum represents a sum metric
type Sum struct {
	DataPoints             []NumberDataPoint `json:"dataPoints"`
	AggregationTemporality string            `json:"aggregationTemporality"`
	IsMonotonic            bool              `json:"isMonotonic"`
}

// Histogram represents a histogram metric
type Histogram struct {
	DataPoints             []HistogramDataPoint `json:"dataPoints"`
	AggregationTemporality string               `json:"aggregationTemporality"`
}

// Summary represents a summary metric
type Summary struct {
	DataPoints []SummaryDataPoint `json:"dataPoints"`
}

// NumberDataPoint represents a number data point
type NumberDataPoint struct {
	Attributes   []Attribute `json:"attributes"`
	TimeUnixNano int64       `json:"timeUnixNano"`
	AsDouble     float64     `json:"asDouble"`
}

// HistogramDataPoint represents a histogram data point
type HistogramDataPoint struct {
	Attributes     []Attribute `json:"attributes"`
	TimeUnixNano   int64       `json:"timeUnixNano"`
	Count          uint64      `json:"count"`
	Sum            float64     `json:"sum"`
	BucketCounts   []uint64    `json:"bucketCounts"`
	ExplicitBounds []float64   `json:"explicitBounds"`
}

// SummaryDataPoint represents a summary data point
type SummaryDataPoint struct {
	Attributes     []Attribute     `json:"attributes"`
	TimeUnixNano   int64           `json:"timeUnixNano"`
	Count          uint64          `json:"count"`
	Sum            float64         `json:"sum"`
	QuantileValues []QuantileValue `json:"quantileValues"`
}

// QuantileValue represents a quantile value
type QuantileValue struct {
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
}

// Attribute represents a key-value attribute
type Attribute struct {
	Key   string         `json:"key"`
	Value AttributeValue `json:"value"`
}

// AttributeValue represents an attribute value
type AttributeValue struct {
	StringValue string  `json:"stringValue,omitempty"`
	IntValue    int64   `json:"intValue,omitempty"`
	DoubleValue float64 `json:"doubleValue,omitempty"`
	BoolValue   bool    `json:"boolValue,omitempty"`
}
