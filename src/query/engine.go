package query

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"telemy/storage"
)

// Engine is the query engine for executing queries against the storage
type Engine struct {
	storageManager *storage.Manager
}

// NewEngine creates a new query engine
func NewEngine(storageManager *storage.Manager) (*Engine, error) {
	return &Engine{
		storageManager: storageManager,
	}, nil
}

// QueryResult represents a query result
type QueryResult struct {
	Data   interface{} `json:"data"`
	Status string      `json:"status"`
}

// MetricsQueryResult represents a metrics query result
type MetricsQueryResult struct {
	ResultType string         `json:"resultType"`
	Result     []MetricSeries `json:"result"`
}

// MetricSeries represents a series of metric data points
type MetricSeries struct {
	Metric map[string]string `json:"metric"`
	Values [][]interface{}   `json:"values"`
}

// LogsQueryResult represents a logs query result
type LogsQueryResult struct {
	Logs []LogEntry `json:"logs"`
}

// LogEntry represents a log entry
type LogEntry struct {
	Timestamp time.Time         `json:"timestamp"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// TracesQueryResult represents a traces query result
type TracesQueryResult struct {
	Traces []Trace `json:"traces"`
}

// Trace represents a trace
type Trace struct {
	TraceID     string    `json:"traceId"`
	Name        string    `json:"name"`
	Duration    float64   `json:"duration"`
	StartTime   time.Time `json:"startTime"`
	EndTime     time.Time `json:"endTime"`
	Status      string    `json:"status"`
	ServiceName string    `json:"serviceName"`
	Spans       []Span    `json:"spans"`
}

// Span represents a span in a trace
type Span struct {
	SpanID       string            `json:"spanId"`
	ParentSpanID string            `json:"parentSpanId,omitempty"`
	Name         string            `json:"name"`
	Duration     float64           `json:"duration"`
	StartTime    time.Time         `json:"startTime"`
	EndTime      time.Time         `json:"endTime"`
	Status       string            `json:"status"`
	Kind         string            `json:"kind"`
	Labels       map[string]string `json:"labels,omitempty"`
	Events       []SpanEvent       `json:"events,omitempty"`
}

// SpanEvent represents an event in a span
type SpanEvent struct {
	Name       string            `json:"name"`
	Timestamp  time.Time         `json:"timestamp"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// QueryMetrics executes a metrics query
func (e *Engine) QueryMetrics(query, startTime, endTime, step string) (*QueryResult, error) {
	// Parse time range
	start, end, err := parseTimeRange(startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("error parsing time range: %w", err)
	}

	// Parse step (for future use in downsampling)
	if step != "" {
		_, err = time.ParseDuration(step)
		if err != nil {
			return nil, fmt.Errorf("error parsing step: %w", err)
		}
		// Note: stepDuration is not currently used, but will be used for downsampling in the future
	}

	// Create a metric query
	metricQuery := &storage.MetricQuery{
		StartTime: start,
		EndTime:   end,
		Limit:     1000, // Limit to 1000 data points
	}

	// Add label filter if query is not empty
	if query != "" {
		metricQuery.LabelFilter = func(labels map[string]string) bool {
			return matchLabels(labels, query)
		}
	}

	// Execute the query
	dataPoints, err := e.storageManager.MetricsStore().QueryMetrics(metricQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing metrics query: %w", err)
	}

	// Group data points by metric name and labels
	seriesMap := make(map[string]*MetricSeries)
	for _, point := range dataPoints {
		// Create a key from the labels
		key := labelsToKey(point.Labels)

		// Get or create the series
		series, ok := seriesMap[key]
		if !ok {
			series = &MetricSeries{
				Metric: point.Labels,
				Values: make([][]interface{}, 0),
			}
			seriesMap[key] = series
		}

		// Add the data point to the series
		series.Values = append(series.Values, []interface{}{
			float64(point.Timestamp.Unix()),
			point.Value,
		})
	}

	// Convert the map to a slice
	result := make([]MetricSeries, 0, len(seriesMap))
	for _, series := range seriesMap {
		result = append(result, *series)
	}

	// Create the query result
	queryResult := &QueryResult{
		Data: MetricsQueryResult{
			ResultType: "matrix",
			Result:     result,
		},
		Status: "success",
	}

	return queryResult, nil
}

// QueryLogs executes a logs query
func (e *Engine) QueryLogs(query, startTime, endTime, limit string) (*QueryResult, error) {
	// Parse time range
	start, end, err := parseTimeRange(startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("error parsing time range: %w", err)
	}

	// Parse limit
	var limitInt int
	if limit != "" {
		limitInt, err = strconv.Atoi(limit)
		if err != nil {
			return nil, fmt.Errorf("error parsing limit: %w", err)
		}
	} else {
		// Default limit is 100
		limitInt = 100
	}

	// Create a log query
	logQuery := &storage.LogQuery{
		StartTime: start,
		EndTime:   end,
		Limit:     limitInt,
	}

	// Add filter if query is not empty
	if query != "" {
		logQuery.Filter = func(entry *storage.LogEntry) bool {
			// Check if the message contains the query
			if strings.Contains(strings.ToLower(entry.Message), strings.ToLower(query)) {
				return true
			}

			// Check if any label matches the query
			for _, value := range entry.Labels {
				if strings.Contains(strings.ToLower(value), strings.ToLower(query)) {
					return true
				}
			}

			return false
		}
	}

	// Execute the query
	logEntries, err := e.storageManager.LogsStore().QueryLogs(logQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing logs query: %w", err)
	}

	// Convert log entries to the result format
	logs := make([]LogEntry, len(logEntries))
	for i, entry := range logEntries {
		logs[i] = LogEntry{
			Timestamp: entry.Timestamp,
			Level:     entry.Level,
			Message:   entry.Message,
			Labels:    entry.Labels,
		}
	}

	// Create the query result
	queryResult := &QueryResult{
		Data: LogsQueryResult{
			Logs: logs,
		},
		Status: "success",
	}

	return queryResult, nil
}

// QueryTraces executes a traces query
func (e *Engine) QueryTraces(query, startTime, endTime, limit string) (*QueryResult, error) {
	// Parse time range
	start, end, err := parseTimeRange(startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("error parsing time range: %w", err)
	}

	// Parse limit
	var limitInt int
	if limit != "" {
		limitInt, err = strconv.Atoi(limit)
		if err != nil {
			return nil, fmt.Errorf("error parsing limit: %w", err)
		}
	} else {
		// Default limit is 100
		limitInt = 100
	}

	// Create a metric query to get trace data
	metricQuery := &storage.MetricQuery{
		StartTime: start,
		EndTime:   end,
		Limit:     limitInt * 100, // Multiply by 100 to account for spans
	}

	// Add label filter if query is not empty
	if query != "" {
		metricQuery.LabelFilter = func(labels map[string]string) bool {
			// Check if the trace ID is in the query
			if traceID, ok := labels["trace.id"]; ok && strings.Contains(traceID, query) {
				return true
			}

			// Check if the service name is in the query
			if serviceName, ok := labels["service.name"]; ok && strings.Contains(serviceName, query) {
				return true
			}

			// Check if the span name is in the query
			if spanName, ok := labels["span.name"]; ok && strings.Contains(spanName, query) {
				return true
			}

			return false
		}
	}

	// Execute the query
	dataPoints, err := e.storageManager.TracesStore().QueryMetrics(metricQuery)
	if err != nil {
		return nil, fmt.Errorf("error executing traces query: %w", err)
	}

	// Group data points by trace ID
	traceMap := make(map[string]*Trace)
	spanMap := make(map[string]*Span)

	// First pass: create traces and spans
	for _, point := range dataPoints {
		// Skip points without trace ID
		traceID, ok := point.Labels["trace.id"]
		if !ok {
			continue
		}

		// Skip points without span ID
		spanID, ok := point.Labels["span.id"]
		if !ok {
			continue
		}

		// Get or create the trace
		trace, ok := traceMap[traceID]
		if !ok {
			serviceName := point.Labels["service.name"]
			if serviceName == "" {
				serviceName = "unknown"
			}

			trace = &Trace{
				TraceID:     traceID,
				Name:        point.Labels["span.name"],
				ServiceName: serviceName,
				StartTime:   point.Timestamp,
				EndTime:     point.Timestamp,
				Status:      "OK",
				Spans:       make([]Span, 0),
			}
			traceMap[traceID] = trace
		}

		// Get or create the span
		span, ok := spanMap[spanID]
		if !ok {
			span = &Span{
				SpanID:       spanID,
				ParentSpanID: point.Labels["parent.span.id"],
				Name:         point.Labels["span.name"],
				StartTime:    point.Timestamp,
				EndTime:      point.Timestamp,
				Status:       point.Labels["status.code"],
				Kind:         point.Labels["span.kind"],
				Labels:       make(map[string]string),
				Events:       make([]SpanEvent, 0),
			}
			spanMap[spanID] = span

			// Copy labels
			for k, v := range point.Labels {
				if k != "trace.id" && k != "span.id" && k != "parent.span.id" &&
					k != "span.name" && k != "status.code" && k != "span.kind" {
					span.Labels[k] = v
				}
			}
		}

		// Update span timestamps
		if point.Timestamp.Before(span.StartTime) {
			span.StartTime = point.Timestamp
		}
		if point.Timestamp.After(span.EndTime) {
			span.EndTime = point.Timestamp
		}

		// Update trace timestamps
		if point.Timestamp.Before(trace.StartTime) {
			trace.StartTime = point.Timestamp
		}
		if point.Timestamp.After(trace.EndTime) {
			trace.EndTime = point.Timestamp
		}

		// Check for events
		if eventName, ok := point.Labels["event.name"]; ok {
			event := SpanEvent{
				Name:       eventName,
				Timestamp:  point.Timestamp,
				Attributes: make(map[string]string),
			}

			// Copy event attributes
			for k, v := range point.Labels {
				if strings.HasPrefix(k, "event.attr.") {
					event.Attributes[k[11:]] = v
				}
			}

			span.Events = append(span.Events, event)
		}
	}

	// Second pass: calculate durations and add spans to traces
	for _, span := range spanMap {
		span.Duration = span.EndTime.Sub(span.StartTime).Seconds() * 1000 // Convert to milliseconds
		if trace, ok := traceMap[span.Labels["trace.id"]]; ok {
			trace.Spans = append(trace.Spans, *span)
		}
	}

	// Calculate trace durations
	for _, trace := range traceMap {
		trace.Duration = trace.EndTime.Sub(trace.StartTime).Seconds() * 1000 // Convert to milliseconds
	}

	// Convert the map to a slice
	traces := make([]Trace, 0, len(traceMap))
	for _, trace := range traceMap {
		traces = append(traces, *trace)
	}

	// Sort traces by start time (newest first)
	// sort.Slice(traces, func(i, j int) bool {
	// 	return traces[i].StartTime.After(traces[j].StartTime)
	// })

	// Limit the number of traces
	if len(traces) > limitInt {
		traces = traces[:limitInt]
	}

	// Create the query result
	queryResult := &QueryResult{
		Data: TracesQueryResult{
			Traces: traces,
		},
		Status: "success",
	}

	return queryResult, nil
}

// parseTimeRange parses the start and end times
func parseTimeRange(startTime, endTime string) (time.Time, time.Time, error) {
	var start, end time.Time
	var err error

	// Parse start time
	if startTime == "" {
		// Default to 1 hour ago
		start = time.Now().Add(-1 * time.Hour)
	} else {
		start, err = time.Parse(time.RFC3339, startTime)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error parsing start time: %w", err)
		}
	}

	// Parse end time
	if endTime == "" {
		// Default to now
		end = time.Now()
	} else {
		end, err = time.Parse(time.RFC3339, endTime)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("error parsing end time: %w", err)
		}
	}

	return start, end, nil
}

// matchLabels checks if the labels match the query
func matchLabels(labels map[string]string, query string) bool {
	// Split the query into label selectors
	selectors := strings.Split(query, ",")

	// Check each selector
	for _, selector := range selectors {
		selector = strings.TrimSpace(selector)
		if selector == "" {
			continue
		}

		// Parse the selector
		parts := strings.SplitN(selector, "=", 2)
		if len(parts) != 2 {
			// Invalid selector
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Check if the label matches
		labelValue, ok := labels[key]
		if !ok || labelValue != value {
			return false
		}
	}

	return true
}

// labelsToKey converts a map of labels to a string key
func labelsToKey(labels map[string]string) string {
	// Create a slice of key-value pairs
	pairs := make([]string, 0, len(labels))
	for k, v := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
	}

	// Sort the pairs
	// sort.Strings(pairs)

	// Join the pairs with commas
	return strings.Join(pairs, ",")
}
