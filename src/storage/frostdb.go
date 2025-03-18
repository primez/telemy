package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/prometheus/client_golang/prometheus"
)

// Create type definitions for our schemas
type MetricsSchema struct {
	Timestamp int64             `frostdb:"timestamp"`
	Value     float64           `frostdb:"value"`
	Labels    map[string]string `frostdb:",asc"`
}

type LogsSchema struct {
	Timestamp int64             `frostdb:"timestamp"`
	Level     string            `frostdb:"level"`
	Message   string            `frostdb:"message"`
	Labels    map[string]string `frostdb:",asc"`
}

type TracesSchema struct {
	Timestamp int64             `frostdb:"timestamp"`
	Value     float64           `frostdb:"value"`
	Labels    map[string]string `frostdb:",asc"`
}

// StoreType represents the type of storage
type StoreType int

const (
	MetricsStoreType StoreType = iota
	LogsStoreType
	TracesStoreType
)

// FrostDBOptions contains configuration options for FrostDB
type FrostDBOptions struct {
	BatchSize        int
	FlushInterval    time.Duration
	ActiveMemorySize int64
	WALEnabled       bool
	TableName        string
	StoreType        StoreType // Added to determine which schema to use
}

// DefaultFrostDBOptions returns default options for FrostDB
func DefaultFrostDBOptions() *FrostDBOptions {
	return &FrostDBOptions{
		BatchSize:        1000,
		FlushInterval:    30 * time.Second,
		ActiveMemorySize: 100 * frostdb.MiB,
		WALEnabled:       true,
		TableName:        "data", // Default table name
	}
}

// FrostDBStore implements either MetricsStore, LogsStore, or TracesStore interface using FrostDB
type FrostDBStore struct {
	columnstore *frostdb.ColumnStore
	database    *frostdb.DB
	table       *frostdb.Table
	tableName   string
	path        string
	retention   time.Duration

	// Query engine
	queryEngine *query.LocalEngine

	// Batching support
	batch     dynparquet.Samples
	batchSize int
	batchMu   sync.Mutex

	batchMaxSize  int
	flushTicker   *time.Ticker
	flushInterval time.Duration
	shutdown      chan struct{}
}

// NewFrostDBStore creates a new FrostDB store
func NewFrostDBStore(path string, retention time.Duration, opts *FrostDBOptions) (*FrostDBStore, error) {
	// If options not provided, use defaults
	if opts == nil {
		opts = DefaultFrostDBOptions()
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create a logger
	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())

	// Create a registry for metrics
	registry := prometheus.NewRegistry()

	// Configure index levels with parquet disk compaction for better performance
	indexConfig := []*index.LevelConfig{
		{
			Level:   index.L0,
			MaxSize: 50 * frostdb.MiB,
			Type:    index.CompactionTypeParquetDisk,
		},
		{
			Level:   index.L1,
			MaxSize: 100 * frostdb.MiB,
			Type:    index.CompactionTypeParquetDisk,
		},
		{
			Level:   index.L2,
			MaxSize: 250 * frostdb.MiB,
		},
	}

	// Build column store options
	columnStoreOpts := []frostdb.Option{
		frostdb.WithLogger(logger),
		frostdb.WithStoragePath(path),
		frostdb.WithActiveMemorySize(opts.ActiveMemorySize),
		frostdb.WithRegistry(registry),
		frostdb.WithIndexConfig(indexConfig),
		frostdb.WithSnapshotTriggerSize(50 * frostdb.MiB),
	}

	// Add WAL if enabled
	if opts.WALEnabled {
		columnStoreOpts = append(columnStoreOpts, frostdb.WithWAL())
	}

	// Create column store with Windows-friendly options
	columnstore, err := frostdb.New(columnStoreOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create column store: %w", err)
	}

	// Open up a database
	database, err := columnstore.DB(context.Background(), "telemy_db")
	if err != nil {
		columnstore.Close()
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Build the query with the FrostDB API - use the query package properly
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())

	// Create store with batching support
	store := &FrostDBStore{
		columnstore:   columnstore,
		database:      database,
		queryEngine:   engine,
		tableName:     opts.TableName,
		path:          path,
		retention:     retention,
		batch:         dynparquet.Samples{},
		batchMaxSize:  opts.BatchSize,
		flushInterval: opts.FlushInterval,
		shutdown:      make(chan struct{}),
	}

	// Create table based on store type
	if err := store.initializeTable(opts.StoreType); err != nil {
		columnstore.Close()
		return nil, fmt.Errorf("failed to initialize table: %w", err)
	}

	// Start the background flush timer
	store.flushTicker = time.NewTicker(store.flushInterval)
	go store.flushRoutine()

	return store, nil
}

// For backward compatibility
func NewDefaultFrostDBStore(path string, retention time.Duration) (*FrostDBStore, error) {
	return NewFrostDBStore(path, retention, nil)
}

// initializeTable creates the table if it doesn't exist
func (s *FrostDBStore) initializeTable(storeType StoreType) error {
	// Use the generic Sample definition for now
	// TODO: Implement custom schemas when proper schema support is available
	schema := dynparquet.SampleDefinition()
	config := frostdb.NewTableConfig(schema)

	// Create table
	var err error
	s.table, err = s.database.Table(s.tableName, config)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", s.tableName, err)
	}

	return nil
}

// flushRoutine periodically flushes batch to storage
func (s *FrostDBStore) flushRoutine() {
	for {
		select {
		case <-s.flushTicker.C:
			s.FlushBatch()
		case <-s.shutdown:
			return
		}
	}
}

// FlushBatch flushes the current batch to storage
func (s *FrostDBStore) FlushBatch() {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	if len(s.batch) == 0 {
		return // Nothing to flush
	}

	// Convert batch to record
	record, err := s.batch.ToRecord()
	if err != nil {
		fmt.Printf("Error creating record during batch flush: %v\n", err)
		return
	}

	// Insert the record
	if _, err := s.table.InsertRecord(context.Background(), record); err != nil {
		fmt.Printf("Error inserting record during batch flush: %v\n", err)
		return
	}

	// Clear the batch
	s.batch = dynparquet.Samples{}
	s.batchSize = 0
}

// Close closes the FrostDB database
func (s *FrostDBStore) Close() error {
	// Stop the flush ticker
	if s.flushTicker != nil {
		s.flushTicker.Stop()
		close(s.shutdown)
	}

	// Flush any remaining data
	s.FlushBatch()

	if s.columnstore != nil {
		return s.columnstore.Close()
	}
	return nil
}

// StoreMetric stores a metric data point
func (s *FrostDBStore) StoreMetric(point *DataPoint) error {
	// Create a sample based on the FrostDB sample format
	sample := dynparquet.Sample{
		Timestamp: point.Timestamp.UnixNano(),
		Value:     int64(point.Value), // Convert to int64 as per sample format
		Labels:    point.Labels,
	}

	// Add to batch
	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	s.batch = append(s.batch, sample)
	s.batchSize++

	// If batch is full, flush it
	if s.batchSize >= s.batchMaxSize {
		// To avoid deferring the mutex unlock until after the flush,
		// we'll create a goroutine to do the flush
		go func() {
			s.FlushBatch()
		}()
	}

	return nil
}

// QueryMetrics queries metrics based on criteria
func (s *FrostDBStore) QueryMetrics(metricQuery *MetricQuery) ([]*DataPoint, error) {
	// Handle time range - use defaults if not provided
	var startTime, endTime int64

	// If start time is zero value, use Unix epoch start (1970)
	if metricQuery.StartTime.IsZero() {
		startTime = 0 // Beginning of time
	} else {
		startTime = metricQuery.StartTime.UnixNano()
	}

	// If end time is zero value, use current time
	if metricQuery.EndTime.IsZero() {
		endTime = time.Now().UnixNano()
	} else {
		endTime = metricQuery.EndTime.UnixNano()
	}

	// Start building the filter expression with timestamp range
	filterExpr := logicalplan.And(
		logicalplan.Col("timestamp").GtEq(logicalplan.Literal(startTime)),
		logicalplan.Col("timestamp").LtEq(logicalplan.Literal(endTime)),
	)

	// If we have specific label filters that can be pushed down to FrostDB, add them
	if len(metricQuery.FilterLabels) > 0 {
		for key, value := range metricQuery.FilterLabels {
			labelPath := fmt.Sprintf("labels.%s", key)
			labelExpr := logicalplan.Col(labelPath).Eq(logicalplan.Literal(value))

			// Combine with the existing filter
			filterExpr = logicalplan.And(filterExpr, labelExpr)
		}
	}

	// Build and execute the query using the reused query engine
	scanner := s.queryEngine.ScanTable(s.tableName).
		Filter(filterExpr).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("value"),
			logicalplan.Col("labels"),
		)

	// If there's a limit, apply it at the query level
	if metricQuery.Limit > 0 {
		scanner = scanner.Limit(logicalplan.Literal(int64(metricQuery.Limit)))
	}

	var dataPoints []*DataPoint

	// Execute the query with callback function
	err := scanner.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		numRows := r.NumRows()
		if numRows == 0 {
			return nil
		}

		// Safety check for minimum columns - just need timestamp
		if r.NumCols() < 1 {
			return nil // Return empty result instead of error for empty tables
		}

		// Find indexes of columns we need
		timestampColIdx := -1
		valueColIdx := -1
		labelsColIdx := -1

		for i, field := range r.Schema().Fields() {
			switch field.Name {
			case "timestamp":
				timestampColIdx = i
			case "value":
				valueColIdx = i
			case "labels":
				labelsColIdx = i
			}
		}

		// Need at least timestamp and value columns
		if timestampColIdx < 0 || valueColIdx < 0 {
			return nil
		}

		// Get timestamp column
		timestampCol, ok := r.Column(timestampColIdx).(*array.Int64)
		if !ok {
			return nil // Return empty result for unexpected column types
		}

		// Process rows
		for i := int64(0); i < numRows; i++ {
			// Extract timestamp
			timestamp := time.Unix(0, timestampCol.Value(int(i)))

			// Extract value - try both Float64 and Int64 types
			value := float64(0)

			valueCol := r.Column(valueColIdx)
			if floatCol, ok := valueCol.(*array.Float64); ok {
				value = floatCol.Value(int(i))
			} else if intCol, ok := valueCol.(*array.Int64); ok {
				value = float64(intCol.Value(int(i)))
			}

			// Extract labels
			labels := make(map[string]string)
			if labelsColIdx >= 0 && labelsColIdx < int(r.NumCols()) {
				if labelsDict, ok := r.Column(labelsColIdx).(*array.Dictionary); ok {
					keyIndex := labelsDict.GetValueIndex(int(i))
					if keyIndex >= 0 {
						dictValues := labelsDict.Dictionary().(*array.String)
						labelStr := dictValues.Value(keyIndex)
						if err := json.Unmarshal([]byte(labelStr), &labels); err != nil {
							// Just create an empty label if we can't parse
							labels = make(map[string]string)
						}
					}
				}
			}

			// Create data point
			dataPoint := &DataPoint{
				Timestamp: timestamp,
				Value:     value,
				Labels:    labels,
			}
			dataPoints = append(dataPoints, dataPoint)
		}
		return nil
	})

	if err != nil {
		// Return empty result instead of error
		return []*DataPoint{}, nil
	}

	// Apply additional filtering in memory if needed
	if metricQuery.LabelFilter != nil {
		filteredPoints := make([]*DataPoint, 0, len(dataPoints))
		for _, point := range dataPoints {
			if metricQuery.LabelFilter(point.Labels) {
				filteredPoints = append(filteredPoints, point)
			}
		}
		dataPoints = filteredPoints
	}

	return dataPoints, nil
}

// StoreLog stores a log entry
func (s *FrostDBStore) StoreLog(entry *LogEntry) error {
	// Create a sample based on the FrostDB sample format
	sample := dynparquet.Sample{
		Timestamp:   entry.Timestamp.UnixNano(),
		ExampleType: entry.Level,
		Labels:      entry.Labels,
	}

	// Add message to labels since there's no direct message field in Sample
	if sample.Labels == nil {
		sample.Labels = make(map[string]string)
	}
	sample.Labels["message"] = entry.Message

	// Add to batch
	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	s.batch = append(s.batch, sample)
	s.batchSize++

	// If batch is full, flush it
	if s.batchSize >= s.batchMaxSize {
		// To avoid deferring the mutex unlock until after the flush,
		// we'll create a goroutine to do the flush
		go func() {
			s.FlushBatch()
		}()
	}

	return nil
}

// QueryLogs queries logs based on criteria
func (s *FrostDBStore) QueryLogs(logQuery *LogQuery) ([]*LogEntry, error) {
	// Handle time range - use defaults if not provided
	var startTime, endTime int64

	// If start time is zero value, use Unix epoch start (1970)
	if logQuery.StartTime.IsZero() {
		startTime = 0 // Beginning of time
	} else {
		startTime = logQuery.StartTime.UnixNano()
	}

	// If end time is zero value, use current time
	if logQuery.EndTime.IsZero() {
		endTime = time.Now().UnixNano()
	} else {
		endTime = logQuery.EndTime.UnixNano()
	}

	// Start building the filter expression with timestamp range
	filterExpr := logicalplan.And(
		logicalplan.Col("timestamp").GtEq(logicalplan.Literal(startTime)),
		logicalplan.Col("timestamp").LtEq(logicalplan.Literal(endTime)),
	)

	// Add log level filter if specified
	if logQuery.Level != "" {
		// Use example_type for level since we're using the Sample schema
		levelExpr := logicalplan.Col("example_type").Eq(logicalplan.Literal(logQuery.Level))
		filterExpr = logicalplan.And(filterExpr, levelExpr)
	}

	// Build and execute the query using the reused query engine
	// Project all fields from the Sample schema that we need
	scanner := s.queryEngine.ScanTable(s.tableName).
		Filter(filterExpr).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("example_type"), // This is where level is stored
			logicalplan.Col("labels"),       // Labels map contains message
		)

	// If there's a limit, apply it at the query level
	if logQuery.Limit > 0 {
		scanner = scanner.Limit(logicalplan.Literal(int64(logQuery.Limit)))
	}

	var logEntries []*LogEntry

	// Execute the query with callback function
	err := scanner.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		numRows := r.NumRows()
		if numRows == 0 {
			return nil
		}

		// Safety check for minimum columns - just need timestamp
		if r.NumCols() < 1 {
			return nil // Return empty result instead of error for empty tables
		}

		// Get timestamp column
		timestampCol, ok := r.Column(0).(*array.Int64)
		if !ok {
			return nil // Return empty result for unexpected column types
		}

		// Find example_type and labels columns
		exampleTypeColIdx := -1
		labelsColIdx := -1

		for i, field := range r.Schema().Fields() {
			switch field.Name {
			case "example_type":
				exampleTypeColIdx = i
			case "labels":
				labelsColIdx = i
			}
		}

		// Process each row
		for i := int64(0); i < numRows; i++ {
			// Extract timestamp
			timestamp := time.Unix(0, timestampCol.Value(int(i)))

			// Extract level from example_type column
			level := ""
			if exampleTypeColIdx >= 0 && exampleTypeColIdx < int(r.NumCols()) {
				level = extractStringValue(r.Column(exampleTypeColIdx), int(i))
			}

			// Extract labels and message
			labels := make(map[string]string)
			message := ""

			if labelsColIdx >= 0 && labelsColIdx < int(r.NumCols()) {
				labelsDict, ok := r.Column(labelsColIdx).(*array.Dictionary)
				if ok {
					keyIndex := labelsDict.GetValueIndex(int(i))
					if keyIndex >= 0 {
						dictValues := labelsDict.Dictionary().(*array.String)
						labelStr := dictValues.Value(keyIndex)
						if err := json.Unmarshal([]byte(labelStr), &labels); err != nil {
							// Just create an empty label if we can't parse
							labels = make(map[string]string)
						}

						// Extract message from labels
						if msg, ok := labels["message"]; ok {
							message = msg
							delete(labels, "message") // Remove from labels to avoid duplication
						}
					}
				}
			}

			// Create log entry
			entry := &LogEntry{
				Timestamp: timestamp,
				Level:     level,
				Message:   message,
				Labels:    labels,
			}
			logEntries = append(logEntries, entry)
		}
		return nil
	})

	if err != nil {
		// Return empty result instead of error
		return []*LogEntry{}, nil
	}

	// Apply any additional filtering
	if logQuery.Filter != nil {
		filteredEntries := make([]*LogEntry, 0, len(logEntries))
		for _, entry := range logEntries {
			if logQuery.Filter(entry) {
				filteredEntries = append(filteredEntries, entry)
			}
		}
		logEntries = filteredEntries
	}

	return logEntries, nil
}

// Helper function to extract string value from different column types
func extractStringValue(col arrow.Array, rowIdx int) string {
	if col == nil || col.IsNull(rowIdx) {
		return ""
	}

	switch typedCol := col.(type) {
	case *array.String:
		return typedCol.Value(rowIdx)
	case *array.Dictionary:
		keyIndex := typedCol.GetValueIndex(rowIdx)
		if keyIndex >= 0 {
			dict := typedCol.Dictionary()
			if dictStr, ok := dict.(*array.String); ok {
				return dictStr.Value(keyIndex)
			}
		}
	}
	return ""
}

// StoreTrace stores a trace data point
func (s *FrostDBStore) StoreTrace(point *DataPoint) error {
	// Create a sample based on the FrostDB sample format
	sample := dynparquet.Sample{
		Timestamp: point.Timestamp.UnixNano(),
		Value:     int64(point.Value), // Convert to int64 as per sample format
		Labels:    point.Labels,
	}

	// Add to batch
	s.batchMu.Lock()
	defer s.batchMu.Unlock()

	s.batch = append(s.batch, sample)
	s.batchSize++

	// If batch is full, flush it
	if s.batchSize >= s.batchMaxSize {
		// To avoid deferring the mutex unlock until after the flush,
		// we'll create a goroutine to do the flush
		go func() {
			s.FlushBatch()
		}()
	}

	return nil
}

// QueryTraces queries traces based on criteria
func (s *FrostDBStore) QueryTraces(metricQuery *MetricQuery) ([]*DataPoint, error) {
	// Handle time range - use defaults if not provided
	var startTime, endTime int64

	// If start time is zero value, use Unix epoch start (1970)
	if metricQuery.StartTime.IsZero() {
		startTime = 0 // Beginning of time
	} else {
		startTime = metricQuery.StartTime.UnixNano()
	}

	// If end time is zero value, use current time
	if metricQuery.EndTime.IsZero() {
		endTime = time.Now().UnixNano()
	} else {
		endTime = metricQuery.EndTime.UnixNano()
	}

	// Start building the filter expression with timestamp range
	filterExpr := logicalplan.And(
		logicalplan.Col("timestamp").GtEq(logicalplan.Literal(startTime)),
		logicalplan.Col("timestamp").LtEq(logicalplan.Literal(endTime)),
	)

	// If we have specific label filters that can be pushed down to FrostDB, add them
	if len(metricQuery.FilterLabels) > 0 {
		for key, value := range metricQuery.FilterLabels {
			labelPath := fmt.Sprintf("labels.%s", key)
			labelExpr := logicalplan.Col(labelPath).Eq(logicalplan.Literal(value))

			// Combine with the existing filter
			filterExpr = logicalplan.And(filterExpr, labelExpr)
		}
	}

	// Build and execute the query using the reused query engine
	scanner := s.queryEngine.ScanTable(s.tableName).
		Filter(filterExpr).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("value"),
			logicalplan.Col("labels"),
		)

	// If there's a limit, apply it at the query level
	if metricQuery.Limit > 0 {
		scanner = scanner.Limit(logicalplan.Literal(int64(metricQuery.Limit)))
	}

	var dataPoints []*DataPoint

	// Execute the query with callback function
	err := scanner.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		numRows := r.NumRows()
		if numRows == 0 {
			return nil
		}

		// Safety check for minimum columns - just need timestamp
		if r.NumCols() < 1 {
			return nil // Return empty result instead of error for empty tables
		}

		// Find indexes of columns we need
		timestampColIdx := -1
		valueColIdx := -1
		labelsColIdx := -1

		for i, field := range r.Schema().Fields() {
			switch field.Name {
			case "timestamp":
				timestampColIdx = i
			case "value":
				valueColIdx = i
			case "labels":
				labelsColIdx = i
			}
		}

		// Need at least timestamp and value columns
		if timestampColIdx < 0 || valueColIdx < 0 {
			return nil
		}

		// Get timestamp column
		timestampCol, ok := r.Column(timestampColIdx).(*array.Int64)
		if !ok {
			return nil // Return empty result for unexpected column types
		}

		// Process rows
		for i := int64(0); i < numRows; i++ {
			// Extract timestamp
			timestamp := time.Unix(0, timestampCol.Value(int(i)))

			// Extract value - try both Float64 and Int64 types
			value := float64(0)

			valueCol := r.Column(valueColIdx)
			if floatCol, ok := valueCol.(*array.Float64); ok {
				value = floatCol.Value(int(i))
			} else if intCol, ok := valueCol.(*array.Int64); ok {
				value = float64(intCol.Value(int(i)))
			}

			// Extract labels
			labels := make(map[string]string)
			if labelsColIdx >= 0 && labelsColIdx < int(r.NumCols()) {
				if labelsDict, ok := r.Column(labelsColIdx).(*array.Dictionary); ok {
					keyIndex := labelsDict.GetValueIndex(int(i))
					if keyIndex >= 0 {
						dictValues := labelsDict.Dictionary().(*array.String)
						labelStr := dictValues.Value(keyIndex)
						if err := json.Unmarshal([]byte(labelStr), &labels); err != nil {
							// Just create an empty label if we can't parse
							labels = make(map[string]string)
						}
					}
				}
			}

			// Create data point
			dataPoint := &DataPoint{
				Timestamp: timestamp,
				Value:     value,
				Labels:    labels,
			}
			dataPoints = append(dataPoints, dataPoint)
		}
		return nil
	})

	if err != nil {
		// Return empty result instead of error
		return []*DataPoint{}, nil
	}

	// Apply additional filtering in memory if needed
	if metricQuery.LabelFilter != nil {
		filteredPoints := make([]*DataPoint, 0, len(dataPoints))
		for _, point := range dataPoints {
			if metricQuery.LabelFilter(point.Labels) {
				filteredPoints = append(filteredPoints, point)
			}
		}
		dataPoints = filteredPoints
	}

	return dataPoints, nil
}
