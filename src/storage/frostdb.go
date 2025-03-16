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
	frostdbQuery "github.com/polarsignals/frostdb/query"
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

// FrostDBStore implements MetricsStore, LogsStore, and TracesStore interfaces using FrostDB
type FrostDBStore struct {
	columnstore *frostdb.ColumnStore
	database    *frostdb.DB
	tables      map[string]*frostdb.Table
	path        string
	retention   time.Duration
	mu          sync.RWMutex
}

// NewFrostDBStore creates a new FrostDB store
func NewFrostDBStore(path string, retention time.Duration) (*FrostDBStore, error) {
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
			MaxSize: 100 * frostdb.MiB,
			Type:    index.CompactionTypeParquetDisk,
		},
		{
			Level:   index.L1,
			MaxSize: 200 * frostdb.MiB,
			Type:    index.CompactionTypeParquetDisk,
		},
		{
			Level:   index.L2,
			MaxSize: 500 * frostdb.MiB,
		},
	}

	// Create column store with Windows-friendly options
	columnstore, err := frostdb.New(
		frostdb.WithLogger(logger),
		frostdb.WithWAL(),
		frostdb.WithStoragePath(path),
		frostdb.WithActiveMemorySize(100*frostdb.MiB),
		frostdb.WithRegistry(registry),
		frostdb.WithIndexConfig(indexConfig),
		frostdb.WithSnapshotTriggerSize(100*frostdb.MiB),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create column store: %w", err)
	}

	// Open up a database
	database, err := columnstore.DB(context.Background(), "telemy_db")
	if err != nil {
		columnstore.Close()
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create store
	store := &FrostDBStore{
		columnstore: columnstore,
		database:    database,
		tables:      make(map[string]*frostdb.Table),
		path:        path,
		retention:   retention,
	}

	// Create tables
	if err := store.initializeTables(); err != nil {
		columnstore.Close()
		return nil, fmt.Errorf("failed to initialize tables: %w", err)
	}

	return store, nil
}

// initializeTables creates the tables if they don't exist
func (s *FrostDBStore) initializeTables() error {
	var err error

	// Create metrics table config with schema
	metricsSchema := dynparquet.SampleDefinition()
	metricsConfig := frostdb.NewTableConfig(metricsSchema)

	// Create metrics table
	s.tables["metrics"], err = s.database.Table("metrics", metricsConfig)
	if err != nil {
		return fmt.Errorf("failed to create metrics table: %w", err)
	}

	// Create logs table config with schema
	logsSchema := dynparquet.SampleDefinition()
	logsConfig := frostdb.NewTableConfig(logsSchema)

	// Create logs table
	s.tables["logs"], err = s.database.Table("logs", logsConfig)
	if err != nil {
		return fmt.Errorf("failed to create logs table: %w", err)
	}

	// Create traces table config with schema
	tracesSchema := dynparquet.SampleDefinition()
	tracesConfig := frostdb.NewTableConfig(tracesSchema)

	// Create traces table
	s.tables["traces"], err = s.database.Table("traces", tracesConfig)
	if err != nil {
		return fmt.Errorf("failed to create traces table: %w", err)
	}

	return nil
}

// Close closes the FrostDB database
func (s *FrostDBStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.columnstore != nil {
		return s.columnstore.Close()
	}
	return nil
}

// StoreMetric stores a metric data point
func (s *FrostDBStore) StoreMetric(point *DataPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the metrics table
	table, ok := s.tables["metrics"]
	if !ok {
		return fmt.Errorf("metrics table not found")
	}

	// Create a sample based on the FrostDB sample format
	sample := dynparquet.Sample{
		Timestamp: point.Timestamp.UnixNano(),
		Value:     int64(point.Value), // Convert to int64 as per sample format
		Labels:    point.Labels,
	}

	// Convert samples to a record batch
	batch := dynparquet.Samples{sample}
	record, err := batch.ToRecord()
	if err != nil {
		return fmt.Errorf("error creating record: %w", err)
	}

	// Insert the record
	if _, err := table.InsertRecord(context.Background(), record); err != nil {
		return fmt.Errorf("error inserting record: %w", err)
	}

	return nil
}

// QueryMetrics queries metrics based on criteria
func (s *FrostDBStore) QueryMetrics(query *MetricQuery) ([]*DataPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create timestamp filters
	startTime := query.StartTime.UnixNano()
	endTime := query.EndTime.UnixNano()

	// Build the query with the FrostDB API - use the query package properly
	engine := frostdbQuery.NewEngine(memory.DefaultAllocator, s.database.TableProvider())

	// Build and execute the query - use string table name instead of ID
	scanner := engine.ScanTable("metrics").
		Filter(
			logicalplan.And(
				logicalplan.Col("timestamp").Gt(logicalplan.Literal(startTime)),
				logicalplan.Col("timestamp").Lt(logicalplan.Literal(endTime)),
			),
		).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("value"),
			logicalplan.Col("labels"),
		)

	var dataPoints []*DataPoint

	// Execute the query with callback function as shown in the example
	err := scanner.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		numRows := r.NumRows()

		// Get columns - need to extract from the record
		timestampCol := r.Column(0).(*array.Int64)
		valueCol := r.Column(1).(*array.Int64)
		labelsCol := r.Column(2)

		// Process each row
		for i := int64(0); i < numRows; i++ {
			// Extract timestamp
			timestamp := time.Unix(0, timestampCol.Value(int(i)))

			// Extract value (stored as int64 in FrostDB)
			value := float64(valueCol.Value(int(i)))

			// Extract labels
			labels := make(map[string]string)
			if labelsDict, ok := labelsCol.(*array.Dictionary); ok {
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
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	// Apply label filter if provided
	if query.LabelFilter != nil {
		filteredPoints := make([]*DataPoint, 0, len(dataPoints))
		for _, point := range dataPoints {
			if query.LabelFilter(point.Labels) {
				filteredPoints = append(filteredPoints, point)
			}
		}
		dataPoints = filteredPoints
	}

	// Apply limit if provided
	if query.Limit > 0 && len(dataPoints) > query.Limit {
		dataPoints = dataPoints[:query.Limit]
	}

	return dataPoints, nil
}

// StoreLog stores a log entry
func (s *FrostDBStore) StoreLog(entry *LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the logs table
	table, ok := s.tables["logs"]
	if !ok {
		return fmt.Errorf("logs table not found")
	}

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

	// Convert samples to a record batch
	batch := dynparquet.Samples{sample}
	record, err := batch.ToRecord()
	if err != nil {
		return fmt.Errorf("error creating record: %w", err)
	}

	// Insert the record
	if _, err := table.InsertRecord(context.Background(), record); err != nil {
		return fmt.Errorf("error inserting record: %w", err)
	}

	return nil
}

// QueryLogs queries logs based on criteria
func (s *FrostDBStore) QueryLogs(query *LogQuery) ([]*LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create timestamp filters
	startTime := query.StartTime.UnixNano()
	endTime := query.EndTime.UnixNano()

	// Build the query with the FrostDB API - use the query package properly
	engine := frostdbQuery.NewEngine(memory.DefaultAllocator, s.database.TableProvider())

	// Build and execute the query - use string table name instead of ID
	scanner := engine.ScanTable("logs").
		Filter(
			logicalplan.And(
				logicalplan.Col("timestamp").Gt(logicalplan.Literal(startTime)),
				logicalplan.Col("timestamp").Lt(logicalplan.Literal(endTime)),
			),
		).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("exampleType"), // For level
			logicalplan.Col("labels"),      // For labels including message
		)

	var logEntries []*LogEntry

	// Execute the query with callback function as shown in the example
	err := scanner.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		numRows := r.NumRows()

		// Get columns - need to extract from the record
		timestampCol := r.Column(0).(*array.Int64)
		levelCol := r.Column(1)
		labelsCol := r.Column(2)

		// Process each row
		for i := int64(0); i < numRows; i++ {
			// Extract timestamp
			timestamp := time.Unix(0, timestampCol.Value(int(i)))

			// Extract level (exampleType)
			level := ""
			if levelDict, ok := levelCol.(*array.Dictionary); ok {
				keyIndex := levelDict.GetValueIndex(int(i))
				if keyIndex >= 0 {
					dictValues := levelDict.Dictionary().(*array.String)
					level = dictValues.Value(keyIndex)
				}
			} else if strCol, ok := levelCol.(*array.String); ok {
				level = strCol.Value(int(i))
			}

			// Extract labels
			labels := make(map[string]string)
			message := ""
			if labelsDict, ok := labelsCol.(*array.Dictionary); ok {
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
						delete(labels, "message") // Remove message from labels
					}
				}
			}

			// Create log entry
			logEntry := &LogEntry{
				Timestamp: timestamp,
				Level:     level,
				Message:   message,
				Labels:    labels,
			}
			logEntries = append(logEntries, logEntry)
		}
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	// Apply filter if provided
	if query.Filter != nil {
		filteredEntries := make([]*LogEntry, 0, len(logEntries))
		for _, entry := range logEntries {
			if query.Filter(entry) {
				filteredEntries = append(filteredEntries, entry)
			}
		}
		logEntries = filteredEntries
	}

	// Apply limit if provided
	if query.Limit > 0 && len(logEntries) > query.Limit {
		logEntries = logEntries[:query.Limit]
	}

	return logEntries, nil
}

// StoreTrace stores a trace data point
func (s *FrostDBStore) StoreTrace(point *DataPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get the traces table
	table, ok := s.tables["traces"]
	if !ok {
		return fmt.Errorf("traces table not found")
	}

	// Create a sample based on the FrostDB sample format
	sample := dynparquet.Sample{
		Timestamp: point.Timestamp.UnixNano(),
		Value:     int64(point.Value), // Convert to int64 as per sample format
		Labels:    point.Labels,
	}

	// Convert samples to a record batch
	batch := dynparquet.Samples{sample}
	record, err := batch.ToRecord()
	if err != nil {
		return fmt.Errorf("error creating record: %w", err)
	}

	// Insert the record
	if _, err := table.InsertRecord(context.Background(), record); err != nil {
		return fmt.Errorf("error inserting record: %w", err)
	}

	return nil
}

// QueryTraces queries traces based on criteria
func (s *FrostDBStore) QueryTraces(query *MetricQuery) ([]*DataPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create timestamp filters
	startTime := query.StartTime.UnixNano()
	endTime := query.EndTime.UnixNano()

	// Build the query with the FrostDB API - use the query package properly
	engine := frostdbQuery.NewEngine(memory.DefaultAllocator, s.database.TableProvider())

	// Build and execute the query - use string table name instead of ID
	scanner := engine.ScanTable("traces").
		Filter(
			logicalplan.And(
				logicalplan.Col("timestamp").Gt(logicalplan.Literal(startTime)),
				logicalplan.Col("timestamp").Lt(logicalplan.Literal(endTime)),
			),
		).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("value"),
			logicalplan.Col("labels"),
		)

	var dataPoints []*DataPoint

	// Execute the query with callback function as shown in the example
	err := scanner.Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
		numRows := r.NumRows()

		// Get columns - need to extract from the record
		timestampCol := r.Column(0).(*array.Int64)
		valueCol := r.Column(1).(*array.Int64)
		labelsCol := r.Column(2)

		// Process each row
		for i := int64(0); i < numRows; i++ {
			// Extract timestamp
			timestamp := time.Unix(0, timestampCol.Value(int(i)))

			// Extract value (stored as int64 in FrostDB)
			value := float64(valueCol.Value(int(i)))

			// Extract labels
			labels := make(map[string]string)
			if labelsDict, ok := labelsCol.(*array.Dictionary); ok {
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
		return nil, fmt.Errorf("error executing query: %w", err)
	}

	// Apply label filter if provided
	if query.LabelFilter != nil {
		filteredPoints := make([]*DataPoint, 0, len(dataPoints))
		for _, point := range dataPoints {
			if query.LabelFilter(point.Labels) {
				filteredPoints = append(filteredPoints, point)
			}
		}
		dataPoints = filteredPoints
	}

	// Apply limit if provided
	if query.Limit > 0 && len(dataPoints) > query.Limit {
		dataPoints = dataPoints[:query.Limit]
	}

	return dataPoints, nil
}
