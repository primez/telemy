// This standalone benchmark measures FrostDB performance
// Run with: go run src/tests/frostdb_benchmark/main.go

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
)

// ResultEntry represents a benchmark result
type ResultEntry struct {
	Operation     string
	DataSize      int
	Duration      time.Duration
	StorageSizeMB float64
	RecordsPerSec float64
}

// calculateDirSize returns the size of a directory in bytes
func calculateDirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}

// printResults formats benchmark results as a table
func printResults(results []ResultEntry) string {
	var b strings.Builder
	w := tabwriter.NewWriter(&b, 0, 0, 3, ' ', tabwriter.AlignRight|tabwriter.Debug)

	fmt.Fprintln(w, "Operation\tData Size\tDuration\tStorage (MB)\tRecords/sec")
	fmt.Fprintln(w, "---------\t---------\t--------\t------------\t-----------")

	for _, r := range results {
		fmt.Fprintf(w, "%s\t%d\t%v\t%.2f\t%.2f\n",
			r.Operation, r.DataSize, r.Duration, r.StorageSizeMB, r.RecordsPerSec)
	}

	w.Flush()
	return b.String()
}

// createSamplePoint generates a FrostDB sample with predictable values
func createSamplePoint(i int, baseTime time.Time) dynparquet.Sample {
	// Use a fixed base time and add incremental seconds
	timestampOffset := int64(i % 3600)
	timestamp := baseTime.Add(time.Duration(timestampOffset) * time.Second).UnixNano()

	return dynparquet.Sample{
		ExampleType: fmt.Sprintf("log_type_%d", i%5),
		Labels: map[string]string{
			"level":      fmt.Sprintf("level_%d", i%4),
			"component":  fmt.Sprintf("component_%d", i%3),
			"instance":   fmt.Sprintf("instance_%d", i%10),
			"stacktrace": fmt.Sprintf("%d: System.Exception", i),
		},
		Stacktrace: []uuid.UUID{
			{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, byte(i % 255)},
		},
		Timestamp: timestamp,
		Value:     int64(i),
	}
}

// benchmarkInsertOperation measures FrostDB insert performance
func benchmarkInsertOperation(storagePathDir string, numRecords int, batchSize int) (time.Duration, float64, error) {
	// Clean the directory if it exists
	if _, err := os.Stat(storagePathDir); !os.IsNotExist(err) {
		if err := os.RemoveAll(storagePathDir); err != nil {
			return 0, 0, fmt.Errorf("failed to clean directory: %v", err)
		}
	}

	if err := os.MkdirAll(storagePathDir, 0755); err != nil {
		return 0, 0, fmt.Errorf("failed to create directory: %v", err)
	}

	// Create logger
	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())

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
	}

	// Create column store
	columnstore, err := frostdb.New(
		frostdb.WithLogger(logger),
		frostdb.WithWAL(),
		frostdb.WithStoragePath(storagePathDir),
		frostdb.WithActiveMemorySize(100*frostdb.MiB),
		frostdb.WithRegistry(prometheus.NewRegistry()),
		frostdb.WithIndexConfig(indexConfig),
		frostdb.WithSnapshotTriggerSize(100*frostdb.MiB),
	)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create FrostDB column store: %v", err)
	}
	defer columnstore.Close()

	// Open database
	database, err := columnstore.DB(context.Background(), "benchmark_db")
	if err != nil {
		return 0, 0, fmt.Errorf("failed to open FrostDB database: %v", err)
	}

	// Set up table configuration
	config := frostdb.NewTableConfig(dynparquet.SampleDefinition())
	table, err := database.Table("benchmark_table", config)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create FrostDB table: %v", err)
	}

	// Use a fixed base time for consistent timestamps
	baseTime := time.Now().Add(-12 * time.Hour)

	// Measure insert performance
	startTime := time.Now()

	for i := 0; i < numRecords; i += batchSize {
		batch := dynparquet.Samples{}
		end := min(i+batchSize, numRecords)

		// Process each batch
		for j := i; j < end; j++ {
			batch = append(batch, createSamplePoint(j, baseTime))
		}

		r, err := batch.ToRecord()
		if err != nil {
			return 0, 0, fmt.Errorf("failed to convert samples to record: %v", err)
		}

		_, err = table.InsertRecord(context.Background(), r)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to insert record: %v", err)
		}

		// Print progress for longer operations
		if numRecords > 10000 && i%(numRecords/10) == 0 && i > 0 {
			fmt.Printf("  Progress: %d%% complete (%d/%d records)\n",
				i*100/numRecords, i, numRecords)
		}

		// Force compaction periodically
		if i%100000 == 0 && i > 0 {
			if err := table.EnsureCompaction(); err != nil {
				return 0, 0, fmt.Errorf("failed to ensure compaction: %v", err)
			}
		}
	}

	duration := time.Since(startTime)

	// Ensure final compaction
	if err := table.EnsureCompaction(); err != nil {
		return 0, 0, fmt.Errorf("failed to ensure final compaction: %v", err)
	}

	// Calculate directory size
	dirSize, err := calculateDirSize(storagePathDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to calculate directory size: %v", err)
	}

	return duration, float64(dirSize) / (1024 * 1024), nil // Return duration and size in MB
}

// benchmarkQueryOperation measures FrostDB query performance
func benchmarkQueryOperation(storagePathDir string, numQueries int) (time.Duration, error) {
	// Create logger
	logger := kitlog.NewLogfmtLogger(kitlog.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowInfo())

	// When reopening the database, configure it the same way as during insert
	// to ensure tables are loaded correctly
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
	}

	// Open existing column store with all the same options as during insert
	columnstore, err := frostdb.New(
		frostdb.WithLogger(logger),
		frostdb.WithWAL(),
		frostdb.WithStoragePath(storagePathDir),
		frostdb.WithActiveMemorySize(100*frostdb.MiB),
		frostdb.WithRegistry(prometheus.NewRegistry()),
		frostdb.WithIndexConfig(indexConfig),
		frostdb.WithSnapshotTriggerSize(100*frostdb.MiB),
	)
	if err != nil {
		return 0, fmt.Errorf("failed to open FrostDB column store: %v", err)
	}
	defer columnstore.Close()

	// Open database
	database, err := columnstore.DB(context.Background(), "benchmark_db")
	if err != nil {
		return 0, fmt.Errorf("failed to open FrostDB database: %v", err)
	}

	// Set up table configuration again to ensure the table is loaded
	config := frostdb.NewTableConfig(dynparquet.SampleDefinition())
	_, err = database.Table("benchmark_table", config)
	if err != nil {
		return 0, fmt.Errorf("failed to open benchmark table: %v", err)
	}

	// Create query engine
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())

	// Measure query performance
	startTime := time.Now()

	for i := 0; i < numQueries; i++ {
		// Simulate different queries by rotating through components and levels
		component := fmt.Sprintf("component_%d", i%3)
		level := fmt.Sprintf("level_%d", i%4)

		err = engine.ScanTable("benchmark_table").
			Filter(
				logicalplan.And(
					logicalplan.Col("labels.component").Eq(logicalplan.Literal(component)),
					logicalplan.Col("labels.level").Eq(logicalplan.Literal(level)),
				),
			).
			Limit(logicalplan.Literal(int64(100))).
			Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
				// Just consume the record
				return nil
			})
		if err != nil {
			return 0, fmt.Errorf("failed to execute query: %v", err)
		}
	}

	return time.Since(startTime), nil
}

func main() {
	fmt.Println("Starting FrostDB Performance Benchmark")
	fmt.Println("=====================================")

	// Test parameters - use the same as TSDB benchmark for comparison
	dataSizes := []int{10000, 100000}
	if len(os.Args) > 1 && os.Args[1] == "full" {
		dataSizes = append(dataSizes, 1000000)
		fmt.Println("Running full benchmark including 1M records test")
	} else {
		fmt.Println("Running smaller benchmark (use 'go run main.go full' for 1M records test)")
	}

	batchSize := 1000
	numQueries := 100
	basePath := "D:/Temp/frostdb_benchmark"
	frostDBPath := filepath.Join(basePath, "frostdb")

	// Ensure base directory exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		fmt.Printf("Error creating base directory: %v\n", err)
		os.Exit(1)
	}

	// Store results
	var results []ResultEntry

	// Run benchmarks for each data size
	for _, dataSize := range dataSizes {
		fmt.Printf("\nRunning benchmarks for %d records...\n", dataSize)

		// Insert benchmark
		fmt.Printf("Benchmarking insert operation with %d records...\n", dataSize)
		insertDuration, storageSize, err := benchmarkInsertOperation(frostDBPath, dataSize, batchSize)
		if err != nil {
			fmt.Printf("Error benchmarking insert: %v\n", err)
			continue
		}
		results = append(results, ResultEntry{
			Operation:     "Insert",
			DataSize:      dataSize,
			Duration:      insertDuration,
			StorageSizeMB: storageSize,
			RecordsPerSec: float64(dataSize) / insertDuration.Seconds(),
		})

		fmt.Printf("Insert completed in %v - %.2f records/sec\n",
			insertDuration, float64(dataSize)/insertDuration.Seconds())
		fmt.Printf("Storage size: %.2f MB (%.2f bytes/record)\n",
			storageSize, (storageSize*1024*1024)/float64(dataSize))

		// Add a brief pause to ensure the database is properly closed and persisted
		fmt.Println("Waiting for database to be properly closed...")
		time.Sleep(2 * time.Second)

		// Query benchmark
		fmt.Printf("Benchmarking query operation with %d queries...\n", numQueries)
		queryDuration, err := benchmarkQueryOperation(frostDBPath, numQueries)
		if err != nil {
			fmt.Printf("Error benchmarking query: %v\n", err)
		} else {
			results = append(results, ResultEntry{
				Operation:     "Query",
				DataSize:      dataSize,
				Duration:      queryDuration,
				StorageSizeMB: storageSize,
				RecordsPerSec: float64(numQueries) / queryDuration.Seconds(),
			})

			fmt.Printf("Query completed in %v - %.2f queries/sec\n",
				queryDuration, float64(numQueries)/queryDuration.Seconds())
		}
	}

	// Print results
	fmt.Println("\nBenchmark Results Summary:")
	fmt.Println("=========================")
	fmt.Println(printResults(results))
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
