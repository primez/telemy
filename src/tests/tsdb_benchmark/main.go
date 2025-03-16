// This standalone benchmark avoids package name conflicts
// Run with: go run src/tests/tsdb_benchmark/main.go

package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// DataPoint represents a single metric data point
type DataPoint struct {
	Timestamp time.Time
	Value     float64
	Labels    map[string]string
}

// MetricQuery represents a query for metrics
type MetricQuery struct {
	StartTime   time.Time
	EndTime     time.Time
	LabelFilter func(map[string]string) bool
	Limit       int
}

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

// createSamplePoint generates a data point with predictable values
func createSamplePoint(i int, baseTime time.Time) *DataPoint {
	// Use a fixed base time and add incremental seconds
	timestampOffset := time.Duration(i%3600) * time.Second // Limit to 1 hour range
	return &DataPoint{
		Timestamp: baseTime.Add(timestampOffset),
		Value:     float64(i),
		Labels: map[string]string{
			"level":        fmt.Sprintf("level_%d", i%4),
			"component":    fmt.Sprintf("component_%d", i%3),
			"instance":     fmt.Sprintf("instance_%d", i%10),
			"example_type": fmt.Sprintf("log_type_%d", i%5),
			"stacktrace":   fmt.Sprintf("%d: System.Exception", i),
		},
	}
}

// convertToLabels converts a map to Prometheus labels
func convertToLabels(m map[string]string) labels.Labels {
	result := make(labels.Labels, 0, len(m))
	for k, v := range m {
		result = append(result, labels.Label{Name: k, Value: v})
	}
	// Sort for Prometheus compatibility
	result = labels.New(result...)
	return result
}

// openTSDB creates and returns a new Prometheus TSDB
func openTSDB(path string, retention time.Duration) (*tsdb.DB, error) {
	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = int64(retention.Seconds() * 1000)
	opts.MaxBlockDuration = int64(time.Hour.Seconds() * 1000)

	return tsdb.Open(path, nil, nil, opts, nil)
}

// benchmarkInsertOperation measures TSDB insert performance
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

	// Create TSDB store
	db, err := openTSDB(storagePathDir, 24*time.Hour)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create TSDB: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Printf("Warning: Error closing DB: %v\n", err)
		}
	}()

	// Measure insert performance
	startTime := time.Now()
	baseTime := startTime.Add(-12 * time.Hour)

	for i := 0; i < numRecords; i += batchSize {
		end := i + batchSize
		if end > numRecords {
			end = numRecords
		}

		// Create a new appender for each batch
		app := db.Appender(context.Background())

		// Process each batch
		for j := i; j < end; j++ {
			point := createSamplePoint(j, baseTime)
			lbls := convertToLabels(point.Labels)

			// Add the sample to the appender
			_, err := app.Append(0, lbls, point.Timestamp.UnixMilli(), point.Value)
			if err != nil {
				app.Rollback()
				return 0, 0, fmt.Errorf("failed to append sample: %v", err)
			}
		}

		// Commit the batch
		if err := app.Commit(); err != nil {
			return 0, 0, fmt.Errorf("failed to commit samples: %v", err)
		}

		// Print progress for longer operations
		if numRecords > 10000 && i%(numRecords/10) == 0 && i > 0 {
			fmt.Printf("  Progress: %d%% complete (%d/%d records)\n",
				i*100/numRecords, i, numRecords)
		}
	}

	duration := time.Since(startTime)

	// Calculate directory size
	dirSize, err := calculateDirSize(storagePathDir)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to calculate directory size: %v", err)
	}

	return duration, float64(dirSize) / (1024 * 1024), nil // Return duration and size in MB
}

// benchmarkQueryOperation measures TSDB query performance
func benchmarkQueryOperation(storagePathDir string, numQueries int) (time.Duration, error) {
	// Open existing TSDB store
	db, err := openTSDB(storagePathDir, 24*time.Hour)
	if err != nil {
		return 0, fmt.Errorf("failed to open TSDB: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			fmt.Printf("Warning: Error closing DB: %v\n", err)
		}
	}()

	// Use a fixed reference time for queries that's close to what was used for insertion
	baseTime := time.Now().Add(-12 * time.Hour)
	startTimeMs := baseTime.UnixMilli()
	endTimeMs := baseTime.Add(1 * time.Hour).UnixMilli()

	// Measure query performance
	startTime := time.Now()

	for i := 0; i < numQueries; i++ {
		// Simulate different queries by rotating through components and levels
		component := fmt.Sprintf("component_%d", i%3)
		level := fmt.Sprintf("level_%d", i%4)

		// Create a query using Prometheus native querier
		q, err := db.Querier(startTimeMs, endTimeMs)
		if err != nil {
			return 0, fmt.Errorf("failed to create querier: %v", err)
		}

		// Create matchers for the component and level
		matchers := []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "component", component),
			labels.MustNewMatcher(labels.MatchEqual, "level", level),
		}

		// Create query hints
		hints := &storage.SelectHints{
			Start: startTimeMs,
			End:   endTimeMs,
			Step:  60 * 1000, // 1 minute in milliseconds
		}

		// Execute the query with the required context and hints
		seriesSet := q.Select(context.Background(), false, hints, matchers...)

		// Count the results (consume the iterator)
		resultCount := 0
		for seriesSet.Next() {
			series := seriesSet.At()

			// Create a chunk iterator
			chunkIter := chunkenc.NewNopIterator()
			iter := series.Iterator(chunkIter)

			// Process the iterator results - just count them
			for iter.Next() != chunkenc.ValNone {
				// Values are available via iter.At() but we just need to count
				resultCount++
				if resultCount >= 100 { // Limit to 100 results
					break
				}
			}

			if err := iter.Err(); err != nil {
				q.Close()
				return 0, fmt.Errorf("error iterating: %v", err)
			}

			if resultCount >= 100 {
				break
			}
		}

		if err := seriesSet.Err(); err != nil {
			q.Close()
			return 0, fmt.Errorf("error in series set: %v", err)
		}

		// Close the querier
		if err := q.Close(); err != nil {
			return 0, fmt.Errorf("failed to close querier: %v", err)
		}
	}

	return time.Since(startTime), nil
}

func main() {
	fmt.Println("Starting TSDB Performance Benchmark")
	fmt.Println("==================================")

	// Test parameters
	dataSizes := []int{10000, 100000}
	if len(os.Args) > 1 && os.Args[1] == "full" {
		dataSizes = append(dataSizes, 1000000)
		fmt.Println("Running full benchmark including 1M records test")
	} else {
		fmt.Println("Running smaller benchmark (use 'go run main.go full' for 1M records test)")
	}

	batchSize := 1000
	numQueries := 100
	basePath := "D:/Temp/tsdb_benchmark"
	tsdbPath := filepath.Join(basePath, "tsdb")

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
		insertDuration, storageSize, err := benchmarkInsertOperation(tsdbPath, dataSize, batchSize)
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

		// Query benchmark
		fmt.Printf("Benchmarking query operation with %d queries...\n", numQueries)
		queryDuration, err := benchmarkQueryOperation(tsdbPath, numQueries)
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
