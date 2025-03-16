package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	kitlog "github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/index"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"

	"net/http"
)

// Helper function to format an Arrow record into a human-readable format
func formatRecord(record arrow.Record) string {
	var sb strings.Builder

	// Print schema in a more user-friendly way
	sb.WriteString("Schema: ")
	for i, field := range record.Schema().Fields() {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(field.Name)
		sb.WriteString(" (")

		// Simplify type representation for readability
		if arrow.TypeEqual(field.Type, &arrow.DictionaryType{}) {
			dictType := field.Type.(*arrow.DictionaryType)
			// For dictionary types, show the value type instead of the complex dictionary structure
			switch dictType.ValueType.ID() {
			case arrow.STRING:
				sb.WriteString("string")
			case arrow.BINARY:
				sb.WriteString("string") // Most binary dictionaries in FrostDB are actually strings
			default:
				sb.WriteString(dictType.ValueType.String())
			}
		} else {
			// Simplify other common types
			switch field.Type.ID() {
			case arrow.INT64:
				sb.WriteString("int64")
			case arrow.FLOAT64:
				sb.WriteString("float64")
			case arrow.BOOL:
				sb.WriteString("boolean")
			case arrow.STRING:
				sb.WriteString("string")
			case arrow.TIMESTAMP:
				sb.WriteString("timestamp")
			default:
				// Fall back to the Arrow type string for other types
				sb.WriteString(field.Type.String())
			}
		}
		sb.WriteString(")")
	}
	sb.WriteString("\n")

	// Get the total number of rows and limit display if too many
	totalRows := int(record.NumRows())
	maxRowsToDisplay := 5
	if totalRows > maxRowsToDisplay {
		sb.WriteString(fmt.Sprintf("Record contains %d rows. Showing first %d rows:\n", totalRows, maxRowsToDisplay))
	} else {
		sb.WriteString(fmt.Sprintf("Record contains %d rows:\n", totalRows))
	}

	// Print data (row by row)
	displayRows := min(totalRows, maxRowsToDisplay)

	for row := range displayRows {
		sb.WriteString(fmt.Sprintf("Row %d: ", row))
		for col := range int(record.NumCols()) {
			if col > 0 {
				sb.WriteString(", ")
			}
			field := record.Schema().Field(col)
			column := record.Column(col)

			// Format based on data type
			sb.WriteString(fmt.Sprintf("%s=", field.Name))
			if column.IsNull(row) {
				sb.WriteString("NULL")
				continue
			}

			switch v := column.(type) {
			case *array.Int64:
				sb.WriteString(fmt.Sprintf("%d", v.Value(row)))
			case *array.Float64:
				sb.WriteString(fmt.Sprintf("%f", v.Value(row)))
			case *array.Boolean:
				sb.WriteString(fmt.Sprintf("%t", v.Value(row)))
			case *array.String:
				sb.WriteString(fmt.Sprintf("\"%s\"", v.Value(row)))
			case *array.Binary:
				// Handle binary data safely
				sb.WriteString(fmt.Sprintf("<binary:%d bytes>", len(v.Value(row))))
			case *array.Dictionary:
				// Handle dictionary-encoded columns
				indices := v.Indices()
				dict := v.Dictionary()
				index := indices.(*array.Uint32).Value(row)

				// Extract the actual value from the dictionary
				switch d := dict.(type) {
				case *array.String:
					sb.WriteString(fmt.Sprintf("\"%s\"", d.Value(int(index))))
				case *array.Binary:
					sb.WriteString(fmt.Sprintf("\"%s\"", string(d.Value(int(index)))))
				default:
					sb.WriteString(fmt.Sprintf("<dict:%v>", dict.GetOneForMarshal(int(index))))
				}
			default:
				// Handle other types generically
				sb.WriteString(fmt.Sprintf("<%s>", field.Type))
			}
		}
		sb.WriteString("\n")
	}

	if totalRows > maxRowsToDisplay {
		sb.WriteString(fmt.Sprintf("... and %d more rows\n", totalRows-maxRowsToDisplay))
	}

	return sb.String()
}

// LogEntry represents a log entry with dynamic labels for indexing.
type LogEntry struct {
	Labels  map[string]string `frostdb:",asc"`
	Message string
}

func main() {
	// Ensure the temp directory exists
	storagePathDir := "D:/Temp/frostdb"
	if _, err := os.Stat(storagePathDir); os.IsNotExist(err) {
		if err := os.MkdirAll(storagePathDir, 0755); err != nil {
			log.Fatalf("Failed to create storage directory: %v", err)
		}
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
	var columnstore, err = frostdb.New(
		frostdb.WithLogger(logger),
		frostdb.WithWAL(),
		frostdb.WithStoragePath(storagePathDir),
		frostdb.WithActiveMemorySize(100*frostdb.MiB),
		frostdb.WithRegistry(registry),
		frostdb.WithIndexConfig(indexConfig),
		frostdb.WithSnapshotTriggerSize(100*frostdb.MiB),
	)
	if err != nil {
		log.Fatalf("Failed to create column store: %v", err)
	}

	// Defer close and handle potential errors
	defer func() {
		if closeErr := columnstore.Close(); closeErr != nil {
			log.Printf("Warning: Error during column store close: %v", closeErr)
		}
	}()

	// Open up a database in the column store with retries
	var database *frostdb.DB
	maxDBRetries := 5
	for i := range maxDBRetries {
		database, err = columnstore.DB(context.Background(), "logs_db")
		if err == nil {
			break
		}
		log.Printf("Attempt %d: Failed to open database: %v", i+1, err)
		time.Sleep(time.Duration(2<<i) * time.Second) // Exponential backoff
	}
	if err != nil {
		log.Fatalf("Failed to open database after %d attempts: %v", maxDBRetries, err)
	}

	// Set up table configuration based on sample definition (similar to tests)
	config := frostdb.NewTableConfig(
		dynparquet.SampleDefinition(),
	)

	// Create a table using the proper configuration
	table, err := database.Table("logs_table", config)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	// Generate sample logs (similar to the test patterns)
	fmt.Println("Generating and inserting log samples...")

	startWrite := time.Now()
	numSamples := 1_000_000
	batchSize := 10_000
	for i := 0; i < numSamples; i += batchSize {
		batch := dynparquet.Samples{}
		end := min(i+batchSize, numSamples)

		for j := i; j < end; j++ {
			batch = append(batch, dynparquet.Sample{
				ExampleType: fmt.Sprintf("log_type_%d", j%5),
				Labels: map[string]string{
					"level":      fmt.Sprintf("level_%d", j%4),
					"component":  fmt.Sprintf("component_%d", j%3),
					"instance":   fmt.Sprintf("instance_%d", j%10),
					"stacktrace": fmt.Sprintf("%d: System.Threading.Tasks.TaskCanceledException: The operation was canceled.\r\n ---> System.TimeoutException: A connection could not be established within the configured ConnectTimeout.\r\n   --- End of inner exception stack trace ---\r\n   at System.Net.Http.HttpConnectionPool.CreateConnectTimeoutException(OperationCanceledException oce)\r\n   at System.Net.Http.HttpConnectionPool.AddHttp11ConnectionAsync(QueueItem queueItem)\r\n   at System.Threading.ExecutionContext.RunInternal(ExecutionContext executionContext, ContextCallback callback, Object state)\r\n   at Sys", j),
				},
				Stacktrace: []uuid.UUID{
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
					{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
				},
				Timestamp: time.Now().UnixNano() - int64(j*1000000), // Different timestamps
				Value:     int64(j),
			})
		}

		r, err := batch.ToRecord()
		if err != nil {
			log.Fatalf("Failed to convert samples to record: %v", err)
		}

		_, err = table.InsertRecord(context.Background(), r)
		if err != nil {
			log.Fatalf("Failed to insert record: %v", err)
		}

		// Force compaction periodically
		if i%100000 == 0 {
			if err := table.EnsureCompaction(); err != nil {
				log.Printf("Warning: Failed to ensure compaction: %v", err)
			}
		}
	}

	writeDuration := time.Since(startWrite)
	fmt.Printf("Write completed. Inserted %d record batches in %v\n", numSamples, writeDuration)

	// Create a new query engine to retrieve data
	engine := query.NewEngine(memory.DefaultAllocator, database.TableProvider())

	// Example 1: Query by label values
	fmt.Println("\n-------------------------------------------------------------")
	fmt.Println("QUERY 1: 100 queries with different level and component values")
	fmt.Println("-------------------------------------------------------------")
	fmt.Println("Running 100 queries with different combinations...")

	totalQueries := 100
	var totalDuration time.Duration
	minDuration := time.Duration(math.MaxInt64)
	maxDuration := time.Duration(0)
	var totalRecords int

	// Track query times for analysis
	queryTimes := make([]time.Duration, totalQueries)
	recordCounts := make([]int, totalQueries)

	for i := 0; i < totalQueries; i++ {
		// Get different level and component combinations
		level := fmt.Sprintf("level_%d", i%4)
		component := fmt.Sprintf("component_%d", i%3)

		queryStart := time.Now()
		recordCount := 0

		err = engine.ScanTable("logs_table").
			Filter(
				logicalplan.And(
					logicalplan.Col("labels.level").Eq(logicalplan.Literal(level)),
					logicalplan.Col("labels.component").Eq(logicalplan.Literal(component)),
				),
			).
			Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
				recordCount++
				// Only print details for first few queries to avoid flooding console
				if i < 3 {
					fmt.Printf("Query %d (level=%s, component=%s) Result batch %d:\n%s\n",
						i+1, level, component, recordCount, formatRecord(r))
				}
				return nil
			})
		if err != nil {
			log.Printf("Query %d error: %v", i+1, err)
			continue
		}

		queryTime := time.Since(queryStart)
		queryTimes[i] = queryTime
		recordCounts[i] = recordCount

		totalDuration += queryTime
		totalRecords += recordCount

		if queryTime < minDuration {
			minDuration = queryTime
		}
		if queryTime > maxDuration {
			maxDuration = queryTime
		}

		// Print progress every 10 queries
		if (i+1)%10 == 0 {
			fmt.Printf("Completed %d/%d queries...\n", i+1, totalQueries)
		}
	}

	avgDuration := totalDuration / time.Duration(totalQueries)
	fmt.Println("\nQuery Summary:")
	fmt.Printf("Total queries: %d\n", totalQueries)
	fmt.Printf("Total records: %d\n", totalRecords)
	fmt.Printf("Total duration: %v\n", totalDuration)
	fmt.Printf("Average duration: %v\n", avgDuration)
	fmt.Printf("Min duration: %v\n", minDuration)
	fmt.Printf("Max duration: %v\n", maxDuration)

	// Display histogram of query times
	fmt.Println("\nQuery Time Distribution:")
	buckets := 5
	bucketSize := (maxDuration - minDuration) / time.Duration(buckets)
	if bucketSize == 0 {
		bucketSize = 1 * time.Microsecond // Prevent division by zero
	}

	histogram := make([]int, buckets)
	for _, duration := range queryTimes {
		bucket := int((duration - minDuration) / bucketSize)
		if bucket >= buckets {
			bucket = buckets - 1
		}
		histogram[bucket]++
	}

	for i := 0; i < buckets; i++ {
		lowerBound := minDuration + time.Duration(i)*bucketSize
		upperBound := lowerBound + bucketSize
		fmt.Printf("%v to %v: %d queries\n", lowerBound, upperBound, histogram[i])
	}

	// Example 2: Query by timestamp range
	fmt.Println("\n-------------------------------------------------------------")
	fmt.Println("QUERY 2: Logs within a time range")
	fmt.Println("-------------------------------------------------------------")
	startQuery := time.Now()
	timeCount := 0
	now := time.Now().UnixNano()
	tenSecondsAgo := now - 10*int64(time.Second)

	err = engine.ScanTable("logs_table").
		Filter(
			logicalplan.And(
				logicalplan.Col("timestamp").Gt(logicalplan.Literal(tenSecondsAgo)),
				logicalplan.Col("timestamp").Lt(logicalplan.Literal(now)),
			),
		).
		Project(
			logicalplan.Col("timestamp"),
			logicalplan.Col("value"),
			logicalplan.Col("example_type"),
			logicalplan.Col("labels.level"),
			logicalplan.Col("labels.component"),
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			timeCount++
			fmt.Printf("Result batch %d:\n%s\n", timeCount, formatRecord(r))
			return nil
		})
	if err != nil {
		log.Printf("Time-based query error: %v", err)
	}
	queryDuration := time.Since(startQuery)
	fmt.Printf("Time query completed. Found %d record batches in %v\n", timeCount, queryDuration)

	// Example 3: Aggregation query
	fmt.Println("\n-------------------------------------------------------------")
	fmt.Println("QUERY 3: Aggregation by log level")
	fmt.Println("-------------------------------------------------------------")
	fmt.Println("Calculating count, sum, min and max grouped by log level:")
	startQuery = time.Now()
	err = engine.ScanTable("logs_table").
		Aggregate(
			[]*logicalplan.AggregationFunction{
				logicalplan.Count(logicalplan.Col("value")),
				logicalplan.Sum(logicalplan.Col("value")),
				logicalplan.Min(logicalplan.Col("timestamp")),
				logicalplan.Max(logicalplan.Col("timestamp")),
			},
			[]logicalplan.Expr{
				logicalplan.Col("labels.level"),
			},
		).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			fmt.Println(formatRecord(r))
			return nil
		})
	if err != nil {
		log.Printf("Aggregation query error: %v", err)
	}
	queryDuration = time.Since(startQuery)
	fmt.Printf("Aggregation query completed in %v\n", queryDuration)

	// Example 4: Full-text search query
	fmt.Println("\n-------------------------------------------------------------")
	fmt.Println("QUERY 4: Full-text search in stacktrace")
	fmt.Println("-------------------------------------------------------------")
	fmt.Println("Searching for 'TimeoutException' in stacktrace:")

	startQuery = time.Now()
	textSearchCount := 0
	err = engine.ScanTable("logs_table").
		Filter(
			logicalplan.Col("labels.stacktrace").Contains("1000: System.Threading.Tasks.TaskCanceledException"),
		).
		Project(
			logicalplan.Col("labels.level"),
			logicalplan.Col("labels.component"),
			logicalplan.Col("labels.stacktrace"),
			logicalplan.Col("timestamp"),
		).
		Limit(logicalplan.Literal(int64(10))).
		Execute(context.Background(), func(ctx context.Context, r arrow.Record) error {
			textSearchCount++
			fmt.Printf("Full-text search result batch %d:\n%s\n", textSearchCount, formatRecord(r))
			return nil
		})
	if err != nil {
		log.Printf("Full-text search query error: %v", err)
	}
	queryDuration = time.Since(startQuery)
	fmt.Printf("Full-text search query completed. Found %d record batches in %v\n", textSearchCount, queryDuration)

	// Set up a metrics endpoint for Prometheus to scrape
	fmt.Println("\nExposing metrics at http://localhost:9090/metrics")
	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	go func() {
		if err := http.ListenAndServe(":9090", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	fmt.Println("\nTest program completed, metrics server is running.")
	fmt.Println("Press Ctrl+C to exit.")

	// Keep the program running so metrics can be viewed
	select {}
}
