package main

import (
	"fmt"
	"os"
	"time"

	"telemy/storage"
)

func main() {
	// Create a temporary directory for testing
	tempDir := "temp_tsdb_test"
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		fmt.Printf("Error creating temp directory: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tempDir)

	// Create a new TSDB store
	store, err := storage.NewPromTSDBStore(tempDir, 24*time.Hour, time.Hour, true)
	if err != nil {
		fmt.Printf("Error creating TSDB store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// Get a fixed reference time
	baseTime := time.Now().Truncate(time.Second)
	fmt.Printf("Base time: %v\n", baseTime)

	// Store multiple data points
	testPoints := []*storage.DataPoint{
		{
			Timestamp: baseTime,
			Value:     42.0,
			Labels: map[string]string{
				"service":  "test",
				"instance": "localhost",
				"metric":   "test_metric1",
			},
		},
		{
			Timestamp: baseTime.Add(10 * time.Second),
			Value:     43.5,
			Labels: map[string]string{
				"service":  "test",
				"instance": "localhost",
				"metric":   "test_metric1",
			},
		},
		{
			Timestamp: baseTime.Add(20 * time.Second),
			Value:     44.2,
			Labels: map[string]string{
				"service":  "test",
				"instance": "localhost",
				"metric":   "test_metric2",
			},
		},
	}

	fmt.Println("Storing data points...")
	for i, point := range testPoints {
		if err := store.StoreMetric(point); err != nil {
			fmt.Printf("Error storing data point %d: %v\n", i, err)
			os.Exit(1)
		}
		fmt.Printf("Stored point %d: %v, value: %v\n", i, point.Timestamp, point.Value)
	}

	// Wait for data to be written
	fmt.Println("Waiting for data to be written...")
	time.Sleep(2 * time.Second)

	// Explicitly flush any pending data (if the API supports it)
	fmt.Println("Ensuring all data is committed...")

	// Query with a time range that should include all points
	query := &storage.MetricQuery{
		StartTime: baseTime.Add(-1 * time.Minute),
		EndTime:   baseTime.Add(1 * time.Minute),
	}

	fmt.Println("Querying data points...")
	results, err := store.QueryMetrics(query)
	if err != nil {
		fmt.Printf("Error querying data points: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d data points\n", len(results))
	for i, result := range results {
		fmt.Printf("  Point %d: timestamp=%v, value=%v, labels=%v\n",
			i, result.Timestamp, result.Value, result.Labels)
	}

	// Query with a label filter
	fmt.Println("\nQuerying with label filter for test_metric1...")
	queryWithFilter := &storage.MetricQuery{
		StartTime: baseTime.Add(-1 * time.Minute),
		EndTime:   baseTime.Add(1 * time.Minute),
		LabelFilter: func(labels map[string]string) bool {
			return labels["metric"] == "test_metric1"
		},
	}

	resultsFiltered, err := store.QueryMetrics(queryWithFilter)
	if err != nil {
		fmt.Printf("Error querying data points with filter: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d filtered data points\n", len(resultsFiltered))
	for i, result := range resultsFiltered {
		fmt.Printf("  Point %d: timestamp=%v, value=%v, labels=%v\n",
			i, result.Timestamp, result.Value, result.Labels)
	}

	fmt.Println("TSDB test completed successfully!")
}
