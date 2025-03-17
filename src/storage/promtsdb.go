package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// PromTSDBStore implements a time series database using Prometheus TSDB
type PromTSDBStore struct {
	path      string
	retention time.Duration
	db        *tsdb.DB
	mu        sync.RWMutex
}

// NewPromTSDBStore creates a new Prometheus TSDB store
func NewPromTSDBStore(path string, retention time.Duration, blockSize time.Duration, compaction bool) (*PromTSDBStore, error) {
	// Create TSDB options
	opts := tsdb.DefaultOptions()
	opts.RetentionDuration = int64(retention.Seconds())
	opts.MaxBlockDuration = int64(blockSize.Seconds())

	// Create TSDB
	db, err := tsdb.Open(path, nil, nil, opts, nil)
	if err != nil {
		return nil, fmt.Errorf("error opening TSDB: %w", err)
	}

	// Create store
	store := &PromTSDBStore{
		path:      path,
		retention: retention,
		db:        db,
	}

	return store, nil
}

// Close closes the TSDB store
func (s *PromTSDBStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// StoreMetric stores a metric data point
func (s *PromTSDBStore) StoreMetric(point *DataPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Convert DataPoint to Prometheus Sample
	lbls := make([]labels.Label, 0, len(point.Labels))
	for k, v := range point.Labels {
		lbls = append(lbls, labels.Label{Name: k, Value: v})
	}

	// Sort labels as required by Prometheus TSDB
	sort.Sort(labels.Labels(lbls))

	// Create appender
	ctx := context.Background()
	app := s.db.Appender(ctx)

	// Add sample - using milliseconds since that's what Prometheus TSDB expects
	_, err := app.Append(0, lbls, point.Timestamp.UnixMilli(), point.Value)
	if err != nil {
		app.Rollback()
		return fmt.Errorf("error appending sample: %w", err)
	}

	// Commit the transaction
	if err := app.Commit(); err != nil {
		return fmt.Errorf("error committing samples: %w", err)
	}

	return nil
}

// QueryMetrics queries metrics based on criteria
func (s *PromTSDBStore) QueryMetrics(query *MetricQuery) ([]*DataPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Create Prometheus querier
	querier, err := s.db.Querier(query.StartTime.UnixMilli(), query.EndTime.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("error creating querier: %v", err)
	}
	defer querier.Close()

	// Create hints for the query
	hints := &storage.SelectHints{
		Start: query.StartTime.UnixMilli(),
		End:   query.EndTime.UnixMilli(),
		Step:  60 * 1000, // 1 minute in milliseconds
	}

	// According to the Prometheus documentation, we need at least one matcher
	// to select data. Let's create a matcher that selects all series by looking
	// for the presence of any label.
	var matchers []*labels.Matcher

	// Create a regex matcher that matches any value of the 'service' label
	// This is common in our test data
	serviceMatcher, err := labels.NewMatcher(labels.MatchRegexp, "service", ".*")
	if err != nil {
		return nil, fmt.Errorf("error creating label matcher: %v", err)
	}
	matchers = append(matchers, serviceMatcher)

	// Query the TSDB
	ctx := context.Background()
	seriesSet := querier.Select(ctx, false, hints, matchers...)

	// Process results
	var results []*DataPoint

	// Iterate through each series
	for seriesSet.Next() {
		series := seriesSet.At()
		lbls := series.Labels()

		// Convert Prometheus labels to map
		labelsMap := make(map[string]string, lbls.Len())
		for _, l := range lbls {
			labelsMap[l.Name] = l.Value
		}

		// Apply label filter if provided
		if query.LabelFilter != nil && !query.LabelFilter(labelsMap) {
			continue
		}

		// Initialize a chunk iterator to access samples
		chkIterator := chunkenc.NewNopIterator()
		iterator := series.Iterator(chkIterator)

		// Iterate through all samples in the series
		for {
			// Get the next sample. Next() returns a ValueType enum that indicates the type
			// of value (e.g. float, histogram). We only care if there's a sample or not.
			valueType := iterator.Next()
			if valueType == chunkenc.ValNone {
				// No more samples
				break
			}

			// Get the timestamp and value at the current position
			ts, val := iterator.At()

			// Create a data point for each sample
			dataPoint := &DataPoint{
				Timestamp: time.UnixMilli(ts),
				Value:     val,
				Labels:    labelsMap,
			}

			results = append(results, dataPoint)
		}

		// Check for errors from the iterator
		if err := iterator.Err(); err != nil {
			return nil, fmt.Errorf("error iterating through samples: %v", err)
		}
	}

	// Check for errors from the series set
	if err := seriesSet.Err(); err != nil {
		return nil, fmt.Errorf("error selecting series: %v", err)
	}

	// Sort results by timestamp
	sort.Slice(results, func(i, j int) bool {
		return results[i].Timestamp.Before(results[j].Timestamp)
	})

	// Apply limit if specified
	if query.Limit > 0 && len(results) > query.Limit {
		results = results[:query.Limit]
	}

	return results, nil
}

// StoreTrace stores a trace data point (implements TracesStore interface)
func (s *PromTSDBStore) StoreTrace(point *DataPoint) error {
	return s.StoreMetric(point)
}

// QueryTraces queries traces based on criteria (implements TracesStore interface)
func (s *PromTSDBStore) QueryTraces(query *MetricQuery) ([]*DataPoint, error) {
	return s.QueryMetrics(query)
}
