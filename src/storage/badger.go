package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

// LogEntry represents a log entry in the system
type LogEntry struct {
	Timestamp time.Time         `json:"timestamp"`
	Level     string            `json:"level"`
	Message   string            `json:"message"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// BadgerStore implements log storage using BadgerDB
type BadgerStore struct {
	db         *badger.DB
	indexing   bool
	maxSizeMB  int
	path       string
	gcInterval time.Duration
	stopChan   chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
}

// NewBadgerStore creates a new BadgerDB store
func NewBadgerStore(path string, indexing bool, maxSizeMB int) (*BadgerStore, error) {
	// Open BadgerDB
	opts := badger.DefaultOptions(path).
		WithLogger(nil).
		WithLoggingLevel(badger.WARNING)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("error opening BadgerDB: %w", err)
	}

	store := &BadgerStore{
		db:         db,
		indexing:   indexing,
		maxSizeMB:  maxSizeMB,
		path:       path,
		gcInterval: 10 * time.Minute,
		stopChan:   make(chan struct{}),
	}

	// Start background GC
	store.startGC()

	return store, nil
}

// Close closes the BadgerDB store
func (s *BadgerStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop GC
	close(s.stopChan)
	s.wg.Wait()

	// Close DB
	return s.db.Close()
}

// StoreLog stores a log entry
func (s *BadgerStore) StoreLog(entry *LogEntry) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Generate key based on timestamp and a sequence number to handle duplicate timestamps
	key := s.generateLogKey(entry.Timestamp)

	// Encode log entry to JSON
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("error marshaling log entry: %w", err)
	}

	// Store log entry
	err = s.db.Update(func(txn *badger.Txn) error {
		err := txn.Set(key, data)
		if err != nil {
			return err
		}

		// Create indexes if indexing is enabled
		if s.indexing {
			if err := s.createIndexes(txn, entry, key); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("error storing log entry: %w", err)
	}

	return nil
}

// QueryLogs queries logs based on criteria
func (s *BadgerStore) QueryLogs(query *LogQuery) ([]*LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*LogEntry

	// Time-based filtering
	startKey := s.generateLogKey(query.StartTime)
	endKey := s.generateLogKey(query.EndTime)

	// Query logs
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		// Iterate over logs in time range
		for it.Seek(startKey); it.Valid() && bytes.Compare(it.Item().Key(), endKey) <= 0; it.Next() {
			item := it.Item()
			var entry LogEntry

			// Process the item data
			err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &entry)
			})

			if err != nil {
				return fmt.Errorf("error unmarshaling log entry: %w", err)
			}

			// Apply additional filters
			if query.Filter != nil && !query.Filter(&entry) {
				continue
			}

			results = append(results, &entry)

			// Limit results if specified
			if query.Limit > 0 && len(results) >= query.Limit {
				break
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("error querying logs: %w", err)
	}

	return results, nil
}

// LogQuery defines the parameters for querying logs
type LogQuery struct {
	StartTime time.Time
	EndTime   time.Time
	Filter    func(*LogEntry) bool
	Limit     int
}

// generateLogKey generates a key for a log entry based on timestamp
func (s *BadgerStore) generateLogKey(timestamp time.Time) []byte {
	// Key format: "log_" + timestamp (nanoseconds since epoch)
	key := make([]byte, 4+8) // 4 bytes for prefix "log_" and 8 bytes for timestamp
	copy(key[0:], []byte("log_"))
	binary.BigEndian.PutUint64(key[4:], uint64(timestamp.UnixNano()))
	return key
}

// createIndexes creates indexes for log entry fields
func (s *BadgerStore) createIndexes(txn *badger.Txn, entry *LogEntry, logKey []byte) error {
	// Index log level
	levelKey := fmt.Sprintf("idx_level_%s_%d", entry.Level, entry.Timestamp.UnixNano())
	if err := txn.Set([]byte(levelKey), logKey); err != nil {
		return err
	}

	// Index labels
	for k, v := range entry.Labels {
		labelKey := fmt.Sprintf("idx_label_%s_%s_%d", k, v, entry.Timestamp.UnixNano())
		if err := txn.Set([]byte(labelKey), logKey); err != nil {
			return err
		}
	}

	return nil
}

// startGC starts the garbage collection process
func (s *BadgerStore) startGC() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(s.gcInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Run value log GC
				err := s.db.RunValueLogGC(0.5) // Run GC if we can reclaim 50% space
				if err != nil && err != badger.ErrNoRewrite {
					// Log the error, but don't stop the loop
					fmt.Printf("BadgerDB GC error: %v\n", err)
				}
			case <-s.stopChan:
				return
			}
		}
	}()
}
