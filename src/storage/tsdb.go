package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// TSDBStore implements a time series database for metrics and traces
type TSDBStore struct {
	path           string
	retention      time.Duration
	blockSize      time.Duration
	compaction     bool
	blocks         map[string]*Block
	mu             sync.RWMutex
	compactionChan chan struct{}
	stopChan       chan struct{}
	wg             sync.WaitGroup
}

// Block represents a time block in the TSDB
type Block struct {
	ID        string
	StartTime time.Time
	EndTime   time.Time
	Path      string
	index     map[string][]DataPoint
	mu        sync.RWMutex
}

// DataPoint represents a single data point in the TSDB
type DataPoint struct {
	Timestamp time.Time         `json:"timestamp"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// NewTSDBStore creates a new TSDB store
func NewTSDBStore(path string, retention, blockSize time.Duration, compaction bool) (*TSDBStore, error) {
	// Create store
	store := &TSDBStore{
		path:           path,
		retention:      retention,
		blockSize:      blockSize,
		compaction:     compaction,
		blocks:         make(map[string]*Block),
		compactionChan: make(chan struct{}, 1),
		stopChan:       make(chan struct{}),
	}

	// Load existing blocks
	if err := store.loadBlocks(); err != nil {
		return nil, fmt.Errorf("error loading blocks: %w", err)
	}

	// Start compaction if enabled
	if compaction {
		store.startCompaction()
	}

	// Start retention checker
	store.startRetentionChecker()

	return store, nil
}

// Close closes the TSDB store
func (s *TSDBStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Stop background goroutines
	close(s.stopChan)
	s.wg.Wait()

	// Write all blocks to disk
	for _, block := range s.blocks {
		if err := s.writeBlock(block); err != nil {
			return fmt.Errorf("error writing block %s: %w", block.ID, err)
		}
	}

	return nil
}

// StoreMetric stores a metric data point
func (s *TSDBStore) StoreMetric(point *DataPoint) error {
	// Find or create a block for this timestamp
	block, err := s.getOrCreateBlock(point.Timestamp)
	if err != nil {
		return fmt.Errorf("error getting block: %w", err)
	}

	// Store the data point in the block
	block.mu.Lock()
	defer block.mu.Unlock()

	// Generate a key from the labels
	key := s.generateLabelKey(point.Labels)

	// Add the data point to the block
	if block.index == nil {
		block.index = make(map[string][]DataPoint)
	}
	block.index[key] = append(block.index[key], *point)

	// Signal compaction if needed
	if s.compaction && len(block.index[key]) > 1000 {
		select {
		case s.compactionChan <- struct{}{}:
		default:
			// Compaction already scheduled
		}
	}

	return nil
}

// QueryMetrics queries metrics based on criteria
func (s *TSDBStore) QueryMetrics(query *MetricQuery) ([]*DataPoint, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []*DataPoint

	// Filter blocks by time range
	blocksToQuery := s.getBlocksInTimeRange(query.StartTime, query.EndTime)

	// Query each block
	for _, block := range blocksToQuery {
		block.mu.RLock()

		// Query by label key
		for labelKey, points := range block.index {
			// Apply label filter if provided
			if query.LabelFilter != nil && !query.LabelFilter(s.parseLabelKey(labelKey)) {
				continue
			}

			// Filter points by time range
			for _, point := range points {
				if (point.Timestamp.Equal(query.StartTime) || point.Timestamp.After(query.StartTime)) &&
					(point.Timestamp.Equal(query.EndTime) || point.Timestamp.Before(query.EndTime)) {
					// Clone the point to avoid race conditions
					pointCopy := point
					results = append(results, &pointCopy)
				}
			}
		}

		block.mu.RUnlock()
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

// MetricQuery defines the parameters for querying metrics
type MetricQuery struct {
	StartTime   time.Time
	EndTime     time.Time
	LabelFilter func(map[string]string) bool
	Limit       int
}

// loadBlocks loads existing blocks from disk
func (s *TSDBStore) loadBlocks() error {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(s.path, 0755); err != nil {
		return fmt.Errorf("error creating directory: %w", err)
	}

	// Find all block files
	blockFiles, err := filepath.Glob(filepath.Join(s.path, "block_*.json"))
	if err != nil {
		return fmt.Errorf("error listing block files: %w", err)
	}

	// Load each block
	for _, blockFile := range blockFiles {
		// Parse block ID from filename
		blockID := filepath.Base(blockFile)
		blockID = blockID[6 : len(blockID)-5] // Remove "block_" prefix and ".json" suffix

		// Read block file
		data, err := os.ReadFile(blockFile)
		if err != nil {
			return fmt.Errorf("error reading block file %s: %w", blockFile, err)
		}

		// Parse block
		var blockData struct {
			StartTime time.Time
			EndTime   time.Time
			Points    map[string][]DataPoint
		}
		if err := json.Unmarshal(data, &blockData); err != nil {
			return fmt.Errorf("error parsing block file %s: %w", blockFile, err)
		}

		// Create block
		block := &Block{
			ID:        blockID,
			StartTime: blockData.StartTime,
			EndTime:   blockData.EndTime,
			Path:      blockFile,
			index:     blockData.Points,
		}

		// Add block to store
		s.blocks[blockID] = block
	}

	return nil
}

// getOrCreateBlock gets an existing block or creates a new one for the given timestamp
func (s *TSDBStore) getOrCreateBlock(timestamp time.Time) (*Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Calculate the block ID for this timestamp
	blockStartTime := timestamp.Truncate(s.blockSize)
	blockEndTime := blockStartTime.Add(s.blockSize)
	blockID := fmt.Sprintf("%d", blockStartTime.Unix())

	// Check if the block already exists
	if block, ok := s.blocks[blockID]; ok {
		return block, nil
	}

	// Create a new block
	blockPath := filepath.Join(s.path, fmt.Sprintf("block_%s.json", blockID))
	block := &Block{
		ID:        blockID,
		StartTime: blockStartTime,
		EndTime:   blockEndTime,
		Path:      blockPath,
		index:     make(map[string][]DataPoint),
	}
	s.blocks[blockID] = block

	return block, nil
}

// getBlocksInTimeRange returns blocks that overlap with the specified time range
func (s *TSDBStore) getBlocksInTimeRange(startTime, endTime time.Time) []*Block {
	var blocks []*Block
	for _, block := range s.blocks {
		if (block.StartTime.Equal(endTime) || block.StartTime.Before(endTime)) &&
			(block.EndTime.Equal(startTime) || block.EndTime.After(startTime)) {
			blocks = append(blocks, block)
		}
	}
	return blocks
}

// writeBlock writes a block to disk
func (s *TSDBStore) writeBlock(block *Block) error {
	block.mu.RLock()
	defer block.mu.RUnlock()

	// Create block data
	blockData := struct {
		StartTime time.Time
		EndTime   time.Time
		Points    map[string][]DataPoint
	}{
		StartTime: block.StartTime,
		EndTime:   block.EndTime,
		Points:    block.index,
	}

	// Encode block data
	data, err := json.Marshal(blockData)
	if err != nil {
		return fmt.Errorf("error encoding block data: %w", err)
	}

	// Write block file
	if err := os.WriteFile(block.Path, data, 0644); err != nil {
		return fmt.Errorf("error writing block file: %w", err)
	}

	return nil
}

// generateLabelKey generates a key from labels
func (s *TSDBStore) generateLabelKey(labels map[string]string) string {
	if len(labels) == 0 {
		return "_no_labels_"
	}

	// Create a sorted slice of key-value pairs
	pairs := make([]string, 0, len(labels))
	for k, v := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(pairs)

	// Join the pairs with commas
	return fmt.Sprintf("{%s}", fmt.Sprintf("%s", pairs))
}

// parseLabelKey parses a label key into a map
func (s *TSDBStore) parseLabelKey(key string) map[string]string {
	if key == "_no_labels_" {
		return make(map[string]string)
	}

	// Remove braces
	if len(key) < 2 {
		return make(map[string]string)
	}
	key = key[1 : len(key)-1]

	// Split by comma
	pairs := make(map[string]string)
	for _, pair := range pairs {
		// Split by equal sign
		kv := pair
		k, v := kv[:len(kv)-1], kv[len(kv)-1:]
		pairs[k] = v
	}

	return pairs
}

// startCompaction starts the compaction process
func (s *TSDBStore) startCompaction() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.compact()
			case <-s.compactionChan:
				s.compact()
			case <-s.stopChan:
				return
			}
		}
	}()
}

// compact compacts blocks
func (s *TSDBStore) compact() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Write all blocks to disk
	for _, block := range s.blocks {
		if err := s.writeBlock(block); err != nil {
			fmt.Printf("Error writing block %s: %v\n", block.ID, err)
		}
	}
}

// startRetentionChecker starts the retention checker
func (s *TSDBStore) startRetentionChecker() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.enforceRetention()
			case <-s.stopChan:
				return
			}
		}
	}()
}

// enforceRetention enforces the retention policy
func (s *TSDBStore) enforceRetention() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Calculate cutoff time
	cutoff := time.Now().Add(-s.retention)

	// Find blocks to delete
	var blocksToDelete []string
	for id, block := range s.blocks {
		if block.EndTime.Before(cutoff) {
			blocksToDelete = append(blocksToDelete, id)
		}
	}

	// Delete blocks
	for _, id := range blocksToDelete {
		block := s.blocks[id]
		// Remove the block file
		if err := os.Remove(block.Path); err != nil && !os.IsNotExist(err) {
			fmt.Printf("Error removing block file %s: %v\n", block.Path, err)
		}
		// Remove the block from memory
		delete(s.blocks, id)
	}

	if len(blocksToDelete) > 0 {
		fmt.Printf("Deleted %d expired blocks\n", len(blocksToDelete))
	}
}
