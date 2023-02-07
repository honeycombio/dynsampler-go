package dynsampler

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"sync/atomic"

	"github.com/stretchr/testify/assert"
)

// AtomicRecord is the naive implementation of blocklist that serves as the reference implementation
// for our tests.
// This datastructure is designed to be completely linearizable, as it has a single lock that it
// acquires with every operation.
type AtomicRecord struct {
	records map[string][]int64
	maxKeys int
	lock    sync.Mutex
}

func NewAtomicRecord(maxKeys int) *AtomicRecord {
	return &AtomicRecord{
		records: make(map[string][]int64),
		maxKeys: maxKeys,
	}
}

func (r *AtomicRecord) IncrementKey(key string, keyIndex int64) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.records) >= r.maxKeys {
		return MaxSizeError{key: key}
	}
	r.records[key] = append([]int64{keyIndex}, r.records[key]...)
	return nil
}

func (r *AtomicRecord) AggregateCounts(
	currentIndex int64,
	lookbackIndex int64,
) (aggregateCounts map[string]int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	startIndex := currentIndex - 1
	finishIndex := startIndex - lookbackIndex

	aggregateCounts = make(map[string]int)
	for key, record := range r.records {
		// Aggregate.
		lastIndex := -1
		for i, r := range record {
			if r <= startIndex && r > finishIndex {
				aggregateCounts[key] += 1
			}
			if lastIndex == -1 && r <= finishIndex {
				lastIndex = i
			}
		}
		if lastIndex == -1 {
			continue
		} else if lastIndex == 0 {
			delete(r.records, key)
			continue
		}
		r.records[key] = record[0:lastIndex]
	}

	return aggregateCounts
}

func getSeededRandom() (*rand.Rand, int64) {
	seed := time.Now().UnixNano()
	s1 := rand.NewSource(seed)
	return rand.New(s1), seed
}

// Basic sanity test.
func TestSanity(t *testing.T) {
	blockList := NewUnboundedBlockList()
	atomicRecord := NewAtomicRecord(10)
	testKey := "test_key"
	currentIndex := int64(0)

	for i := 0; i < 10; i++ {
		blockList.IncrementKey(testKey, currentIndex)
		atomicRecord.IncrementKey(testKey, currentIndex)
		currentIndex += 1
	}

	assert.Equal(t, atomicRecord.AggregateCounts(1, 5), blockList.AggregateCounts(1, 5))
	assert.Equal(t, atomicRecord.AggregateCounts(0, 2), blockList.AggregateCounts(0, 2))
	assert.Equal(t, atomicRecord.AggregateCounts(6, 5), blockList.AggregateCounts(6, 5))
}

func TestBounded(t *testing.T) {
	blockList := NewBoundedBlockList(10)
	atomicRecord := NewAtomicRecord(10)

	currentIndex := int64(0)

	// Test basic dropping.
	for i := 0; i < 15; i++ {
		testKey := fmt.Sprintf("test_%d", i)
		actualErr := blockList.IncrementKey(testKey, currentIndex)
		expectedErr := atomicRecord.IncrementKey(testKey, currentIndex)
		assert.Equal(t, expectedErr, actualErr)
	}

	// Test expire.
	currentIndex = 10
	assert.Equal(t, atomicRecord.AggregateCounts(currentIndex, 5),
		blockList.AggregateCounts(currentIndex, 5))

	// Consistent single insert per count.
	for i := 0; i < 15; i++ {
		testKey := fmt.Sprintf("test_%d", i)
		actualErr := blockList.IncrementKey(testKey, currentIndex)
		expectedErr := atomicRecord.IncrementKey(testKey, currentIndex)
		assert.Equal(t, expectedErr, actualErr)
		assert.Equal(t, atomicRecord.AggregateCounts(currentIndex, 10),
			blockList.AggregateCounts(currentIndex, 10))
		currentIndex += 1
	}

	// Random insert number of each key.
	random, _ := getSeededRandom()
	for i := 0; i < 30; i++ {
		for j := 0; j < 10; j++ {
			keySuffix := random.Intn(20)
			testKey := fmt.Sprintf("test_%d", keySuffix)
			actualErr := blockList.IncrementKey(testKey, currentIndex)
			expectedErr := atomicRecord.IncrementKey(testKey, currentIndex)
			assert.Equal(t, expectedErr, actualErr)
		}

		assert.Equal(t, atomicRecord.AggregateCounts(currentIndex, 10),
			blockList.AggregateCounts(currentIndex, 10))
		currentIndex += 1
	}
}

// Simulate a real world use case and compare it against our reference implementation.
func compareConcurrency(t *testing.T, reference BlockList, actual BlockList) {
	globalIndex := int64(0)
	testKey := "test_key"

	done := make(chan bool)
	iterations := 50
	lock := sync.Mutex{}

	random, _ := getSeededRandom()

	// Index Ticker
	indexTicker := time.NewTicker(50 * time.Millisecond)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-indexTicker.C:
				atomic.AddInt64(&globalIndex, 1)
			}
		}
	}()

	// Update and aggregation ticker
	updateTicker := time.NewTicker(55 * time.Millisecond)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-updateTicker.C:
				currentIndex := atomic.LoadInt64(&globalIndex)

				lock.Lock()
				referenceAggregate := reference.AggregateCounts(currentIndex, 10)
				actualAggregate := actual.AggregateCounts(currentIndex, 10)
				assert.Equal(t, referenceAggregate, actualAggregate)
				lock.Unlock()
			}
		}
	}()

	wg := sync.WaitGroup{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				currentIndex := atomic.LoadInt64(&globalIndex)

				// These need to be performed atomically.
				lock.Lock()
				referenceErr := reference.IncrementKey(testKey, currentIndex)
				actualErr := actual.IncrementKey(testKey, currentIndex)
				assert.Equal(t, referenceErr, actualErr)
				sleepTime := time.Duration(random.Intn(100)) * time.Millisecond
				lock.Unlock()

				time.Sleep(sleepTime)
			}
		}()
	}
	wg.Wait()
	done <- true
}

func concurrentUpdates(t *testing.T, blockList BlockList) {
	start := make(chan bool)
	globalIndex := int64(0)

	// Concurrent inserts.
	go func() {
		<-start
		for i := 0; i < 1000; i++ {
			for j := 0; j < 15; j++ {
				currentIndex := atomic.LoadInt64(&globalIndex)
				testKey := fmt.Sprintf("test_%d", j)
				blockList.IncrementKey(testKey, currentIndex)
			}
		}
	}()
	// Concurrent aggregations.
	go func() {
		<-start
		for i := 0; i < 1000; i++ {
			currentIndex := atomic.LoadInt64(&globalIndex)
			blockList.AggregateCounts(currentIndex, 10)
			atomic.AddInt64(&globalIndex, 1)
		}
	}()
	close(start)
}

func TestAllConcurrency(t *testing.T) {
	compareConcurrency(t, NewUnboundedBlockList(), NewAtomicRecord(10))
	compareConcurrency(t, NewBoundedBlockList(10), NewAtomicRecord(10))

	concurrentUpdates(t, NewUnboundedBlockList())
	concurrentUpdates(t, NewBoundedBlockList(10))
}
