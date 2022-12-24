package dynsampler

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"sync/atomic"

	"github.com/stretchr/testify/assert"
)

// AtomicRecord is the naive implementation of blocklist
type AtomicRecord struct {
	records map[string][]int64
	lock    sync.Mutex
}

func NewAtomicRecord() *AtomicRecord {
	return &AtomicRecord{
		records: make(map[string][]int64),
	}
}

func (r *AtomicRecord) incrementKey(key string, keyIndex int64) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.records[key] = append(r.records[key], keyIndex)
}

func (r *AtomicRecord) aggregateCounts(currentIndex int64, lookbackIndex int64) (aggregateCounts map[string]int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	startIndex := currentIndex - 1
	finishIndex := startIndex - lookbackIndex

	aggregateCounts = make(map[string]int)
	for key, record := range r.records {
		for _, r := range record {
			if r <= startIndex && r > finishIndex {
				aggregateCounts[key] += 1
			}
		}
	}

	return aggregateCounts
}

func TestSanity(t *testing.T) {
	blockList := NewBlockList()
	atomicRecord := NewAtomicRecord()
	testKey := "test_key"
	currentIndex := int64(0)

	for i := 0; i < 10; i++ {
		blockList.incrementKey(testKey, currentIndex)
		atomicRecord.incrementKey(testKey, currentIndex)
		currentIndex += 1
	}

	assert.Equal(t, atomicRecord.aggregateCounts(1, 5), blockList.aggregateCounts(1, 5))
	assert.Equal(t, atomicRecord.aggregateCounts(0, 2), blockList.aggregateCounts(0, 2))
	assert.Equal(t, atomicRecord.aggregateCounts(6, 5), blockList.aggregateCounts(6, 5))
}

func TestConcurrency(t *testing.T) {
	blockList := NewBlockList()
	atomicRecord := NewAtomicRecord()
	globalIndex := int64(0)
	testKey := "test_key"

	done := make(chan bool)
	iterations := 50
	lock := sync.Mutex{}

	seed := time.Now().UnixNano()
	s1 := rand.NewSource(seed)
	t.Log("Running with random seed: ", seed)
	random := rand.New(s1)

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

	// Index Ticker
	updateTicker := time.NewTicker(55 * time.Millisecond)
	go func() {
		for {
			select {
			case <-done:
				return
			case <-updateTicker.C:
				currentIndex := globalIndex

				lock.Lock()
				blockAggregate := blockList.aggregateCounts(currentIndex, 10)
				atomicAggregate := atomicRecord.aggregateCounts(currentIndex, 10)
				assert.Equal(t, blockAggregate, atomicAggregate)
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
				currentIndex := globalIndex

				// These need to be performed atomically.
				lock.Lock()
				blockList.incrementKey(testKey, currentIndex)
				atomicRecord.incrementKey(testKey, currentIndex)
				lock.Unlock()

				time.Sleep(time.Duration(random.Intn(100)) * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	done <- true
}
