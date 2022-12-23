package dynsampler

import (
	"math"
	"sync"
	"time"
)

type Block struct {
	index    int64 // MUST be monotonically increasing.
	countMap map[string]int
	next     *Block
}

type BlockList struct {
	head *Block // Sentinel node for our linked list.
}

func NewBlockList() *BlockList {
	// Create a sentinel node.

	head := &Block{
		index:    math.MaxInt64,
		countMap: make(map[string]int),
		next:     nil,
	}
	return &BlockList{
		head: head,
	}
}

func (b *BlockList) incrementKey(key string, keyIndex int64) {
	// A block matching keyStamp exists. Just increment the key there.
	if b.head.next != nil && b.head.next.index == keyIndex {
		b.head.next.countMap[key] += 1
		return
	}

	// We need to create a new block.
	currentFront := b.head.next
	b.head.next = &Block{
		index:    keyIndex,
		countMap: make(map[string]int),
		next:     currentFront,
	}
	b.head.next.countMap[key] += 1
}

func (b *BlockList) aggregateCounts(currentIndex int64, lookbackIndex int64) (aggregateCounts map[string]int) {
	aggregateCounts = make(map[string]int)

	// Aggregate from currentIndex - 1 and lookback lookbackIndex.
	startIndex := currentIndex - 1
	finishIndex := startIndex - lookbackIndex

	// front is a pointer that iterates through our linked list. Start at the sentinel.
	front := b.head
	for front != nil {
		// Start aggregation at current index - 1.
		if front.index <= startIndex {
			for k, v := range front.countMap {
				aggregateCounts[k] += v
			}
		}

		// Stop and drop remaining blocks after t - LookBackFrequencySec
		if front.next != nil && front.next.index < finishIndex {
			front.next = nil
			break
		}
		front = front.next
	}

	return aggregateCounts
}

type WindowedThroughput struct {
	// UpdateFrequency is how often the sampling rate is recomputed, default is 1s
	UpdateFrequencySec int

	// LookbackFrequency is how far back in time we lookback to dynamically adjust our sampling rate. Default is 30s.
	LookbackFrequencySec int

	// GoalThroughputPerSec is the target number of events to send per second.
	// Sample rates are generated to squash the total throughput down to match the
	// goal throughput. Actual throughput may exceed goal throughput. default 100
	GoalThroughputPerSec int

	// MaxKeys, if greater than 0, limits the number of distinct keys used to build
	// the sample rate map within the interval defined by `ClearFrequencySec`. Once
	// MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	savedSampleRates map[string]int
	countList        *BlockList

	indexGenerator IndexGenerator

	lock sync.Mutex
}

type IndexGenerator interface {
	getCurrentIndex() int64
}

type UnixSecondsIndexGenerator struct{}

func (g *UnixSecondsIndexGenerator) getCurrentIndex() int64 {
	return time.Now().Unix()
}

func (t *WindowedThroughput) Start() error {
	// apply defaults
	if t.UpdateFrequencySec == 0 {
		t.UpdateFrequencySec = 1
	}
	if t.LookbackFrequencySec == 0 {
		t.GoalThroughputPerSec = 30
	}
	if t.GoalThroughputPerSec == 0 {
		t.GoalThroughputPerSec = 100
	}

	// initialize internal variables
	t.savedSampleRates = make(map[string]int)
	// Create a sentinel node to represent start.
	t.countList = NewBlockList()

	t.indexGenerator = &UnixSecondsIndexGenerator{}

	// spin up calculator
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(t.UpdateFrequencySec))
		for range ticker.C {
			t.updateMaps()
		}
	}()
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (t *WindowedThroughput) updateMaps() {
	currentIndex := t.indexGenerator.getCurrentIndex()
	aggregateCounts := t.countList.aggregateCounts(currentIndex, int64(t.LookbackFrequencySec))

	// Apply the same aggregation algorithm as total throughput
	// Short circuit if no traffic
	numKeys := len(aggregateCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		t.lock.Lock()
		defer t.lock.Unlock()
		t.savedSampleRates = make(map[string]int)
		return
	}
	// figure out our target throughput per key over the lookback window.
	totalGoalThroughput := t.GoalThroughputPerSec * t.LookbackFrequencySec
	// floor the throughput but min should be 1 event per bucket per time period
	throughputPerKey := int(math.Max(1, float64(totalGoalThroughput)/float64(numKeys)))
	// for each key, calculate sample rate by dividing counted events by the
	// desired number of events
	newSavedSampleRates := make(map[string]int)
	for k, v := range aggregateCounts {
		rate := int(math.Max(1, (float64(v) / float64(throughputPerKey))))
		newSavedSampleRates[k] = rate
	}
	// save newly calculated sample rates
	t.lock.Lock()
	defer t.lock.Unlock()
	t.savedSampleRates = newSavedSampleRates
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key
func (t *WindowedThroughput) GetSampleRate(key string) int {
	// Insert the new key into the map.
	current := t.indexGenerator.getCurrentIndex()
	t.countList.incrementKey(key, current)

	t.lock.Lock()
	defer t.lock.Unlock()
	if rate, found := t.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

// SaveState is not implemented
func (t *WindowedThroughput) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (t *WindowedThroughput) LoadState(state []byte) error {
	return nil
}
