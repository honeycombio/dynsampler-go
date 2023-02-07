package dynsampler

import (
	"fmt"
	"math"
	"sync"
)

// BlockList is a data structure that keeps track of how often keys occur in a given time range in
// order to perform windowed lookback sampling. BlockList operates with monotonically increasing
// indexes, instead of timestamps.
// A BlockList is a single linked list of Blocks. Each Block has a frequency hashmap and an unique
// index.
type BlockList interface {
	IncrementKey(key string, keyIndex int64) error
	AggregateCounts(currentIndex int64, lookbackIndex int64) map[string]int
}

type Block struct {
	index      int64 // MUST be monotonically increasing.
	keyToCount map[string]int
	next       *Block
}

// UnboundedBlockList can have unlimited keys.
type UnboundedBlockList struct {
	head *Block // Sentinel node for our linked list.
	lock sync.Mutex
}

// Creates a new BlockList with a sentinel node.
func NewUnboundedBlockList() BlockList {
	// Create a sentinel node.

	head := &Block{
		index:      math.MaxInt64,
		keyToCount: make(map[string]int),
		next:       nil,
	}
	return &UnboundedBlockList{
		head: head,
	}
}

// IncrementKey is used when we've encounted a new key. The current keyIndex is also provided.
// This function will increment the key in the current block or create a new block, if needed.
// The happy path invocation is very fast, O(1) and lock-less. We do have to lock to create a new
// block.
func (b *UnboundedBlockList) IncrementKey(key string, keyIndex int64) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.doIncrement(key, keyIndex)
}

func (b *UnboundedBlockList) doIncrement(key string, keyIndex int64) error {
	// A block matching keyStamp exists. Just increment the key there.
	if b.head.next != nil && b.head.next.index == keyIndex {
		b.head.next.keyToCount[key] += 1
		return nil
	}

	// We need to create a new block.
	currentFront := b.head.next
	b.head.next = &Block{
		index:      keyIndex,
		keyToCount: make(map[string]int),
		next:       currentFront,
	}
	b.head.next.keyToCount[key] += 1
	return nil
}

// AggregateCounts returns a frequency hashmap of all counts from the currentIndex to the
// lookbackIndex. It also drops old blocks. This is an O(N) operation, where N is the length of the
// linked list.
func (b *UnboundedBlockList) AggregateCounts(
	currentIndex int64,
	lookbackIndex int64,
) map[string]int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.doAggregation(currentIndex, lookbackIndex)
}

// Split out the actual functionality into doAggregation to support better locking semantics.
func (b *UnboundedBlockList) doAggregation(
	currentIndex int64,
	lookbackIndex int64,
) (aggregateCounts map[string]int) {
	aggregateCounts = make(map[string]int)

	// Aggregate from currentIndex - 1 and lookback lookbackIndex.
	startIndex := currentIndex - 1
	finishIndex := startIndex - lookbackIndex

	// front is a pointer that iterates through our linked list. Start at the sentinel.
	front := b.head
	for front != nil {
		// Start aggregation at current index - 1.
		if front.index <= startIndex {
			for k, v := range front.keyToCount {
				aggregateCounts[k] += v
			}
		}

		// Stop and drop remaining blocks after t - lookbackIndex.
		// Never drop the first block.
		if front.next != nil && front.next.index <= finishIndex {
			front.next = nil
			break
		}
		front = front.next
	}

	return aggregateCounts
}

// BoundedBlockList have a limit on the maximum number of keys within the blocklist. Additional keys
// will be dropped by IncrementKey.
// This is implemented with another data structure ontop of an UnboundedBlockList that keeps track
// of total keys. We use a map from keys to indexes that the key appears in.
type BoundedBlockList struct {
	baseList *UnboundedBlockList

	maxKeys      int
	keyToIndexes map[string][]int64
}

// Error encounted when the BoundedBlockList has reached maxKeys capacity.
type MaxSizeError struct {
	key string
}

func (e MaxSizeError) Error() string {
	return fmt.Sprintf("Max size for blocklist reached, new key %s rejected.", e.key)
}

// Creates a new BlockList with a sentinel node.
func NewBoundedBlockList(maxKeys int) BlockList {
	return &BoundedBlockList{
		baseList:     NewUnboundedBlockList().(*UnboundedBlockList),
		maxKeys:      maxKeys,
		keyToIndexes: make(map[string][]int64),
	}
}

// IncrementKey will always increment an existing key. If the key is new, it will be rejected if
// there are maxKeys existing entries.
func (b *BoundedBlockList) IncrementKey(key string, keyIndex int64) error {
	b.baseList.lock.Lock()
	defer b.baseList.lock.Unlock()

	canInsert := b.tryInsert(key, keyIndex)
	if !canInsert {
		return MaxSizeError{key: key}
	}

	b.baseList.doIncrement(key, keyIndex)
	return nil
}

// tryInsert checks if we can insert a new key. This function is NOT idempotent.
func (b *BoundedBlockList) tryInsert(key string, keyIndex int64) bool {
	// See if we can insert through reads.

	// Reject new keys at max capacity.
	if len(b.keyToIndexes) >= b.maxKeys {
		return false
	}

	indexes, exists := b.keyToIndexes[key]
	if exists && len(indexes) > 0 && indexes[0] == keyIndex {
		return true
	}

	if exists {
		b.keyToIndexes[key] = append([]int64{keyIndex}, indexes...)
	} else {
		b.keyToIndexes[key] = []int64{keyIndex}
	}
	return true
}

func (b *BoundedBlockList) AggregateCounts(
	currentIndex int64,
	lookbackIndex int64,
) (aggregateCounts map[string]int) {
	b.baseList.lock.Lock()
	defer b.baseList.lock.Unlock()
	aggregateCounts = b.baseList.doAggregation(currentIndex, lookbackIndex)

	startIndex := currentIndex - 1
	finishIndex := startIndex - lookbackIndex

	for key, indexes := range b.keyToIndexes {
		dropIdx := -1
		for i := 0; i < len(indexes); i++ {
			if indexes[i] <= finishIndex {
				dropIdx = i
				break
			}
		}
		if dropIdx == -1 { // Nothing needs to be dropped.
			continue
		} else if dropIdx == 0 { // Everything needs to be dropped.
			delete(b.keyToIndexes, key)
		} else { // Perform a partial drop.
			b.keyToIndexes[key] = indexes[0:dropIdx]
		}
	}

	return aggregateCounts
}
