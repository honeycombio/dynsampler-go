package dynsampler

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
)

// BlockList is a data structure that keeps track of how often keys occur in a given time range in
// order to perform windowed lookback sampling. BlockList operates with monotonically increasing
// indexes, instead of timestamps.
// A BlockList is a single linked list of Blocks. Each Block has a frequency hashmap and a unique
// index.
type BlockList interface {
	IncrementKey(key string, keyIndex int64, count int) error
	AggregateCounts(currentIndex int64, lookbackIndex int64) map[string]int
}

type Block struct {
	index      int64 // MUST be monotonically increasing.
	keyToCount sync.Map // map[string]*int64; values incremented atomically
	next       *Block
}

// UnboundedBlockList can have unlimited keys.
type UnboundedBlockList struct {
	head *Block     // sentinel node; list structure protected by lock
	lock sync.Mutex // held only when the time index advances (~once/s)
}

// Creates a new BlockList with a sentinel node.
func NewUnboundedBlockList() BlockList {
	return &UnboundedBlockList{
		head: &Block{index: math.MaxInt64},
	}
}

// incrementInBlock atomically adds count to the key's counter inside block.
// Safe to call without any lock held.
func incrementInBlock(block *Block, key string, count int) {
	if v, ok := block.keyToCount.Load(key); ok {
		atomic.AddInt64(v.(*int64), int64(count))
		return
	}
	// Key not yet in this block — race to store the initial pointer.
	newPtr := new(int64)
	actual, loaded := block.keyToCount.LoadOrStore(key, newPtr)
	if loaded {
		// Another goroutine won the race; add to their pointer.
		atomic.AddInt64(actual.(*int64), int64(count))
	} else {
		// We stored newPtr; write our count into it.
		atomic.AddInt64(newPtr, int64(count))
	}
}

// IncrementKey increments the count for key in the block for keyIndex.
func (b *UnboundedBlockList) IncrementKey(key string, keyIndex int64, count int) error {
	b.lock.Lock()
	cur := b.head.next
	if cur == nil || cur.index != keyIndex {
		newBlock := &Block{index: keyIndex, next: b.head.next}
		b.head.next = newBlock
		cur = newBlock
	}
	b.lock.Unlock()

	incrementInBlock(cur, key, count)
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
			front.keyToCount.Range(func(k, v interface{}) bool {
				// A key may be stored in the sync.Map with a zero count if
				// incrementInBlock is mid-flight (between LoadOrStore and
				// atomic.AddInt64). Skip those entries to avoid inflating numKeys.
				if count := int(atomic.LoadInt64(v.(*int64))); count > 0 {
					aggregateCounts[k.(string)] += count
				}
				return true
			})
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
// Lock is held for tryInsert + block creation, released before the map write.
func (b *BoundedBlockList) IncrementKey(key string, keyIndex int64, count int) error {
	b.baseList.lock.Lock()
	if !b.tryInsert(key, keyIndex) {
		b.baseList.lock.Unlock()
		return MaxSizeError{key: key}
	}
	cur := b.baseList.head.next
	if cur == nil || cur.index != keyIndex {
		newBlock := &Block{index: keyIndex, next: b.baseList.head.next}
		b.baseList.head.next = newBlock
		cur = newBlock
	}
	b.baseList.lock.Unlock()
	incrementInBlock(cur, key, count)
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
