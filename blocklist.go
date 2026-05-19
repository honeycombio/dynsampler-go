package dynsampler

import (
	"fmt"
	"math"
	"sync"
)

// ShardedBlockList distributes keys across numShards independent UnboundedBlockLists,
// each with its own lock. IncrementKey contention drops by ~numShards under uniform
// key distribution. AggregateCounts acquires each shard's lock in turn and merges results.
//
// When maxKeys > 0 a global key guard is layered on top:
//   - Existing keys take a lock-free fast path via sync.Map.Load.
//   - New keys acquire newKeyMu (contended only on first appearance of a key).
//   - AggregateCounts GCs keys that have fallen out of the lookback window so
//     new keys can be admitted again once old ones age out.
type ShardedBlockList struct {
	shards    []*UnboundedBlockList
	numShards uint32

	// used only when maxKeys > 0
	maxKeys   int
	newKeyMu  sync.Mutex  // serialises new-key admission; not held on the hot path
	keyCount  int         // protected by newKeyMu
	knownKeys sync.Map    // map[string]struct{}; written once per key, read on every call
}

// NewShardedBlockList creates a sharded blocklist with numShards shards.
// If maxKeys > 0 the total number of distinct tracked keys is capped globally.
func NewShardedBlockList(numShards, maxKeys int) BlockList {
	shards := make([]*UnboundedBlockList, numShards)
	for i := range shards {
		shards[i] = NewUnboundedBlockList().(*UnboundedBlockList)
	}
	return &ShardedBlockList{shards: shards, numShards: uint32(numShards), maxKeys: maxKeys}
}

// shardFor returns the shard responsible for key using inline FNV-1a.
func (s *ShardedBlockList) shardFor(key string) *UnboundedBlockList {
	var h uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return s.shards[h%s.numShards]
}

func (s *ShardedBlockList) IncrementKey(key string, keyIndex int64, count int) error {
	if s.maxKeys > 0 {
		if _, exists := s.knownKeys.Load(key); !exists {
			// New key — serialise admission check.
			s.newKeyMu.Lock()
			// Double-check: another goroutine may have added this key while we waited.
			if _, exists := s.knownKeys.Load(key); !exists {
				if s.keyCount >= s.maxKeys {
					s.newKeyMu.Unlock()
					return MaxSizeError{key: key}
				}
				s.knownKeys.Store(key, struct{}{})
				s.keyCount++
			}
			s.newKeyMu.Unlock()
		}
	}
	return s.shardFor(key).IncrementKey(key, keyIndex, count)
}

func (s *ShardedBlockList) AggregateCounts(currentIndex int64, lookbackIndex int64) map[string]int {
	merged := make(map[string]int)
	for _, shard := range s.shards {
		for k, v := range shard.AggregateCounts(currentIndex, lookbackIndex) {
			merged[k] += v
		}
	}

	if s.maxKeys > 0 {
		// GC: any key absent from the merged window has aged out; remove it so
		// new keys can be admitted once old ones expire.
		s.newKeyMu.Lock()
		s.knownKeys.Range(func(k, _ interface{}) bool {
			if _, inWindow := merged[k.(string)]; !inWindow {
				s.knownKeys.Delete(k)
				s.keyCount--
			}
			return true
		})
		s.newKeyMu.Unlock()
	}

	return merged
}

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

// IncrementKey is used when we've encounted a new key. The current keyIndex is
// also provided. This function will increment the key in the current block or
// create a new block, if needed. The happy path invocation is very fast, O(1).
// The count is the number of events that this call represents.
func (b *UnboundedBlockList) IncrementKey(key string, keyIndex int64, count int) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.doIncrement(key, keyIndex, count)
}

func (b *UnboundedBlockList) doIncrement(key string, keyIndex int64, count int) error {
	// A block matching keyStamp exists. Just increment the key there.
	if b.head.next != nil && b.head.next.index == keyIndex {
		b.head.next.keyToCount[key] += count
		return nil
	}

	// We need to create a new block.
	currentFront := b.head.next
	b.head.next = &Block{
		index:      keyIndex,
		keyToCount: make(map[string]int),
		next:       currentFront,
	}
	b.head.next.keyToCount[key] += count
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
func (b *BoundedBlockList) IncrementKey(key string, keyIndex int64, count int) error {
	b.baseList.lock.Lock()
	defer b.baseList.lock.Unlock()

	canInsert := b.tryInsert(key, keyIndex)
	if !canInsert {
		return MaxSizeError{key: key}
	}

	b.baseList.doIncrement(key, keyIndex, count)
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
