package dynsampler

import (
	"math"
	"sync"
)

// BlockList is a data structure that keeps track of how often keys occur in order to perform windowed lookback sampling.
// In order to do windowed lookback sampling, we must be able to answer how many keys occurred from time T - W to time T, where W is the length of the window.
// Let's call this function AGG_COUNTS(T, W) => returns the count of all keys between time T - W and T.
// A naive way to implement AGG_COUNTS(T, W) would be to keep track of all timestamps for each key, and simply perform a basic count aggregation.
// This solution is not efficient for two main reasons:
// 1) Samplers can see O(10000) traces per second, and the naive solution is far too memory intensive, even if we do drop outdated spans.
// 2) It's not algorithmically efficent and doesn't lock well.
// BlockList is a new data structure designed to solve this problem efficiently.

type Block struct {
	index    int64 // MUST be monotonically increasing.
	countMap map[string]int
	next     *Block
}

type BlockList struct {
	head *Block // Sentinel node for our linked list.
	lock sync.Mutex
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

	b.lock.Lock()
	defer b.lock.Unlock()
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
	b.lock.Lock()
	defer b.lock.Unlock()
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
		if front.next != nil && front.next.index <= finishIndex {
			front.next = nil
			break
		}
		front = front.next
	}

	return aggregateCounts
}
