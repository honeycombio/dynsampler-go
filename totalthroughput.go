package dynsampler

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// TotalThroughput implements Sampler and attempts to meet a goal of a fixed
// number of events per second sent to Honeycomb.
//
// If your key space is sharded across different servers, this is a good method
// for making sure each server sends roughly the same volume of content to
// Honeycomb. It performs poorly when the active keyspace is very large.
//
// GoalThroughputSec * ClearFrequencyDuration (in seconds) defines the upper
// limit of the number of keys that can be reported and stay under the goal, but
// with that many keys, you'll only get one event per key per ClearFrequencySec,
// which is very coarse. You should aim for at least 1 event per key per sec to
// 1 event per key per 10sec to get reasonable data. In other words, the number
// of active keys should be less than 10*GoalThroughputSec.
type TotalThroughput struct {
	// DEPRECATED -- use ClearFrequencyDuration.
	// ClearFrequencySec is how often the counters reset in seconds.
	ClearFrequencySec int

	// ClearFrequencyDuration is how often the counters reset as a Duration.
	// Note that either this or ClearFrequencySec can be specified, but not both.
	// If neither one is set, the default is 30s.
	ClearFrequencyDuration time.Duration

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
	currentCounts    map[string]int
	done             chan struct{}

	lock sync.Mutex
}

// Ensure we implement the sampler interface
var _ Sampler = (*TotalThroughput)(nil)

func (t *TotalThroughput) Start() error {
	// apply defaults
	if t.ClearFrequencyDuration != 0 && t.ClearFrequencySec != 0 {
		return fmt.Errorf("the ClearFrequencySec configuration value is deprecated; use only ClearFrequencyDuration")
	}

	if t.ClearFrequencyDuration == 0 && t.ClearFrequencySec == 0 {
		t.ClearFrequencyDuration = 30 * time.Second
	} else {
		if t.ClearFrequencySec != 0 {
			t.ClearFrequencyDuration = time.Duration(t.ClearFrequencySec) * time.Second
		}
	}

	if t.GoalThroughputPerSec == 0 {
		t.GoalThroughputPerSec = 100
	}

	// initialize internal variables
	t.savedSampleRates = make(map[string]int)
	t.currentCounts = make(map[string]int)
	t.done = make(chan struct{})

	// spin up calculator
	go func() {
		ticker := time.NewTicker(t.ClearFrequencyDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.updateMaps()
			case <-t.done:
				return
			}
		}
	}()
	return nil
}

func (t *TotalThroughput) Stop() error {
	close(t.done)
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (t *TotalThroughput) updateMaps() {
	// make a local copy of the sample counters for calculation
	t.lock.Lock()
	tmpCounts := t.currentCounts
	t.currentCounts = make(map[string]int)
	t.lock.Unlock()
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		t.lock.Lock()
		defer t.lock.Unlock()
		t.savedSampleRates = make(map[string]int)
		return
	}
	// figure out our target throughput per key over ClearFrequencyDuration
	totalGoalThroughput := float64(t.GoalThroughputPerSec) * t.ClearFrequencyDuration.Seconds()
	// floor the throughput but min should be 1 event per bucket per time period
	throughputPerKey := int(math.Max(1, totalGoalThroughput/float64(numKeys)))
	// for each key, calculate sample rate by dividing counted events by the
	// desired number of events
	newSavedSampleRates := make(map[string]int)
	for k, v := range tmpCounts {
		rate := int(math.Max(1, (float64(v) / float64(throughputPerKey))))
		newSavedSampleRates[k] = rate
	}
	// save newly calculated sample rates
	t.lock.Lock()
	defer t.lock.Unlock()
	t.savedSampleRates = newSavedSampleRates
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (t *TotalThroughput) GetSampleRate(key string) int {
	return t.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (t *TotalThroughput) GetSampleRateMulti(key string, count int) int {
	t.lock.Lock()
	defer t.lock.Unlock()
	// Enforce MaxKeys limit on the size of the map
	if t.MaxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := t.currentCounts[key]; found || len(t.currentCounts) < t.MaxKeys {
			t.currentCounts[key] += count
		}
	} else {
		t.currentCounts[key] += count
	}
	if rate, found := t.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

// SaveState is not implemented
func (t *TotalThroughput) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (t *TotalThroughput) LoadState(state []byte) error {
	return nil
}
