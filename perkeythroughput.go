package dynsampler

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// PerKeyThroughput implements Sampler and attempts to meet a goal of a fixed
// number of events per key per second sent to Honeycomb.
//
// This method is to guarantee that at most a certain number of events per key
// get transmitted, no matter how many keys you have or how much traffic comes
// through. In other words, if capturing a minimum amount of traffic per key is
// important but beyond that doesn't matter much, this is the best method.
type PerKeyThroughput struct {
	// DEPRECATED -- use ClearFrequencyDuration.
	// ClearFrequencySec is how often the counters reset in seconds.
	ClearFrequencySec int

	// ClearFrequencyDuration is how often the counters reset as a Duration.
	// Note that either this or ClearFrequencySec can be specified, but not both.
	// If neither one is set, the default is 30s.
	ClearFrequencyDuration time.Duration

	// PerKeyThroughputPerSec is the target number of events to send per second
	// per key. Sample rates are generated on a per key basis to squash the
	// throughput down to match the goal throughput. default 10
	PerKeyThroughputPerSec int

	// MaxKeys, if greater than 0, limits the number of distinct keys used to build
	// the sample rate map within the interval defined by `ClearFrequencyDuration`. Once
	// MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	savedSampleRates map[string]int
	currentCounts    map[string]int
	done             chan struct{}

	lock sync.Mutex

	// metrics
	requestCount int64
	eventCount   int64
}

// Ensure we implement the sampler interface
var _ Sampler = (*PerKeyThroughput)(nil)

func (p *PerKeyThroughput) Start() error {
	// apply defaults
	if p.ClearFrequencyDuration != 0 && p.ClearFrequencySec != 0 {
		return fmt.Errorf("the ClearFrequencySec configuration value is deprecated; use only ClearFrequencyDuration")
	}

	if p.ClearFrequencyDuration == 0 && p.ClearFrequencySec == 0 {
		p.ClearFrequencyDuration = 30 * time.Second
	} else if p.ClearFrequencySec != 0 {
		p.ClearFrequencyDuration = time.Duration(p.ClearFrequencySec) * time.Second
	}

	if p.PerKeyThroughputPerSec == 0 {
		p.PerKeyThroughputPerSec = 10
	}

	// initialize internal variables
	p.savedSampleRates = make(map[string]int)
	p.currentCounts = make(map[string]int)
	p.done = make(chan struct{})

	// spin up calculator
	go func() {
		ticker := time.NewTicker(p.ClearFrequencyDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.updateMaps()
			case <-p.done:
				return
			}
		}
	}()
	return nil
}

func (p *PerKeyThroughput) Stop() error {
	close(p.done)
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (p *PerKeyThroughput) updateMaps() {
	// make a local copy of the sample counters for calculation
	p.lock.Lock()
	tmpCounts := p.currentCounts
	p.currentCounts = make(map[string]int)
	p.lock.Unlock()
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		p.lock.Lock()
		defer p.lock.Unlock()
		p.savedSampleRates = make(map[string]int)
		return
	}
	actualPerKeyRate := p.PerKeyThroughputPerSec * int(p.ClearFrequencyDuration.Seconds())
	// for each key, calculate sample rate by dividing counted events by the
	// desired number of events
	newSavedSampleRates := make(map[string]int)
	for k, v := range tmpCounts {
		rate := int(math.Max(1, (float64(v) / float64(actualPerKeyRate))))
		newSavedSampleRates[k] = rate
	}
	// save newly calculated sample rates
	p.lock.Lock()
	defer p.lock.Unlock()
	p.savedSampleRates = newSavedSampleRates
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (p *PerKeyThroughput) GetSampleRate(key string) int {
	return p.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (p *PerKeyThroughput) GetSampleRateMulti(key string, count int) int {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.requestCount++
	p.eventCount += int64(count)

	// Enforce MaxKeys limit on the size of the map
	if p.MaxKeys > 0 {
		// If a key already exists, add the count. If not, but we're under the limit, store a new key
		if _, found := p.currentCounts[key]; found || len(p.currentCounts) < p.MaxKeys {
			p.currentCounts[key] += count
		}
	} else {
		p.currentCounts[key] += count
	}
	if rate, found := p.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

// SaveState is not implemented
func (p *PerKeyThroughput) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (p *PerKeyThroughput) LoadState(state []byte) error {
	return nil
}

func (p *PerKeyThroughput) GetMetrics(prefix string) map[string]int64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	mets := map[string]int64{
		prefix + "_request_count": p.requestCount,
		prefix + "_event_count":   p.eventCount,
		prefix + "_keyspace_size": int64(len(p.currentCounts)),
	}
	return mets
}
