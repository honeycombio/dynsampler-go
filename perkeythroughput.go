package dynsampler

import (
	"fmt"
	"math"
	"slices"
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
//
// An optional MaxTotalThroughputPerSec sets a maximum for the combined output across
// all keys, for when the number of keys is large enough that the per-key goal alone
// allows more traffic than the destination should receive.
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
	// Rejected keys are not counted, so they do not contribute to the rates computed
	// for keys accepted under MaxKeys. Total output can exceed the goal or the
	// maximum while keys are being rejected.
	MaxKeys int

	// OverflowSampleRate, if greater than 0, is the sample rate returned for a key
	// that is rejected because MaxKeys has been reached, instead of the default
	// keep-everything rate of 1. Defaults to 0, which preserves that default behavior.
	OverflowSampleRate int

	// MaxTotalThroughputPerSec, if greater than 0, is the maximum number of events to
	// send per second across all keys combined. Defaults to 0, which applies no
	// maximum and leaves each key with the full PerKeyThroughputPerSec target.
	// When the per-key targets would sum above the maximum, every key's allowance
	// drops to a single shared level: keys sending less than that level are
	// unaffected and the rest share what is left equally. Sample rates are whole
	// numbers rounded up, so the delivered total sits at or under the maximum
	// rather than exactly on it. Keys rejected because MaxKeys has been reached
	// are not counted and so are not bounded by the maximum.
	//
	// The maximum bounds expected output per interval rather than guaranteeing it.
	// Rates are fixed for the length of an interval, so a burst that arrives after
	// one opens is sampled at the previous interval's rates and the total can
	// overshoot until the next recalculation.
	MaxTotalThroughputPerSec int

	savedSampleRates map[string]int
	currentCounts    map[string]int
	done             chan struct{}

	lock sync.Mutex

	// metrics
	requestCount    int64
	eventCount      int64
	prefix          string
	requestCountKey string
	eventCountKey   string
	keyspaceSizeKey string
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
	perKeyThroughputPerSec := p.PerKeyThroughputPerSec
	maxTotalThroughputPerSec := p.MaxTotalThroughputPerSec
	clearFrequencyDuration := p.ClearFrequencyDuration
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
	// figure out our target throughput per key over ClearFrequencyDuration
	intervalSeconds := clearFrequencyDuration.Seconds()
	perKeyGoal := float64(perKeyThroughputPerSec) * intervalSeconds
	// a maximum lowers every key's allowance to one shared level, leaving keys
	// already under that level untouched
	level := math.Inf(1)
	if maxTotalThroughputPerSec > 0 {
		// A rate is a whole number rounded down, so a key counting between one and
		// two times the goal keeps everything and delivers up to twice it. Demand
		// has to be that delivery rather than the goal, or the maximum is weighed
		// against a total the sampler does not produce.
		demands := make([]float64, 0, numKeys)
		for _, v := range tmpCounts {
			demands = append(demands, float64(v)/float64(max(1, int(float64(v)/perKeyGoal))))
		}
		level = waterFillLevel(demands, float64(maxTotalThroughputPerSec)*intervalSeconds)
	}
	// any finite level came from the maximum, so rates round up from here on
	limitedByMax := !math.IsInf(level, 1)
	effectiveGoal := min(perKeyGoal, level)
	// for each key, calculate sample rate by dividing counted events by the
	// desired number of events
	newSavedSampleRates := make(map[string]int)
	for k, v := range tmpCounts {
		rate := float64(v) / effectiveGoal
		if limitedByMax {
			// rounding down would put the total back above the maximum
			rate = math.Ceil(rate)
		}
		newSavedSampleRates[k] = int(max(1, rate))
	}
	// save newly calculated sample rates
	p.lock.Lock()
	defer p.lock.Unlock()
	p.savedSampleRates = newSavedSampleRates
}

// waterFillLevel returns the per-key allowance that spends maxTotal in full when
// the given demands sum above it. Keys demanding less than the level are left
// alone and the rest share what remains equally, so a key is only lowered to pay
// for the maximum once every smaller key has been satisfied. It returns +Inf when
// the demands already fit within maxTotal, meaning no key needs lowering. The
// demands slice is sorted in place.
func waterFillLevel(demands []float64, maxTotal float64) float64 {
	slices.Sort(demands)
	remaining := maxTotal
	for i, d := range demands {
		n := float64(len(demands) - i)
		if d*n > remaining {
			return remaining / n
		}
		remaining -= d
	}
	return math.Inf(1)
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
	overflowed := false
	if p.MaxKeys > 0 {
		// If a key already exists, add the count. If not, but we're under the limit, store a new key
		if _, found := p.currentCounts[key]; found || len(p.currentCounts) < p.MaxKeys {
			p.currentCounts[key] += count
		} else {
			overflowed = true
		}
	} else {
		p.currentCounts[key] += count
	}
	if rate, found := p.savedSampleRates[key]; found {
		return rate
	}
	if overflowed && p.OverflowSampleRate > 0 {
		return p.OverflowSampleRate
	}
	return 1
}

// SetPerKeyThroughputPerSec updates the per key throughput per second in a
// concurrency-safe manner. Values of 0 or less are ignored.
func (p *PerKeyThroughput) SetPerKeyThroughputPerSec(throughput int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if throughput > 0 {
		p.PerKeyThroughputPerSec = throughput
	}
}

// SetMaxTotalThroughputPerSec updates the maximum total throughput in a
// concurrency-safe manner. Values of 0 or less are ignored, so the maximum
// cannot be enabled or removed at runtime by passing one.
func (p *PerKeyThroughput) SetMaxTotalThroughputPerSec(throughput int) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if throughput > 0 {
		p.MaxTotalThroughputPerSec = throughput
	}
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

	if p.prefix == "" {
		p.prefix = prefix
		p.requestCountKey = p.prefix + requestCountSuffix
		p.eventCountKey = p.prefix + eventCountSuffix
		p.keyspaceSizeKey = p.prefix + keyspaceSizeSuffix
	}
	if p.prefix != prefix {
		return nil // if the prefix doesn't match, return nil
	}

	mets := map[string]int64{
		p.requestCountKey: p.requestCount,
		p.eventCountKey:   p.eventCount,
		p.keyspaceSizeKey: int64(len(p.currentCounts)),
	}
	return mets
}
