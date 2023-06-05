package dynsampler

import (
	"math"
	"sync"
	"time"
)

// Windowed Throughput sampling is an enhanced version of total throughput sampling.
// Just like the original throughput sampler, it attempts to meet the goal of fixed number of events
// per second sent to Honeycomb.
//
// The original throughput sampler updates the sampling rate every "ClearFrequency" seconds. While
// this parameter is configurable, it suffers from the following tradeoff:
//   - Decreasing it makes you more responsive to load spikes, but with the cost of making the
//     sampling decision on less data.
//   - Increasing it makes you less responsive to load spikes, but your sample rates will be more
//     stable because they are made with more data.
//
// The windowed throughput sampler resolves this by introducing two different, tunable parameters:
//   - UpdateFrequency: how often the sampling rate is recomputed
//   - LookbackFrequency: how far back we look back in time to recompute our sampling rate.
//
// A standard configuration would be to set UpdateFrequency to 1s and LookbackFrequency to 30s. In
// this configuration, every second, we lookback at the last 30s of data in order to compute the new
// sampling rate. The actual sampling rate computation is nearly identical to the original
// throughput sampler, but this variant has better support for floating point numbers.
//
// Because our lookback window is _rolling_ instead of static, we need a special datastructure to
// quickly and efficiently store our data. The code and additional information for this
// datastructure can be found in blocklist.go.
type WindowedThroughput struct {
	// UpdateFrequency is how often the sampling rate is recomputed, default is 1s.
	UpdateFrequencyDuration time.Duration

	// LookbackFrequency is how far back in time we lookback to dynamically adjust our sampling
	// rate. Default is 30 * UpdateFrequencyDuration. This will be 30s assuming the default
	// configuration of UpdateFrequencyDuration. We enforce this to be an _integer multiple_ of
	// UpdateFrequencyDuration.
	LookbackFrequencyDuration time.Duration

	// Target throughput per second.
	GoalThroughputPerSec float64

	// MaxKeys, if greater than 0, limits the number of distinct keys used to build
	// the sample rate map within the interval defined by `LookbackFrequencyDuration`. Once
	// MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	// If MaxKeys is set to 0 (default), there is no upper bound on the number of distinct keys.
	MaxKeys int

	savedSampleRates map[string]int
	done             chan struct{}
	countList        BlockList

	indexGenerator IndexGenerator

	lock sync.Mutex

	// metrics
	requestCount int64
	eventCount   int64
	numKeys      int
}

// Ensure we implement the sampler interface
var _ Sampler = (*WindowedThroughput)(nil)

// An index generator turns timestamps into indexes. This is essentially a bucketing mechanism.
type IndexGenerator interface {
	// Get the index corresponding to the current time.
	GetCurrentIndex() int64

	// Return the index differential for a particular duration -- i.e. 5 seconds = how many ticks of
	// the index.
	DurationToIndexes(duration time.Duration) int64
}

// The standard implementation of the index generator.
type UnixSecondsIndexGenerator struct {
	DurationPerIndex time.Duration
}

func (g *UnixSecondsIndexGenerator) GetCurrentIndex() int64 {
	nsec := time.Now().UnixNano()
	return nsec / g.DurationPerIndex.Nanoseconds()
}

func (g *UnixSecondsIndexGenerator) DurationToIndexes(duration time.Duration) int64 {
	return duration.Nanoseconds() / g.DurationPerIndex.Nanoseconds()
}

func (t *WindowedThroughput) Start() error {
	// apply defaults
	if t.UpdateFrequencyDuration == 0 {
		t.UpdateFrequencyDuration = time.Second
	}
	if t.LookbackFrequencyDuration == 0 {
		t.LookbackFrequencyDuration = 30 * t.UpdateFrequencyDuration
	}
	// Floor LookbackFrequencyDuration to be an integer multiple of UpdateFrequencyDuration.
	t.LookbackFrequencyDuration = t.UpdateFrequencyDuration *
		(t.LookbackFrequencyDuration / t.UpdateFrequencyDuration)

	if t.GoalThroughputPerSec == 0 {
		t.GoalThroughputPerSec = 100
	}

	// Initialize countList.
	if t.MaxKeys > 0 {
		t.countList = NewBoundedBlockList(t.MaxKeys)
	} else {
		t.countList = NewUnboundedBlockList()
	}

	// Initialize internal variables.
	t.savedSampleRates = make(map[string]int)
	t.done = make(chan struct{})
	// Initialize the index generator. Each UpdateFrequencyDuration represents a single tick of the
	// index.
	t.indexGenerator = &UnixSecondsIndexGenerator{
		DurationPerIndex: t.UpdateFrequencyDuration,
	}

	// Spin up calculator.
	go func() {
		ticker := time.NewTicker(t.UpdateFrequencyDuration)
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

func (t *WindowedThroughput) Stop() error {
	close(t.done)
	return nil
}

// updateMaps recomputes the sample rate based on the countList.
func (t *WindowedThroughput) updateMaps() {
	currentIndex := t.indexGenerator.GetCurrentIndex()
	lookbackIndexes := t.indexGenerator.DurationToIndexes(t.LookbackFrequencyDuration)
	aggregateCounts := t.countList.AggregateCounts(currentIndex, lookbackIndexes)

	// Apply the same aggregation algorithm as total throughput
	// Short circuit if no traffic
	t.numKeys = len(aggregateCounts)
	if t.numKeys == 0 {
		// no traffic during the last period.
		t.lock.Lock()
		defer t.lock.Unlock()
		t.savedSampleRates = make(map[string]int)
		return
	}
	// figure out our target throughput per key over the lookback window.
	totalGoalThroughput := t.GoalThroughputPerSec * t.LookbackFrequencyDuration.Seconds()
	// floor the throughput but min should be 1 event per bucket per time period
	throughputPerKey := math.Max(1, float64(totalGoalThroughput)/float64(t.numKeys))
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
// key.
func (t *WindowedThroughput) GetSampleRate(key string) int {
	return t.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (t *WindowedThroughput) GetSampleRateMulti(key string, count int) int {
	t.requestCount++
	t.eventCount += int64(count)

	// Insert the new key into the map.
	current := t.indexGenerator.GetCurrentIndex()
	err := t.countList.IncrementKey(key, current, count)

	// We've reached MaxKeys, return 0.
	if err != nil {
		return 0
	}

	t.lock.Lock()
	defer t.lock.Unlock()
	if rate, found := t.savedSampleRates[key]; found {
		return rate
	}
	return 0
}

// SaveState is not implemented
func (t *WindowedThroughput) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (t *WindowedThroughput) LoadState(state []byte) error {
	return nil
}

func (t *WindowedThroughput) GetMetrics(prefix string) map[string]int64 {
	t.lock.Lock()
	defer t.lock.Unlock()
	mets := map[string]int64{
		prefix + "_request_count": t.requestCount,
		prefix + "_event_count":   t.eventCount,
		prefix + "_keyspace_size": int64(t.numKeys),
	}
	return mets
}
