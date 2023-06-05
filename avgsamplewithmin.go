package dynsampler

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// AvgSampleWithMin implements Sampler and attempts to average a given sample
// rate, with a minimum number of events per second (i.e. it will reduce
// sampling if it would end up sending fewer than the mininum number of events).
// This method attempts to get the best of the normal average sample rate
// method, without the failings it shows on the low end of total traffic
// throughput
//
// Keys that occur only once within ClearFrequencyDuration will always have a sample
// rate of 1. Keys that occur more frequently will be sampled on a logarithmic
// curve. In other words, every key will be represented at least once per
// ClearFrequencyDuration and more frequent keys will have their sample rate
// increased proportionally to wind up with the goal sample rate.
type AvgSampleWithMin struct {
	// DEPRECATED -- use ClearFrequencyDuration.
	// ClearFrequencySec is how often the counters reset in seconds.
	ClearFrequencySec int

	// ClearFrequencyDuration is how often the counters reset as a Duration.
	// Note that either this or ClearFrequencySec can be specified, but not both.
	// If neither one is set, the default is 30s.
	ClearFrequencyDuration time.Duration

	// GoalSampleRate is the average sample rate we're aiming for, across all
	// events. Default 10
	GoalSampleRate int

	// MaxKeys, if greater than 0, limits the number of distinct keys used to build
	// the sample rate map within the interval defined by `ClearFrequencyDuration`. Once
	// MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	// MinEventsPerSec - when the total number of events drops below this
	// threshold, sampling will cease. default 50
	MinEventsPerSec int

	savedSampleRates map[string]int
	currentCounts    map[string]float64

	// haveData indicates that we have gotten a sample of traffic. Before we've
	// gotten any samples of traffic, we should we should use the default goal
	// sample rate for all events instead of sampling everything at 1
	haveData bool
	done     chan struct{}

	lock sync.Mutex

	// metrics
	requestCount int64
	eventCount   int64
}

// Ensure we implement the sampler interface
var _ Sampler = (*AvgSampleWithMin)(nil)

func (a *AvgSampleWithMin) Start() error {
	// apply defaults
	if a.ClearFrequencyDuration != 0 && a.ClearFrequencySec != 0 {
		return fmt.Errorf("the ClearFrequencySec configuration value is deprecated; use only ClearFrequencyDuration")
	}

	if a.ClearFrequencyDuration == 0 && a.ClearFrequencySec == 0 {
		a.ClearFrequencyDuration = 30 * time.Second
	} else if a.ClearFrequencySec != 0 {
		a.ClearFrequencyDuration = time.Duration(a.ClearFrequencySec) * time.Second
	}

	if a.GoalSampleRate == 0 {
		a.GoalSampleRate = 10
	}
	if a.MinEventsPerSec == 0 {
		a.MinEventsPerSec = 50
	}

	// initialize internal variables
	a.savedSampleRates = make(map[string]int)
	a.currentCounts = make(map[string]float64)
	a.done = make(chan struct{})

	// spin up calculator
	go func() {
		ticker := time.NewTicker(a.ClearFrequencyDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				a.updateMaps()
			case <-a.done:
				return
			}
		}
	}()
	return nil
}

func (a *AvgSampleWithMin) Stop() error {
	close(a.done)
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (a *AvgSampleWithMin) updateMaps() {
	// make a local copy of the sample counters for calculation
	a.lock.Lock()
	tmpCounts := a.currentCounts
	a.currentCounts = make(map[string]float64)
	a.lock.Unlock()
	newSavedSampleRates := make(map[string]int)
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		a.lock.Lock()
		defer a.lock.Unlock()
		a.savedSampleRates = newSavedSampleRates
		return
	}

	// Goal events to send this interval is the total count of received events
	// divided by the desired average sample rate
	var sumEvents float64
	for _, count := range tmpCounts {
		sumEvents += count
	}
	goalCount := float64(sumEvents) / float64(a.GoalSampleRate)
	// check to see if we fall below the minimum
	if sumEvents < float64(a.MinEventsPerSec)*a.ClearFrequencyDuration.Seconds() {
		// we still need to go through each key to set sample rates individually
		for k := range tmpCounts {
			newSavedSampleRates[k] = 1
		}
		a.lock.Lock()
		defer a.lock.Unlock()
		a.savedSampleRates = newSavedSampleRates
		return
	}
	// goalRatio is the goalCount divided by the sum of all the log values - it
	// determines what percentage of the total event space belongs to each key
	var logSum float64
	for _, count := range tmpCounts {
		logSum += math.Log10(float64(count))
	}
	// Note that this can produce Inf if logSum is 0
	goalRatio := goalCount / logSum

	newSavedSampleRates = calculateSampleRates(goalRatio, tmpCounts)
	a.lock.Lock()
	defer a.lock.Unlock()
	a.savedSampleRates = newSavedSampleRates
	a.haveData = true
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (a *AvgSampleWithMin) GetSampleRate(key string) int {
	return a.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (a *AvgSampleWithMin) GetSampleRateMulti(key string, count int) int {
	a.lock.Lock()
	defer a.lock.Unlock()

	a.requestCount++
	a.eventCount += int64(count)

	// Enforce MaxKeys limit on the size of the map
	if a.MaxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := a.currentCounts[key]; found || len(a.currentCounts) < a.MaxKeys {
			a.currentCounts[key] += float64(count)
		}
	} else {
		a.currentCounts[key] += float64(count)
	}
	if !a.haveData {
		return a.GoalSampleRate
	}
	if rate, found := a.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

// SaveState is not implemented
func (a *AvgSampleWithMin) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (a *AvgSampleWithMin) LoadState(state []byte) error {
	return nil
}

func (a *AvgSampleWithMin) GetMetrics(prefix string) map[string]int64 {
	a.lock.Lock()
	defer a.lock.Unlock()
	mets := map[string]int64{
		prefix + "_request_count": a.requestCount,
		prefix + "_event_count":   a.eventCount,
		prefix + "_keyspace_size": int64(len(a.currentCounts)),
	}
	return mets
}
