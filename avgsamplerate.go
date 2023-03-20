package dynsampler

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// AvgSampleRate implements Sampler and attempts to average a given sample rate,
// weighting rare traffic and frequent traffic differently so as to end up with
// the correct average. This method breaks down when total traffic is low
// because it will be excessively sampled.
//
// Keys that occur only once within ClearFrequencySec will always have a sample
// rate of 1. Keys that occur more frequently will be sampled on a logarithmic
// curve. In other words, every key will be represented at least once per
// ClearFrequencySec and more frequent keys will have their sample rate
// increased proportionally to wind up with the goal sample rate.
type AvgSampleRate struct {
	// ClearFrequencySec is how often the counters reset in seconds; default 30.
	// DEPRECATED -- use ClearFrequencyDuration.
	ClearFrequencySec int

	// ClearFrequencyDuration is how often the counters reset as a Duration.
	// Note that either this or ClearFrequencySec can be specified, but not both.
	// If neither one is set, the default is 30s.
	ClearFrequencyDuration time.Duration

	// GoalSampleRate is the average sample rate we're aiming for, across all
	// events. Default 10
	GoalSampleRate int

	// MaxKeys, if greater than 0, limits the number of distinct keys used to build
	// the sample rate map within the interval defined by `ClearFrequencySec`. Once
	// MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	savedSampleRates map[string]int
	currentCounts    map[string]float64

	// haveData indicates that we have gotten a sample of traffic. Before we've
	// gotten any samples of traffic, we should we should use the default goal
	// sample rate for all events instead of sampling everything at 1
	haveData bool
	done     chan struct{}

	lock sync.Mutex
}

// Ensure we implement the sampler interface
var _ Sampler = (*AvgSampleRate)(nil)

func (a *AvgSampleRate) Start() error {
	// apply defaults
	if a.ClearFrequencyDuration != 0 && a.ClearFrequencySec != 0 {
		return fmt.Errorf("the ClearFrequencySec configuration value is deprecated; use only ClearFrequencyDuration")
	}

	if a.ClearFrequencyDuration == 0 && a.ClearFrequencySec == 0 {
		a.ClearFrequencyDuration = 30 * time.Second
	} else {
		if a.ClearFrequencySec != 0 {
			a.ClearFrequencyDuration = time.Duration(a.ClearFrequencySec) * time.Second
		}
	}

	if a.GoalSampleRate == 0 {
		a.GoalSampleRate = 10
	}

	// initialize internal variables
	// Create saved sample rate map if we're not loading from a previous state
	if a.savedSampleRates == nil {
		a.savedSampleRates = make(map[string]int)
	}
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

func (a *AvgSampleRate) Stop() error {
	close(a.done)
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (a *AvgSampleRate) updateMaps() {
	// make a local copy of the sample counters for calculation
	a.lock.Lock()
	tmpCounts := a.currentCounts
	a.currentCounts = make(map[string]float64)
	a.lock.Unlock()
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last 30s. clear the result map
		a.lock.Lock()
		defer a.lock.Unlock()
		a.savedSampleRates = make(map[string]int)
		return
	}

	// Goal events to send this interval is the total count of received events
	// divided by the desired average sample rate
	var sumEvents float64
	for _, count := range tmpCounts {
		sumEvents += count
	}
	goalCount := sumEvents / float64(a.GoalSampleRate)
	// goalRatio is the goalCount divided by the sum of all the log values - it
	// determines what percentage of the total event space belongs to each key
	var logSum float64
	for _, count := range tmpCounts {
		logSum += math.Log10(count)
	}
	goalRatio := goalCount / logSum

	newSavedSampleRates := calculateSampleRates(goalRatio, tmpCounts)
	a.lock.Lock()
	defer a.lock.Unlock()
	a.savedSampleRates = newSavedSampleRates
	a.haveData = true
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (a *AvgSampleRate) GetSampleRate(key string) int {
	return a.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (a *AvgSampleRate) GetSampleRateMulti(key string, count int) int {
	a.lock.Lock()
	defer a.lock.Unlock()

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

type avgSampleRateState struct {
	// This field is exported for use by `JSON.Marshal` and `JSON.Unmarshal`
	SavedSampleRates map[string]int `json:"saved_sample_rates"`
}

// SaveState returns a byte array with a JSON representation of the sampler state
func (a *AvgSampleRate) SaveState() ([]byte, error) {
	a.lock.Lock()
	defer a.lock.Unlock()

	if a.savedSampleRates == nil {
		return nil, errors.New("saved sample rate map is nil")
	}
	s := &avgSampleRateState{SavedSampleRates: a.savedSampleRates}
	return json.Marshal(s)
}

// LoadState accepts a byte array with a JSON representation of a previous instance's
// state
func (a *AvgSampleRate) LoadState(state []byte) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	s := avgSampleRateState{}
	err := json.Unmarshal(state, &s)
	if err != nil {
		return err
	}

	// Load the previously calculated sample rates
	a.savedSampleRates = s.SavedSampleRates
	// Allow GetSampleRate to return calculated sample rates from the loaded map
	a.haveData = true

	return nil
}
