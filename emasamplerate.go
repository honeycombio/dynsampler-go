package dynsampler

import (
	"math"
	"sort"
	"sync"
	"time"
)

// EMASampleRate implements Sampler and attempts to average a given sample rate,
// weighting rare traffic and frequent traffic differently so as to end up with
// the correct average. This method breaks down when total traffic is low
// because it will be excessively sampled.
//
// Based on the AvgSampleRate implementation, EMASampleRate differs in that rather
// than compute rate based on a periodic sample of traffic, it maintains an Exponential
// Moving Average of counts seen per key, and adjusts this average at regular intervals.
// The weight applied to more recent intervals is defined by `weight`, a number between
// (0, 1) - larger values weight the average more toward recent observations.
//
// Keys that are not found in the EMA will always have a sample
// rate of 1. Keys that occur more frequently will be sampled on a logarithmic
// curve. In other words, every key will be represented at least once in any
// given window and more frequent keys will have their sample rate
// increased proportionally to wind up with the goal sample rate.
type EMASampleRate struct {
	// AdjustmentInterval defines how often we adjust the moving average from recent observations
	// Default 15s
	AdjustmentInterval int

	// Weight is a value between (0, 1) indicating the weighting factor used to adjust
	// the EMA. With larger values, newer data will influence the average more, and older
	// values will be factored out more quickly. Default is 0.5
	Weight float64

	// GoalSampleRate is the average sample rate we're aiming for, across all
	// events. Default 10
	GoalSampleRate int

	// MaxKeys, if greater than 0, limits the number of distinct keys tracked in EMA.
	// Once MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	MaxKeys int

	// AgeOutValue indicates the threshold for removing keys from the EMA.
	// The EMA of any key will approach 0 if it is not repeatedly observed, but will never truly reach it, so
	// we have to decide what constitutes "zero".
	// Keys with averages below this threshold will be removed from the EMA. Default is the same as Weight
	AgeOutValue float64

	savedSampleRates map[string]int
	currentCounts    map[string]float64
	movingAverage    map[string]float64

	// haveData indicates that we have gotten a sample of traffic. Before we've
	// gotten any samples of traffic, we should we should use the default goal
	// sample rate for all events instead of sampling everything at 1
	haveData bool

	lock sync.Mutex
}

func (e *EMASampleRate) Start() error {
	// apply defaults
	if e.AdjustmentInterval == 0 {
		e.AdjustmentInterval = 15
	}
	if e.GoalSampleRate == 0 {
		e.GoalSampleRate = 10
	}
	if e.Weight == 0 {
		e.Weight = 0.5
	}
	if e.AgeOutValue == 0 {
		e.AgeOutValue = e.Weight
	}

	// initialize internal variables
	e.savedSampleRates = make(map[string]int)
	e.currentCounts = make(map[string]float64)
	e.movingAverage = make(map[string]float64)

	// spin up calculator
	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(e.AdjustmentInterval))
		for range ticker.C {
			e.updateMaps()
		}
	}()
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (e *EMASampleRate) updateMaps() {
	// make a local copy of the sample counters for calculation
	e.lock.Lock()
	tmpCounts := e.currentCounts
	e.currentCounts = make(map[string]float64)
	e.lock.Unlock()
	// short circuit if no traffic
	numKeys := len(tmpCounts)
	if numKeys == 0 {
		// no traffic the last interval, don't update anything
		return
	}

	e.updateEMA(tmpCounts)

	// Goal events to send this interval is the total count of events in the EMA
	// divided by the desired average sample rate
	var sumEvents float64
	for _, count := range e.movingAverage {
		sumEvents += count
	}
	goalCount := float64(sumEvents) / float64(e.GoalSampleRate)
	// goalRatio is the goalCount divided by the sum of all the log values - it
	// determines what percentage of the total event space belongs to each key
	var logSum float64
	for _, count := range e.movingAverage {
		logSum += math.Log10(float64(count))
	}
	goalRatio := goalCount / logSum

	// must go through the keys in a fixed order to prevent rounding from changing
	// results
	keys := make([]string, len(e.movingAverage))
	var i int
	for k := range e.movingAverage {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// goal number of events per key is goalRatio * key count, but never less than
	// one. If a key falls below its goal, it gets a sample rate of 1 and the
	// extra available events get passed on down the line.
	newSavedSampleRates := make(map[string]int)
	keysRemaining := len(e.movingAverage)
	var extra float64
	for _, key := range keys {
		count := e.movingAverage[key]
		// take the max of 1 or my log10 share of the total
		goalForKey := math.Max(1, math.Log10(count)*goalRatio)
		// take this key's share of the extra and pass the rest along
		extraForKey := extra / float64(keysRemaining)
		goalForKey += extraForKey
		extra -= extraForKey
		keysRemaining--
		if count <= goalForKey {
			// there are fewer samples than the allotted number for this key. set
			// sample rate to 1 and redistribute the unused slots for future keys
			newSavedSampleRates[key] = 1
			extra += goalForKey - count
		} else {
			// there are more samples than the allotted number. Sample this key enough
			// to knock it under the limit (aka round up)
			rate := math.Ceil(count / goalForKey)
			// if counts are <= 1 we can get values for goalForKey that are +Inf
			// and subsequent division ends up with NaN. If that's the case,
			// fall back to 1
			if math.IsNaN(rate) {
				newSavedSampleRates[key] = 1
			} else {
				newSavedSampleRates[key] = int(rate)
			}
			extra += goalForKey - (count / float64(newSavedSampleRates[key]))
		}
	}
	e.lock.Lock()
	defer e.lock.Unlock()
	e.savedSampleRates = newSavedSampleRates
	e.haveData = true
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key. Will never return zero.
func (e *EMASampleRate) GetSampleRate(key string) int {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Enforce MaxKeys limit on the size of the map
	if e.MaxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := e.currentCounts[key]; found || len(e.currentCounts) < e.MaxKeys {
			e.currentCounts[key]++
		}
	} else {
		e.currentCounts[key]++
	}
	if !e.haveData {
		return e.GoalSampleRate
	}
	if rate, found := e.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

func (e *EMASampleRate) updateEMA(newCounts map[string]float64) {
	keysToUpdate := make([]string, 0, len(e.movingAverage))
	for key := range e.movingAverage {
		keysToUpdate = append(keysToUpdate, key)
	}

	// Update any existing keys with new values
	for _, key := range keysToUpdate {
		var newAvg float64
		// Was this key seen in the last interval? Adjust by that amount
		if val, found := newCounts[key]; found {
			newAvg = adjustAverage(e.movingAverage[key], val, e.Weight)
		} else {
			// Otherwise adjust by zero
			newAvg = adjustAverage(e.movingAverage[key], 0, e.Weight)
		}

		// Age out this value if it's too small to care about for calculating sample rates
		// This is also necessary to keep our map from going forever.
		if newAvg < e.AgeOutValue {
			delete(e.movingAverage, key)
		} else {
			e.movingAverage[key] = newAvg
		}
		// We've processed this key - don't process it again when we look at new counts
		delete(newCounts, key)
	}

	for key := range newCounts {
		newAvg := adjustAverage(0, newCounts[key], e.Weight)
		if newAvg >= e.AgeOutValue {
			e.movingAverage[key] = newAvg
		}
	}
}

func adjustAverage(oldAvg, value float64, alpha float64) float64 {
	adjustedNewVal := value * alpha
	adjustedOldAvg := (1.0 - alpha) * oldAvg

	return adjustedNewVal + adjustedOldAvg
}
