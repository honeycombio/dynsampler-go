package dynsampler

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// EMAThroughput implements Sampler and attempts to achieve a given throughput
// rate, weighting rare traffic and frequent traffic differently so as to end up
// with the the desired throughput.
//
// Based on the EMASampleRate implementation, EMAThroughput differs in that
// instead of trying to achieve a given sample rate, it tries to reach a given
// throughput of events. During bursts of traffic, it will reduce sample
// rates so as to keep the number of events per second roughly constant.
//
// Like the EMA sampler, it maintains an Exponential Moving Average of counts
// seen per key, and adjusts this average at regular intervals. The weight
// applied to more recent intervals is defined by `weight`, a number between (0,
// 1) - larger values weight the average more toward recent observations. In
// other words, a larger weight will cause sample rates to more quickly adapt to
// traffic patterns, while a smaller weight will result in sample rates that are
// less sensitive to bursts or drops in traffic and thus more consistent over
// time.
//
// New keys that are not found in the EMA will always have a sample
// rate of 1. Keys that occur more frequently will be sampled on a logarithmic
// curve. In other words, every key will be represented at least once in any
// given window and more frequent keys will have their sample rate
// increased proportionally to wind up with the goal throughput.
type EMAThroughput struct {
	// AdjustmentInterval defines how often we adjust the moving average from
	// recent observations. Default 15s.
	AdjustmentInterval time.Duration

	// Weight is a value between (0, 1) indicating the weighting factor used to adjust
	// the EMA. With larger values, newer data will influence the average more, and older
	// values will be factored out more quickly.  In mathematical literature concerning EMA,
	// this is referred to as the `alpha` constant.
	// Default is 0.5
	Weight float64

	// InitialSampleRate is the sample rate to use during startup, before we
	// have accumulated enough data to calculate a reasonable desired sample
	// rate. This is mainly useful in situations where unsampled throughput is
	// high enough to cause problems.
	// Default 10.
	InitialSampleRate int

	// GoalThroughputPerSec is the target number of events to send per second.
	// Sample rates are generated to squash the total throughput down to match the
	// goal throughput. Actual throughput may exceed goal throughput. default 100
	GoalThroughputPerSec int

	// MaxKeys, if greater than 0, limits the number of distinct keys tracked in EMA.
	// Once MaxKeys is reached, new keys will not be included in the sample rate map, but
	// existing keys will continue to be be counted.
	// Defaults to 0
	MaxKeys int

	// AgeOutValue indicates the threshold for removing keys from the EMA. The EMA of any key will approach 0
	// if it is not repeatedly observed, but will never truly reach it, so we have to decide what constitutes "zero".
	// Keys with averages below this threshold will be removed from the EMA. Default is the same as Weight, as this prevents
	// a key with the smallest integer value (1) from being aged out immediately. This value should generally be <= Weight,
	// unless you have very specific reasons to set it higher.
	AgeOutValue float64

	// BurstMultiple, if set, is multiplied by the sum of the running average of counts to define
	// the burst detection threshold. If total counts observed for a given interval exceed the threshold
	// EMA is updated immediately, rather than waiting on the AdjustmentInterval.
	// Defaults to 2; negative value disables. With a default of 2, if your traffic suddenly doubles,
	// burst detection will kick in.
	BurstMultiple float64

	// BurstDetectionDelay indicates the number of intervals to run after Start is called before burst detection kicks in.
	// Defaults to 3
	BurstDetectionDelay uint

	savedSampleRates map[string]int
	currentCounts    map[string]float64
	movingAverage    map[string]float64
	burstThreshold   float64
	currentBurstSum  float64
	intervalCount    uint
	burstSignal      chan struct{}

	// haveData indicates that we have gotten a sample of traffic. Before we've
	// gotten any samples of traffic, we should use the default goal sample rate
	// for all events instead of sampling everything at 1
	haveData bool
	updating bool
	done     chan struct{}

	lock sync.Mutex

	// used only in tests
	testSignalMapsDone chan struct{}

	// metrics
	requestCount     int64
	eventCount       int64
	burstCount       int64
	prefix           string
	requestCountKey  string
	eventCountKey    string
	keyspaceSizeKey  string
	burstCountKey    string
	intervalCountKey string
}

// Ensure we implement the sampler interface
var _ Sampler = (*EMAThroughput)(nil)

func (e *EMAThroughput) Start() error {
	// apply defaults
	if e.AdjustmentInterval == 0 {
		e.AdjustmentInterval = 15 * time.Second
	}
	if e.AdjustmentInterval < 1*time.Millisecond {
		return fmt.Errorf("the AdjustmentInterval %v is unreasonably short for a throughput sampler", e.AdjustmentInterval)
	}
	if e.InitialSampleRate == 0 {
		e.InitialSampleRate = 10
	}
	if e.GoalThroughputPerSec == 0 {
		e.GoalThroughputPerSec = 100
	}
	if e.Weight == 0 {
		e.Weight = 0.5
	}
	if e.AgeOutValue == 0 {
		e.AgeOutValue = e.Weight
	}
	if e.BurstMultiple == 0 {
		e.BurstMultiple = 2
	}
	if e.BurstDetectionDelay == 0 {
		e.BurstDetectionDelay = 3
	}

	// Don't override these maps at startup in case they were loaded from a previous state
	e.currentCounts = make(map[string]float64)
	if e.savedSampleRates == nil {
		e.savedSampleRates = make(map[string]int)
	}
	if e.movingAverage == nil {
		e.movingAverage = make(map[string]float64)
	}
	e.burstSignal = make(chan struct{})
	e.done = make(chan struct{})

	go func() {
		ticker := time.NewTicker(e.AdjustmentInterval)
		defer ticker.Stop()
		for {
			select {
			case <-e.burstSignal:
				// reset ticker when we get a burst
				ticker.Stop()
				ticker = time.NewTicker(e.AdjustmentInterval)
				e.updateMaps()
			case <-ticker.C:
				e.updateMaps()
				e.intervalCount++
			case <-e.done:
				return
			}
		}
	}()
	return nil
}

func (e *EMAThroughput) Stop() error {
	close(e.done)
	return nil
}

// updateMaps calculates a new saved rate map based on the contents of the
// counter map
func (e *EMAThroughput) updateMaps() {
	e.lock.Lock()
	if e.testSignalMapsDone != nil {
		defer func() {
			e.testSignalMapsDone <- struct{}{}
		}()
	}
	// short circuit if no traffic
	if len(e.currentCounts) == 0 {
		// No traffic the last interval, don't update anything. This is deliberate to avoid
		// the average decaying when there's no traffic (comes in bursts, or there's some kind of outage).
		e.lock.Unlock()
		return
	}
	// If there is another updateMaps going, bail
	if e.updating {
		e.lock.Unlock()
		return
	}
	e.updating = true
	// make a local copy of the sample counters for calculation
	tmpCounts := e.currentCounts
	e.currentCounts = make(map[string]float64)
	e.currentBurstSum = 0
	e.lock.Unlock()

	e.updateEMA(tmpCounts)

	// Goal events to send this interval is the total count of events in the EMA
	// divided by the desired average sample rate
	var sumEvents float64
	for _, count := range e.movingAverage {
		sumEvents += math.Max(1, count)
	}

	// Store this for burst detection. This is checked in GetSampleRate
	// so we need to grab the lock when we update it.
	e.lock.Lock()
	e.burstThreshold = sumEvents * e.BurstMultiple
	e.lock.Unlock()

	// Calculate the desired average sample rate per second based on the volume we've received.
	// This is the number of events we'd like to let through per adjustment interval.
	goalCount := float64(e.GoalThroughputPerSec) * e.AdjustmentInterval.Seconds()

	// goalRatio is the goalCount divided by the sum of all the log values - it
	// determines what percentage of the total event space belongs to each key
	var logSum float64
	for _, count := range e.movingAverage {
		// We take the max of (1, count) because count * weight is < 1 for
		// very small counts, which throws off the logSum and can cause
		// incorrect samples rates to be computed when throughput is low
		logSum += math.Log10(math.Max(1, count))
	}
	goalRatio := goalCount / logSum

	newSavedSampleRates := calculateSampleRates(goalRatio, e.movingAverage)
	e.lock.Lock()
	defer e.lock.Unlock()
	e.savedSampleRates = newSavedSampleRates
	e.haveData = true
	e.updating = false
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (e *EMAThroughput) GetSampleRate(key string) int {
	return e.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (e *EMAThroughput) GetSampleRateMulti(key string, count int) int {
	e.lock.Lock()
	defer e.lock.Unlock()

	e.requestCount++
	e.eventCount += int64(count)

	// Enforce MaxKeys limit on the size of the map
	if e.MaxKeys > 0 {
		// If a key already exists, increment it. If not, but we're under the limit, store a new key
		if _, found := e.currentCounts[key]; found || len(e.currentCounts) < e.MaxKeys {
			e.currentCounts[key] += float64(count)
			e.currentBurstSum += float64(count)
		}
	} else {
		e.currentCounts[key] += float64(count)
		e.currentBurstSum += float64(count)
	}

	// Enforce the burst threshold
	if e.burstThreshold > 0 && e.currentBurstSum >= e.burstThreshold && e.intervalCount >= e.BurstDetectionDelay {
		// reset the burst sum to prevent additional burst updates from occurring while updateMaps is running
		e.currentBurstSum = 0
		e.burstCount++
		// send but don't block - consuming is blocked on updateMaps, which takes the same lock we're holding
		select {
		case e.burstSignal <- struct{}{}:
		default:
		}
	}

	if !e.haveData {
		return e.InitialSampleRate
	}
	if rate, found := e.savedSampleRates[key]; found {
		return rate
	}
	return 1
}

func (e *EMAThroughput) updateEMA(newCounts map[string]float64) {
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

type emaThroughputState struct {
	// These fields are exported for use by `JSON.Marshal` and `JSON.Unmarshal`
	SavedSampleRates map[string]int     `json:"saved_sample_rates"`
	MovingAverage    map[string]float64 `json:"moving_average"`
}

// SaveState returns a byte array with a JSON representation of the sampler state
func (e *EMAThroughput) SaveState() ([]byte, error) {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.savedSampleRates == nil {
		return nil, errors.New("saved sample rate map is nil")
	}
	if e.movingAverage == nil {
		return nil, errors.New("moving average map is nil")
	}
	s := &emaThroughputState{SavedSampleRates: e.savedSampleRates, MovingAverage: e.movingAverage}
	return json.Marshal(s)
}

// LoadState accepts a byte array with a JSON representation of a previous instance's
// state
func (e *EMAThroughput) LoadState(state []byte) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	s := emaThroughputState{}
	err := json.Unmarshal(state, &s)
	if err != nil {
		return err
	}

	// Load the previously calculated sample rates
	e.savedSampleRates = s.SavedSampleRates
	e.movingAverage = s.MovingAverage
	// Allow GetSampleRate to return calculated sample rates from the loaded map
	e.haveData = true

	return nil
}

func (e *EMAThroughput) GetMetrics(prefix string) map[string]int64 {
	e.lock.Lock()
	defer e.lock.Unlock()

	if e.prefix == "" {
		e.prefix = prefix
		e.requestCountKey = e.prefix + requestCountSuffix
		e.eventCountKey = e.prefix + eventCountSuffix
		e.keyspaceSizeKey = e.prefix + keyspaceSizeSuffix
		e.burstCountKey = e.prefix + burstCountSuffix
		e.intervalCountKey = e.prefix + intervalCountSuffix
	}

	// If the prefix is set but does not match with the current prefix, return nil
	if e.prefix != prefix {
		return nil
	}

	return map[string]int64{
		e.requestCountKey:  e.requestCount,
		e.eventCountKey:    e.eventCount,
		e.keyspaceSizeKey:  int64(len(e.currentCounts)),
		e.burstCountKey:    e.burstCount,
		e.intervalCountKey: int64(e.intervalCount),
	}
}
