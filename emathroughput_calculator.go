package dynsampler

import (
	"math"
	"time"
)

// EMAThroughputCalculator is the timer-free computation core of EMAThroughput.
// Feed it one interval of counts with Update, then read the current per-key
// rate table with Rates. It lets a caller drive the EMA computation on its own
// schedule and over counts it supplies, for example counts merged across a
// fleet of processes, so every caller that folds identical counts derives an
// identical rate table with no coordination beyond sharing the counts.
//
// EMAThroughput is a timer-driven wrapper around this type; both produce the
// same rates from the same sequence of interval counts. A calculator is not
// safe for concurrent use; callers serialize access.
type EMAThroughputCalculator struct {
	// Weight is the EMA alpha in (0, 1); larger adapts faster. Default 0.5.
	Weight float64
	// AgeOutValue is the threshold below which a decaying key is dropped,
	// bounding the tracked set. Default is the same as Weight.
	AgeOutValue float64
	// GoalThroughputPerSec is the target events per second. Default 100.
	GoalThroughputPerSec float64
	// AdjustmentInterval is the wall-clock span each Update represents; it
	// scales the goal into a per-interval budget. Default 15s.
	AdjustmentInterval time.Duration

	movingAverage map[string]float64
}

func (c *EMAThroughputCalculator) ensureInit() {
	if c.movingAverage == nil {
		c.movingAverage = make(map[string]float64)
	}
	if c.Weight == 0 {
		c.Weight = 0.5
	}
	if c.AgeOutValue == 0 {
		c.AgeOutValue = c.Weight
	}
	if c.GoalThroughputPerSec == 0 {
		c.GoalThroughputPerSec = 100
	}
	if c.AdjustmentInterval == 0 {
		c.AdjustmentInterval = 15 * time.Second
	}
}

// Update folds one interval of counts into the moving average, consuming the
// map. An empty interval leaves the average untouched: traffic gaps must not
// decay it (a burst-then-quiet key should not be treated as having dropped to
// zero the moment traffic pauses).
func (c *EMAThroughputCalculator) Update(counts map[string]float64) {
	c.ensureInit()
	if len(counts) == 0 {
		return
	}
	keysToUpdate := make([]string, 0, len(c.movingAverage))
	for key := range c.movingAverage {
		keysToUpdate = append(keysToUpdate, key)
	}
	for _, key := range keysToUpdate {
		var newAvg float64
		if val, found := counts[key]; found {
			newAvg = adjustAverage(c.movingAverage[key], val, c.Weight)
		} else {
			newAvg = adjustAverage(c.movingAverage[key], 0, c.Weight)
		}
		if newAvg < c.AgeOutValue {
			delete(c.movingAverage, key)
		} else {
			c.movingAverage[key] = newAvg
		}
		delete(counts, key)
	}
	for key := range counts {
		newAvg := adjustAverage(0, counts[key], c.Weight)
		if newAvg >= c.AgeOutValue {
			c.movingAverage[key] = newAvg
		}
	}
}

// Rates returns the per-key sample rates for the current moving average. It is
// empty until the first non-empty Update. Keys absent from the returned table
// have no learned rate yet; the caller decides what to do with them (typically
// a bootstrap rate).
func (c *EMAThroughputCalculator) Rates() map[string]int {
	c.ensureInit()
	if len(c.movingAverage) == 0 {
		return map[string]int{}
	}
	goalCount := c.GoalThroughputPerSec * c.AdjustmentInterval.Seconds()
	var logSum float64
	for _, count := range c.movingAverage {
		// max(1, count) because count can be < 1 for very small averages,
		// which would throw off the logSum at low throughput.
		logSum += math.Log10(math.Max(1, count))
	}
	goalRatio := goalCount / logSum
	return calculateSampleRates(goalRatio, c.movingAverage)
}

// movingAverageState and loadMovingAverage give the timer-driven EMAThroughput
// wrapper access to the calculator's state for SaveState/LoadState without
// exposing the internal map directly.
func (c *EMAThroughputCalculator) movingAverageState() map[string]float64 {
	c.ensureInit()
	return c.movingAverage
}

func (c *EMAThroughputCalculator) loadMovingAverage(avg map[string]float64) {
	c.ensureInit()
	if avg != nil {
		c.movingAverage = avg
	}
}
