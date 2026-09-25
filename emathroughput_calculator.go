package dynsampler

import (
	"encoding/json"
	"fmt"
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

// Update folds one interval of counts into the moving average. The counts map
// is only read, never retained or modified. An empty interval leaves the
// average untouched: traffic gaps must not decay it (a burst-then-quiet key
// should not be treated as having dropped to zero the moment traffic pauses).
// Counts must be finite and positive; a NaN or infinite count for a tracked
// key is treated as 0 (so the key decays normally), and a NaN, infinite, or
// non-positive count for a new key is ignored, since an unbounded value can
// never age back out of the average.
func (c *EMAThroughputCalculator) Update(counts map[string]float64) {
	c.ensureInit()
	if len(counts) == 0 {
		return
	}
	// Snapshot the currently tracked keys so the new-key loop below can tell
	// them apart from keys already in the moving average, without deleting
	// from the caller's counts map.
	tracked := make(map[string]struct{}, len(c.movingAverage))
	for key := range c.movingAverage {
		tracked[key] = struct{}{}
	}
	// counts[key] is 0 for a tracked key absent this interval, which decays it.
	for key := range tracked {
		val := counts[key]
		if math.IsNaN(val) || math.IsInf(val, 0) || val < 0 {
			val = 0
		}
		newAvg := adjustAverage(c.movingAverage[key], val, c.Weight)
		if newAvg < c.AgeOutValue {
			delete(c.movingAverage, key)
		} else {
			c.movingAverage[key] = newAvg
		}
	}
	for key, val := range counts {
		if _, ok := tracked[key]; ok {
			continue
		}
		if val <= 0 || math.IsNaN(val) || math.IsInf(val, 0) {
			continue
		}
		newAvg := adjustAverage(0, val, c.Weight)
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

// emaThroughputCalculatorKind identifies an EMAThroughputCalculator blob, so
// a blob from a different type (or no kind at all, as with a legacy
// EMAThroughput sampler blob, or `null`) is rejected by LoadState rather than
// silently half-loading.
const emaThroughputCalculatorKind = "ema_throughput_calculator"

// emaThroughputCalculatorVersion is the current wire format version for
// EMAThroughputCalculator blobs.
const emaThroughputCalculatorVersion = 1

// emaThroughputCalculatorState is the JSON wire format for
// EMAThroughputCalculator.SaveState/LoadState. Kind and Version guard against
// loading a blob from a different type or format; AdjustmentInterval guards
// against loading into a calculator configured with a different timing,
// which would reprice the moving average against the wrong interval.
type emaThroughputCalculatorState struct {
	Kind               string             `json:"kind"`
	Version            int                `json:"version"`
	AdjustmentInterval time.Duration      `json:"adjustment_interval"`
	MovingAverage      map[string]float64 `json:"moving_average"`
}

// SaveState serializes the calculator's moving average, along with its kind,
// version, and timing config, to a byte blob. Not safe for concurrent use
// with Update/Rates; callers serialize access.
func (c *EMAThroughputCalculator) SaveState() ([]byte, error) {
	c.ensureInit()
	return json.Marshal(emaThroughputCalculatorState{
		Kind:               emaThroughputCalculatorKind,
		Version:            emaThroughputCalculatorVersion,
		AdjustmentInterval: c.AdjustmentInterval,
		MovingAverage:      c.movingAverage,
	})
}

// LoadState restores the calculator's moving average from a blob produced by
// SaveState, replacing the current state wholesale. It rejects a blob that
// isn't an EMAThroughputCalculator blob (wrong kind, including `null` or a
// legacy EMAThroughput sampler blob, which decodes to an empty kind), a
// different wire version, or one saved under a different AdjustmentInterval
// than this calculator is configured with (the average would be repriced
// against the wrong interval). On error the calculator's existing state is
// left untouched. Not safe for concurrent use with Update/Rates; callers
// serialize access.
func (c *EMAThroughputCalculator) LoadState(state []byte) error {
	var s emaThroughputCalculatorState
	if err := json.Unmarshal(state, &s); err != nil {
		return err
	}
	if s.Kind != emaThroughputCalculatorKind {
		return fmt.Errorf("dynsampler: cannot load state: expected kind %q, got %q", emaThroughputCalculatorKind, s.Kind)
	}
	if s.Version != emaThroughputCalculatorVersion {
		return fmt.Errorf("dynsampler: cannot load state: expected version %d, got %d", emaThroughputCalculatorVersion, s.Version)
	}
	c.ensureInit()
	if s.AdjustmentInterval != c.AdjustmentInterval {
		return fmt.Errorf("dynsampler: cannot load state: blob AdjustmentInterval %v does not match calculator's %v", s.AdjustmentInterval, c.AdjustmentInterval)
	}
	if s.MovingAverage == nil {
		s.MovingAverage = make(map[string]float64)
	}
	c.movingAverage = s.MovingAverage
	return nil
}
