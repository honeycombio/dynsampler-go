package dynsampler

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// The calculator is the computation core of EMAThroughput. Feeding both the
// same sequence of interval counts must produce identical rate tables, which
// also pins the golden vector (a steady 40 events/interval settles at rate 4).
func TestEMAThroughputCalculator_MatchesSampler(t *testing.T) {
	e := &EMAThroughput{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2}
	e.savedSampleRates = make(map[string]int)

	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2}

	for i := 0; i <= 100; i++ {
		samplerInput := map[string]float64{"largest_count": 40}
		calcInput := map[string]float64{"largest_count": 40}
		for j := 0; j < 5; j++ {
			// deterministic single-count keys that come and go each interval
			key := fmt.Sprintf("sporadic-%d-%d", i, j)
			samplerInput[key] = 1
			calcInput[key] = 1
		}
		e.currentCounts = samplerInput
		e.updateMaps()
		c.Update(calcInput)
	}

	assert.Equal(t, 4, c.Rates()["largest_count"], "golden vector: steady 40/interval settles at rate 4")
	assert.Equal(t, e.savedSampleRates, c.Rates(), "calculator must match the timer sampler over identical inputs")
}

func TestEMAThroughputCalculator_EmptyIntervalDoesNotDecay(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	c.Update(map[string]float64{"foo": 100})
	before := c.movingAverageState()["foo"]
	c.Update(map[string]float64{})
	assert.Equal(t, before, c.movingAverageState()["foo"], "an empty interval must not decay the average")
}

func TestEMAThroughputCalculator_DoesNotModifyCounts(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	// Seed a tracked key so the update touches both the existing-key and
	// new-key paths.
	c.Update(map[string]float64{"tracked": 100})

	counts := map[string]float64{"tracked": 50, "fresh": 20}
	c.Update(counts)

	assert.Equal(t, map[string]float64{"tracked": 50, "fresh": 20}, counts, "Update must not modify the caller's counts map")
}

func TestEMAThroughputCalculator_SaveLoadRoundTrip(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2}
	for i := 0; i < 5; i++ {
		c.Update(map[string]float64{"foo": 40, "bar": 10})
	}

	state, err := c.SaveState()
	assert.NoError(t, err)

	loaded := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2}
	assert.NoError(t, loaded.LoadState(state))
	assert.Equal(t, c.Rates(), loaded.Rates(), "rates must match immediately after a save/load handoff")

	// Feed both identical further intervals: a fleet handoff must stay
	// deterministic, not just match at the moment of the handoff.
	for i := 0; i < 5; i++ {
		next := map[string]float64{"foo": 40, "bar": 10, fmt.Sprintf("new-%d", i): 5}
		c.Update(next)
		loaded.Update(next)
		assert.Equal(t, c.Rates(), loaded.Rates(), "rates must stay identical after further identical updates")
	}
}

func TestEMAThroughputCalculator_LoadRejectsGarbage(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	c.Update(map[string]float64{"foo": 100})
	before := c.Rates()

	err := c.LoadState([]byte("not json"))
	assert.Error(t, err)
	assert.Equal(t, before, c.Rates(), "a failed load must leave prior state untouched")
}

func TestEMAThroughputCalculator_LoadRejectsNull(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	c.Update(map[string]float64{"foo": 100})
	before := c.Rates()

	err := c.LoadState([]byte("null"))
	assert.Error(t, err)
	assert.Equal(t, before, c.Rates(), "a rejected null load must leave prior state untouched")
}

func TestEMAThroughputCalculator_LoadRejectsVersionMismatch(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	c.Update(map[string]float64{"foo": 100})
	before := c.Rates()

	blob := fmt.Sprintf(`{"kind":%q,"version":2,"adjustment_interval":%d,"moving_average":{}}`,
		emaThroughputCalculatorKind, time.Second)
	err := c.LoadState([]byte(blob))
	assert.Error(t, err)
	assert.Equal(t, before, c.Rates(), "a rejected version mismatch must leave prior state untouched")
}

func TestEMAThroughputCalculator_LoadRejectsTimingMismatch(t *testing.T) {
	source := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	source.Update(map[string]float64{"foo": 100})
	state, err := source.SaveState()
	assert.NoError(t, err)

	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: 5 * time.Second, Weight: 0.5}
	c.Update(map[string]float64{"bar": 50})
	before := c.Rates()

	err = c.LoadState(state)
	assert.Error(t, err)
	assert.Equal(t, before, c.Rates(), "a rejected timing mismatch must leave prior state untouched")
}

// A blob from the windowed calculator must not be accepted by the EMA
// calculator's LoadState; the kind discriminator must catch it.
func TestEMAThroughputCalculator_LoadRejectsCrossType(t *testing.T) {
	w := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	w.Update(map[string]float64{"a": 20})
	wState, err := w.SaveState()
	assert.NoError(t, err)

	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	c.Update(map[string]float64{"foo": 100})
	before := c.Rates()

	err = c.LoadState(wState)
	assert.Error(t, err)
	assert.Equal(t, before, c.Rates(), "a rejected cross-type load must leave prior state untouched")
}

// A legacy EMAThroughput SAMPLER blob (saved_sample_rates + moving_average,
// no kind/version) must not half-load into the calculator via the matching
// moving_average field name.
func TestEMAThroughputCalculator_LoadRejectsSamplerBlob(t *testing.T) {
	sampler := &EMAThroughput{AdjustmentInterval: time.Hour}
	assert.NoError(t, sampler.Start())
	defer sampler.Stop()
	sampler.lock.Lock()
	sampler.savedSampleRates = map[string]int{"foo": 2}
	sampler.calc.loadMovingAverage(map[string]float64{"foo": 100})
	sampler.lock.Unlock()
	samplerState, err := sampler.SaveState()
	assert.NoError(t, err)

	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5}
	c.Update(map[string]float64{"foo": 100})
	before := c.Rates()

	err = c.LoadState(samplerState)
	assert.Error(t, err)
	assert.Equal(t, before, c.Rates(), "a rejected sampler blob load must leave prior state untouched")
}

func TestEMAThroughputCalculator_IgnoresPoisonCounts(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.5, AgeOutValue: 0.5}
	c.Update(map[string]float64{"poison": math.Inf(1), "ok": 100})

	_, found := c.movingAverageState()["poison"]
	assert.False(t, found, "an infinite count must never enter the moving average")
	_, found = c.Rates()["poison"]
	assert.False(t, found, "an infinite count must never produce a rate")

	_, found = c.movingAverageState()["ok"]
	assert.True(t, found, "a valid count alongside a poison one must still be tracked")
}

func TestEMAThroughputCalculator_AgesOut(t *testing.T) {
	c := &EMAThroughputCalculator{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2}
	for i := 0; i < 100; i++ {
		c.Update(map[string]float64{"foo": 500})
	}
	assert.Equal(t, float64(500), math.Round(c.movingAverageState()["foo"]))
	for i := 0; i < 100; i++ {
		c.Update(map[string]float64{"asdf": 1})
	}
	_, found := c.movingAverageState()["foo"]
	assert.False(t, found, "a key that stops appearing must age out")
}
