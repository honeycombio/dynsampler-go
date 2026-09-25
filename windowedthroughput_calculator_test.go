package dynsampler

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Reproduces the TestHappyPath golden vector (5s lookback, goal 2/s, counts
// 20, 10, 50, 0, 0, 0, 40 over successive ticks) driven through the calculator
// one tick per Update.
func TestWindowedThroughputCalculator_HappyPath(t *testing.T) {
	c := &WindowedThroughputCalculator{
		GoalThroughputPerSec: 2,
		UpdateFrequency:      time.Second,
		LookbackFrequency:    5 * time.Second,
	}
	key := "test_key"

	c.Update(map[string]float64{key: 20})
	assert.Equal(t, 2, c.Rates()[key], "window [20], goal 10/key")
	c.Update(map[string]float64{key: 10})
	assert.Equal(t, 3, c.Rates()[key], "window [20,10]")
	c.Update(map[string]float64{key: 50})
	c.Update(map[string]float64{})
	c.Update(map[string]float64{})
	c.Update(map[string]float64{})
	assert.Equal(t, 6, c.Rates()[key], "20 has aged out of the 5-tick window; [10,50,0,0,0]=60")
	c.Update(map[string]float64{key: 40})
	assert.Equal(t, 9, c.Rates()[key], "[50,0,0,0,40]=90")
}

func TestWindowedThroughputCalculator_CopiesCounts(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	counts := map[string]float64{"k": 20}
	c.Update(counts)
	// Mutating the caller's map after Update must not disturb the stored bucket.
	counts["k"] = 999
	counts["injected"] = 500
	assert.Equal(t, 2, c.Rates()["k"], "stored bucket must be a copy, unaffected by later mutation")
	_, injected := c.Rates()["injected"]
	assert.False(t, injected, "a key added to the caller's map after Update must not appear")
}

func TestWindowedThroughputCalculator_EmptyWindow(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 3 * time.Second}
	assert.Empty(t, c.Rates(), "no traffic yet")
	c.Update(map[string]float64{"k": 20})
	assert.NotEmpty(t, c.Rates())
	// Drain the 3-tick window with 3 empty ticks; the key drops out and rates reset.
	c.Update(map[string]float64{})
	c.Update(map[string]float64{})
	c.Update(map[string]float64{})
	assert.Empty(t, c.Rates(), "old counts drop out of the window")
}

func TestWindowedThroughputCalculator_FloorsLookback(t *testing.T) {
	c := &WindowedThroughputCalculator{UpdateFrequency: 2 * time.Second, LookbackFrequency: 7 * time.Second}
	c.ensureInit()
	assert.Equal(t, 6*time.Second, c.LookbackFrequency, "floored to a whole multiple of the update frequency")
	assert.Len(t, c.buckets, 3)
}

// A lookback shorter than the update frequency must be raised to it, not
// floored to zero and then repaired to a default 30x window on a later
// ensureInit call (a repeated ensureInit must be idempotent).
func TestWindowedThroughputCalculator_LookbackShorterThanUpdate(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 100, UpdateFrequency: 10 * time.Second, LookbackFrequency: 5 * time.Second}
	c.Update(map[string]float64{"k": 1e6})
	// Equivalent to a correctly-configured 1-tick (lookback == update) calculator:
	// goal 100/s * 10s window / 1 key = 1000 events/key; 1e6/1000 = 1000.
	assert.Equal(t, 1000, c.Rates()["k"])
	// A second ensureInit (triggered by this Rates call) must not have reset
	// the lookback to a 30-tick default.
	assert.Equal(t, 1000, c.Rates()["k"])
}

// Mutating LookbackFrequency after the ring has been sized must not reprice
// the window; the effective lookback is snapshotted at first use.
func TestWindowedThroughputCalculator_LookbackFrozenAfterFirstUse(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 100, UpdateFrequency: 10 * time.Second, LookbackFrequency: 5 * time.Second}
	c.Update(map[string]float64{"k": 1e6})
	before := c.Rates()["k"]

	c.LookbackFrequency = 100 * time.Second
	after := c.Rates()["k"]
	assert.Equal(t, before, after, "mutating LookbackFrequency after first use must have no effect")
}

func TestWindowedThroughputCalculator_UpdateSkipsInvalidCounts(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	c.Update(map[string]float64{
		"real": 20,
		"inf":  math.Inf(1),
		"ninf": math.Inf(-1),
		"nan":  math.NaN(),
		"neg":  -5,
		"zero": 0,
	})
	rates := c.Rates()
	assert.Equal(t, map[string]int{"real": 2}, rates, "only the finite, positive count survives")
}

func TestWindowedThroughputCalculator_ZeroCountKeysDoNotDilute(t *testing.T) {
	solo := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	solo.Update(map[string]float64{"real": 20})
	soloRate := solo.Rates()["real"]

	diluted := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	counts := map[string]float64{"real": 20}
	for i := 0; i < 9; i++ {
		counts[fmt.Sprintf("idle-%d", i)] = 0
	}
	diluted.Update(counts)

	assert.Equal(t, map[string]int{"real": soloRate}, diluted.Rates(), "zero-count keys must not dilute the equal split")
}
