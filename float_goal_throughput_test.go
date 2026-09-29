package dynsampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// canSetGoalThroughputPerSec mirrors the interface Refinery asserts against at
// runtime (sample.CanSetGoalThroughputPerSec). Refinery resolves it with a type
// assertion rather than a compile-time check, so changing this method's
// signature would make that assertion fail silently and stop Refinery adjusting
// its goals. These assertions keep the int method's shape pinned.
type canSetGoalThroughputPerSec interface {
	SetGoalThroughputPerSec(int)
}

var (
	_ canSetGoalThroughputPerSec = (*EMAThroughput)(nil)
	_ canSetGoalThroughputPerSec = (*WindowedThroughput)(nil)
	_ canSetGoalThroughputPerSec = (*TotalThroughput)(nil)
)

// canSetGoalThroughputPerSecFloat is the fractional counterpart, used by callers
// dividing a fleet-wide budget across instances.
type canSetGoalThroughputPerSecFloat interface {
	SetGoalThroughputPerSecFloat(float64)
}

var (
	_ canSetGoalThroughputPerSecFloat = (*EMAThroughput)(nil)
	_ canSetGoalThroughputPerSecFloat = (*WindowedThroughput)(nil)
	_ canSetGoalThroughputPerSecFloat = (*TotalThroughput)(nil)
)

func TestSetGoalThroughputPerSecFloat_KeepsFraction(t *testing.T) {
	t.Run("EMAThroughput", func(t *testing.T) {
		e := &EMAThroughput{GoalThroughputPerSec: 100}
		e.SetGoalThroughputPerSecFloat(0.25)
		assert.Equal(t, 0.25, e.GoalThroughputPerSec)
	})

	t.Run("WindowedThroughput", func(t *testing.T) {
		w := &WindowedThroughput{GoalThroughputPerSec: 100}
		w.SetGoalThroughputPerSecFloat(0.25)
		assert.Equal(t, 0.25, w.GoalThroughputPerSec)
	})

	t.Run("TotalThroughput", func(t *testing.T) {
		tt := &TotalThroughput{GoalThroughputPerSec: 100}
		tt.SetGoalThroughputPerSecFloat(0.25)
		assert.Equal(t, 0.25, tt.GoalThroughputPerSec)
	})
}

func TestSetGoalThroughputPerSecFloat_IgnoresNonPositive(t *testing.T) {
	e := &EMAThroughput{GoalThroughputPerSec: 100}
	e.SetGoalThroughputPerSecFloat(0)
	assert.Equal(t, float64(100), e.GoalThroughputPerSec)
	e.SetGoalThroughputPerSecFloat(-0.5)
	assert.Equal(t, float64(100), e.GoalThroughputPerSec)
}

// The int setter now delegates to the float one, so its documented behaviour
// (positive values applied, everything else ignored) must be unchanged.
func TestSetGoalThroughputPerSec_IntStillApplies(t *testing.T) {
	w := &WindowedThroughput{GoalThroughputPerSec: 1}
	w.SetGoalThroughputPerSec(250)
	assert.Equal(t, float64(250), w.GoalThroughputPerSec)
	w.SetGoalThroughputPerSec(0)
	assert.Equal(t, float64(250), w.GoalThroughputPerSec)
	w.SetGoalThroughputPerSec(-10)
	assert.Equal(t, float64(250), w.GoalThroughputPerSec)
}

// A fractional goal must actually reach the rate calculation rather than being
// truncated to zero, which is the whole point of the float setter. A goal of
// 0.5/sec over a 1s interval targets roughly one event every two seconds, so
// the sampler settles on a much higher rate than it would at the default 100/sec.
func TestEMAThroughputFractionalGoalProducesHigherRates(t *testing.T) {
	newSampler := func(goal float64) *EMAThroughput {
		e := &EMAThroughput{
			AdjustmentInterval: time.Second,
			Weight:             0.5,
			AgeOutValue:        0.5,
		}
		e.SetGoalThroughputPerSecFloat(goal)
		e.movingAverage = make(map[string]float64)
		e.savedSampleRates = make(map[string]int)
		for i := 0; i < 100; i++ {
			e.currentCounts = map[string]float64{"foo": 1000}
			e.updateMaps()
		}
		return e
	}

	fractional := newSampler(0.5).savedSampleRates["foo"]
	whole := newSampler(100).savedSampleRates["foo"]

	require.Positive(t, fractional, "a fractional goal must still produce a usable rate")
	assert.Greater(t, fractional, whole,
		"a 0.5/sec goal must sample harder than a 100/sec goal, not truncate to the same or a lower rate")
}
