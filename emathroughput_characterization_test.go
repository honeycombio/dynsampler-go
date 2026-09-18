package dynsampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These characterization tests lock the observable behaviour of EMAThroughput
// through GetSampleRate, the currentCounts accumulator, updateMaps and
// SaveState/LoadState. The assertions never read movingAverage (the field that
// moves into EMAThroughputCalculator), so they are the proof that observable
// behaviour is preserved across that refactor. The only refactor-touch is the
// manual map init in setup, which the refactor makes unnecessary.
//
// updateMaps is driven by hand (no Start, so no background ticker) for
// determinism, matching the existing tests in this package.

func TestEMAThroughput_Characterization_ColdStart(t *testing.T) {
	e := &EMAThroughput{GoalThroughputPerSec: 100, AdjustmentInterval: time.Second, InitialSampleRate: 7}
	e.currentCounts = make(map[string]float64)

	assert.Equal(t, 7, e.GetSampleRate("anything"), "before any data the initial sample rate applies")
}

func TestEMAThroughput_Characterization_AgesOutObservably(t *testing.T) {
	e := &EMAThroughput{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2, InitialSampleRate: 10}
	e.savedSampleRates = make(map[string]int)

	for i := 0; i < 100; i++ {
		e.currentCounts = map[string]float64{"foo": 500}
		e.updateMaps()
	}
	assert.Greater(t, e.GetSampleRate("foo"), 1, "a steady heavy key gets a learned rate above 1")

	for i := 0; i < 100; i++ {
		e.currentCounts = map[string]float64{"asdf": 1}
		e.updateMaps()
	}
	assert.Equal(t, 1, e.GetSampleRate("foo"), "a key that stops appearing ages out and reads as unknown (rate 1)")
}

func TestEMAThroughput_Characterization_SaveLoadPreservesRates(t *testing.T) {
	a := &EMAThroughput{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2}
	a.savedSampleRates = make(map[string]int)
	for i := 0; i < 50; i++ {
		a.currentCounts = map[string]float64{"foo": 5000, "bar": 50}
		a.updateMaps()
	}
	fooRate := a.GetSampleRate("foo")
	barRate := a.GetSampleRate("bar")
	require.Greater(t, fooRate, 1, "sanity: foo has a learned rate to preserve")

	state, err := a.SaveState()
	require.NoError(t, err)

	b := &EMAThroughput{GoalThroughputPerSec: 10, AdjustmentInterval: time.Second, Weight: 0.2, AgeOutValue: 0.2}
	require.NoError(t, b.LoadState(state))
	require.NoError(t, b.Start())
	defer b.Stop()

	assert.Equal(t, fooRate, b.GetSampleRate("foo"), "loaded state must reproduce the saved rate for foo")
	assert.Equal(t, barRate, b.GetSampleRate("bar"), "loaded state must reproduce the saved rate for bar")
}
