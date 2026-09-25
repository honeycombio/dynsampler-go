package dynsampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// These tests cover the opt-in OverflowSampleRate field: when set, keys
// rejected because MaxKeys has been reached return that rate instead of the
// default keep-everything behaviour. Already-tracked keys are unaffected.

func TestEMAThroughput_OverflowSampleRate_AppliesToRejectedKeys(t *testing.T) {
	e := &EMAThroughput{
		GoalThroughputPerSec: 10,
		AdjustmentInterval:   time.Second,
		Weight:               0.2,
		AgeOutValue:          0.2,
		MaxKeys:              2,
		OverflowSampleRate:   20,
	}
	e.savedSampleRates = make(map[string]int)
	e.currentCounts = make(map[string]float64)
	e.haveData = true

	// Fill currentCounts to MaxKeys.
	e.GetSampleRateMulti("a", 1)
	e.GetSampleRateMulti("b", 1)

	// A brand-new third key arrives while at the cap: returns the overflow rate.
	assert.Equal(t, 20, e.GetSampleRate("c"))
	assert.NotContains(t, e.currentCounts, "c")
}

func TestEMAThroughput_OverflowSampleRate_DoesNotAffectTrackedKeys(t *testing.T) {
	e := &EMAThroughput{
		GoalThroughputPerSec: 10,
		AdjustmentInterval:   time.Second,
		Weight:               0.2,
		AgeOutValue:          0.2,
		MaxKeys:              2,
		OverflowSampleRate:   20,
	}
	e.savedSampleRates = make(map[string]int)

	for i := 0; i < 100; i++ {
		e.currentCounts = map[string]float64{"foo": 500}
		e.updateMaps()
	}
	learnedRate := e.GetSampleRate("foo")
	assert.Greater(t, learnedRate, 1, "sanity: foo has a learned rate")
	assert.NotEqual(t, 20, learnedRate, "learned rate must not be affected by OverflowSampleRate")
}

func TestWindowedThroughput_OverflowSampleRate_AppliesToRejectedKeys(t *testing.T) {
	sampler := &WindowedThroughput{
		UpdateFrequencyDuration:   1 * time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      2,
		indexGenerator:            &TestIndexGenerator{},
		countList:                 NewBoundedBlockList(2),
		OverflowSampleRate:        20,
	}

	// Fill the bounded list to capacity.
	sampler.GetSampleRate("a")
	sampler.GetSampleRate("b")

	// A brand-new third key arrives while at the cap: returns the overflow rate.
	assert.Equal(t, 20, sampler.GetSampleRate("c"))
}

func TestWindowedThroughput_OverflowSampleRate_DoesNotAffectTrackedKeys(t *testing.T) {
	indexGenerator := &TestIndexGenerator{}
	sampler := &WindowedThroughput{
		UpdateFrequencyDuration:   1 * time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      2,
		indexGenerator:            indexGenerator,
		countList:                 NewUnboundedBlockList(),
		OverflowSampleRate:        20,
	}
	key := "test_key"

	// Time 0: 20 traces seen.
	for i := 0; i < 20; i++ {
		assert.Equal(t, 0, sampler.GetSampleRate(key))
	}
	indexGenerator.CurrentIndex += 1
	sampler.updateMaps()

	// Time 1: 10 traces seen, learned rate is unaffected by OverflowSampleRate.
	for i := 0; i < 10; i++ {
		assert.Equal(t, 2, sampler.GetSampleRate(key))
	}
}
