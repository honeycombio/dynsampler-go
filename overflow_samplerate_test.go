package dynsampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover the opt-in OverflowSampleRate field: when set, keys
// rejected because MaxKeys has been reached return that rate instead of the
// default keep-everything behaviour. A key that already has a computed rate
// keeps it, except in WindowedThroughput, where the cap is checked first and
// rejects tracked keys too.

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
	e.movingAverage = make(map[string]float64)

	// Give "foo" a computed rate from previous intervals.
	for i := 0; i < 100; i++ {
		e.currentCounts = map[string]float64{"foo": 500}
		e.updateMaps()
	}
	savedRate := e.savedSampleRates["foo"]
	require.Greater(t, savedRate, 1, "foo must have a computed rate to keep")
	require.True(t, e.haveData, "haveData must be true so the cold-start path is not taken")

	// Fill currentCounts to MaxKeys with other keys so "foo" is rejected.
	e.currentCounts = make(map[string]float64)
	e.GetSampleRate("a")
	e.GetSampleRate("b")
	require.Len(t, e.currentCounts, e.MaxKeys, "currentCounts must be at MaxKeys for 'foo' to be rejected")

	rate := e.GetSampleRate("foo")
	require.NotContains(t, e.currentCounts, "foo", "foo was rejected")
	assert.Equal(t, savedRate, rate, "a rejected key with a saved rate keeps that rate")
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

// Unlike the other samplers, WindowedThroughput checks the cap before its
// savedSampleRates lookup, and BoundedBlockList rejects at capacity without
// first checking whether the key is already tracked. A key with a computed rate
// therefore gets the overflow rate once the list is full.
func TestWindowedThroughput_OverflowSampleRate_AppliesToTrackedKeysAtTheCap(t *testing.T) {
	indexGenerator := &TestIndexGenerator{}
	sampler := &WindowedThroughput{
		UpdateFrequencyDuration:   1 * time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      2,
		indexGenerator:            indexGenerator,
		countList:                 NewBoundedBlockList(2),
		OverflowSampleRate:        20,
	}

	// Give "foo" a computed rate from the previous window.
	for i := 0; i < 20; i++ {
		sampler.GetSampleRate("foo")
	}
	indexGenerator.CurrentIndex += 1
	sampler.updateMaps()
	require.Equal(t, 2, sampler.savedSampleRates["foo"], "foo must have a computed rate")

	// Below the cap, "foo" keeps that rate.
	require.Equal(t, 2, sampler.GetSampleRate("foo"))

	// Fill the list to capacity, then "foo" is rejected along with new keys.
	sampler.GetSampleRate("bar")
	assert.Equal(t, 20, sampler.GetSampleRate("baz"), "a new key at the cap gets the overflow rate")
	assert.Equal(t, 20, sampler.GetSampleRate("foo"), "a tracked key at the cap also gets the overflow rate")
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
	require.Equal(t, 2, sampler.savedSampleRates[key], "the key must have a computed rate")

	// Time 1: 10 traces seen. The list is unbounded, so the key is never
	// rejected and keeps its computed rate.
	for i := 0; i < 10; i++ {
		assert.Equal(t, 2, sampler.GetSampleRate(key))
	}
}
