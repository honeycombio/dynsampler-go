package dynsampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover OverflowSampleRate: a key rejected at the MaxKeys cap gets
// that rate instead of the keep-everything default. A key with a computed rate
// keeps it, except in WindowedThroughput, which checks the cap first.

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

	// A new key at the cap gets the overflow rate.
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

	// Give "foo" a computed rate.
	for i := 0; i < 100; i++ {
		e.currentCounts = map[string]float64{"foo": 500}
		e.updateMaps()
	}
	savedRate := e.savedSampleRates["foo"]
	require.Greater(t, savedRate, 1, "foo must have a computed rate")
	require.True(t, e.haveData, "not a cold start")

	// Fill to MaxKeys with other keys, so "foo" is rejected.
	e.currentCounts = make(map[string]float64)
	e.GetSampleRate("a")
	e.GetSampleRate("b")
	require.Len(t, e.currentCounts, e.MaxKeys, "must be at MaxKeys to reject 'foo'")

	rate := e.GetSampleRate("foo")
	require.NotContains(t, e.currentCounts, "foo", "foo was rejected")
	assert.Equal(t, savedRate, rate, "a rejected key keeps its saved rate")
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

	// A new key at the cap gets the overflow rate.
	assert.Equal(t, 20, sampler.GetSampleRate("c"))
}

// WindowedThroughput checks the cap before its savedSampleRates lookup, and
// BoundedBlockList rejects at capacity without checking whether the key is
// already tracked, so tracked keys get the overflow rate too.
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

	// Give "foo" a computed rate.
	for i := 0; i < 20; i++ {
		sampler.GetSampleRate("foo")
	}
	indexGenerator.CurrentIndex += 1
	sampler.updateMaps()
	require.Equal(t, 2, sampler.savedSampleRates["foo"], "foo must have a computed rate")
	require.Equal(t, 2, sampler.GetSampleRate("foo"), "below the cap, foo keeps that rate")

	// Fill to capacity.
	sampler.GetSampleRate("bar")

	assert.Equal(t, 20, sampler.GetSampleRate("baz"), "a new key at the cap overflows")
	assert.Equal(t, 20, sampler.GetSampleRate("foo"), "a tracked key at the cap overflows too")
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

	// Time 1: the list is unbounded, so the key is never rejected.
	for i := 0; i < 10; i++ {
		assert.Equal(t, 2, sampler.GetSampleRate(key))
	}
}
