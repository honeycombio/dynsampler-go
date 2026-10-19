package dynsampler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests cover the opt-in OverflowSampleRate field on the five
// remaining MaxKeys-capped samplers: AvgSampleRate, AvgSampleWithMin,
// EMASampleRate, PerKeyThroughput and TotalThroughput. When set, keys
// rejected because MaxKeys has been reached return that rate instead of the
// default keep-everything behaviour. Already-tracked keys, and cold-start
// lookups where applicable, are unaffected.

func TestAvgSampleRate_OverflowSampleRate_AppliesToRejectedKeys(t *testing.T) {
	a := &AvgSampleRate{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	a.currentCounts = make(map[string]float64)
	a.savedSampleRates = make(map[string]int)
	a.haveData = true

	// Fill currentCounts to MaxKeys so "c" is rejected.
	a.GetSampleRate("a")
	a.GetSampleRate("b")
	require.Len(t, a.currentCounts, a.MaxKeys, "currentCounts must be at MaxKeys for 'c' to be rejected")

	assert.Equal(t, 20, a.GetSampleRate("c"))
	assert.NotContains(t, a.currentCounts, "c")
}

func TestAvgSampleRate_OverflowSampleRate_UnsetOrNegativeIsUnchanged(t *testing.T) {
	for _, rate := range []int{0, -5} {
		a := &AvgSampleRate{
			MaxKeys:            2,
			OverflowSampleRate: rate,
		}
		a.currentCounts = make(map[string]float64)
		a.savedSampleRates = make(map[string]int)
		a.haveData = true

		a.GetSampleRate("a")
		a.GetSampleRate("b")

		assert.Equal(t, 1, a.GetSampleRate("c"))
	}
}

func TestAvgSampleRate_OverflowSampleRate_DoesNotAffectTrackedKeys(t *testing.T) {
	a := &AvgSampleRate{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	a.currentCounts = make(map[string]float64)
	// "foo" has a computed rate from the previous interval.
	a.savedSampleRates = map[string]int{"foo": 7}
	a.haveData = true

	// Fill currentCounts to MaxKeys with other keys so "foo" is rejected.
	a.GetSampleRate("a")
	a.GetSampleRate("b")
	require.Len(t, a.currentCounts, a.MaxKeys, "currentCounts must be at MaxKeys for 'foo' to be rejected")

	rate := a.GetSampleRate("foo")
	require.NotContains(t, a.currentCounts, "foo", "foo was rejected")
	assert.Equal(t, 7, rate, "a rejected key with a saved rate keeps that rate")
}

func TestAvgSampleRate_OverflowSampleRate_ColdStartTakesPrecedence(t *testing.T) {
	a := &AvgSampleRate{
		MaxKeys:            2,
		OverflowSampleRate: 20,
		GoalSampleRate:     7,
	}
	a.currentCounts = map[string]float64{"a": 1, "b": 1}
	a.savedSampleRates = make(map[string]int)
	require.False(t, a.haveData, "haveData must be false to simulate cold start")

	assert.Equal(t, 7, a.GetSampleRate("c"))
}

func TestAvgSampleWithMin_OverflowSampleRate_AppliesToRejectedKeys(t *testing.T) {
	a := &AvgSampleWithMin{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	a.currentCounts = make(map[string]float64)
	a.savedSampleRates = make(map[string]int)
	a.haveData = true

	// Fill currentCounts to MaxKeys so "c" is rejected.
	a.GetSampleRate("a")
	a.GetSampleRate("b")
	require.Len(t, a.currentCounts, a.MaxKeys, "currentCounts must be at MaxKeys for 'c' to be rejected")

	assert.Equal(t, 20, a.GetSampleRate("c"))
	assert.NotContains(t, a.currentCounts, "c")
}

func TestAvgSampleWithMin_OverflowSampleRate_UnsetOrNegativeIsUnchanged(t *testing.T) {
	for _, rate := range []int{0, -5} {
		a := &AvgSampleWithMin{
			MaxKeys:            2,
			OverflowSampleRate: rate,
		}
		a.currentCounts = make(map[string]float64)
		a.savedSampleRates = make(map[string]int)
		a.haveData = true

		a.GetSampleRate("a")
		a.GetSampleRate("b")

		assert.Equal(t, 1, a.GetSampleRate("c"))
	}
}

func TestAvgSampleWithMin_OverflowSampleRate_DoesNotAffectTrackedKeys(t *testing.T) {
	a := &AvgSampleWithMin{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	a.currentCounts = make(map[string]float64)
	// "foo" has a computed rate from the previous interval.
	a.savedSampleRates = map[string]int{"foo": 7}
	a.haveData = true

	// Fill currentCounts to MaxKeys with other keys so "foo" is rejected.
	a.GetSampleRate("a")
	a.GetSampleRate("b")
	require.Len(t, a.currentCounts, a.MaxKeys, "currentCounts must be at MaxKeys for 'foo' to be rejected")

	rate := a.GetSampleRate("foo")
	require.NotContains(t, a.currentCounts, "foo", "foo was rejected")
	assert.Equal(t, 7, rate, "a rejected key with a saved rate keeps that rate")
}

func TestAvgSampleWithMin_OverflowSampleRate_ColdStartTakesPrecedence(t *testing.T) {
	a := &AvgSampleWithMin{
		MaxKeys:            2,
		OverflowSampleRate: 20,
		GoalSampleRate:     7,
	}
	a.currentCounts = map[string]float64{"a": 1, "b": 1}
	a.savedSampleRates = make(map[string]int)
	require.False(t, a.haveData, "haveData must be false to simulate cold start")

	assert.Equal(t, 7, a.GetSampleRate("c"))
}

func TestEMASampleRate_OverflowSampleRate_AppliesToRejectedKeys(t *testing.T) {
	e := &EMASampleRate{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	e.savedSampleRates = make(map[string]int)
	e.currentCounts = make(map[string]float64)
	e.haveData = true

	// Fill currentCounts to MaxKeys so "c" is rejected.
	e.GetSampleRateMulti("a", 1)
	e.GetSampleRateMulti("b", 1)
	require.Len(t, e.currentCounts, e.MaxKeys, "currentCounts must be at MaxKeys for 'c' to be rejected")

	assert.Equal(t, 20, e.GetSampleRate("c"))
	assert.NotContains(t, e.currentCounts, "c")
}

func TestEMASampleRate_OverflowSampleRate_UnsetOrNegativeIsUnchanged(t *testing.T) {
	for _, rate := range []int{0, -5} {
		e := &EMASampleRate{
			MaxKeys:            2,
			OverflowSampleRate: rate,
		}
		e.savedSampleRates = make(map[string]int)
		e.currentCounts = make(map[string]float64)
		e.haveData = true

		e.GetSampleRateMulti("a", 1)
		e.GetSampleRateMulti("b", 1)

		assert.Equal(t, 1, e.GetSampleRate("c"))
	}
}

func TestEMASampleRate_OverflowSampleRate_DoesNotAffectTrackedKeys(t *testing.T) {
	e := &EMASampleRate{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	e.currentCounts = make(map[string]float64)
	// "foo" has a computed rate from the previous interval.
	e.savedSampleRates = map[string]int{"foo": 7}
	e.haveData = true

	// Fill currentCounts to MaxKeys with other keys so "foo" is rejected.
	e.GetSampleRate("a")
	e.GetSampleRate("b")
	require.Len(t, e.currentCounts, e.MaxKeys, "currentCounts must be at MaxKeys for 'foo' to be rejected")

	rate := e.GetSampleRate("foo")
	require.NotContains(t, e.currentCounts, "foo", "foo was rejected")
	assert.Equal(t, 7, rate, "a rejected key with a saved rate keeps that rate")
}

func TestEMASampleRate_OverflowSampleRate_ColdStartTakesPrecedence(t *testing.T) {
	e := &EMASampleRate{
		MaxKeys:            2,
		OverflowSampleRate: 20,
		GoalSampleRate:     7,
	}
	e.currentCounts = map[string]float64{"a": 1, "b": 1}
	e.savedSampleRates = make(map[string]int)
	require.False(t, e.haveData, "haveData must be false to simulate cold start")

	assert.Equal(t, 7, e.GetSampleRate("c"))
}

func TestPerKeyThroughput_OverflowSampleRate_AppliesToRejectedKeys(t *testing.T) {
	p := &PerKeyThroughput{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	p.currentCounts = make(map[string]int)
	p.savedSampleRates = make(map[string]int)

	// Fill currentCounts to MaxKeys so "c" is rejected.
	p.GetSampleRate("a")
	p.GetSampleRate("b")
	require.Len(t, p.currentCounts, p.MaxKeys, "currentCounts must be at MaxKeys for 'c' to be rejected")

	assert.Equal(t, 20, p.GetSampleRate("c"))
	assert.NotContains(t, p.currentCounts, "c")
}

func TestPerKeyThroughput_OverflowSampleRate_UnsetOrNegativeIsUnchanged(t *testing.T) {
	for _, rate := range []int{0, -5} {
		p := &PerKeyThroughput{
			MaxKeys:            2,
			OverflowSampleRate: rate,
		}
		p.currentCounts = make(map[string]int)
		p.savedSampleRates = make(map[string]int)

		p.GetSampleRate("a")
		p.GetSampleRate("b")

		assert.Equal(t, 1, p.GetSampleRate("c"))
	}
}

func TestPerKeyThroughput_OverflowSampleRate_DoesNotAffectTrackedKeys(t *testing.T) {
	p := &PerKeyThroughput{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	p.currentCounts = make(map[string]int)
	// "foo" has a computed rate from the previous interval.
	p.savedSampleRates = map[string]int{"foo": 7}

	// Fill currentCounts to MaxKeys with other keys so "foo" is rejected.
	p.GetSampleRate("a")
	p.GetSampleRate("b")
	require.Len(t, p.currentCounts, p.MaxKeys, "currentCounts must be at MaxKeys for 'foo' to be rejected")

	rate := p.GetSampleRate("foo")
	require.NotContains(t, p.currentCounts, "foo", "foo was rejected")
	assert.Equal(t, 7, rate, "a rejected key with a saved rate keeps that rate")
}

func TestTotalThroughput_OverflowSampleRate_AppliesToRejectedKeys(t *testing.T) {
	tt := &TotalThroughput{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	tt.currentCounts = make(map[string]int)
	tt.savedSampleRates = make(map[string]int)

	// Fill currentCounts to MaxKeys so "c" is rejected.
	tt.GetSampleRate("a")
	tt.GetSampleRate("b")
	require.Len(t, tt.currentCounts, tt.MaxKeys, "currentCounts must be at MaxKeys for 'c' to be rejected")

	assert.Equal(t, 20, tt.GetSampleRate("c"))
	assert.NotContains(t, tt.currentCounts, "c")
}

func TestTotalThroughput_OverflowSampleRate_UnsetOrNegativeIsUnchanged(t *testing.T) {
	for _, rate := range []int{0, -5} {
		tt := &TotalThroughput{
			MaxKeys:            2,
			OverflowSampleRate: rate,
		}
		tt.currentCounts = make(map[string]int)
		tt.savedSampleRates = make(map[string]int)

		tt.GetSampleRate("a")
		tt.GetSampleRate("b")

		assert.Equal(t, 1, tt.GetSampleRate("c"))
	}
}

func TestTotalThroughput_OverflowSampleRate_DoesNotAffectTrackedKeys(t *testing.T) {
	tt := &TotalThroughput{
		MaxKeys:            2,
		OverflowSampleRate: 20,
	}
	tt.currentCounts = make(map[string]int)
	// "foo" has a computed rate from the previous interval.
	tt.savedSampleRates = map[string]int{"foo": 7}

	// Fill currentCounts to MaxKeys with other keys so "foo" is rejected.
	tt.GetSampleRate("a")
	tt.GetSampleRate("b")
	require.Len(t, tt.currentCounts, tt.MaxKeys, "currentCounts must be at MaxKeys for 'foo' to be rejected")

	rate := tt.GetSampleRate("foo")
	require.NotContains(t, tt.currentCounts, "foo", "foo was rejected")
	assert.Equal(t, 7, rate, "a rejected key with a saved rate keeps that rate")
}
