package dynsampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// These tests pin today's behaviour when a timer sampler's MaxKeys cap is
// reached and a brand-new key shows up: the key is not recorded, and its
// sample rate keeps everything (never dropped). They must keep passing
// unmodified once OverflowSampleRate is added, proving the zero-value
// default is unchanged.

func TestEMAThroughputCharacterization_OverflowKeepsEverything(t *testing.T) {
	e := &EMAThroughput{
		GoalThroughputPerSec: 10,
		AdjustmentInterval:   time.Second,
		Weight:               0.2,
		AgeOutValue:          0.2,
		MaxKeys:              2,
	}
	e.savedSampleRates = make(map[string]int)
	e.currentCounts = make(map[string]float64)
	// Simulate that we already have historical data, so lookups fall through
	// to the savedSampleRates/currentCounts path instead of InitialSampleRate.
	e.haveData = true

	// Fill currentCounts to MaxKeys via GetSampleRateMulti.
	e.GetSampleRateMulti("a", 1)
	e.GetSampleRateMulti("b", 1)

	// A brand-new third key arrives while at the cap: not recorded, and
	// returns 1 (keep everything).
	assert.Equal(t, 1, e.GetSampleRate("c"))
	assert.NotContains(t, e.currentCounts, "c")
}

func TestWindowedThroughputCharacterization_OverflowKeepsEverything(t *testing.T) {
	sampler := &WindowedThroughput{
		UpdateFrequencyDuration:   1 * time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      2,
		indexGenerator:            &TestIndexGenerator{},
		countList:                 NewBoundedBlockList(2),
	}

	// Fill the bounded list to capacity.
	sampler.GetSampleRate("a")
	sampler.GetSampleRate("b")

	// A brand-new third key arrives while at the cap: keeps everything (0).
	assert.Equal(t, 0, sampler.GetSampleRate("c"))
}

func TestAvgSampleRateCharacterization_OverflowKeepsEverything(t *testing.T) {
	a := &AvgSampleRate{
		MaxKeys: 2,
	}
	a.currentCounts = make(map[string]float64)
	a.savedSampleRates = make(map[string]int)
	// Simulate that we already have historical data, so lookups fall through
	// to the savedSampleRates/currentCounts path instead of GoalSampleRate.
	a.haveData = true

	// Fill currentCounts to MaxKeys via GetSampleRate.
	a.GetSampleRate("a")
	a.GetSampleRate("b")

	// A brand-new third key arrives while at the cap: not recorded, and
	// returns 1 (keep everything).
	assert.Equal(t, 1, a.GetSampleRate("c"))
	assert.NotContains(t, a.currentCounts, "c")
}

func TestAvgSampleWithMinCharacterization_OverflowKeepsEverything(t *testing.T) {
	a := &AvgSampleWithMin{
		MaxKeys: 2,
	}
	a.currentCounts = make(map[string]float64)
	a.savedSampleRates = make(map[string]int)
	// Simulate that we already have historical data, so lookups fall through
	// to the savedSampleRates/currentCounts path instead of GoalSampleRate.
	a.haveData = true

	// Fill currentCounts to MaxKeys via GetSampleRate.
	a.GetSampleRate("a")
	a.GetSampleRate("b")

	// A brand-new third key arrives while at the cap: not recorded, and
	// returns 1 (keep everything).
	assert.Equal(t, 1, a.GetSampleRate("c"))
	assert.NotContains(t, a.currentCounts, "c")
}

func TestEMASampleRateCharacterization_OverflowKeepsEverything(t *testing.T) {
	e := &EMASampleRate{
		MaxKeys: 2,
	}
	e.savedSampleRates = make(map[string]int)
	e.currentCounts = make(map[string]float64)
	// Simulate that we already have historical data, so lookups fall through
	// to the savedSampleRates/currentCounts path instead of GoalSampleRate.
	e.haveData = true

	// Fill currentCounts to MaxKeys via GetSampleRateMulti.
	e.GetSampleRateMulti("a", 1)
	e.GetSampleRateMulti("b", 1)

	// A brand-new third key arrives while at the cap: not recorded, and
	// returns 1 (keep everything).
	assert.Equal(t, 1, e.GetSampleRate("c"))
	assert.NotContains(t, e.currentCounts, "c")
}

func TestPerKeyThroughputCharacterization_OverflowKeepsEverything(t *testing.T) {
	p := &PerKeyThroughput{
		MaxKeys: 2,
	}
	p.currentCounts = make(map[string]int)
	p.savedSampleRates = make(map[string]int)

	// Fill currentCounts to MaxKeys via GetSampleRate.
	p.GetSampleRate("a")
	p.GetSampleRate("b")

	// A brand-new third key arrives while at the cap: not recorded, and
	// returns 1 (keep everything).
	assert.Equal(t, 1, p.GetSampleRate("c"))
	assert.NotContains(t, p.currentCounts, "c")
}

func TestTotalThroughputCharacterization_OverflowKeepsEverything(t *testing.T) {
	tt := &TotalThroughput{
		MaxKeys: 2,
	}
	tt.currentCounts = make(map[string]int)
	tt.savedSampleRates = make(map[string]int)

	// Fill currentCounts to MaxKeys via GetSampleRate.
	tt.GetSampleRate("a")
	tt.GetSampleRate("b")

	// A brand-new third key arrives while at the cap: not recorded, and
	// returns 1 (keep everything).
	assert.Equal(t, 1, tt.GetSampleRate("c"))
	assert.NotContains(t, tt.currentCounts, "c")
}
