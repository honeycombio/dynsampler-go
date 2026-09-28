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
