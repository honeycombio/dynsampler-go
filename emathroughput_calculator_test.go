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
	e.movingAverage = make(map[string]float64)
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
