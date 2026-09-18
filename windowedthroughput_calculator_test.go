package dynsampler

import (
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
