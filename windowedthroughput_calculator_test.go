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

func TestWindowedThroughputCalculator_CopiesCounts(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	counts := map[string]float64{"k": 20}
	c.Update(counts)
	// Mutating the caller's map after Update must not disturb the stored bucket.
	counts["k"] = 999
	counts["injected"] = 500
	assert.Equal(t, 2, c.Rates()["k"], "stored bucket must be a copy, unaffected by later mutation")
	_, injected := c.Rates()["injected"]
	assert.False(t, injected, "a key added to the caller's map after Update must not appear")
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

func TestWindowedThroughputCalculator_SaveLoadRoundTrip(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	c.Update(map[string]float64{"a": 20})
	c.Update(map[string]float64{"a": 10, "b": 5})
	c.Update(map[string]float64{}) // empty mid-window tick
	c.Update(map[string]float64{"a": 50})

	state, err := c.SaveState()
	assert.NoError(t, err)

	loaded := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	assert.NoError(t, loaded.LoadState(state))
	assert.Equal(t, c.Rates(), loaded.Rates(), "rates must match immediately after a save/load handoff")

	// Feed both identical further ticks past the window edge, proving eviction
	// order (oldest-first) was preserved through the round trip.
	for i := 0; i < 5; i++ {
		next := map[string]float64{"a": float64(10 + i), "b": float64(i)}
		c.Update(next)
		loaded.Update(next)
		assert.Equal(t, c.Rates(), loaded.Rates(), "rates must stay identical after further identical ticks")
	}
}

func TestWindowedThroughputCalculator_LoadIntoShorterWindow(t *testing.T) {
	source := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	ticks := []map[string]float64{
		{"t0": 5},
		{"t1": 10},
		{"t2": 15},
		{"t3": 20},
		{"t4": 25},
	}
	for _, tick := range ticks {
		source.Update(tick)
	}

	state, err := source.SaveState()
	assert.NoError(t, err)

	shorter := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 3 * time.Second}
	assert.NoError(t, shorter.LoadState(state))

	want := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 3 * time.Second}
	for _, tick := range ticks[len(ticks)-3:] {
		want.Update(tick)
	}

	assert.Equal(t, want.Rates(), shorter.Rates(), "loading into a shorter ring must keep only the newest ticks")
}

func TestWindowedThroughputCalculator_LoadRejectsGarbage(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	c.Update(map[string]float64{"a": 20})
	before := c.Rates()

	err := c.LoadState([]byte("not json"))
	assert.Error(t, err)
	assert.Equal(t, before, c.Rates(), "a failed load must leave prior state untouched")
}

func TestWindowedThroughputCalculator_SaveFreshRoundTripsEmpty(t *testing.T) {
	c := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}

	state, err := c.SaveState()
	assert.NoError(t, err)

	loaded := &WindowedThroughputCalculator{GoalThroughputPerSec: 2, UpdateFrequency: time.Second, LookbackFrequency: 5 * time.Second}
	assert.NoError(t, loaded.LoadState(state))
	assert.Empty(t, loaded.Rates())
}

// The calculator is the computation core of WindowedThroughput. Feeding both
// the same sequence of per-tick counts must produce identical rate tables.
func TestWindowedThroughputCalculator_MatchesSampler(t *testing.T) {
	indexGenerator := &TestIndexGenerator{}
	sampler := WindowedThroughput{
		UpdateFrequencyDuration:   time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      2,
		indexGenerator:            indexGenerator,
		countList:                 NewUnboundedBlockList(),
	}
	sampler.savedSampleRates = make(map[string]int)

	c := &WindowedThroughputCalculator{
		GoalThroughputPerSec: 2,
		UpdateFrequency:      time.Second,
		LookbackFrequency:    5 * time.Second,
	}

	ticks := []map[string]int{
		{"a": 20},
		{"a": 10, "b": 5},
		{},
		{"a": 50, "b": 5, "c": 1},
		{},
		{"c": 40},
	}
	for _, tick := range ticks {
		calcInput := make(map[string]float64, len(tick))
		for key, count := range tick {
			for i := 0; i < count; i++ {
				sampler.GetSampleRate(key)
			}
			calcInput[key] = float64(count)
		}
		indexGenerator.CurrentIndex++
		sampler.updateMaps()
		c.Update(calcInput)
		assert.Equal(t, sampler.savedSampleRates, c.Rates(), "calculator must match the timer sampler over identical inputs")
	}
}
