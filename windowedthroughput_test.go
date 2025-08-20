package dynsampler

import (
	"fmt"
	"sync"
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
)

func TestWindowedThroughputSetGoalThroughputPerSec(t *testing.T) {
	s := &WindowedThroughput{
		GoalThroughputPerSec: 100.0,
	}

	// Test SetGoalThroughputPerSec method
	s.SetGoalThroughputPerSec(200.5)
	assert.Equal(t, 200.5, s.GoalThroughputPerSec)

	// Test that invalid values are ignored
	s.SetGoalThroughputPerSec(0)
	assert.Equal(t, 200.5, s.GoalThroughputPerSec)

	s.SetGoalThroughputPerSec(-10.5)
	assert.Equal(t, 200.5, s.GoalThroughputPerSec)
}

type TestIndexGenerator struct {
	CurrentIndex int64
}

func (g *TestIndexGenerator) GetCurrentIndex() int64 {
	return g.CurrentIndex
}

func (g *TestIndexGenerator) DurationToIndexes(duration time.Duration) int64 {
	return int64(duration.Seconds())
}

func TestHappyPath(t *testing.T) {
	indexGenerator := &TestIndexGenerator{}
	sampler := WindowedThroughput{
		UpdateFrequencyDuration:   1 * time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      2,
		indexGenerator:            indexGenerator,
		countList:                 NewUnboundedBlockList(),
	}
	key := "test_key"

	// Time 0: 20 traces seen.
	for i := 0; i < 20; i++ {
		assert.Equal(t, 0, sampler.GetSampleRate(key))
	}
	indexGenerator.CurrentIndex += 1
	sampler.updateMaps()

	// Time 1: 10 traces seen
	for i := 0; i < 10; i++ {
		assert.Equal(t, 2, sampler.GetSampleRate(key))
	}
	indexGenerator.CurrentIndex += 1
	sampler.updateMaps()

	// Time 2: 50 traces seen
	for i := 0; i < 50; i++ {
		assert.Equal(t, 3, sampler.GetSampleRate(key))
	}
	indexGenerator.CurrentIndex += 1
	sampler.updateMaps()

	// Time 3 & 4 & 5: 0 traces seen
	for i := 0; i < 3; i++ {
		indexGenerator.CurrentIndex += 1
		sampler.updateMaps()
	}

	// Time 6: 40 traces seen.
	for i := 0; i < 40; i++ {
		// This should look back from time (0 - 5]
		assert.Equal(t, 6, sampler.GetSampleRate(key))
	}
	indexGenerator.CurrentIndex += 1
	sampler.updateMaps()

	// Time 7: 5 traces seen.
	for i := 0; i < 5; i++ {
		// This should look back from time (1 - 6]
		assert.Equal(t, 9, sampler.GetSampleRate(key))
	}
}

func TestDropsOldBlocks(t *testing.T) {
	indexGenerator := &TestIndexGenerator{}
	sampler := WindowedThroughput{
		UpdateFrequencyDuration:   1 * time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      2,
		indexGenerator:            indexGenerator,
		countList:                 NewUnboundedBlockList(),
	}
	key := "test_key"

	// Time 0: 20 traces seen.
	for i := 0; i < 20; i++ {
		assert.Equal(t, 0, sampler.GetSampleRate(key))
	}

	for i := 0; i < 7; i++ {
		indexGenerator.CurrentIndex += 1
		sampler.updateMaps()
	}

	// Time 6: 20 traces seen.
	for i := 0; i < 20; i++ {
		assert.Equal(t, 0, sampler.GetSampleRate(key))
	}
}

func TestSetsDefaultsCorrectly(t *testing.T) {
	sampler1 := WindowedThroughput{}
	sampler1.Start()

	assert.Equal(t, time.Second, sampler1.UpdateFrequencyDuration)
	assert.Equal(t, 30*time.Second, sampler1.LookbackFrequencyDuration)

	sampler2 := WindowedThroughput{
		UpdateFrequencyDuration:   5 * time.Second,
		LookbackFrequencyDuration: 18 * time.Second,
	}
	sampler2.Start()
	assert.Equal(t, 5*time.Second, sampler2.UpdateFrequencyDuration)
	assert.Equal(t, 15*time.Second, sampler2.LookbackFrequencyDuration)
}

// TestWindowedThroughputConcurrency tests that GetSampleRateMulti is safe to call concurrently
func TestWindowedThroughputConcurrency(t *testing.T) {
	sampler := &WindowedThroughput{
		UpdateFrequencyDuration:   1 * time.Second,
		LookbackFrequencyDuration: 5 * time.Second,
		GoalThroughputPerSec:      100,
		MaxKeys:                   1000,
	}

	err := sampler.Start()
	if err != nil {
		t.Fatalf("Failed to start sampler: %v", err)
	}
	defer sampler.Stop()

	const numGoroutines = 10
	const iterationsPerGoroutine = 100

	var wg sync.WaitGroup

	// Test concurrent GetSampleRateMulti calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := 0; j < iterationsPerGoroutine; j++ {
				key := fmt.Sprintf("test-key-%d-%d", goroutineID, j%10)
				count := (j % 5) + 1

				// This should not cause race conditions
				rate := sampler.GetSampleRateMulti(key, count)

				// Basic sanity check
				if rate < 0 {
					t.Errorf("Got negative sample rate: %d", rate)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify metrics were updated (basic smoke test)
	metrics := sampler.GetMetrics("test")
	if metrics["testrequest_count"] == 0 {
		t.Error("Expected request count to be greater than 0")
	}
	if metrics["testevent_count"] == 0 {
		t.Error("Expected event count to be greater than 0")
	}
}
