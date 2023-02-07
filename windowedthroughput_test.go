package dynsampler

import (
	"testing"

	"time"

	"github.com/stretchr/testify/assert"
)

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
