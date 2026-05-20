package dynsampler

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkWindowedThroughputContention measures GetSampleRateMulti throughput
// across concurrency levels with 1500 unique keys. The ns/op cost rising with
// parallelism indicates how much lock contention reduces the number of events
// the sampler can observe per second.
func BenchmarkWindowedThroughputContention(b *testing.B) {
	const numKeys = 1500
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	newSampler := func() (*WindowedThroughput, func()) {
		s := &WindowedThroughput{
			GoalThroughputPerSec:      100,
			LookbackFrequencyDuration: 5 * time.Second,
			UpdateFrequencyDuration:   1 * time.Second,
		}
		s.Start()
		return s, func() { s.Stop() }
	}

	b.Run("goroutines=1", func(b *testing.B) {
		s, stop := newSampler()
		defer stop()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.GetSampleRateMulti(keys[i%numKeys], 1)
		}
	})

	for _, parallelism := range []int{4, 16, 64} {
		parallelism := parallelism
		b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
			s, stop := newSampler()
			defer stop()
			b.SetParallelism(parallelism)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					s.GetSampleRateMulti(keys[i%numKeys], 1)
					i++
				}
			})
		})
	}
}

// BenchmarkEMAThroughputContention measures GetSampleRateMulti throughput
// across concurrency levels with 1500 unique keys.
func BenchmarkEMAThroughputContention(b *testing.B) {
	const numKeys = 1500
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	newSampler := func() (*EMAThroughput, func()) {
		e := &EMAThroughput{
			GoalThroughputPerSec: 100,
			AdjustmentInterval:   5 * time.Second,
			Weight:               0.5,
			AgeOutValue:          0.5,
		}
		e.Start()
		return e, func() { e.Stop() }
	}

	b.Run("goroutines=1", func(b *testing.B) {
		e, stop := newSampler()
		defer stop()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			e.GetSampleRateMulti(keys[i%numKeys], 1)
		}
	})

	for _, parallelism := range []int{4, 16, 64} {
		parallelism := parallelism
		b.Run(fmt.Sprintf("parallelism=%d", parallelism), func(b *testing.B) {
			e, stop := newSampler()
			defer stop()
			b.SetParallelism(parallelism)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					e.GetSampleRateMulti(keys[i%numKeys], 1)
					i++
				}
			})
		})
	}
}
