package dynsampler_test

import (
	"math"
	"testing"
	"time"

	"github.com/honeycombio/dynsampler-go"
)

// If given consistent data, the samplers very quickly settle to their target
// rates and we can expect exact results. This test specifically hands different
// samplers identical data each time and expects them to find the right values
// quickly. This is a slightly higher-level test that only depends on the public
// interface of samplers.
func TestGenericSamplerBehavior(t *testing.T) {
	tests := []struct {
		name    string
		sampler dynsampler.Sampler
		want    []int
	}{
		{"AvgSampleRate",
			&dynsampler.AvgSampleRate{
				ClearFrequencySec: 1,
			}, []int{1, 1, 1, 1, 2, 4, 9, 21},
		},
		{"AvgSampleWithMin",
			&dynsampler.AvgSampleWithMin{
				ClearFrequencySec: 1,
			}, []int{1, 1, 1, 1, 1, 2, 4, 9, 21},
		},
		{"EMASampler",
			&dynsampler.EMASampleRate{
				AdjustmentInterval: 1,
			}, []int{1, 1, 1, 1, 2, 4, 9, 21},
		},
		{"OnlyOnce",
			&dynsampler.OnlyOnce{
				ClearFrequencySec: 1,
			}, []int{1, 1, 1, 1, 1, 1, 1, 1},
		},
		{"PerKeyThroughput",
			&dynsampler.PerKeyThroughput{
				ClearFrequencySec: 1,
			}, []int{1, 1, 1, 2, 8, 24, 72, 218},
		},
		{"TotalThroughput",
			&dynsampler.TotalThroughput{
				ClearFrequencySec:    1,
				GoalThroughputPerSec: 5,
			}, []int{1, 3, 9, 27, 81, 243, 729, 2187},
		},
		{"WindowedThroughput",
			&dynsampler.WindowedThroughput{
				UpdateFrequencyDuration:   100 * time.Millisecond,
				LookbackFrequencyDuration: 1 * time.Second,
			}, []int{1, 1, 1, 2, 6, 19, 58, 174},
		},
		{"EMAThroughput",
			&dynsampler.EMAThroughput{
				AdjustmentInterval:   1 * time.Second,
				GoalThroughputPerSec: 100,
			}, []int{1, 1, 2, 3, 6, 13, 31, 77},
		},
		{"EMAThroughputLowTraffic",
			&dynsampler.EMAThroughput{
				AdjustmentInterval:   1 * time.Second,
				GoalThroughputPerSec: 100000,
			}, []int{1, 1, 1, 1, 1, 1, 1, 1},
		},
	}

	const (
		NRounds = 8
	)

	keys := []string{
		"arm", "bag", "bed", "bee", "box", "boy", "cat", "cow", "cup", "dog",
		"ear", "egg", "eye", "fly", "gun", "hat", "key", "leg", "lip", "map",
		"net", "nut", "pen", "pig", "pin", "pot", "rat", "rod", "sun", "toe",
	}

	for i := range tests {
		tt := tests[i]
		t.Run(tt.name, func(t *testing.T) {
			// we can run all of these at once
			t.Parallel()
			s := tt.sampler

			err := s.Start()
			if err != nil {
				t.Errorf("%v starting sampler", err)
			}

			nkeys := len(tt.want)
			results := make([]int, nkeys)
			for round := 0; round < NRounds; round++ {
				for k := 0; k < nkeys; k++ {
					key := keys[k%nkeys]
					nsamples := int(math.Pow(3, float64(k%9))) // up to 6K
					results[k] = s.GetSampleRateMulti(key, nsamples)
				}
				time.Sleep(1010 * time.Millisecond) // just over the 1 second clear time
			}
			s.Stop()

			for k := 0; k < nkeys; k++ {
				// if !isCloseTo(tt.want[k], results[k]) {
				if tt.want[k] != results[k] {
					t.Errorf("results not = for key %s (%d) want %d, got %d\n", keys[k], k, tt.want[k], results[k])
				}
			}
		})
	}
}
