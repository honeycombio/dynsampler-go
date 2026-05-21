package dynsampler

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWindowedThroughputMetricRatePerSecond runs two scenarios — single caller
// and 100 concurrent goroutines — each with 1500 unique keys under steady-state
// traffic for 5 seconds. It logs the per-second delta of request_count and
// event_count for each scenario so throughput differences are visible over time.
//
// NOTE: meaningful only with GOMAXPROCS >= 2.
func TestWindowedThroughputMetricRatePerSecond(t *testing.T) {
	const (
		numKeys       = 1500
		numGoroutines = 10
		runSeconds    = 30
	)

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	type snapshot struct{ requests, events int64 }

	// runScenario drives numWorkers goroutines against a fresh sampler for
	// runSeconds seconds. It samples metrics once per second and returns the
	// per-second deltas.
	runScenario := func(numWorkers int) []snapshot {
		s := &WindowedThroughput{
			GoalThroughputPerSec:      1000,
			LookbackFrequencyDuration: 5 * time.Second,
			UpdateFrequencyDuration:   1 * time.Second,
		}

		//	s := &EMAThroughput{
		//		GoalThroughputPerSec: 1000,
		//		AdjustmentInterval:   1 * time.Second,
		//		InitialSampleRate:    1,
		//		MaxKeys:              2000,
		//	}
		s.Start()
		// only count events after the first updateMaps tick fires
		warmupEnd := time.Now().Add(s.UpdateFrequencyDuration + 50*time.Millisecond)

		type result struct {
			numTries int
			numKept  int
		}
		results := make(chan result, numWorkers)
		done := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < numWorkers; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				var numTries, numKept int
				for i := 0; ; i++ {
					select {
					case <-done:
						results <- result{
							numTries: numTries,
							numKept:  numKept,
						}
						return
					default:
						count := int(math.Log(float64(i + 1)))
						rate := s.GetSampleRateMulti(keys[i%numKeys], count)
						if time.Now().After(warmupEnd) {
							numTries++
							if rate == 0 {
								rate = 1
							}
							if rand.Intn(rate) == 0 {
								numKept += count
							}
						}
					}
				}
			}(g)
		}

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var snaps []snapshot
		var prevReq, prevEvt int64
		for range ticker.C {
			m := s.GetMetrics("wt_")
			req := m["wt_request_count"] - prevReq
			evt := m["wt_event_count"] - prevEvt
			prevReq = m["wt_request_count"]
			prevEvt = m["wt_event_count"]
			snaps = append(snaps, snapshot{req, evt})
			if len(snaps) == runSeconds {
				break
			}
		}
		close(done)
		wg.Wait()
		close(results)

		var totalTries, totalKept int
		for r := range results {
			totalTries += r.numTries
			totalKept += r.numKept
		}

		fmt.Println("totalTries", totalTries, "totalKept", totalKept)
		fmt.Println("ratio", totalKept/runSeconds)
		return snaps
	}

	t.Log("--- single caller (1 goroutine, 1500 keys) ---")
	singleSnaps := runScenario(1)
	for i, s := range singleSnaps {
		t.Logf("  sec %d: request_count/s=%d  event_count/s=%d", i+1, s.requests, s.events)
	}

	t.Logf("--- concurrent (%d goroutines, 1500 keys) ---", numGoroutines)
	concurrentSnaps := runScenario(numGoroutines)
	for i, s := range concurrentSnaps {
		t.Logf("  sec %d: request_count/s=%d  event_count/s=%d", i+1, s.requests, s.events)
	}

	require.Len(t, singleSnaps, runSeconds)
	require.Len(t, concurrentSnaps, runSeconds)
}
