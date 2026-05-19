package dynsampler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		runSeconds    = 5
	)

	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("key%d", i)
	}

	type snapshot struct{ requests, events int64 }

	// runScenario drives numWorkers goroutines against a fresh sampler for
	// runSeconds seconds. It samples metrics once per second and returns the
	// per-second deltas.
	runScenario := func(numWorkers int, list BlockList) []snapshot {
		s := &WindowedThroughput{
			GoalThroughputPerSec:      10,
			LookbackFrequencyDuration: 5 * time.Second,
			UpdateFrequencyDuration:   1 * time.Second,
			indexGenerator:            &TestIndexGenerator{CurrentIndex: 1},
			countList:                 list,
		}

		done := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < numWorkers; g++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-done:
						return
					default:
						s.GetSampleRateMulti(keys[i%numKeys], 1)
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
		return snaps
	}

	t.Log("--- single caller, unsharded (1 goroutine, 1500 keys) ---")
	singleSnaps := runScenario(1, NewUnboundedBlockList())
	for i, s := range singleSnaps {
		t.Logf("  sec %d: request_count/s=%d  event_count/s=%d", i+1, s.requests, s.events)
	}

	t.Logf("--- concurrent, unsharded (%d goroutines, 1500 keys) ---", numGoroutines)
	unshardedSnaps := runScenario(numGoroutines, NewUnboundedBlockList())
	for i, s := range unshardedSnaps {
		t.Logf("  sec %d: request_count/s=%d  event_count/s=%d", i+1, s.requests, s.events)
	}

	t.Logf("--- concurrent, sharded 32 (%d goroutines, 1500 keys) ---", numGoroutines)
	shardedSnaps := runScenario(numGoroutines, NewShardedBlockList(32, 0))
	for i, s := range shardedSnaps {
		t.Logf("  sec %d: request_count/s=%d  event_count/s=%d", i+1, s.requests, s.events)
	}

	require.Len(t, singleSnaps, runSeconds)
	require.Len(t, unshardedSnaps, runSeconds)
	require.Len(t, shardedSnaps, runSeconds)

	// requestCount and eventCount are updated by two separate atomic ops, so a
	// concurrent GetMetrics read can observe them a few counts apart. Assert
	// they stay within 0.01% of each other per second — any larger gap would
	// indicate a structural bug, not a sampling artifact.
	const tolerance = 0.0001
	for _, snaps := range [][]snapshot{singleSnaps, unshardedSnaps, shardedSnaps} {
		for i, s := range snaps {
			if s.requests == 0 {
				continue
			}
			diff := s.requests - s.events
			if diff < 0 {
				diff = -diff
			}
			assert.LessOrEqual(t, float64(diff)/float64(s.requests), tolerance,
				"sec %d: request/event count diverged beyond tolerance", i+1)
		}
	}
}

// --- WindowedThroughput: rate staleness / lock starvation ---

// TestWindowedThroughputRateStaleness shows that even under steady-state traffic,
// events in the current index are always processed at rates computed from the
// previous cycle's lookback window. savedSampleRates is a snapshot that only
// updates when updateMaps runs, so there is always a one-cycle lag.
func TestWindowedThroughputRateStaleness(t *testing.T) {
	indexGen := &TestIndexGenerator{CurrentIndex: 0}
	s := &WindowedThroughput{
		GoalThroughputPerSec:      10,
		LookbackFrequencyDuration: 5 * time.Second,
		UpdateFrequencyDuration:   1 * time.Second,
		indexGenerator:            indexGen,
		countList:                 NewUnboundedBlockList(),
	}

	// Fill the 5-index lookback window with 50 events/index.
	// totalGoalThroughput = 10*5 = 50; throughputPerKey = 50/1 = 50
	// rate = int(max(1, 250/50)) = 5
	for idx := int64(0); idx < 5; idx++ {
		indexGen.CurrentIndex = idx
		for j := 0; j < 50; j++ {
			s.GetSampleRateMulti("key", 1)
		}
		indexGen.CurrentIndex = idx + 1
		s.updateMaps()
	}

	rates, _ := s.savedSampleRates.Load().(map[string]int)
	steadyRate := rates["key"]
	require.Equal(t, 5, steadyRate)

	// Steady-state traffic continues at index 5: same 50 events/index as before.
	// savedSampleRates still holds the rate from the previous cycle; the current
	// index's events have not yet been counted toward any rate recalculation.
	indexGen.CurrentIndex = 5
	for j := 0; j < 50; j++ {
		rate := s.GetSampleRateMulti("key", 1)
		assert.Equal(t, steadyRate, rate,
			"steady-state events use rate from the previous cycle, not the current one")
	}

	// Only after updateMaps runs does the window advance. In steady state the
	// rate is unchanged (same traffic → same window count).
	indexGen.CurrentIndex = 6
	s.updateMaps()
	rates2, _ := s.savedSampleRates.Load().(map[string]int)
	assert.Equal(t, steadyRate, rates2["key"],
		"rate unchanged in steady state after update")
}

// TestWindowedThroughputConcurrentUpdateDelay shows that with 1500 keys in the
// lookback window, AggregateCounts holds b.lock over ~O(7500) map entries.
// Goroutines contending for t.lock pile up during that window and flush
// through at the stale rate before updateMaps can write new rates.
//
// NOTE: meaningful only with GOMAXPROCS >= 2 (parallel goroutines required).
func TestWindowedThroughputConcurrentUpdateDelay(t *testing.T) {
	const numKeys = 1500
	indexGen := &TestIndexGenerator{CurrentIndex: 1}
	s := &WindowedThroughput{
		GoalThroughputPerSec:      10,
		LookbackFrequencyDuration: 5 * time.Second,
		UpdateFrequencyDuration:   1 * time.Second,
		indexGenerator:            indexGen,
		countList:                 NewUnboundedBlockList(),
	}

	// Seed 1500 keys × 5 blocks so AggregateCounts iterates ~7500 entries,
	// holding b.lock long enough for goroutines to accumulate at t.lock.
	for idx := int64(-5); idx < 0; idx++ {
		for k := 0; k < numKeys; k++ {
			s.countList.IncrementKey(fmt.Sprintf("key%d", k), idx, 50)
		}
	}

	// Inject a sentinel rate. Reads returning 999 are stale; anything else is post-update.
	s.savedSampleRates.Store(map[string]int{"key0": 999})

	var staleCount, updatedCount int64
	start := make(chan struct{})
	done := make(chan struct{})
	var wg sync.WaitGroup

	const numGoroutines = 100
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				select {
				case <-done:
					return
				default:
					if s.GetSampleRateMulti("key0", 1) == 999 {
						atomic.AddInt64(&staleCount, 1)
					} else {
						atomic.AddInt64(&updatedCount, 1)
					}
				}
			}
		}()
	}

	// Release all goroutines at once to maximise contention on t.lock.
	// updateMaps must compete to write savedSampleRates after AggregateCounts finishes.
	close(start)
	s.updateMaps()
	close(done)
	wg.Wait()

	stale := atomic.LoadInt64(&staleCount)
	updated := atomic.LoadInt64(&updatedCount)
	t.Logf("stale-rate events: %d, updated-rate events: %d", stale, updated)

	// Both buckets must be non-zero: goroutines were processed at the old rate
	// while blocked waiting for b.lock (held by AggregateCounts), and then at
	// the new rate once updateMaps completed its write.
	assert.Greater(t, stale, int64(0),
		"goroutines should see stale rates while t.lock/b.lock are contended")
	assert.Greater(t, updated, int64(0),
		"goroutines should see updated rates after updateMaps writes")
	finalRates, _ := s.savedSampleRates.Load().(map[string]int)
	assert.NotEqual(t, 999, finalRates["key0"])
}

// --- EMAThroughput: update suppression under load ---

// TestEMAThroughputRatesStaleWhenUpdateBails shows that when updating=true —
// set by the background goroutine at the start of every updateMaps execution —
// any concurrent updateMaps call bails immediately without processing
// currentCounts. Under steady-state traffic this causes one interval's worth
// of counts to accumulate into the next interval, temporarily over-adjusting
// the rate when the deferred update finally runs.
func TestEMAThroughputRatesStaleWhenUpdateBails(t *testing.T) {
	// Use AdjustmentInterval=1s so goalCount=10*1=10, giving rate>1 at typical counts.
	// We call updateMaps directly (no Start) so the background ticker cannot interfere.
	e := &EMAThroughput{
		GoalThroughputPerSec: 10,
		AdjustmentInterval:   1 * time.Second,
		Weight:               0.5,
		AgeOutValue:          0.5,
		movingAverage:        make(map[string]float64),
		savedSampleRates:     make(map[string]int),
	}

	// Converge EMA to steady state at 100 events/s.
	// After convergence: EMA≈100, logSum=2, goalRatio=5, rate=ceil(100/10)=10.
	for i := 0; i < 50; i++ {
		e.currentCounts = map[string]float64{"key": 100}
		e.updateMaps()
	}
	steadyRate := e.savedSampleRates["key"]
	require.Equal(t, 10, steadyRate)

	// Simulate the background goroutine being inside updateMaps: set updating=true.
	// In production this guard (emathroughput.go:199) prevents concurrent runs;
	// a burst-triggered call arriving while updateMaps is already running hits
	// this check and returns without touching savedSampleRates or movingAverage.
	e.lock.Lock()
	e.updating = true
	e.currentCounts = map[string]float64{"key": 100} // steady-state interval
	e.lock.Unlock()

	e.updateMaps() // bails: updating=true, currentCounts not processed

	assert.Equal(t, steadyRate, e.savedSampleRates["key"],
		"rate unchanged after bail: steady-state interval's counts not incorporated")

	// The unprocessed counts stay in currentCounts. The next real interval
	// adds its own steady-state traffic on top, doubling the effective window count.
	e.lock.Lock()
	e.currentCounts["key"] += 100 // next interval's 100 events accumulate with the missed 100
	e.updating = false
	e.lock.Unlock()

	// Real update sees 200 accumulated events instead of the expected 100.
	// EMA: 0.5*100 + 0.5*200 = 150 → rate = ceil(150/10) = 15.
	e.updateMaps()

	rateAfterAccumulation := e.savedSampleRates["key"]
	assert.Greater(t, rateAfterAccumulation, steadyRate,
		"bail caused accumulation: deferred update sees two intervals of traffic at once, temporarily over-adjusting the rate")
	t.Logf("steady-state rate: %d, rate after accumulated update: %d",
		steadyRate, rateAfterAccumulation)
}
