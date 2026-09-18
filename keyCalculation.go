package dynsampler

import (
	"math"
	"sort"
	"time"
)

// windowedSampleRates splits the goal throughput equally across the keys
// present in an aggregated window and returns the per-key sample rates. It is
// the shared allocation used by both WindowedThroughput (timer-driven, backed
// by a BlockList) and WindowedThroughputCalculator (caller-driven, backed by a
// ring of buckets), so the two cannot drift. An empty window yields an empty
// table.
func windowedSampleRates(agg map[string]float64, goalThroughputPerSec float64, lookback time.Duration) map[string]int {
	rates := make(map[string]int, len(agg))
	if len(agg) == 0 {
		return rates
	}
	totalGoal := goalThroughputPerSec * lookback.Seconds()
	perKey := totalGoal / float64(len(agg))
	for k, v := range agg {
		rates[k] = int(math.Max(1, v/perKey))
	}
	return rates
}

// This is an extraction of common calculation logic for all the key-based samplers.
func calculateSampleRates(goalRatio float64, buckets map[string]float64) map[string]int {
	// must go through the keys in a fixed order to prevent rounding from changing
	// results
	keys := make([]string, len(buckets))
	var i int
	for k := range buckets {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	// goal number of events per key is goalRatio * key count, but never less than
	// one. If a key falls below its goal, it gets a sample rate of 1 and the
	// extra available events get passed on down the line.
	newSampleRates := make(map[string]int)
	keysRemaining := len(buckets)
	var extra float64
	for _, key := range keys {
		count := math.Max(1, buckets[key])
		// take the max of 1 or my log10 share of the total
		goalForKey := math.Max(1, math.Log10(count)*goalRatio)
		// take this key's share of the extra and pass the rest along
		extraForKey := extra / float64(keysRemaining)
		goalForKey += extraForKey
		extra -= extraForKey
		keysRemaining--
		if count <= goalForKey {
			// there are fewer samples than the allotted number for this key. set
			// sample rate to 1 and redistribute the unused slots for future keys
			newSampleRates[key] = 1
			extra += goalForKey - count
		} else {
			// there are more samples than the allotted number. Sample this key enough
			// to knock it under the limit (aka round up)
			rate := math.Ceil(count / goalForKey)
			// if counts are <= 1 we can get values for goalForKey that are +Inf
			// and subsequent division ends up with NaN. If that's the case,
			// fall back to 1
			if math.IsNaN(rate) {
				newSampleRates[key] = 1
			} else {
				newSampleRates[key] = int(rate)
			}
			extra += goalForKey - (count / float64(newSampleRates[key]))
		}
	}
	return newSampleRates
}
