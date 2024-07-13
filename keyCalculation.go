package dynsampler

import (
	"math"
	"sort"
)

// This is an extraction of common calculation logic for all the key-based samplers.
func calculateSampleRates(goalRatio float64, buckets map[string]float64) map[string]int {
	// Optimization for single bucket
	if len(buckets) == 1 {
		for k, v := range buckets {
			return map[string]int{k: _calculateSingleBucketRate(goalRatio, v)}
		}
	}

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

func calculateSampleRates_original(goalRatio float64, buckets map[string]float64) map[string]int {
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

func _calculateSingleBucketRate(goalRatio float64, count float64) int {
	count = math.Max(1, count)
	goalForKey := math.Max(1, math.Log10(count)*goalRatio)
	if count <= goalForKey {
		return 1
	}
	rate := math.Ceil(count / goalForKey)
	if math.IsNaN(rate) {
		return 1
	}
	return int(rate)
}
