package dynsampler

import (
	"fmt"
	"math"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateEMA(t *testing.T) {
	e := &EMASampleRate{
		movingAverage: make(map[string]float64),
		Weight:        0.2,
		AgeOutValue:   0.2,
	}

	tests := []struct {
		keyAValue    float64
		keyAExpected float64
		keyBValue    float64
		keyBExpected float64
		keyCValue    float64
		keyCExpected float64
	}{
		{463, 93, 235, 47, 0, 0},
		{176, 109, 458, 129, 0, 0},
		{345, 156, 470, 197, 0, 0},
		{339, 193, 317, 221, 0, 0},
		{197, 194, 165, 210, 0, 0},
		{387, 232, 95, 187, 6960, 1392},
	}

	for _, tt := range tests {
		counts := make(map[string]float64)
		counts["a"] = tt.keyAValue
		counts["b"] = tt.keyBValue
		counts["c"] = tt.keyCValue
		e.updateEMA(counts)
		assert.Equal(t, tt.keyAExpected, math.Round(e.movingAverage["a"]))
		assert.Equal(t, tt.keyBExpected, math.Round(e.movingAverage["b"]))
		assert.Equal(t, tt.keyCExpected, math.Round(e.movingAverage["c"]))
	}
}

func TestEMASampleGetSampleRateStartup(t *testing.T) {
	e := &EMASampleRate{
		GoalSampleRate: 10,
		currentCounts:  map[string]float64{},
	}
	rate := e.GetSampleRate("key")
	assert.Equal(t, rate, 10)
	assert.Equal(t, e.currentCounts["key"], float64(1))
}

func TestEMASampleUpdateMaps(t *testing.T) {
	e := &EMASampleRate{
		GoalSampleRate: 20,
		Weight:         0.2,
		AgeOutValue:    0.2,
	}
	tsts := []struct {
		inputSampleCount         map[string]float64
		expectedSavedSampleRates map[string]int
	}{
		{
			map[string]float64{
				"one":   1,
				"two":   1,
				"three": 2,
				"four":  5,
				"five":  8,
				"six":   15,
				"seven": 45,
				"eight": 612,
				"nine":  2000,
				"ten":   10000,
			},
			map[string]int{
				"one":   1,
				"two":   1,
				"three": 1,
				"four":  1,
				"five":  1,
				"six":   1,
				"seven": 1,
				"eight": 6,
				"nine":  14,
				"ten":   47,
			},
		},
		{
			map[string]float64{
				"one":   1,
				"two":   1,
				"three": 2,
				"four":  5,
				"five":  8,
				"six":   15,
				"seven": 45,
				"eight": 50,
				"nine":  60,
			},
			map[string]int{
				"one":   1,
				"two":   1,
				"three": 2,
				"four":  5,
				"five":  8,
				"six":   11,
				"seven": 24,
				"eight": 26,
				"nine":  30,
			},
		},
		{
			map[string]float64{
				"one":   1,
				"two":   1,
				"three": 2,
				"four":  5,
				"five":  7,
			},
			map[string]int{
				"one":   1,
				"two":   1,
				"three": 2,
				"four":  5,
				"five":  7,
			},
		},
		{
			map[string]float64{
				"one":   1000,
				"two":   1000,
				"three": 2000,
				"four":  5000,
				"five":  7000,
			},
			map[string]int{
				"one":   7,
				"two":   7,
				"three": 13,
				"four":  29,
				"five":  39,
			},
		},
		{
			map[string]float64{
				"one":   6000,
				"two":   6000,
				"three": 6000,
				"four":  6000,
				"five":  6000,
			},
			map[string]int{
				"one":   20,
				"two":   20,
				"three": 20,
				"four":  20,
				"five":  20,
			},
		},
		{
			map[string]float64{
				"one": 12000,
			},
			map[string]int{
				"one": 20,
			},
		},
		{
			map[string]float64{},
			map[string]int{},
		},
		{
			map[string]float64{
				"one":       10,
				"two":       1,
				"three":     1,
				"four":      1,
				"five":      1,
				"six":       1,
				"seven":     1,
				"eight":     1,
				"nine":      1,
				"ten":       1,
				"eleven":    1,
				"twelve":    1,
				"thirteen":  1,
				"fourteen":  1,
				"fifteen":   1,
				"sixteen":   1,
				"seventeen": 1,
				"eighteen":  1,
				"nineteen":  1,
				"twenty":    1,
			},
			map[string]int{
				"one":       7,
				"two":       1,
				"three":     1,
				"four":      1,
				"five":      1,
				"six":       1,
				"seven":     1,
				"eight":     1,
				"nine":      1,
				"ten":       1,
				"eleven":    1,
				"twelve":    1,
				"thirteen":  1,
				"fourteen":  1,
				"fifteen":   1,
				"sixteen":   1,
				"seventeen": 1,
				"eighteen":  1,
				"nineteen":  1,
				"twenty":    1,
			},
		},
	}
	for i, tst := range tsts {
		e.movingAverage = make(map[string]float64)
		e.savedSampleRates = make(map[string]int)

		// Test data is based on `TestAvgSampleUpdateMaps` for AvgSampleRate.
		// To get the same sample rates though, we must reach averages that match
		// the inputs - for the EMA, the way to do this is to just apply the same
		// input values over and over and converge on that average
		for i := 0; i <= 100; i++ {
			input := make(map[string]float64)
			for k, v := range tst.inputSampleCount {
				input[k] = v
			}
			e.currentCounts = input
			e.updateMaps()
		}
		assert.Equal(t, 0, len(e.currentCounts))
		assert.Equal(t, tst.expectedSavedSampleRates, e.savedSampleRates, fmt.Sprintf("test %d failed", i))
	}
}

func TestEMASampleUpdateMapsSparseCounts(t *testing.T) {
	e := &EMASampleRate{
		GoalSampleRate: 20,
		Weight:         0.2,
		AgeOutValue:    0.2,
	}

	e.movingAverage = make(map[string]float64)
	e.savedSampleRates = make(map[string]int)

	for i := 0; i <= 100; i++ {
		input := make(map[string]float64)
		// simulate steady stream of input from one key
		input["largest_count"] = 20
		// sporadic keys with single counts that come and go with each interval
		for j := 0; j < 5; j++ {
			key := randomString(8)
			input[key] = 1
		}
		e.currentCounts = input
		e.updateMaps()
	}
	assert.Equal(t, 16, e.savedSampleRates["largest_count"])
}

func TestEMAAgesOutSmallValues(t *testing.T) {
	e := &EMASampleRate{
		GoalSampleRate: 20,
		Weight:         0.2,
		AgeOutValue:    0.2,
	}
	e.movingAverage = make(map[string]float64)
	for i := 0; i < 100; i++ {
		e.currentCounts = map[string]float64{"foo": 500.0}
		e.updateMaps()
	}
	assert.Equal(t, 1, len(e.movingAverage))
	assert.Equal(t, float64(500), math.Round(e.movingAverage["foo"]))
	for i := 0; i < 100; i++ {
		// "observe" no occurrences of foo for many iterations
		e.currentCounts = map[string]float64{"asdf": 1}
		e.updateMaps()
	}
	_, found := e.movingAverage["foo"]
	assert.Equal(t, false, found)
	_, found = e.movingAverage["asdf"]
	assert.Equal(t, true, found)
}

func TestEMABurstDetection(t *testing.T) {
	// Set the adjustment interval very high so that we never run the regular interval
	e := &EMASampleRate{AdjustmentIntervalDuration: 1 * time.Hour}
	err := e.Start()
	assert.Nil(t, err)

	// set some counts and compute the EMA
	e.currentCounts = map[string]float64{"foo": 1000}
	e.updateMaps()
	// should have a burst threshold computed now from this average
	// 1000 = 0.5 (weight) * 1000 * 2 (threshold multiplier)
	assert.Equal(t, float64(1000), e.burstThreshold)

	// Let's try and trigger a burst:
	for i := 0; i <= 1000; i++ {
		e.GetSampleRate("bar")
	}
	// burst sum isn't reset even though we're above our burst threshold
	// This is because we haven't processed enough intervals to do burst detection yet
	assert.Equal(t, float64(1001), e.currentBurstSum)
	// Now let's cheat and say we have
	e.intervalCount = e.BurstDetectionDelay
	e.testSignalMapsDone = make(chan struct{})
	e.GetSampleRate("bar")
	// wait on updateMaps to complete
	<-e.testSignalMapsDone
	// currentBurstSum has been reset
	assert.Equal(t, float64(0), e.currentBurstSum)

	// ensure EMA is updated
	assert.Equal(t, float64(501), e.movingAverage["bar"])
}

func TestEMAUpdateMapsRace(t *testing.T) {
	e := &EMASampleRate{AdjustmentIntervalDuration: 1 * time.Hour}
	e.testSignalMapsDone = make(chan struct{}, 1000)
	err := e.Start()
	assert.Nil(t, err)
	for i := 0; i < 1000; i++ {
		e.GetSampleRate("foo")
		go e.updateMaps()
	}
	done := 0
	for done != 1000 {
		<-e.testSignalMapsDone
		done++
	}
}

func TestEMASampleRateSaveState(t *testing.T) {
	var sampler Sampler
	esr := &EMASampleRate{}
	// ensure the interface is implemented
	sampler = esr
	err := sampler.Start()
	assert.Nil(t, err)

	esr.lock.Lock()
	esr.savedSampleRates = map[string]int{"foo": 2, "bar": 4}
	esr.movingAverage = map[string]float64{"foo": 500.1234, "bar": 9999.99}
	esr.haveData = true
	esr.lock.Unlock()

	assert.Equal(t, 2, sampler.GetSampleRate("foo"))
	assert.Equal(t, 4, sampler.GetSampleRate("bar"))

	state, err := sampler.SaveState()
	assert.Nil(t, err)

	var newSampler Sampler
	esr2 := &EMASampleRate{}
	newSampler = esr2

	err = newSampler.LoadState(state)
	assert.Nil(t, err)
	err = newSampler.Start()
	assert.Nil(t, err)

	assert.Equal(t, 2, newSampler.GetSampleRate("foo"))
	assert.Equal(t, 4, newSampler.GetSampleRate("bar"))
	esr2.lock.Lock()
	defer esr2.lock.Unlock()
	assert.Equal(t, float64(500.1234), esr2.movingAverage["foo"])
	assert.Equal(t, float64(9999.99), esr2.movingAverage["bar"])
}

// This is a long test because we generate a lot of random data and run it through the sampler
// The goal is to determine if we actually hit the specified target rate (within a tolerance) an acceptable
// number of times. Most of the time, the average sample rate of observations kept should be close
// to the target rate
func TestEMASampleRateHitsTargetRate(t *testing.T) {
	mrand.Seed(time.Now().Unix())
	testRates := []int{50, 100}
	testKeyCount := []int{10, 100}
	tolerancePct := float64(0.2)

	for _, rate := range testRates {
		tolerance := float64(rate) * tolerancePct
		toleranceUpper := float64(rate) + tolerance
		toleranceLower := float64(rate) - tolerance

		for _, keyCount := range testKeyCount {
			sampler := &EMASampleRate{GoalSampleRate: rate, Weight: 0.5, AgeOutValue: 0.5, currentCounts: make(map[string]float64), movingAverage: make(map[string]float64)}

			// build a consistent set of keys to use
			keys := make([]string, keyCount)
			for i := 0; i < keyCount; i++ {
				keys[i] = randomString(8)
			}

			for i, key := range keys {
				// generate key counts of different magnitudes
				base := math.Pow10(i%3 + 1)
				count := float64(((i%10)+1))*base + float64(mrand.Intn(int(base)))
				sampler.currentCounts[key] = count
			}

			// build an initial set of sample rates so we don't just return the target rate
			sampler.updateMaps()

			var success float64

			for i := 0; i < 100; i++ {
				totalSampleRate := 0
				totalKeptObservations := 0
				for j, key := range keys {
					base := math.Pow10(j%3 + 1)
					count := float64(((j%10)+1))*base + float64(mrand.Intn(int(base)))
					for k := 0; k < int(count); k++ {
						rate := sampler.GetSampleRate(key)
						if mrand.Intn(rate) == 0 {
							totalSampleRate += rate
							totalKeptObservations++
						}
					}
				}

				avgSampleRate := float64(totalSampleRate) / float64(totalKeptObservations)
				if avgSampleRate <= toleranceUpper && avgSampleRate >= toleranceLower {
					success++
				}
				sampler.updateMaps()
			}

			assert.True(t, success/100.0 >= 0.95, "target rate test %d with key count %d failed with success rate of only %f", rate, keyCount, success/100.0)
		}
	}
}

// This is very like the above test, but it uses GetSampleRateMulti with a random value between 1 and 100.
func TestEMASampleRateMultiHitsTargetRate(t *testing.T) {
	mrand.Seed(time.Now().Unix())
	testRates := []int{50, 100}
	testKeyCount := []int{10, 50}
	tolerancePct := float64(0.2)

	for _, rate := range testRates {
		tolerance := float64(rate) * tolerancePct
		toleranceUpper := float64(rate) + tolerance
		toleranceLower := float64(rate) - tolerance

		for _, keyCount := range testKeyCount {
			sampler := &EMASampleRate{GoalSampleRate: rate, Weight: 0.5, AgeOutValue: 0.5, currentCounts: make(map[string]float64), movingAverage: make(map[string]float64)}

			// build a consistent set of keys to use
			keys := make([]string, keyCount)
			for i := 0; i < keyCount; i++ {
				keys[i] = randomString(8)
			}

			for i, key := range keys {
				// generate key counts of different magnitudes
				base := math.Pow10(i%3 + 1)
				count := float64(((i%10)+1))*base + float64(mrand.Intn(int(base)))
				sampler.currentCounts[key] = count
			}

			// build an initial set of sample rates so we don't just return the target rate
			sampler.updateMaps()

			var success float64

			for i := 0; i < 100; i++ {
				totalSampleRate := 0
				totalKeptObservations := 0
				for j, key := range keys {
					base := math.Pow10(j%3 + 1)
					count := float64(((j%10)+1))*base + float64(mrand.Intn(int(base)))
					for k := 0; k < int(count); k++ {
						rate := sampler.GetSampleRateMulti(key, mrand.Intn(100))
						if mrand.Intn(rate) == 0 {
							totalSampleRate += rate
							totalKeptObservations++
						}
					}
				}

				avgSampleRate := float64(totalSampleRate) / float64(totalKeptObservations)
				if avgSampleRate <= toleranceUpper && avgSampleRate >= toleranceLower {
					success++
				}
				sampler.updateMaps()
			}

			assert.True(t, success/100.0 >= 0.95, "target rate test %d with key count %d failed with success rate of only %f", rate, keyCount, success/100.0)
		}
	}
}

func TestEMASampleRate_Start(t *testing.T) {
	tests := []struct {
		name                       string
		AdjustmentInterval         int
		AdjustmentIntervalDuration time.Duration
		wantDuration               time.Duration
		wantErr                    bool
	}{
		{"sec only", 2, 0, 2 * time.Second, false},
		{"dur only", 0, 1003 * time.Millisecond, 1003 * time.Millisecond, false},
		{"default", 0, 0, 15 * time.Second, false},
		{"both", 2, 2 * time.Second, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &EMASampleRate{
				AdjustmentInterval:         tt.AdjustmentInterval,
				AdjustmentIntervalDuration: tt.AdjustmentIntervalDuration,
			}
			err := a.Start()
			if (err != nil) != tt.wantErr {
				t.Errorf("EMASampleRate error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				defer a.Stop()
				if tt.wantDuration != a.AdjustmentIntervalDuration {
					t.Errorf("EMASampleRate duration mismatch = want %v, got %v", tt.wantDuration, a.AdjustmentIntervalDuration)
				}
			}
		})
	}
}

func TestEMASampleRate_GetMetrics(t *testing.T) {
	tests := []struct {
		name           string
		prefix         string
		requestCount   int64
		eventCount     int64
		currentCounts  map[string]float64
		burstCount     int64
		intervalCount  uint
		existingPrefix string
		expectedResult map[string]int64
		expectNil      bool
	}{
		{
			name:          "first call with prefix",
			prefix:        "test_",
			requestCount:  100,
			eventCount:    500,
			currentCounts: map[string]float64{"key1": 10, "key2": 20},
			burstCount:    5,
			intervalCount: 3,
			expectedResult: map[string]int64{
				"test_request_count":  100,
				"test_event_count":    500,
				"test_keyspace_size":  2,
				"test_burst_count":    5,
				"test_interval_count": 3,
			},
		},
		{
			name:          "empty prefix",
			prefix:        "",
			requestCount:  42,
			eventCount:    84,
			burstCount:    0,
			intervalCount: 1,
			currentCounts: map[string]float64{"key1": 5},
			expectedResult: map[string]int64{
				"request_count":  42,
				"event_count":    84,
				"burst_count":    0,
				"interval_count": 1,
				"keyspace_size":  1,
			},
		},
		{
			name:          "zero counts",
			prefix:        "zero_",
			currentCounts: map[string]float64{},
			expectedResult: map[string]int64{
				"zero_request_count":  0,
				"zero_event_count":    0,
				"zero_keyspace_size":  0,
				"zero_burst_count":    0,
				"zero_interval_count": 0,
			},
		},
		{
			name:           "same prefix second call",
			prefix:         "same_",
			requestCount:   200,
			eventCount:     1000,
			currentCounts:  map[string]float64{"key1": 15},
			burstCount:     10,
			intervalCount:  2,
			existingPrefix: "same_",
			expectedResult: map[string]int64{
				"same_request_count":  200,
				"same_event_count":    1000,
				"same_keyspace_size":  1,
				"same_burst_count":    10,
				"same_interval_count": 2,
			},
		},
		{
			name:           "different prefix returns nil",
			prefix:         "new_",
			requestCount:   150,
			eventCount:     750,
			currentCounts:  map[string]float64{"key1": 25},
			existingPrefix: "old_",
			expectNil:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &EMASampleRate{
				requestCount:  tt.requestCount,
				eventCount:    tt.eventCount,
				currentCounts: tt.currentCounts,
				burstCount:    tt.burstCount,
				intervalCount: tt.intervalCount,
			}

			// Set existing prefix if specified
			if tt.existingPrefix != "" {
				a.prefix = tt.existingPrefix
				a.requestCountKey = a.prefix + requestCountSuffix
				a.eventCountKey = a.prefix + eventCountSuffix
				a.keyspaceSizeKey = a.prefix + keyspaceSizeSuffix
				a.burstCountKey = a.prefix + burstCountSuffix
				a.intervalCountKey = a.prefix + intervalCountSuffix
			}

			result := a.GetMetrics(tt.prefix)

			if tt.expectNil {
				assert.Nil(t, result)
				return
			}

			assert.NotNil(t, result)
			assert.Equal(t, tt.expectedResult, result)

			assert.Equal(t, tt.prefix, a.prefix)
			assert.Equal(t, tt.prefix+requestCountSuffix, a.requestCountKey)
			assert.Equal(t, tt.prefix+eventCountSuffix, a.eventCountKey)
			assert.Equal(t, tt.prefix+keyspaceSizeSuffix, a.keyspaceSizeKey)
			assert.Equal(t, tt.prefix+burstCountSuffix, a.burstCountKey)
			assert.Equal(t, tt.prefix+intervalCountSuffix, a.intervalCountKey)
		})
	}
}
