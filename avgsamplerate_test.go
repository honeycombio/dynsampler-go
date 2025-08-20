package dynsampler

import (
	"crypto/rand"
	"fmt"
	"math"
	mrand "math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetGoalSampleRate(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate: 20,
	}

	// Test SetGoalSampleRate method
	a.SetGoalSampleRate(25)
	assert.Equal(t, 25, a.GoalSampleRate)

	// Test that invalid values are ignored
	a.SetGoalSampleRate(0)
	assert.Equal(t, 25, a.GoalSampleRate)

	a.SetGoalSampleRate(-5)
	assert.Equal(t, 25, a.GoalSampleRate)
}

func TestAvgSampleUpdateMaps(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate: 20,
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
		a.currentCounts = tst.inputSampleCount
		a.updateMaps()
		assert.Equal(t, len(a.currentCounts), 0)
		assert.Equal(t, a.savedSampleRates, tst.expectedSavedSampleRates, fmt.Sprintf("test %d failed", i))
	}
}

func TestAvgSampleGetSampleRateStartup(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate: 10,
		currentCounts:  map[string]float64{},
	}
	rate := a.GetSampleRate("key")
	assert.Equal(t, rate, 10)
	// and the counters still get bumped
	assert.Equal(t, a.currentCounts["key"], 1.0)
}

func TestAvgSampleRace(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate:   2,
		currentCounts:    map[string]float64{},
		savedSampleRates: map[string]int{},
		haveData:         true,
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	wg.Add(1)
	// set up 100 parallel readers, each reading 1000 times
	go func() {
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(i int) {
				for j := 0; j < 1000; j++ {
					rate := a.GetSampleRate("key" + strconv.Itoa(i))
					assert.NotEqual(t, rate <= 0, true, "rate should never be lte zero")
				}
				wg.Done()
			}(i)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 100; i++ {
			a.updateMaps()
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestAvgSampleRateGetSampleRate(t *testing.T) {
	a := &AvgSampleRate{
		haveData: true,
	}
	a.currentCounts = map[string]float64{
		"one": 5,
		"two": 8,
	}
	a.savedSampleRates = map[string]int{
		"one":   10,
		"two":   1,
		"three": 5,
	}
	tsts := []struct {
		inputKey                   string
		expectedSampleRate         int
		expectedCurrentCountForKey float64
	}{
		{"one", 10, 6},
		{"two", 1, 9},
		{"two", 1, 10},
		{"three", 5, 1}, // key missing from current counts
		{"three", 5, 2},
		{"four", 1, 1}, // key missing from current and saved counts
		{"four", 1, 2},
	}
	for _, tst := range tsts {
		rate := a.GetSampleRate(tst.inputKey)
		assert.Equal(t, rate, tst.expectedSampleRate)
		assert.Equal(t, a.currentCounts[tst.inputKey], tst.expectedCurrentCountForKey)
	}
}

func TestAvgSampleRateMaxKeys(t *testing.T) {
	a := &AvgSampleRate{
		MaxKeys: 3,
	}
	a.currentCounts = map[string]float64{
		"one": 1,
		"two": 1,
	}
	a.savedSampleRates = map[string]int{}

	// with MaxKeys 3, we are under the key limit, so three should get added
	a.GetSampleRate("three")
	assert.Equal(t, 3, len(a.currentCounts))
	assert.Equal(t, 1., a.currentCounts["three"])
	// Now we're at 3 keys - four should not be added
	a.GetSampleRate("four")
	assert.Equal(t, 3, len(a.currentCounts))
	_, found := a.currentCounts["four"]
	assert.Equal(t, false, found)
	// We should still support bumping counts for existing keys
	a.GetSampleRate("one")
	assert.Equal(t, 3, len(a.currentCounts))
	assert.Equal(t, 2., a.currentCounts["one"])
}

func TestAvgSampleRateSaveState(t *testing.T) {
	var sampler Sampler
	asr := &AvgSampleRate{}
	// ensure the interface is implemented
	sampler = asr
	err := sampler.Start()
	assert.Nil(t, err)

	asr.lock.Lock()
	asr.savedSampleRates = map[string]int{"foo": 2, "bar": 4}
	asr.haveData = true
	asr.lock.Unlock()

	assert.Equal(t, 2, sampler.GetSampleRate("foo"))
	assert.Equal(t, 4, sampler.GetSampleRate("bar"))

	state, err := sampler.SaveState()
	assert.Nil(t, err)

	var newSampler Sampler = &AvgSampleRate{}

	err = newSampler.LoadState(state)
	assert.Nil(t, err)
	err = newSampler.Start()
	assert.Nil(t, err)

	assert.Equal(t, 2, newSampler.GetSampleRate("foo"))
	assert.Equal(t, 4, newSampler.GetSampleRate("bar"))
}

// This is a long test because we generate a lot of random data and run it through the sampler
// The goal is to determine if we actually hit the specified target rate (within a tolerance) an acceptable
// number of times. Most of the time, the average sample rate of observations kept should be close
// to the target rate
func TestAvgSampleRateHitsTargetRate(t *testing.T) {
	mrand.Seed(time.Now().Unix())
	testRates := []int{50, 100}
	testKeyCount := []int{10, 100}
	tolerancePct := float64(0.2)

	for _, rate := range testRates {
		tolerance := float64(rate) * tolerancePct
		toleranceUpper := float64(rate) + tolerance
		toleranceLower := float64(rate) - tolerance

		for _, keyCount := range testKeyCount {
			sampler := &AvgSampleRate{GoalSampleRate: rate, currentCounts: make(map[string]float64)}

			// build a consistent set of keys to use
			keys := make([]string, keyCount)
			for i := 0; i < keyCount; i++ {
				keys[i] = randomString(8)
			}

			for i, key := range keys {
				// generate key counts of different magnitudes - keys reliably get the same magnitude
				// so that count ranges are reasonable (i.e. they don't go from 1 to 10000 back to 100)
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

func TestAvgSampleUpdateMapsSparseCounts(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate: 20,
	}

	a.savedSampleRates = make(map[string]int)

	for i := 0; i <= 100; i++ {
		input := make(map[string]float64)
		// simulate steady stream of input from one key
		input["largest_count"] = 20
		// sporadic keys with single counts that come and go with each interval
		for j := 0; j < 5; j++ {
			key := randomString(8)
			input[key] = 1
		}
		a.currentCounts = input
		a.updateMaps()
	}

	assert.Equal(t, 16, a.savedSampleRates["largest_count"])
}

func randomString(length int) string {
	b := make([]byte, length/2)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func TestAvgSampleRate_Start(t *testing.T) {
	tests := []struct {
		name                   string
		ClearFrequencySec      int
		ClearFrequencyDuration time.Duration
		wantDuration           time.Duration
		wantErr                bool
	}{
		{"sec only", 2, 0, 2 * time.Second, false},
		{"dur only", 0, 1003 * time.Millisecond, 1003 * time.Millisecond, false},
		{"default", 0, 0, 30 * time.Second, false},
		{"both", 2, 2 * time.Second, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AvgSampleRate{
				ClearFrequencySec:      tt.ClearFrequencySec,
				ClearFrequencyDuration: tt.ClearFrequencyDuration,
			}
			err := a.Start()
			if (err != nil) != tt.wantErr {
				t.Errorf("AvgSampleRate error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				defer a.Stop()
				if tt.wantDuration != a.ClearFrequencyDuration {
					t.Errorf("AvgSampleRate duration mismatch = want %v, got %v", tt.wantDuration, a.ClearFrequencyDuration)
				}
			}
		})
	}
}

func TestAvgSampleRateGetMetrics(t *testing.T) {
	tests := []struct {
		name           string
		prefix         string
		requestCount   int64
		eventCount     int64
		currentCounts  map[string]float64
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
			expectedResult: map[string]int64{
				"test_request_count": 100,
				"test_event_count":   500,
				"test_keyspace_size": 2,
			},
		},
		{
			name:          "empty prefix",
			prefix:        "",
			requestCount:  42,
			eventCount:    84,
			currentCounts: map[string]float64{"key1": 5},
			expectedResult: map[string]int64{
				"request_count": 42,
				"event_count":   84,
				"keyspace_size": 1,
			},
		},
		{
			name:          "zero counts",
			prefix:        "zero_",
			requestCount:  0,
			eventCount:    0,
			currentCounts: map[string]float64{},
			expectedResult: map[string]int64{
				"zero_request_count": 0,
				"zero_event_count":   0,
				"zero_keyspace_size": 0,
			},
		},
		{
			name:         "large numbers",
			prefix:       "large_",
			requestCount: 1000000,
			eventCount:   5000000,
			currentCounts: map[string]float64{
				"key1": 100, "key2": 200, "key3": 300, "key4": 400, "key5": 500,
			},
			expectedResult: map[string]int64{
				"large_request_count": 1000000,
				"large_event_count":   5000000,
				"large_keyspace_size": 5,
			},
		},
		{
			name:           "same prefix second call",
			prefix:         "same_",
			requestCount:   200,
			eventCount:     1000,
			currentCounts:  map[string]float64{"key1": 15},
			existingPrefix: "same_",
			expectedResult: map[string]int64{
				"same_request_count": 200,
				"same_event_count":   1000,
				"same_keyspace_size": 1,
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
			a := &AvgSampleRate{
				requestCount:  tt.requestCount,
				eventCount:    tt.eventCount,
				currentCounts: tt.currentCounts,
			}

			// Set existing prefix if specified
			if tt.existingPrefix != "" {
				a.prefix = tt.existingPrefix
				a.requestCountKey = a.prefix + requestCountSuffix
				a.eventCountKey = a.prefix + eventCountSuffix
				a.keyspaceSizeKey = a.prefix + keyspaceSizeSuffix
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
		})
	}
}
