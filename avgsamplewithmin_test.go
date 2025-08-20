package dynsampler

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAvgSampleWithMinSetGoalSampleRate(t *testing.T) {
	a := &AvgSampleWithMin{
		GoalSampleRate:         20,
		MinEventsPerSec:        50,
		ClearFrequencyDuration: 30 * time.Second,
	}

	// Test SetGoalSampleRate method
	a.SetGoalSampleRate(30)
	assert.Equal(t, 30, a.GoalSampleRate)

	// Test that invalid values are ignored
	a.SetGoalSampleRate(0)
	assert.Equal(t, 30, a.GoalSampleRate)

	a.SetGoalSampleRate(-5)
	assert.Equal(t, 30, a.GoalSampleRate)
}

func TestAvgSampleWithMinUpdateMaps(t *testing.T) {
	a := &AvgSampleWithMin{
		GoalSampleRate:         20,
		MinEventsPerSec:        50,
		ClearFrequencyDuration: 30 * time.Second,
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
				"three": 1,
				"four":  1,
				"five":  1,
				"six":   1,
				"seven": 1,
				"eight": 1,
				"nine":  1,
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
				"three": 1,
				"four":  1,
				"five":  1,
			},
		},
		{
			map[string]float64{
				"one": 1,
			},
			map[string]int{
				"one": 1,
			},
		},
		{
			map[string]float64{
				"one": 8,
			},
			map[string]int{
				"one": 1,
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
			map[string]float64{},
			map[string]int{},
		},
	}
	for i, tst := range tsts {
		a.currentCounts = tst.inputSampleCount
		a.updateMaps()
		assert.Equal(t, len(a.currentCounts), 0)
		assert.Equal(t, a.savedSampleRates, tst.expectedSavedSampleRates, fmt.Sprintf("test %d failed", i))
	}
}

func TestAvgSampleWithMinGetSampleRateStartup(t *testing.T) {
	a := &AvgSampleWithMin{
		GoalSampleRate: 10,
		currentCounts:  map[string]float64{},
	}
	rate := a.GetSampleRate("key")
	assert.Equal(t, rate, 10)
	// and the counters still get bumped
	assert.Equal(t, a.currentCounts["key"], 1.)
}

func TestAvgSampleWithMinGetSampleRate(t *testing.T) {
	a := &AvgSampleWithMin{
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

func TestAvgSampleWithMinRace(t *testing.T) {
	a := &AvgSampleWithMin{
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
					assert.NotEqual(t, rate <= 0, true, "rate should never be lte zero", rate)
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

func TestAvgSampleWithMinMaxKeys(t *testing.T) {
	a := &AvgSampleWithMin{
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

func TestAvgSampleWithMin_Start(t *testing.T) {
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
			a := &AvgSampleWithMin{
				ClearFrequencySec:      tt.ClearFrequencySec,
				ClearFrequencyDuration: tt.ClearFrequencyDuration,
			}
			err := a.Start()
			if (err != nil) != tt.wantErr {
				t.Errorf("AvgSampleWithMin error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				defer a.Stop()
				if tt.wantDuration != a.ClearFrequencyDuration {
					t.Errorf("AvgSampleWithMin duration mismatch = want %v, got %v", tt.wantDuration, a.ClearFrequencyDuration)
				}
			}
		})
	}
}

func TestAvgSampleWithMin_GetMetrics(t *testing.T) {
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
			a := &AvgSampleWithMin{
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
