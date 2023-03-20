package dynsampler

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAvgSampleWithMinUpdateMaps(t *testing.T) {
	a := &AvgSampleWithMin{
		GoalSampleRate:    20,
		MinEventsPerSec:   50,
		ClearFrequencySec: 30,
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
