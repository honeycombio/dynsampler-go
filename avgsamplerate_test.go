package dynsampler

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAvgSampleUpdateMaps(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate: 20,
	}
	tsts := []struct {
		inputSampleCount         map[string]int
		expectedSavedSampleRates map[string]int
	}{
		{
			map[string]int{
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
			map[string]int{
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
			map[string]int{
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
			map[string]int{
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
			map[string]int{
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
			map[string]int{
				"one": 12000,
			},
			map[string]int{
				"one": 20,
			},
		},
		{
			map[string]int{},
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

func TestAvgSampleGetSampleRateStartup(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate: 10,
		currentCounts:  map[string]int{},
	}
	rate := a.GetSampleRate("key")
	assert.Equal(t, rate, 10)
	// and the counters still get bumped
	assert.Equal(t, a.currentCounts["key"], 1)
}

func TestAvgSampleRace(t *testing.T) {
	a := &AvgSampleRate{
		GoalSampleRate:   2,
		currentCounts:    map[string]int{},
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
					assert.NotEqual(t, rate, 0, "rate should never be zero")
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
	a.currentCounts = map[string]int{
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
		expectedCurrentCountForKey int
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
