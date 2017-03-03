package dynsampler

import (
	"fmt"
	"testing"

	"github.com/honeycombio/hound/test"
)

func TestTotalThroughputUpdateMaps(t *testing.T) {
	s := &TotalThroughput{
		ClearFrequencySec:    30,
		GoalThroughputPerSec: 20,
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
				"eight": 10,
				"nine":  33,
				"ten":   166,
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
				"three": 1,
				"four":  1,
				"five":  1,
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
				"one":   8,
				"two":   8,
				"three": 16,
				"four":  41,
				"five":  58,
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
				"one":   50,
				"two":   50,
				"three": 50,
				"four":  50,
				"five":  50,
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
		s.currentCounts = tst.inputSampleCount
		s.updateMaps()
		test.Equals(t, len(s.currentCounts), 0)
		test.Equals(t, s.savedSampleRates, tst.expectedSavedSampleRates, fmt.Sprintf("test %d failed", i))
	}
}

func TestTotalThroughputGetSampleRate(t *testing.T) {
	s := &TotalThroughput{}
	s.currentCounts = map[string]int{
		"one": 5,
		"two": 8,
	}
	s.savedSampleRates = map[string]int{
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
		rate := s.GetSampleRate(tst.inputKey)
		test.Equals(t, rate, tst.expectedSampleRate)
		test.Equals(t, s.currentCounts[tst.inputKey], tst.expectedCurrentCountForKey)
	}
}
