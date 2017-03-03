package dynsampler

import (
	"fmt"
	"testing"

	"github.com/honeycombio/hound/test"
)

func TestPerKeyThroughputUpdateMaps(t *testing.T) {
	p := &PerKeyThroughput{
		ClearFrequencySec:      30,
		PerKeyThroughputPerSec: 5,
	}
	tsts := []struct {
		inputCount               map[string]int
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
				"eight": 4,
				"nine":  13,
				"ten":   66,
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
				"one":   6,
				"two":   6,
				"three": 13,
				"four":  33,
				"five":  46,
			},
		},
		{
			map[string]int{
				"one":   1000,
				"two":   1000,
				"three": 2000,
				"four":  5000,
				"five":  70000,
			},
			map[string]int{
				"one":   6,
				"two":   6,
				"three": 13,
				"four":  33,
				"five":  466,
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
				"one":   40,
				"two":   40,
				"three": 40,
				"four":  40,
				"five":  40,
			},
		},
		{
			map[string]int{
				"one": 12000,
			},
			map[string]int{
				"one": 80,
			},
		},
		{
			map[string]int{},
			map[string]int{},
		},
	}
	for i, tst := range tsts {
		p.currentCounts = tst.inputCount
		p.updateMaps()
		test.Equals(t, len(p.currentCounts), 0)
		test.Equals(t, p.savedSampleRates, tst.expectedSavedSampleRates, fmt.Sprintf("test %d failed", i))
	}
}

func TestPerKeyThroughputGetSampleRate(t *testing.T) {
	p := &PerKeyThroughput{}
	p.currentCounts = map[string]int{
		"one": 5,
		"two": 8,
	}
	p.savedSampleRates = map[string]int{
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
		rate := p.GetSampleRate(tst.inputKey)
		test.Equals(t, rate, tst.expectedSampleRate)
		test.Equals(t, p.currentCounts[tst.inputKey], tst.expectedCurrentCountForKey)
	}
}
