package dynsampler

import (
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPerKeyThroughputUpdateMaps(t *testing.T) {
	p := &PerKeyThroughput{
		ClearFrequencyDuration: 30 * time.Second,
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
		assert.Equal(t, len(p.currentCounts), 0)
		assert.Equal(t, p.savedSampleRates, tst.expectedSavedSampleRates, fmt.Sprintf("test %d failed", i))
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
		assert.Equal(t, rate, tst.expectedSampleRate)
		assert.Equal(t, p.currentCounts[tst.inputKey], tst.expectedCurrentCountForKey)
	}
}

func TestPerKeyThroughputRace(t *testing.T) {
	p := &PerKeyThroughput{
		PerKeyThroughputPerSec: 2,
		currentCounts:          map[string]int{},
		savedSampleRates:       map[string]int{},
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
					rate := p.GetSampleRate("key" + strconv.Itoa(i))
					assert.NotEqual(t, rate, 0, "rate should never be zero")
				}
				wg.Done()
			}(i)
		}
		wg.Done()
	}()
	go func() {
		for i := 0; i < 100; i++ {
			p.updateMaps()
		}
		wg.Done()
	}()
	wg.Wait()
}

func TestPerKeyThroughputMaxKeys(t *testing.T) {
	p := &PerKeyThroughput{
		MaxKeys: 3,
	}
	p.currentCounts = map[string]int{
		"one": 1,
		"two": 1,
	}
	p.savedSampleRates = map[string]int{}

	// with MaxKeys 3, we are under the key limit, so three should get added
	p.GetSampleRate("three")
	assert.Equal(t, 3, len(p.currentCounts))
	assert.Equal(t, 1, p.currentCounts["three"])
	// Now we're at 3 keys - four should not be added
	p.GetSampleRate("four")
	assert.Equal(t, 3, len(p.currentCounts))
	_, found := p.currentCounts["four"]
	assert.Equal(t, false, found)
	// We should still support bumping counts for existing keys
	p.GetSampleRate("one")
	assert.Equal(t, 3, len(p.currentCounts))
	assert.Equal(t, 2, p.currentCounts["one"])
}

func TestPerKeyThroughput_Start(t *testing.T) {
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
			a := &PerKeyThroughput{
				ClearFrequencySec:      tt.ClearFrequencySec,
				ClearFrequencyDuration: tt.ClearFrequencyDuration,
			}
			err := a.Start()
			if (err != nil) != tt.wantErr {
				t.Errorf("PerKeyThroughput error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				defer a.Stop()
				if tt.wantDuration != a.ClearFrequencyDuration {
					t.Errorf("PerKeyThroughput duration mismatch = want %v, got %v", tt.wantDuration, a.ClearFrequencyDuration)
				}
			}
		})
	}
}
