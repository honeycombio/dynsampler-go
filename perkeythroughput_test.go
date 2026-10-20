package dynsampler

import (
	"fmt"
	"maps"
	"math"
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
		ClearFrequencyDuration:   time.Second,
		PerKeyThroughputPerSec:   2,
		MaxTotalThroughputPerSec: 50,
		currentCounts:            map[string]int{},
		savedSampleRates:         map[string]int{},
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
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
	go func() {
		for i := 1; i <= 100; i++ {
			p.SetPerKeyThroughputPerSec(i)
			p.SetMaxTotalThroughputPerSec(i * 10)
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

func TestWaterFillLevel(t *testing.T) {
	tsts := []struct {
		name     string
		demands  []float64
		capacity float64
		expected float64
	}{
		{"no demands", []float64{}, 10, math.Inf(1)},
		{"well under capacity", []float64{10, 20, 30}, 100, math.Inf(1)},
		{"exactly at capacity", []float64{10, 20, 30}, 60, math.Inf(1)},
		{"uniform demands", []float64{100, 100, 100, 100, 100}, 250, 50},
		{"long tail", []float64{10, 20, 30, 100, 100}, 150, 45},
		{"capacity below every demand", []float64{10, 20, 30}, 15, 5},
		{"unsorted input", []float64{100, 10, 30, 20, 100}, 150, 45},
		{"zero demands present", []float64{0, 0, 50}, 10, 10},
	}
	for _, tst := range tsts {
		t.Run(tst.name, func(t *testing.T) {
			level := waterFillLevel(tst.demands, tst.capacity)
			if math.IsInf(tst.expected, 1) {
				assert.True(t, math.IsInf(level, 1), "expected +Inf, got %v", level)
				return
			}
			assert.InDelta(t, tst.expected, level, 0.0001)
		})
	}
}

func TestPerKeyThroughputMaxTotalThroughput(t *testing.T) {
	tsts := []struct {
		name                     string
		perKeyThroughputPerSec   int
		maxTotalThroughputPerSec int
		inputCount               map[string]int
		expectedSavedSampleRates map[string]int
	}{
		{
			// the per-key demands sum to 250, well under the ceiling, so every rate
			// matches what the per-key target alone would produce
			name:                     "ceiling does not bind",
			perKeyThroughputPerSec:   100,
			maxTotalThroughputPerSec: 1000,
			inputCount:               map[string]int{"a": 50, "b": 150, "c": 300},
			expectedSavedSampleRates: map[string]int{"a": 1, "b": 1, "c": 3},
		},
		{
			// five keys all want 100 but only 250 is available, so each is lowered to 50
			name:                     "ceiling binds uniformly",
			perKeyThroughputPerSec:   100,
			maxTotalThroughputPerSec: 250,
			inputCount:               map[string]int{"a": 1000, "b": 1000, "c": 1000, "d": 1000, "e": 1000},
			expectedSavedSampleRates: map[string]int{"a": 20, "b": 20, "c": 20, "d": 20, "e": 20},
		},
		{
			// the three small keys fit under the level of 45 and keep everything, while
			// the two heavy keys are both lowered to that one shared level
			name:                     "ceiling binds on a long tail",
			perKeyThroughputPerSec:   100,
			maxTotalThroughputPerSec: 150,
			inputCount:               map[string]int{"s1": 10, "s2": 20, "s3": 30, "h1": 1000, "h2": 5000},
			expectedSavedSampleRates: map[string]int{"s1": 1, "s2": 1, "s3": 1, "h1": 23, "h2": 112},
		},
		{
			name:                     "ceiling below the per-key target with one key",
			perKeyThroughputPerSec:   100,
			maxTotalThroughputPerSec: 10,
			inputCount:               map[string]int{"a": 1000},
			expectedSavedSampleRates: map[string]int{"a": 100},
		},
		{
			// 55 spans against a ceiling of 10 needs a rate of 6, not the 5 that
			// rounding down would give
			name:                     "rate rounds up when the ceiling binds",
			perKeyThroughputPerSec:   100,
			maxTotalThroughputPerSec: 10,
			inputCount:               map[string]int{"a": 55},
			expectedSavedSampleRates: map[string]int{"a": 6},
		},
	}
	for _, tst := range tsts {
		t.Run(tst.name, func(t *testing.T) {
			p := &PerKeyThroughput{
				ClearFrequencyDuration:   time.Second,
				PerKeyThroughputPerSec:   tst.perKeyThroughputPerSec,
				MaxTotalThroughputPerSec: tst.maxTotalThroughputPerSec,
			}
			p.currentCounts = tst.inputCount
			p.updateMaps()
			assert.Equal(t, tst.expectedSavedSampleRates, p.savedSampleRates)
		})
	}
}

func TestPerKeyThroughputZeroCeilingMatchesNoCeiling(t *testing.T) {
	counts := map[string]int{"a": 50, "b": 150, "c": 300, "d": 12000}

	withCeiling := &PerKeyThroughput{
		ClearFrequencyDuration:   time.Second,
		PerKeyThroughputPerSec:   100,
		MaxTotalThroughputPerSec: 0,
	}
	withCeiling.currentCounts = maps.Clone(counts)
	withCeiling.updateMaps()

	withoutCeiling := &PerKeyThroughput{
		ClearFrequencyDuration: time.Second,
		PerKeyThroughputPerSec: 100,
	}
	withoutCeiling.currentCounts = maps.Clone(counts)
	withoutCeiling.updateMaps()

	assert.Equal(t, withoutCeiling.savedSampleRates, withCeiling.savedSampleRates)
}

func TestPerKeyThroughputCeilingHoldsAcrossManyKeys(t *testing.T) {
	// 500 keys each sending 199 against a ceiling of 50000 land on a level of 100,
	// giving a rate of 1.99 per key. Rounding that down would mean a rate of 1 and
	// no sampling at all, taking the output to 99500, nearly twice the ceiling.
	const (
		numKeys  = 500
		perKey   = 199
		maxTotal = 50000
	)
	p := &PerKeyThroughput{
		ClearFrequencyDuration:   time.Second,
		PerKeyThroughputPerSec:   1000,
		MaxTotalThroughputPerSec: maxTotal,
	}
	counts := make(map[string]int, numKeys)
	for i := 0; i < numKeys; i++ {
		counts["key"+strconv.Itoa(i)] = perKey
	}
	p.currentCounts = counts
	p.updateMaps()

	kept := 0.0
	for k, rate := range p.savedSampleRates {
		assert.Equal(t, 2, rate, "key %s", k)
		kept += float64(perKey) / float64(rate)
	}
	assert.Len(t, p.savedSampleRates, numKeys)
	assert.LessOrEqual(t, kept, float64(maxTotal))
}

func TestPerKeyThroughputMaxKeysExcludedFromCeiling(t *testing.T) {
	p := &PerKeyThroughput{
		ClearFrequencyDuration:   time.Second,
		PerKeyThroughputPerSec:   100,
		MaxTotalThroughputPerSec: 100,
		MaxKeys:                  2,
	}
	p.currentCounts = map[string]int{}
	p.savedSampleRates = map[string]int{}

	p.GetSampleRateMulti("a", 1000)
	p.GetSampleRateMulti("b", 1000)
	p.GetSampleRateMulti("c", 1000) // rejected, MaxKeys reached

	p.updateMaps()

	// only the two accepted keys contribute demand, so the level is 50 rather than
	// the 33 three keys would have produced
	assert.Equal(t, map[string]int{"a": 20, "b": 20}, p.savedSampleRates)
}

func TestPerKeyThroughputFractionalClearFrequency(t *testing.T) {
	tsts := []struct {
		name                     string
		clearFrequencyDuration   time.Duration
		perKeyThroughputPerSec   int
		maxTotalThroughputPerSec int
		inputCount               map[string]int
		expectedSavedSampleRates map[string]int
	}{
		{
			// 10/sec over 500ms is a goal of 5 for the interval
			name:                   "sub-second interval",
			clearFrequencyDuration: 500 * time.Millisecond,
			perKeyThroughputPerSec: 10,
			inputCount:             map[string]int{"a": 100},
			expectedSavedSampleRates: map[string]int{
				"a": 20,
			},
		},
		{
			// and a ceiling of 4/sec is a capacity of 2 for the interval
			name:                     "sub-second interval with a ceiling",
			clearFrequencyDuration:   500 * time.Millisecond,
			perKeyThroughputPerSec:   10,
			maxTotalThroughputPerSec: 4,
			inputCount:               map[string]int{"a": 100},
			expectedSavedSampleRates: map[string]int{"a": 50},
		},
		{
			// 10/sec over 1.5s is a goal of 15, not the 10 that truncating to whole
			// seconds would give
			name:                     "fractional interval above a second",
			clearFrequencyDuration:   1500 * time.Millisecond,
			perKeyThroughputPerSec:   10,
			inputCount:               map[string]int{"a": 150},
			expectedSavedSampleRates: map[string]int{"a": 10},
		},
	}
	for _, tst := range tsts {
		t.Run(tst.name, func(t *testing.T) {
			p := &PerKeyThroughput{
				ClearFrequencyDuration:   tst.clearFrequencyDuration,
				PerKeyThroughputPerSec:   tst.perKeyThroughputPerSec,
				MaxTotalThroughputPerSec: tst.maxTotalThroughputPerSec,
			}
			p.currentCounts = tst.inputCount
			p.updateMaps()
			assert.Equal(t, tst.expectedSavedSampleRates, p.savedSampleRates)
		})
	}
}

func TestPerKeyThroughputSetters(t *testing.T) {
	p := &PerKeyThroughput{
		ClearFrequencyDuration:   time.Second,
		PerKeyThroughputPerSec:   100,
		MaxTotalThroughputPerSec: 1000,
	}

	p.SetPerKeyThroughputPerSec(50)
	assert.Equal(t, 50, p.PerKeyThroughputPerSec)
	p.SetPerKeyThroughputPerSec(0)
	assert.Equal(t, 50, p.PerKeyThroughputPerSec)
	p.SetPerKeyThroughputPerSec(-1)
	assert.Equal(t, 50, p.PerKeyThroughputPerSec)

	p.SetMaxTotalThroughputPerSec(100)
	assert.Equal(t, 100, p.MaxTotalThroughputPerSec)
	p.SetMaxTotalThroughputPerSec(0)
	assert.Equal(t, 100, p.MaxTotalThroughputPerSec)
	p.SetMaxTotalThroughputPerSec(-1)
	assert.Equal(t, 100, p.MaxTotalThroughputPerSec)

	// the new values are the ones updateMaps uses: five keys sharing a ceiling of
	// 100 gives a level of 20, not the 200 the original ceiling would have allowed
	p.currentCounts = map[string]int{"a": 1000, "b": 1000, "c": 1000, "d": 1000, "e": 1000}
	p.updateMaps()
	assert.Equal(t, map[string]int{"a": 50, "b": 50, "c": 50, "d": 50, "e": 50}, p.savedSampleRates)
}
