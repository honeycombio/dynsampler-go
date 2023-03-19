package dynsampler

import (
	"math"
	mrand "math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateEMAThroughput(t *testing.T) {
	e := &EMAThroughput{
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

func TestEMAThroughputSampleGetSampleRateStartup(t *testing.T) {
	e := &EMAThroughput{
		InitialSampleRate: 10,
		currentCounts:     map[string]float64{},
	}
	rate := e.GetSampleRate("key")
	assert.Equal(t, rate, 10)
	assert.Equal(t, e.currentCounts["key"], float64(1))
}

func TestEMAThroughputSampleUpdateMapsSparseCounts(t *testing.T) {
	e := &EMAThroughput{
		GoalThroughputPerSec: 10,
		AdjustmentInterval:   1 * time.Second,
		Weight:               0.2,
		AgeOutValue:          0.2,
	}

	e.movingAverage = make(map[string]float64)
	e.savedSampleRates = make(map[string]int)

	for i := 0; i <= 100; i++ {
		input := make(map[string]float64)
		// simulate steady stream of input from one key
		input["largest_count"] = 40
		// sporadic keys with single counts that come and go with each interval
		for j := 0; j < 5; j++ {
			key := randomString(8)
			input[key] = 1
		}
		e.currentCounts = input
		e.updateMaps()
	}
	assert.Equal(t, 4, e.savedSampleRates["largest_count"])
}

func TestEMAThroughputAgesOutSmallValues(t *testing.T) {
	e := &EMAThroughput{
		GoalThroughputPerSec: 10,
		AdjustmentInterval:   1 * time.Second,
		Weight:               0.2,
		AgeOutValue:          0.2,
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

func TestEMAThroughputBurstDetection(t *testing.T) {
	// Set the adjustment interval very high so that we never run the regular interval
	e := &EMAThroughput{AdjustmentInterval: 1 * time.Hour}
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

func TestEMAThroughputUpdateMapsRace(t *testing.T) {
	e := &EMAThroughput{AdjustmentInterval: 1 * time.Hour}
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

func TestEMAThroughputSampleRateSaveState(t *testing.T) {
	var sampler Sampler
	esr := &EMAThroughput{}
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
	esr2 := &EMAThroughput{}
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

// This is a long test that generates a lot of random data and run it through the sampler
// The goal is to determine if we actually hit the specified target throughput (within a tolerance) an acceptable
// number of times. Most of the time, the throughput of observations kept should be close
// to the target rate.
func TestEMAThroughputSampleRateHitsTargetRate(t *testing.T) {
	mrand.Seed(time.Now().Unix())
	testThroughputs := []int{100, 1000}
	testKeyCount := []int{10, 30}
	toleranceFraction := float64(0.2)

	for _, throughput := range testThroughputs {
		tolerance := float64(throughput) * toleranceFraction
		toleranceUpper := float64(throughput) + tolerance
		toleranceLower := float64(throughput) - tolerance

		for _, keyCount := range testKeyCount {
			sampler := &EMAThroughput{
				AdjustmentInterval:   1 * time.Second,
				GoalThroughputPerSec: throughput,
				Weight:               0.5,
				AgeOutValue:          0.5,
				currentCounts:        make(map[string]float64),
				movingAverage:        make(map[string]float64),
			}

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

			// build an initial set of sample values so we don't just return the target
			sampler.updateMaps()

			var success int

			grandTotalKept := 0
			// each tick is 1 second
			for i := 0; i < 100; i++ {
				totalKeptObservations := 0
				for j, key := range keys {
					base := math.Pow10(j%3 + 1)
					count := float64(((j%10)+1))*base + float64(mrand.Intn(int(base)))
					for k := 0; k < int(count); k++ {
						rate := sampler.GetSampleRate(key)
						if mrand.Intn(rate) == 0 {
							totalKeptObservations++
						}
					}
				}
				grandTotalKept += totalKeptObservations

				if totalKeptObservations <= int(toleranceUpper) && totalKeptObservations >= int(toleranceLower) {
					success++
				}
				sampler.updateMaps()
			}
			assert.GreaterOrEqual(t, grandTotalKept, throughput*90, "totalKept too low: %d expected: %d\n", grandTotalKept, throughput*100)
			assert.LessOrEqual(t, grandTotalKept, throughput*110, "totalKept too high: %d expected: %d\n", grandTotalKept, throughput*100)

			assert.True(t, success >= 90, "target throughput test %d with key count %d failed with success rate of %d%%", throughput, keyCount, success)
		}
	}
}
