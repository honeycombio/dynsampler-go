package dynsampler

import (
	"math/rand"
	"testing"
)

func randomFieldName() string {
	dictionary := []string{
		"http.response.status_code",
		"http.request.method",
		"http.request.route",
		"service.name",
	}
	return dictionary[rand.Intn(len(dictionary))]
}

func generateTestData(numKeys int) map[string]float64 {
	data := make(map[string]float64, numKeys)
	for i := 0; i < numKeys; i++ {
		key := randomFieldName()
		if key == "http.request.route" {
			data[key] = rand.Float64() * 100
		} else {
			data[key] = rand.Float64() * 10
		}
	}
	return data
}

func BenchmarkCalculateSampleRates(b *testing.B) {
	testCases := []struct {
		name    string
		numKeys int
	}{
		{"One_", 1},
		{"Two", 2},
		{"Three_", 3},
		{"Four_", 4},
		{"Five_", 5},
		{"Ten_", 10},
	}

	for _, tc := range testCases {
		testData := generateTestData(tc.numKeys)

		b.Run(tc.name+"Optimized", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				calculateSampleRates(0.1, testData)
			}
		})

		b.Run(tc.name+"Original", func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				calculateSampleRates_original(0.1, testData)
			}
		})
	}
}
