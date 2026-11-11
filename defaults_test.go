package dynsampler_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honeycombio/dynsampler-go"
)

type defaulter interface {
	dynsampler.Sampler
	ApplyDefaults() error
}

// exportedFields returns a sampler's exported fields, which are its
// configuration, leaving out its internal state.
func exportedFields(s any) map[string]any {
	v := reflect.ValueOf(s).Elem()
	fields := make(map[string]any)
	for i := 0; i < v.NumField(); i++ {
		if f := v.Type().Field(i); f.IsExported() {
			fields[f.Name] = v.Field(i).Interface()
		}
	}
	return fields
}

// TestApplyDefaultsMatchesStart checks that ApplyDefaults leaves a sampler
// configured exactly as Start does, and that a sampler defaulted first then
// started ends up the same. A caller that reads the configuration after
// ApplyDefaults relies on both.
func TestApplyDefaultsMatchesStart(t *testing.T) {
	tests := []struct {
		name string
		new  func() defaulter
	}{
		{"AvgSampleRate", func() defaulter { return &dynsampler.AvgSampleRate{} }},
		{"AvgSampleRate/ClearFrequencySec", func() defaulter { return &dynsampler.AvgSampleRate{ClearFrequencySec: 5} }},
		{"AvgSampleWithMin", func() defaulter { return &dynsampler.AvgSampleWithMin{} }},
		{"AvgSampleWithMin/ClearFrequencySec", func() defaulter { return &dynsampler.AvgSampleWithMin{ClearFrequencySec: 5} }},
		{"EMASampleRate", func() defaulter { return &dynsampler.EMASampleRate{} }},
		{"EMASampleRate/AdjustmentInterval", func() defaulter { return &dynsampler.EMASampleRate{AdjustmentInterval: 5, Weight: 0.3} }},
		{"EMAThroughput", func() defaulter { return &dynsampler.EMAThroughput{} }},
		{"OnlyOnce", func() defaulter { return &dynsampler.OnlyOnce{} }},
		{"OnlyOnce/ClearFrequencySec", func() defaulter { return &dynsampler.OnlyOnce{ClearFrequencySec: 5} }},
		{"PerKeyThroughput", func() defaulter { return &dynsampler.PerKeyThroughput{} }},
		{"PerKeyThroughput/ClearFrequencySec", func() defaulter { return &dynsampler.PerKeyThroughput{ClearFrequencySec: 5} }},
		{"Static", func() defaulter { return &dynsampler.Static{} }},
		{"TotalThroughput", func() defaulter { return &dynsampler.TotalThroughput{} }},
		{"TotalThroughput/ClearFrequencySec", func() defaulter { return &dynsampler.TotalThroughput{ClearFrequencySec: 5} }},
		{"WindowedThroughput", func() defaulter { return &dynsampler.WindowedThroughput{} }},
		{"WindowedThroughput/LookbackFloored", func() defaulter {
			return &dynsampler.WindowedThroughput{UpdateFrequencyDuration: 2 * time.Second, LookbackFrequencyDuration: 95 * time.Second}
		}},
		{"WindowedThroughput/LookbackShorterThanWindow", func() defaulter {
			return &dynsampler.WindowedThroughput{UpdateFrequencyDuration: 2 * time.Second, LookbackFrequencyDuration: time.Second}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			started := tt.new()
			require.NoError(t, started.Start())
			defer started.Stop()

			defaulted := tt.new()
			require.NoError(t, defaulted.ApplyDefaults())
			assert.Equal(t, exportedFields(started), exportedFields(defaulted))

			require.NoError(t, defaulted.Start())
			defer defaulted.Stop()
			assert.Equal(t, exportedFields(started), exportedFields(defaulted))
		})
	}
}

// TestWindowedThroughputLookbackShorterThanWindow checks that a lookback shorter
// than one update window takes the default rather than flooring to zero, which
// would leave the sampler with no window to compute a rate over.
func TestWindowedThroughputLookbackShorterThanWindow(t *testing.T) {
	s := &dynsampler.WindowedThroughput{UpdateFrequencyDuration: 2 * time.Second, LookbackFrequencyDuration: time.Second}
	require.NoError(t, s.ApplyDefaults())
	assert.Equal(t, 60*time.Second, s.LookbackFrequencyDuration)
}
