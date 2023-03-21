package dynsampler

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOnlyOnceUpdateMaps(t *testing.T) {
	o := &OnlyOnce{
		ClearFrequencyDuration: 30 * time.Second,
	}
	tsts := []struct {
		inputSeen    map[string]bool
		expectedSeen map[string]bool
	}{
		{
			map[string]bool{
				"one":   true,
				"two":   true,
				"three": true,
			},
			map[string]bool{},
		},
		{
			map[string]bool{},
			map[string]bool{},
		},
	}
	for i, tst := range tsts {
		o.seen = tst.inputSeen
		o.updateMaps()
		assert.Equal(t, o.seen, tst.expectedSeen, fmt.Sprintf("test %d failed", i))
	}
}

func TestOnlyOnceGetSampleRate(t *testing.T) {
	o := &OnlyOnce{}
	o.seen = map[string]bool{
		"one": true,
		"two": true,
	}
	tsts := []struct {
		inputKey                         string
		expectedSampleRate               int
		expectedCurrentCountForKeyBefore bool
		expectedCurrentCountForKeyAfter  bool
	}{
		{"one", 1000000000, true, true},
		{"two", 1000000000, true, true},
		{"two", 1000000000, true, true},
		{"three", 1, false, true}, // key missing from seen
		{"three", 1000000000, true, true},
		{"four", 1, false, true}, // key missing from seen
		{"four", 1000000000, true, true},
	}
	for _, tst := range tsts {
		assert.Equal(t, o.seen[tst.inputKey], tst.expectedCurrentCountForKeyBefore)
		rate := o.GetSampleRate(tst.inputKey)
		assert.Equal(t, rate, tst.expectedSampleRate)
		assert.Equal(t, o.seen[tst.inputKey], tst.expectedCurrentCountForKeyAfter)
	}
}

func TestOnlyOnce_Start(t *testing.T) {
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
		{"negative sec", -1, 0, -1 * time.Second, false},
		{"negative dur", 0, -1 * time.Second, -1 * time.Second, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &OnlyOnce{
				ClearFrequencySec:      tt.ClearFrequencySec,
				ClearFrequencyDuration: tt.ClearFrequencyDuration,
			}
			err := a.Start()
			if (err != nil) != tt.wantErr {
				t.Errorf("OnlyOnce error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				defer a.Stop()
				if tt.wantDuration != a.ClearFrequencyDuration {
					t.Errorf("OnlyOnce duration mismatch = want %v, got %v", tt.wantDuration, a.ClearFrequencyDuration)
				}
			}
		})
	}
}
