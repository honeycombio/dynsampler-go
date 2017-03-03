package dynsampler

import (
	"fmt"
	"testing"

	"github.com/honeycombio/hound/test"
)

func TestOnlyOnceUpdateMaps(t *testing.T) {
	o := &OnlyOnce{
		ClearFrequencySec: 30,
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
		test.Equals(t, o.seen, tst.expectedSeen, fmt.Sprintf("test %d failed", i))
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
		test.Equals(t, o.seen[tst.inputKey], tst.expectedCurrentCountForKeyBefore)
		rate := o.GetSampleRate(tst.inputKey)
		test.Equals(t, rate, tst.expectedSampleRate)
		test.Equals(t, o.seen[tst.inputKey], tst.expectedCurrentCountForKeyAfter)
	}
}
