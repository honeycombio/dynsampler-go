package dynsampler

import (
	"testing"

	"github.com/honeycombio/hound/test"
)

func TestStaticGetSampleRate(t *testing.T) {
	s := &Static{
		Rates: map[string]int{
			"one": 5,
			"two": 10,
		},
		Default: 3,
	}
	test.Equals(t, s.GetSampleRate("one"), 5)
	test.Equals(t, s.GetSampleRate("two"), 10)
	test.Equals(t, s.GetSampleRate("three"), 3)

}
