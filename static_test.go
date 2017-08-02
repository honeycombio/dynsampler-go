package dynsampler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStaticGetSampleRate(t *testing.T) {
	s := &Static{
		Rates: map[string]int{
			"one": 5,
			"two": 10,
		},
		Default: 3,
	}
	assert.Equal(t, s.GetSampleRate("one"), 5)
	assert.Equal(t, s.GetSampleRate("two"), 10)
	assert.Equal(t, s.GetSampleRate("three"), 3)

}
