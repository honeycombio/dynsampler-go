package dynsampler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Both EMA samplers announce a burst from GetSampleRateMulti with a
// non-blocking select, because the consumer goroutine takes the same lock the
// caller is holding. That makes the buffer load bearing: on an unbuffered
// channel the send only lands while the consumer is parked on its select, so a
// burst raised while it is mid updateMaps, or before it is first scheduled, is
// dropped. currentBurstSum is already reset and burstCount already incremented
// by that point, so the burst is counted and never acted on, and a caller
// waiting on the recalculation waits forever. That is the hang in #105.
func TestBurstSignalIsBuffered(t *testing.T) {
	t.Run("EMASampleRate", func(t *testing.T) {
		e := &EMASampleRate{AdjustmentIntervalDuration: time.Hour}
		require.NoError(t, e.Start())
		t.Cleanup(func() { require.NoError(t, e.Stop()) })
		assert.Equal(t, 1, cap(e.burstSignal),
			"burst signal must be buffered, or a non-blocking send drops the burst when the consumer is not parked")
	})

	t.Run("EMAThroughput", func(t *testing.T) {
		e := &EMAThroughput{AdjustmentInterval: time.Hour}
		require.NoError(t, e.Start())
		t.Cleanup(func() { require.NoError(t, e.Stop()) })
		assert.Equal(t, 1, cap(e.burstSignal),
			"burst signal must be buffered, or a non-blocking send drops the burst when the consumer is not parked")
	})
}
