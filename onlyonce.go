package dynsampler

import (
	"fmt"
	"sync"
	"time"
)

// OnlyOnce implements Sampler and returns a sample rate of 1 the first time a
// key is seen and 1,000,000,000 every subsequent time.  Essentially, this means
// that every key will be reported the first time it's seen during each
// ClearFrequencySec and never again.  Set ClearFrequencySec to a negative
// number to report each key only once for the life of the process.
//
// (Note that it's not guaranteed that each key will be reported exactly once,
// just that the first seen event will be reported and subsequent events are
// unlikely to be reported. It is probable that an additional event will be
// reported for every billion times the key appears.)
//
// This emulates what you might expect from something catching stack traces -
// the first one is important but every subsequent one just repeats the same
// information.
type OnlyOnce struct {
	// DEPRECATED -- use ClearFrequencyDuration.
	// ClearFrequencySec is how often the counters reset in seconds.
	ClearFrequencySec int

	// ClearFrequencyDuration is how often the counters reset as a Duration.
	// Note that either this or ClearFrequencySec can be specified, but not both.
	// If neither one is set, the default is 30s.
	ClearFrequencyDuration time.Duration

	seen map[string]bool
	done chan struct{}

	// metrics
	requestCount int64
	eventCount   int64

	lock sync.Mutex
}

// Ensure we implement the sampler interface
var _ Sampler = (*OnlyOnce)(nil)

// Start initializes the static dynsampler
func (o *OnlyOnce) Start() error {
	if o.ClearFrequencyDuration != 0 && o.ClearFrequencySec != 0 {
		return fmt.Errorf("the ClearFrequencySec configuration value is deprecated; use only ClearFrequencyDuration")
	}

	if o.ClearFrequencyDuration == 0 && o.ClearFrequencySec == 0 {
		o.ClearFrequencyDuration = 30 * time.Second
	} else if o.ClearFrequencySec != 0 {
		o.ClearFrequencyDuration = time.Duration(o.ClearFrequencySec) * time.Second
	}

	// if it's negative, we don't even start something
	if o.ClearFrequencyDuration < 0 {
		return nil
	}

	o.seen = make(map[string]bool)
	o.done = make(chan struct{})

	// spin up calculator
	go func() {
		ticker := time.NewTicker(o.ClearFrequencyDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				o.updateMaps()
			case <-o.done:
				return
			}
		}
	}()
	return nil
}

func (o *OnlyOnce) Stop() error {
	if o.done != nil {
		close(o.done)
	}
	return nil
}

func (o *OnlyOnce) updateMaps() {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.seen = make(map[string]bool)
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (o *OnlyOnce) GetSampleRate(key string) int {
	return o.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (o *OnlyOnce) GetSampleRateMulti(key string, count int) int {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.requestCount++
	o.eventCount += int64(count)

	if _, found := o.seen[key]; found {
		return 1000000000
	}
	o.seen[key] = true
	return 1
}

// SaveState is not implemented
func (o *OnlyOnce) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (o *OnlyOnce) LoadState(state []byte) error {
	return nil
}

func (o *OnlyOnce) GetMetrics(prefix string) map[string]int64 {
	o.lock.Lock()
	defer o.lock.Unlock()
	mets := map[string]int64{
		prefix + "request_count": o.requestCount,
		prefix + "event_count":   o.eventCount,
		prefix + "keyspace_size": int64(len(o.seen)),
	}
	return mets
}
