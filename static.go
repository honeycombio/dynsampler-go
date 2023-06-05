package dynsampler

import "sync"

// Static implements Sampler with a static mapping for sample rates. This is
// useful if you have a known set of keys that you want to sample at specific
// rates and apply a default to everything else.
type Static struct {
	// Rates is the set of sample rates to use
	Rates map[string]int
	// Default is the value to use if the key is not whitelisted in Rates
	Default int

	lock sync.Mutex

	// metrics
	requestCount int64
	eventCount   int64
}

// Ensure we implement the sampler interface
var _ Sampler = (*Static)(nil)

// Start initializes the static dynsampler
func (s *Static) Start() error {
	if s.Default == 0 {
		s.Default = 1
	}
	return nil
}

func (s *Static) Stop() error {
	return nil
}

// GetSampleRate takes a key and returns the appropriate sample rate for that
// key.
func (s *Static) GetSampleRate(key string) int {
	return s.GetSampleRateMulti(key, 1)
}

// GetSampleRateMulti takes a key representing count spans and returns the
// appropriate sample rate for that key.
func (s *Static) GetSampleRateMulti(key string, count int) int {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.requestCount++
	s.eventCount += int64(count)
	if rate, found := s.Rates[key]; found {
		return rate
	}
	return s.Default
}

// SaveState is not implemented
func (s *Static) SaveState() ([]byte, error) {
	return nil, nil
}

// LoadState is not implemented
func (s *Static) LoadState(state []byte) error {
	return nil
}

func (s *Static) GetMetrics(prefix string) map[string]int64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	mets := map[string]int64{
		prefix + "_request_count": s.requestCount,
		prefix + "_event_count":   s.eventCount,
		prefix + "_keyspace_size": int64(len(s.Rates)),
	}
	return mets
}
