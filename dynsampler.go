package dynsampler

// Sampler is the interface to samplers using different methods to determine
// sample rate. You should instantiate one of the actual samplers in this
// package, depending on the sample method you'd like to use. Each sampling
// method has its own set of struct variables you should set before Start()ing
// the sampler.
type Sampler interface {
	// Start initializes the sampler. You should call Start() before using the
	// sampler.
	Start() error

	// Stop halts the sampler and any background goroutines
	Stop() error

	// GetSampleRate will return the sample rate to use for the given key
	// string. You should call it with whatever key you choose to use to
	// partition traffic into different sample rates. It assumes that you're
	// calling it for a single item to be sampled (typically a span from a
	// trace), and simply calls GetSampleRateMulti with 1 for the second
	// parameter.
	GetSampleRate(string) int

	// GetSampleRateMulti will return the sample rate to use for the given key
	// string. You should call it with whatever key you choose to use to
	// partition traffic into different sample rates. It assumes you're calling
	// it for a group of samples. The second parameter is the number of samples
	// this call represents.
	GetSampleRateMulti(string, int) int

	// SaveState returns a byte array containing the state of the Sampler implementation.
	// It can be used to persist state between process restarts.
	SaveState() ([]byte, error)

	// LoadState accepts a byte array containing the serialized, previous state of the sampler
	// implementation. It should be called before `Start`.
	LoadState([]byte) error

	// GetMetrics returns a map of metrics about the sampler's performance.
	// All values are returned as int64; counters are cumulative and the names
	// always end with "_count", while gauges are instantaneous with no particular naming convention.
	// All names are prefixed with the given string.
	GetMetrics(prefix string) map[string]int64
}

// metrics suffixes for the sampler
const (
	requestCountSuffix  = "request_count"
	eventCountSuffix    = "event_count"
	keyspaceSizeSuffix  = "keyspace_size"
	burstCountSuffix    = "burst_count"
	intervalCountSuffix = "interval_count"
)
