package dynsampler

import (
	"time"
)

// WindowedThroughputCalculator is the timer-free computation core of
// WindowedThroughput. Each Update supplies one update-tick of counts; Rates
// aggregates the counts over the lookback window and splits the goal equally
// across the keys present, the same algorithm WindowedThroughput runs on its
// timer. Like EMAThroughputCalculator, it lets a caller drive the computation
// on its own schedule and over counts it supplies, for example counts merged
// across a fleet of processes, so identical inputs yield an identical table.
//
// WindowedThroughput is a timer-driven wrapper around this type. A calculator
// is not safe for concurrent use; callers serialize access.
type WindowedThroughputCalculator struct {
	// GoalThroughputPerSec is the target events per second. Default 100.
	GoalThroughputPerSec float64
	// UpdateFrequency is the wall-clock span each Update represents. Default 1s.
	UpdateFrequency time.Duration
	// LookbackFrequency is how far back Rates aggregates. It is floored to a
	// whole multiple of UpdateFrequency. Default is 30x UpdateFrequency.
	LookbackFrequency time.Duration

	// buckets is a ring of the last lookback ticks; pos is the next write slot.
	buckets []map[string]float64
	pos     int
}

func (c *WindowedThroughputCalculator) ensureInit() {
	if c.GoalThroughputPerSec == 0 {
		c.GoalThroughputPerSec = 100
	}
	if c.UpdateFrequency == 0 {
		c.UpdateFrequency = time.Second
	}
	if c.LookbackFrequency == 0 {
		c.LookbackFrequency = 30 * c.UpdateFrequency
	}
	// Floor the lookback to a whole multiple of the update frequency, matching
	// WindowedThroughput.
	c.LookbackFrequency = c.UpdateFrequency * (c.LookbackFrequency / c.UpdateFrequency)
	if c.buckets == nil {
		ticks := int(c.LookbackFrequency / c.UpdateFrequency)
		if ticks < 1 {
			ticks = 1
		}
		c.buckets = make([]map[string]float64, ticks)
	}
}

// Update installs one update-tick of counts, evicting the oldest tick once the
// lookback window is full. It copies the counts into its own bucket, so the
// caller is free to reuse or mutate the map afterwards.
func (c *WindowedThroughputCalculator) Update(counts map[string]float64) {
	c.ensureInit()
	bucket := make(map[string]float64, len(counts))
	for k, v := range counts {
		bucket[k] = v
	}
	c.buckets[c.pos] = bucket
	c.pos = (c.pos + 1) % len(c.buckets)
}

// Rates aggregates the lookback window and splits the goal equally across the
// keys seen, matching WindowedThroughput. It is empty when the window holds no
// traffic; keys absent from it have no learned rate and are the caller's to
// handle (typically a bootstrap rate).
func (c *WindowedThroughputCalculator) Rates() map[string]int {
	c.ensureInit()
	agg := make(map[string]float64)
	for _, b := range c.buckets {
		for k, v := range b {
			agg[k] += v
		}
	}
	return windowedSampleRates(agg, c.GoalThroughputPerSec, c.LookbackFrequency)
}
