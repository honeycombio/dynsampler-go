package dynsampler

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
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
	// UpdateFrequency is the wall-clock span each Update represents. Default
	// 1s. Read at first use (the first Update or Rates call); changes made
	// afterwards have no effect.
	UpdateFrequency time.Duration
	// LookbackFrequency is how far back Rates aggregates. It is floored to a
	// whole multiple of UpdateFrequency, and raised to UpdateFrequency if
	// shorter than it. Default is 30x UpdateFrequency. Read at first use (the
	// first Update or Rates call); changes made afterwards have no effect.
	LookbackFrequency time.Duration
	// MaxKeys caps the number of distinct keys tracked across the current
	// window. 0 (the default) is unbounded. A key already in the window is
	// always kept; when full, brand-new keys are admitted in sorted key order
	// as evicted buckets free slots, and the rest are dropped for that tick.
	MaxKeys int

	// buckets is a ring of the last lookback ticks; pos is the next write slot.
	buckets []map[string]float64
	pos     int
	// lookback is the effective LookbackFrequency snapshotted at ring-creation
	// time, so a later mutation of the exported field can't reprice a
	// already-sized ring.
	lookback time.Duration
	// keyRefs counts, across all buckets currently in the ring, how many
	// buckets each key appears in. A key's slot is freed once its last
	// referencing bucket is evicted, which is what lets MaxKeys admit a new
	// key after age-out.
	keyRefs map[string]int
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
	if c.LookbackFrequency < c.UpdateFrequency {
		c.LookbackFrequency = c.UpdateFrequency
	}
	// Floor the lookback to a whole multiple of the update frequency, matching
	// WindowedThroughput.
	c.LookbackFrequency = c.UpdateFrequency * (c.LookbackFrequency / c.UpdateFrequency)
	if c.buckets == nil {
		c.lookback = c.LookbackFrequency
		ticks := int(c.lookback / c.UpdateFrequency)
		if ticks < 1 {
			ticks = 1
		}
		c.buckets = make([]map[string]float64, ticks)
	}
	if c.keyRefs == nil {
		c.keyRefs = make(map[string]int)
	}
}

// Update installs one update-tick of counts, evicting the oldest tick once the
// lookback window is full. It copies the counts into its own bucket, skipping
// any entry whose value is not finite and positive, so the caller is free to
// reuse or mutate the map afterwards.
func (c *WindowedThroughputCalculator) Update(counts map[string]float64) {
	c.ensureInit()
	bucket := make(map[string]float64, len(counts))
	if c.MaxKeys == 0 {
		for k, v := range counts {
			if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			bucket[k] = v
		}
		c.push(bucket)
		return
	}
	// MaxKeys > 0: a key already in the window is always admitted; brand-new
	// keys are admitted in sorted order while the distinct-key count stays
	// under the cap, so two calculators fed identical counts admit the
	// identical subset on overflow, regardless of map iteration order.
	var newKeys []string
	for k, v := range counts {
		if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		if c.keyRefs[k] > 0 {
			bucket[k] = v
			continue
		}
		newKeys = append(newKeys, k)
	}
	sort.Strings(newKeys)
	distinct := len(c.keyRefs)
	for _, k := range newKeys {
		if distinct >= c.MaxKeys {
			break
		}
		bucket[k] = counts[k]
		distinct++
	}
	c.push(bucket)
}

// push installs a bucket at the current ring position and advances it. The
// caller owns bucket; push does not copy it. It updates keyRefs first,
// dropping refs for every key in the bucket being evicted, then adding refs
// for every key in the incoming bucket, so keyRefs always reflects the keys
// present across the ring after the call.
func (c *WindowedThroughputCalculator) push(bucket map[string]float64) {
	for k := range c.buckets[c.pos] {
		c.keyRefs[k]--
		if c.keyRefs[k] <= 0 {
			delete(c.keyRefs, k)
		}
	}
	for k := range bucket {
		c.keyRefs[k]++
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
	return windowedSampleRates(agg, c.GoalThroughputPerSec, c.lookback)
}

// windowedThroughputCalculatorKind identifies a WindowedThroughputCalculator
// blob, so a blob from a different type (or no kind at all, as with `null`)
// is rejected by LoadState rather than silently wiping state.
const windowedThroughputCalculatorKind = "windowed_throughput_calculator"

// windowedThroughputCalculatorVersion is the current wire format version for
// WindowedThroughputCalculator blobs.
const windowedThroughputCalculatorVersion = 1

// windowedThroughputCalculatorState is the JSON wire format for
// WindowedThroughputCalculator.SaveState/LoadState. Buckets are ordered
// oldest-to-newest, independent of the ring's internal write position. Kind
// and Version guard against loading a blob from a different type or format;
// UpdateFrequency and LookbackFrequency guard against loading into a
// calculator configured with a different timing, which would reprice the
// window against the wrong span.
type windowedThroughputCalculatorState struct {
	Kind              string               `json:"kind"`
	Version           int                  `json:"version"`
	UpdateFrequency   time.Duration        `json:"update_frequency"`
	LookbackFrequency time.Duration        `json:"lookback_frequency"`
	Buckets           []map[string]float64 `json:"buckets"`
}

// SaveState serializes the calculator's window of ticks, oldest-to-newest,
// along with its kind, version, and timing config, to a byte blob. Not safe
// for concurrent use with Update/Rates; callers serialize access.
//
// The blob is O(window length x distinct keys per tick); it's meant for
// periodic checkpointing, not per-decision use.
func (c *WindowedThroughputCalculator) SaveState() ([]byte, error) {
	c.ensureInit()
	buckets := make([]map[string]float64, 0, len(c.buckets))
	for i := 0; i < len(c.buckets); i++ {
		b := c.buckets[(c.pos+i)%len(c.buckets)]
		if b == nil {
			continue
		}
		buckets = append(buckets, b)
	}
	return json.Marshal(windowedThroughputCalculatorState{
		Kind:              windowedThroughputCalculatorKind,
		Version:           windowedThroughputCalculatorVersion,
		UpdateFrequency:   c.UpdateFrequency,
		LookbackFrequency: c.lookback,
		Buckets:           buckets,
	})
}

// LoadState restores the calculator's window from a blob produced by
// SaveState, replacing the current state wholesale. It rejects a blob that
// isn't a WindowedThroughputCalculator blob (wrong kind, including `null`),
// a different wire version, one saved under a different UpdateFrequency or
// LookbackFrequency than this calculator is effectively configured with (the
// window would be repriced against the wrong span), or one whose bucket
// count exceeds this calculator's ring length (which a blob with matching
// timing cannot legitimately produce, so this catches a corrupt or tampered
// blob). Saved buckets replay oldest-first through the ring, via push, which
// rebuilds keyRefs as a side effect. On error the calculator's existing state
// is left untouched. Not safe for concurrent use with Update/Rates; callers
// serialize access.
//
// A loaded blob may legitimately hold more distinct keys than MaxKeys (for
// example it was saved by an unbounded or larger-cap calculator); LoadState
// never rejects already-loaded data on that basis. MaxKeys only governs new
// key admissions in Update after the load.
func (c *WindowedThroughputCalculator) LoadState(state []byte) error {
	var s windowedThroughputCalculatorState
	if err := json.Unmarshal(state, &s); err != nil {
		return err
	}
	if s.Kind != windowedThroughputCalculatorKind {
		return fmt.Errorf("dynsampler: cannot load state: expected kind %q, got %q", windowedThroughputCalculatorKind, s.Kind)
	}
	if s.Version != windowedThroughputCalculatorVersion {
		return fmt.Errorf("dynsampler: cannot load state: expected version %d, got %d", windowedThroughputCalculatorVersion, s.Version)
	}
	c.ensureInit()
	if s.UpdateFrequency != c.UpdateFrequency {
		return fmt.Errorf("dynsampler: cannot load state: blob UpdateFrequency %v does not match calculator's %v", s.UpdateFrequency, c.UpdateFrequency)
	}
	if s.LookbackFrequency != c.lookback {
		return fmt.Errorf("dynsampler: cannot load state: blob LookbackFrequency %v does not match calculator's %v", s.LookbackFrequency, c.lookback)
	}
	if len(s.Buckets) > len(c.buckets) {
		return fmt.Errorf("dynsampler: cannot load state: blob has %d buckets, more than this calculator's ring length %d", len(s.Buckets), len(c.buckets))
	}
	c.buckets = make([]map[string]float64, len(c.buckets))
	c.pos = 0
	c.keyRefs = make(map[string]int)
	for _, b := range s.Buckets {
		if b == nil {
			b = make(map[string]float64)
		}
		c.push(b)
	}
	return nil
}
