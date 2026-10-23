# dynsampler-go

[![OSS Lifecycle](https://img.shields.io/osslifecycle/honeycombio/dynsampler-go?color=success)](https://github.com/honeycombio/home/blob/main/honeycomb-oss-lifecycle-and-practices.md)

Dynsampler is a golang library for doing dynamic sampling of traffic before sending it on to [Honeycomb](https://honeycomb.io) (or another analytics system)
It contains several sampling algorithms to help you select a representative set of events instead of a full stream.

A "sample rate" of 100 means that for every 100 requests, we capture a single event and indicate that it represents 100 similar requests.

For full documentation, look at the [official documentation](https://pkg.go.dev/github.com/honeycombio/dynsampler-go).

For more information about using Honeycomb, see our [docs](https://honeycomb.io/docs).

## Sampling Techniques

This package is intended to help sample a stream of tracking events, where events are typically created in response to a stream of traffic (for the purposes of logging or debugging). In general, sampling is used to reduce the total volume of events necessary to represent the stream of traffic in a meaningful way.

There are a variety of available techniques for reducing a high-volume stream of incoming events to a lower-volume, more manageable stream of events.
Depending on the shape of your traffic, one may serve better than another, or you may need to write a new one! Please consider contributing it back to this package if you do.

* If your system has a completely homogeneous stream of requests: use `Static` sampling to use a constant sample rate.
* If your system has a steady stream of requests and a well-known low cardinality partition key (e.g. http status): use `Static` sampling and override sample rates on a per-key basis (e.g. if you know want to sample `HTTP 200/OK` events at a different rate from `HTTP 503/Server Error`).
* If your logging system has a strict cap on the rate it can receive events, use `TotalThroughput`, which will calculate sample rates based on keeping *the entire system's* representative event throughput right around (or under) particular cap.
* If you need a throughput sampler that is responsive to spikes, but also averages sample rates over a longer period of time, use `WindowedThroughput`.
* If your system has a rough cap on the rate it can receive events and your partitioned keyspace is fairly steady, use `PerKeyThroughput`, which will calculate sample rates based on keeping the event throughput roughly constant *per key/partition* (e.g. per user id)
* The best choice for a system with a large key space and a large disparity between the highest volume and lowest volume keys is `AvgSampleWithMin` - it will increase the sample rate of higher volume traffic proportionally to the logarithm of the specific key's volume. If total traffic falls below a configured minimum, it stops sampling to avoid any sampling when the traffic is too low to warrant it.
* `EMASampleRate` works like `AvgSampleRate`, but calculates sample rates based on a moving average (Exponential Moving Average) of many measurement intervals rather than a single isolated interval. In addition, it can detect large bursts in traffic and will trigger a recalculation of sample rates before the regular interval.
* If you want the benefit of a key-based sampler that also has limits on throughput, use `EMAThroughput`. It will adjust sample rates across a key space to achieve a given throughput while still ensuring that all keys are represented.

## Sampler behaviour

Picking a rate for a key the sampler already knows about is only part of what a sampler does. Three other situations come up in practice, and the adaptive samplers do not all answer them the same way.

| Sampler | Before the first recalculation | A key it has not seen before | A key rejected at `MaxKeys` |
|---|---|---|---|
| `AvgSampleRate` | `GoalSampleRate` | 1 | `OverflowSampleRate`, or 1 |
| `AvgSampleWithMin` | `GoalSampleRate` | 1 | `OverflowSampleRate`, or 1 |
| `EMASampleRate` | `GoalSampleRate` | 1 | `OverflowSampleRate`, or 1 |
| `EMAThroughput` | `InitialSampleRate` (default 10) | 1 | `OverflowSampleRate`, or 1 |
| `TotalThroughput` | 1 | 1 | `OverflowSampleRate`, or 1 |
| `PerKeyThroughput` | 1 | 1 | `OverflowSampleRate`, or 1 |
| `WindowedThroughput` | 0 | 0 | `OverflowSampleRate`, or 0 |

`Static` and `OnlyOnce` are not in the table because they do not adapt. `Static` returns its `Default` for any key it has no rate for, and `OnlyOnce` keeps the first observation of a key and returns a very large rate for every one after it.

### Before the first recalculation

A sampler cannot produce rates until it has seen an interval of traffic. The samplers that target a sample rate already know the answer, because the goal is itself a rate, so they return `GoalSampleRate` and need no configuration for it.

The samplers that target a throughput cannot, because a volume goal only becomes a rate once there is volume to divide. `EMAThroughput` takes an explicit `InitialSampleRate` for this. `TotalThroughput` and `PerKeyThroughput` have no equivalent and keep everything until their first recalculation, so a process that starts under load sends its first interval unsampled.

### A key it has not seen before

Once a sampler has rates, a key that is not among them is kept, because a new key has no measured volume to sample against. It gets a real rate at the next recalculation. This is usually what you want, since a genuinely rare key stays at 1 and a key arriving in volume is sampled from the second interval onward.

`WindowedThroughput` instead returns 0, meaning it has no opinion, and expects the caller to substitute a rate of their own. Treat a 0 as "decide for yourself" rather than as a rate, because using it directly keeps every event.

### A key rejected at `MaxKeys`

`MaxKeys` bounds how many distinct keys a sampler tracks within an interval. Once that many have been seen, further new keys are rejected. A rejected key is not counted, so it does not affect the rates computed for the keys that were accepted, and it is also outside whatever goal the sampler is working to. Total output can exceed the goal for as long as keys are being rejected.

Setting `OverflowSampleRate` gives rejected keys a known rate instead of keeping them, so a cardinality explosion degrades towards the goal rather than flooding. It defaults to 0, which preserves the older keep-everything behaviour.

The keyspace is cleared at every recalculation, so what matters is the number of distinct keys within one interval rather than the number your traffic produces overall.

### Burst detection

`EMASampleRate` and `EMAThroughput` watch incoming volume between recalculations and trigger an early one when it exceeds `BurstMultiple` times the running average, after `BurstDetectionDelay` intervals have passed. Every observation counts towards that, including keys rejected at `MaxKeys`, since a flood arriving on new keys is one of the cases an early recalculation is for.

The other samplers recalculate only on their own schedule, so traffic that arrives just after an interval opens is sampled at the previous interval's rates until it closes. Shortening the interval reduces that exposure at the cost of recalculating across the whole keyspace more often.
