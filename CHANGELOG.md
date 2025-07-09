# dynsampler-go changelog

## 0.6.3 2025-07-09

This version improves the performance of `GetMetrics` function by reducing repeated dynamic string operations for metric keys creation.

## What's Changed

### 💡 Enhancements

- perf: pre-compute metric keys by @VinozzZ in https://github.com/honeycombio/dynsampler-go/pull/81

### 🛠 Maintenance

- maint(deps): bump github.com/stretchr/testify from 1.8.4 to 1.9.0 by @dependabot in https://github.com/honeycombio/dynsampler-go/pull/77
- docs: update vulnerability reporting process by @robbkidd in https://github.com/honeycombio/dynsampler-go/pull/79
- maint(deps): bump github.com/stretchr/testify from 1.9.0 to 1.10.0 by @dependabot in https://github.com/honeycombio/dynsampler-go/pull/80
- maint: prepare for release v0.6.1 by @VinozzZ in https://github.com/honeycombio/dynsampler-go/pull/82
- fix: use a newer go version in publish_github step by @VinozzZ in https://github.com/honeycombio/dynsampler-go/pull/83
- fix: our Go executor takes a minor version as param by @robbkidd in https://github.com/honeycombio/dynsampler-go/pull/84
- maint: update release process by @VinozzZ in https://github.com/honeycombio/dynsampler-go/pull/85

## 0.6.1 & 0.6.2 2025-07-08

These versions are the result of updates to the release process. They can be safely ignored.

## 0.6.0 2024-01-12

This version tweaks Throughput samplers to permit calculating non-integer sample rates, which makes them choose better sample rates in many scenarios. It also fixes a race condition that was recently detected by an improved Go runtime.

### Fixes

- fix: allow throughput samplers to have non-integer rates (#74) | [Yi Zhao](https://github.com/yizzlez)
- fix: race condition in WindowedThroughput sampler (#73) | [Kent Quirk](https://github.com/KentQuirk)

## Maintenance

- maint: update codeowners to pipeline-team (#72) | [Jamie Danielson](https://github.com/JamieDanielson)
- maint: update project workflow for pipeline (#71) | [Jamie Danielson](https://github.com/JamieDanielson)
- maint: update codeowners to pipeline (#70) | [Jamie Danielson](https://github.com/JamieDanielson)


## 0.5.1 2023-06-26

This version corrects a math error in the EMAThroughput sampler.

### Fixes

- fix: Correct EMAThroughput math error (#67) | [Kent Quirk](https://github.com/kentquirk)

## 0.5.0 2023-06-08

This version extends the Sampler interface to include a new GetMetrics function,
which returns a collection of metrics relevant to that specific sampler. This
improves visibility into the sampler and will be used in an upcoming release of
Honeycomb's Refinery. This is a breaking change for code implemented so as to
conform to the `dynsampler.Sampler` interface, such as hand-coded mocks used for
testing. Code using the interface is unaffected.

### Features
- feat: Add metrics counter retrieval (#65) | [Kent Quirk](https://github.com/kentquirk)

### Maintenance
- maint(deps): bump github.com/stretchr/testify from 1.8.2 to 1.8.4 (#64) | [dependabot[bot]](https://github.com/dependabot[bot])
- maint: update dependabot.yml (#63) | [Vera Reynolds](https://github.com/vreynolds)

## 0.4.0 2023-03-22

This version contains two new samplers and some (backwards-compatible) changes to the API:
- Many thanks to [Yi Zhao](https://github.com/yizzlez) for the contribution of the `WindowedThroughput` sampler. This sampler is like the Throughput sampler, but uses a moving average to accumulate sample rates across multiple sampling periods.
- The new `EMAThroughput` sampler adjusts overall throughput to achieve a goal while also ensuring that all values in the key space are represented.
- The `GetSampleRateMulti()` function allows a single request to represent multiple events. This is particularly useful when tail-sampling at the trace level (because each trace represents a number of spans).
- All samplers now support specifying a `time.Duration` instead of a time in seconds. Fields like `ClearFrequencySec` are now deprecated and will be dropped in a future release.

⚠️ As of this version, dynsampler-go requires and is tested on versions of Go 1.17 and greater.

### Features

- feat: EMAThroughput sampler (#58) | [Kent Quirk](https://github.com/kentquirk)
- feat: Deprecate integer seconds and replace with time.Duration (#59) | [Kent Quirk](https://github.com/kentquirk)
- feat: add GetSampleRateMulti (#53) | [Kent Quirk](https://github.com/kentquirk)
- feat: Windowed Throughput Sampling (#45) | [Yi Zhao](https://github.com/yizzlez)
  - fix: Fix flaky blocklist test (#52) | [Yi Zhao](https://github.com/yizzlez)

### Maintenance

- maint: Pull out common calculation into a function (#57) | [Kent Quirk](https://github.com/kentquirk)
- maint: bump the go versions we support (#55) | [Kent Quirk](https://github.com/kentquirk)
- maint(deps): bump github.com/stretchr/testify from 1.6.1 to 1.8.2 (#49) | [dependabot[bot]](https://github.com/dependabot[bot])
- maint: remove buildevents from circle (#48) | [Jamie Danielson](https://github.com/JamieDanielson)
- chore: Update workflow (#47) | [Tyler Helmuth](https://github.com/TylerHelmuth)
- chore: Update CODEOWNERS (#46) | [Tyler Helmuth](https://github.com/TylerHelmuth)
- chore: update dependabot.yml (#44) | [Kent Quirk](https://github.com/kentquirk)

## 0.3.0 2022-12-07

⚠️ As of this version, dynsampler-go is only tested on Go 1.16 or greater.

### Maintenance

- maint: drop versions of go below 1.16 (#39) | @vreynolds
- maint: add go 1.18, 1.19 to CI (#30, #31) | @vreynolds
- maint: add go 1.16, 1.17 to CI (#28) | @MikeGoldsmith
- ... and a lot of project management stuff.
  [Details in the commits](https://github.com/honeycombio/dynsampler-go/compare/v0.2.1...0356ba0).

## 0.2.1 2019-08-07

Fixes

- Corrects some sample rate calculations in the Exponential Moving Averge for very small counts.

## 0.2.0 2019-07-31

Features

- Adds Exponential Moving Average (`EMASampleRate`) implementation with Burst Detection, based on the `AvgSampleRate` implementation. See docs for description.
- Adds `SaveState` and `LoadState` to interface to enable serialization of internal state for persistence between process restarts.

## 0.1.0 2019-05-22

Versioning introduced.
