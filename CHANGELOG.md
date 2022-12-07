# dynsampler-go changelog

## 0.3.0 2022-12-07

### ⚠️ Breaking Changes ⚠️

- Minimum Go version required is 1.16

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
