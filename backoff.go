package goose

import (
	"math"
	"math/rand"
	"time"
)

// backoff computes an exponentially increasing, jittered delay for retry loops
// (SSE reconnects, poll recovery). It uses equal jitter — half the exponential
// window is fixed and half is random — so delays never collapse to ~0 (which
// would hammer a recovering server) yet still spread a fleet of clients out to
// avoid a thundering herd. Delays are capped and reset to the base on success.
//
// backoff is not safe for concurrent use; each retry loop owns its own instance.
type backoff struct {
	base    time.Duration
	max     time.Duration
	attempt int
}

func newBackoff(base, max time.Duration) *backoff {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	return &backoff{base: base, max: max}
}

// reset returns the sequence to its base delay after a healthy interval.
func (b *backoff) reset() { b.attempt = 0 }

// duration returns the next delay and advances the sequence. The result is in
// [base, max]: it is the midpoint of the current exponential window plus up to
// that much again in jitter.
func (b *backoff) duration() time.Duration {
	window := float64(b.base) * math.Pow(2, float64(b.attempt))
	if window > float64(b.max) || math.IsInf(window, 1) {
		window = float64(b.max)
	} else {
		// Only keep climbing while we are below the cap.
		b.attempt++
	}
	half := window / 2
	jitter := 0.0
	if half >= 1 {
		jitter = float64(rand.Int63n(int64(half)))
	}
	d := time.Duration(half + jitter)
	return min(max(d, b.base), b.max)
}

// durationAtLeast returns the next backoff delay, but never shorter than min
// (used to honor a server's Retry-After).
func (b *backoff) durationAtLeast(min time.Duration) time.Duration {
	d := b.duration()
	if min > d {
		return min
	}
	return d
}
