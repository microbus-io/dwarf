/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package pipeline supplies one shard's step candidates, in two independently paced loops:
//
//	Tallier    tallying                        -> planner.Tally
//	Supplier   planning -> fetching -> pushing  -> the candidate cache the workers drain
//
// They meet only at the planner, and share nothing else. The Tallier publishes what this shard has due;
// the Supplier reads the fleet-wide plan that follows from every shard's report, resolves its own slice of
// it to steps, and pushes them. Keeping them apart is what keeps the scan - whose cost grows with the
// backlog - off the path that feeds the workers.
//
// Neither type runs a loop or holds a goroutine. Each Cycle paces itself - it sleeps at the FRONT, for
// whatever remains of the interval since that loop's last cycle began - so a caller drives each in a tight
// loop of its own and holds no cadence policy:
//
//	for {
//	    select {
//	    case <-ctx.Done():
//	        return
//	    default:
//	    }
//	    observe(t.Cycle(ctx))
//	}
//
// Any delay the caller adds between calls is therefore absorbed rather than added to the period.
//
// Neither Cycle ever returns an error, only a result carrying one: every failure either can hit is already
// dealt with here, and the next cycle retries. A caller reads the result for its log line and carries on.
//
// SetInterval and SetMinGap are live on both and safe to call from anywhere; a Cycle itself is not safe
// for concurrent use - one Tallier and one Supplier per shard, each driven by one goroutine.
package pipeline

import (
	"context"
	"sync/atomic"
	"time"
)

const (
	// DefaultInterval is the starting cycle period for either loop. A caller that derives its own replaces
	// it through SetInterval; this only keeps a freshly-built loop from running flat out.
	DefaultInterval = 50 * time.Millisecond
	// DefaultMinGap is the starting quiet time between cycles.
	DefaultMinGap = 20 * time.Millisecond
)

// pacer is the cadence one loop keeps, and the work window an owner reads its liveness from. Both loops
// embed one; it knows nothing about phases, shards or what the cycle does.
//
// It is shared rather than written twice because the self-correcting part is easy to get subtly wrong: the
// wait is computed from ELAPSED time, not from the previous cycle's duration, so any delay a caller
// introduces between calls is absorbed rather than added. Two copies would drift.
//
// A pacer is not safe for concurrent use except through its setters and WorkingFor - the timestamps are
// single-goroutine state, on the same basis a Cycle is.
type pacer struct {
	// Cadence knobs, in nanoseconds. Atomic because they are set from wherever the caller derives them
	// while a cycle may be running.
	interval atomic.Int64
	minGap   atomic.Int64

	// Cadence state, touched only by the driving goroutine. lastStart anchors the interval (start to
	// start) and lastEnd the gap (end to start); both zero until the first cycle runs.
	lastStart time.Time
	lastEnd   time.Time
	// workStart is when the current cycle entered its work, in nanoseconds, or 0 between cycles - see
	// WorkingFor. Atomic because the driving goroutine sets it and an owner's publisher reads it.
	workStart atomic.Int64
}

// init applies the paced defaults, so a freshly-built loop does not run flat out until someone remembers
// to configure it.
func (p *pacer) init() {
	p.interval.Store(int64(DefaultInterval))
	p.minGap.Store(int64(DefaultMinGap))
}

// SetInterval updates the cycle period - start of work to start of work - live. The next cycle picks it
// up: the value is read once per cycle rather than captured, so a caller that re-derives it takes effect
// without a restart. Zero paces nothing, which is what driving cycles back to back wants.
func (p *pacer) SetInterval(d time.Duration) {
	p.interval.Store(int64(max(0, d)))
}

// Interval is the current cycle period.
func (p *pacer) Interval() time.Duration {
	return time.Duration(p.interval.Load())
}

// SetMinGap updates the minimum quiet time between the END of one cycle and the START of the next. It is
// the fuse for the case the interval alone cannot cover: a cycle that outruns its interval would otherwise
// leave no gap at all and run back to back, which is the duty cycle the interval exists to prevent. Zero
// disables it.
func (p *pacer) SetMinGap(d time.Duration) {
	p.minGap.Store(int64(max(0, d)))
}

// MinGap is the current minimum quiet time between cycles.
func (p *pacer) MinGap() time.Duration {
	return time.Duration(p.minGap.Load())
}

// Period is this loop's effective cycle period: its interval, its gap, or the DefaultMinGap floor -
// whichever is largest. It is what anything reasoning about "has this loop been working longer than a cycle"
// or "how long does a healthy owner take to come round again" must ask, and it must be asked of the LOOP
// rather than of a shared setting, because the two loops are paced independently by design.
//
// Floored at the CONSTANT, not at the configured MinGap: a caller may legitimately pin both interval and gap
// to zero (a bench sweep, a hand-driven cycle), and a zero period turns every predicate built on this into
// the degenerate one it exists to replace.
func (p *pacer) Period() time.Duration {
	return max(p.Interval(), p.MinGap(), DefaultMinGap)
}

// WorkingFor is how long the current cycle has been inside its work, or zero when none is - excluding the
// pace it sleeps first.
//
// A DURATION rather than a bool, and that is the load-bearing part. It exists for a caller publishing this
// shard's liveness on its own clock, where a completed cycle is the ordinary evidence but one scan can
// outrun any sane publishing cadence on a deep backlog (the band scan is O(due rows at the band) on every
// dialect). A bool cannot serve that: a cycle whose query fails INSTANTLY is also briefly inside its work -
// building the error, recording the phase, logging it - and a caller sampling often enough will keep
// catching that. Measured at ~1.2% of samples with a failing scan, which is easily enough to keep a broken
// shard looking alive indefinitely. Only a cycle that has run longer than the caller's own expectations is
// evidence, and a duration lets the caller decide what that means.
//
// Safe to call from any goroutine.
func (p *pacer) WorkingFor() time.Duration {
	started := p.workStart.Load()
	if started == 0 {
		return 0
	}
	return max(0, time.Since(time.Unix(0, started)))
}

// wait sleeps out whatever the cadence still owes and reports how long that took. The wait is the larger of
// the two constraints - interval measured from the last cycle's start, gap measured from its end - so
// whichever binds, binds. The first cycle waits for neither: a starting loop should look immediately.
func (p *pacer) wait(ctx context.Context) time.Duration {
	if p.lastStart.IsZero() {
		return 0
	}
	now := time.Now()
	wait := max(
		p.Interval()-now.Sub(p.lastStart),
		p.MinGap()-now.Sub(p.lastEnd),
	)
	if wait <= 0 {
		return 0
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return time.Since(now)
	case <-timer.C:
		return wait
	}
}

// begin opens a cycle's work window - anchoring the interval and starting the clock WorkingFor reads - and
// returns the moment it opened. The window spans the cycle's work and NOT the pace before it, which is most
// of a healthy cycle's wall clock and would make any predicate over WorkingFor permanently true.
func (p *pacer) begin() time.Time {
	p.lastStart = time.Now()
	p.workStart.Store(p.lastStart.UnixNano())
	return p.lastStart
}

// end closes the work window, anchors the gap, and reports how long the work took.
func (p *pacer) end(start time.Time) time.Duration {
	p.workStart.Store(0)
	p.lastEnd = time.Now()
	return p.lastEnd.Sub(start)
}
