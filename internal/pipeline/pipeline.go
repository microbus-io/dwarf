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

// Package pipeline runs one shard's supply cycle: it looks at what is due, asks the planner what it may
// serve, fetches that, and pushes it to the candidate cache the workers drain.
//
// One cycle has five phases, and they name everything here:
//
//	sleep -> tallying -> planning -> fetching -> pushing
//
// There is no loop and no goroutine in this package. Cycle paces itself - it sleeps at the FRONT, for
// whatever remains of the interval since the last cycle began - so a caller drives it in a tight loop and
// holds no cadence policy of its own:
//
//	for {
//	    select {
//	    case <-ctx.Done():
//	        return
//	    default:
//	    }
//	    observe(p.Cycle(ctx))
//	}
//
// Any delay the caller adds between calls is therefore absorbed rather than added to the period.
//
// Cycle NEVER returns an error, only a Result carrying one: every failure it can hit is already dealt
// with here, and the next cycle retries. A caller reads the Result for its log line and carries on.
//
// SetInterval and SetMinGap are live and safe to call from anywhere; Cycle itself is not safe for
// concurrent use - one Pipeline per shard, driven by one goroutine.
package pipeline

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/errors"
)

// Source is the database side of a cycle - the only two queries a cycle makes, both read-only.
type Source interface {
	// ScanBand reports this shard's minimum due priority band and one tally per fairness key at that
	// band, or math.MaxInt and no tallies when nothing is due here. It returns O(distinct keys) rows,
	// never O(backlog); each tally's Count is expected to be capped at the planning capacity.
	ScanBand(ctx context.Context, shard int) (band int, tallies []planner.Tally, err error)
	// FetchSteps returns up to perKey step ids for each of keys at the given band, OLDEST FIRST within
	// each key. The ordering is the Source's responsibility - the cycle consumes the lists in order and
	// does not re-sort them. A key may come back short or missing.
	FetchSteps(ctx context.Context, shard, band int, keys []string, perKey int) (map[string][]int, error)
}

// NoBand is the band reported when nothing is due - on this shard (Result.Band) or anywhere in the
// fleet (Result.GlobalBand).
const NoBand = math.MaxInt

// Result is one cycle's outcome, for logging and metrics. Nothing here is a control signal: a caller
// reads it and carries on.
type Result struct {
	// Per-phase durations. Total spans tallying through pushing and EXCLUDES Slept, so it is the cycle's
	// cost rather than its period; the period is Slept+Total.
	Slept    time.Duration
	Tallying time.Duration
	Planning time.Duration
	Fetching time.Duration
	Pushing  time.Duration
	Total    time.Duration

	// Band is this shard's own minimum due band, GlobalBand the best band any shard holds. Band >
	// GlobalBand means this shard was outranked and served nothing - the ordinary strict-priority case,
	// not a fault. Either is NoBand when nothing is due.
	Band       int
	GlobalBand int

	// Selected is how many candidates were pushed, Discarded how many un-popped ones that replaced.
	// Discarded rising toward Selected means the cycle is turning faster than the workers drain.
	Selected  int
	Discarded int

	// Err is set when the cycle ended early, wrapped with the phase that failed. The cycle has already
	// dealt with it; this is for the log line.
	Err error
}

const (
	// DefaultInterval is the starting cycle period. A caller that derives its own replaces it through
	// SetInterval; this only keeps a freshly-built pipeline from scanning flat out.
	DefaultInterval = 50 * time.Millisecond
	// DefaultMinGap is the starting quiet time between cycles.
	DefaultMinGap = 20 * time.Millisecond
)

// Pipeline runs one shard's supply cycle.
//
// Cycle is NOT safe for concurrent use - one Pipeline per shard, driven by one goroutine, is the whole
// intended shape, and the cadence timestamps are unsynchronized on that basis. SetInterval is the one
// exception and may be called from anywhere.
type Pipeline struct {
	shard   int
	source  Source
	planner *planner.Planner
	cache   *candidates.Cache

	// Cadence knobs, in nanoseconds. Atomic because they are set from wherever the caller derives them
	// while a cycle may be running.
	interval atomic.Int64
	minGap   atomic.Int64

	// Cadence state, touched only by Cycle. lastTallyStart anchors the interval (start to start) and
	// lastCycleEnd the gap (end to start); both zero until the first cycle runs.
	lastTallyStart time.Time
	// workStart is when the current cycle entered its queries, in nanoseconds, or 0 between cycles - see
	// WorkingFor. Atomic because the driving goroutine sets it and an owner's publisher reads it.
	workStart    atomic.Int64
	lastCycleEnd time.Time
}

// New returns a Pipeline for one shard, paced at DefaultInterval and DefaultMinGap until told otherwise.
// It is where a wiring mistake is caught, which is why Cycle itself has no error return to spend on one.
func New(shard int, source Source, planner *planner.Planner, cache *candidates.Cache) (*Pipeline, error) {
	if shard < 1 {
		return nil, errors.New("shard must be positive, got %d", shard)
	}
	if source == nil {
		return nil, errors.New("source is required")
	}
	if planner == nil {
		return nil, errors.New("planner is required")
	}
	if cache == nil {
		return nil, errors.New("cache is required")
	}
	p := &Pipeline{shard: shard, source: source, planner: planner, cache: cache}
	p.interval.Store(int64(DefaultInterval))
	p.minGap.Store(int64(DefaultMinGap))
	return p, nil
}

// Shard is the shard this pipeline supplies.
func (p *Pipeline) Shard() int { return p.shard }

// SetInterval updates the cycle period - start of tallying to start of tallying - live. The next cycle
// picks it up: the value is read once per cycle rather than captured, so a caller that re-derives it
// takes effect without a restart. Zero paces nothing, which is what driving cycles back to back wants.
func (p *Pipeline) SetInterval(d time.Duration) {
	p.interval.Store(int64(max(0, d)))
}

// Interval is the current cycle period.
func (p *Pipeline) Interval() time.Duration {
	return time.Duration(p.interval.Load())
}

// SetMinGap updates the minimum quiet time between the END of one cycle and the START of the next. It is
// the fuse for the case the interval alone cannot cover: a cycle that outruns its interval would
// otherwise leave no gap at all and scan back to back, which is the duty cycle the interval exists to
// prevent. Zero disables it.
func (p *Pipeline) SetMinGap(d time.Duration) {
	p.minGap.Store(int64(max(0, d)))
}

// MinGap is the current minimum quiet time between cycles.
func (p *Pipeline) MinGap() time.Duration {
	return time.Duration(p.minGap.Load())
}

// Cycle runs one full cycle - sleeping first for whatever the cadence still owes - and reports what
// happened. It never returns an error; see the package doc.
func (p *Pipeline) Cycle(ctx context.Context) Result {
	r := Result{Band: NoBand, GlobalBand: NoBand}
	r.Slept = p.pace(ctx)
	// A cancellation during the sleep ends the cycle before it touches anything. The caller's own loop
	// sees the same cancellation and stops; nothing here needs to signal it.
	if err := ctx.Err(); err != nil {
		r.Err = errors.Trace(err)
		return r
	}
	p.lastTallyStart = time.Now()
	// The work window spans the queries and nothing else - deliberately not the pace above, which is most of
	// a healthy cycle's wall clock.
	p.workStart.Store(p.lastTallyStart.UnixNano())
	p.run(ctx, &r)
	p.workStart.Store(0)
	p.lastCycleEnd = time.Now()
	r.Total = p.lastCycleEnd.Sub(p.lastTallyStart)
	return r
}

// WorkingFor is how long the current cycle has been inside its queries, or zero when none is - excluding
// the pace it sleeps first.
//
// A DURATION rather than a bool, and that is the load-bearing part. It exists for a caller publishing this
// shard's liveness on its own clock, where a completed cycle is the ordinary evidence but one scan can
// outrun any sane publishing cadence on a deep backlog (phase one is O(backlog) on every dialect without
// the run-condition early-stop). A bool cannot serve that: a cycle whose scan fails INSTANTLY is also
// briefly inside its queries - building the error, recording the phase, logging it - and a caller sampling
// often enough will keep catching that. Measured at ~1.2% of samples with a failing scan, which is easily
// enough to keep a broken shard looking alive indefinitely. Only a cycle that has run longer than the
// caller's own expectations is evidence, and a duration lets the caller decide what that means.
//
// Safe to call from any goroutine.
func (p *Pipeline) WorkingFor() time.Duration {
	started := p.workStart.Load()
	if started == 0 {
		return 0
	}
	return max(0, time.Since(time.Unix(0, started)))
}

// pace sleeps out whatever the cadence still owes and reports how long that took. The wait is the larger
// of the two constraints - interval measured from the last tally start, gap measured from the last cycle
// end - so whichever binds, binds. The first cycle waits for neither: a starting shard should look
// immediately.
func (p *Pipeline) pace(ctx context.Context) time.Duration {
	if p.lastTallyStart.IsZero() {
		return 0
	}
	now := time.Now()
	wait := max(
		p.Interval()-now.Sub(p.lastTallyStart),
		p.MinGap()-now.Sub(p.lastCycleEnd),
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

// run executes tallying through pushing, filling r as it goes.
//
// The error policy is asymmetric, and that asymmetry is the point:
//
//   - A failed SCAN means this shard could not look. It clears itself from planning, because a stale
//     claim on the best band makes every peer find no keys of its own there and dispatch nothing. It
//     leaves the cache alone: the failure means "unknown", not "nothing is due", and wholesale-replacing
//     a healthy partition with nothing because the database blipped would idle every worker for a cycle.
//   - A failed FETCH is different. The tally already succeeded and is still true - the shard looked, saw
//     its band, and reported it honestly - so it must NOT clear, which would drop a valid band claim and
//     let peers serve worse work for no reason. It just pushes nothing this cycle.
//   - An EMPTY plan is not a failure at all, and is the one case that DOES clear the partition: nothing
//     here is dispatchable, so every cached candidate is a dead hint a worker would pop and burn a claim
//     round-trip on.
func (p *Pipeline) run(ctx context.Context, r *Result) {
	// Tallying.
	t := time.Now()
	band, tallies, err := p.source.ScanBand(ctx, p.shard)
	r.Tallying = time.Since(t)
	if err != nil {
		p.planner.Clear(p.shard)
		r.Err = errors.New("tallying", err)
		return
	}
	r.Band = band
	p.planner.Tally(p.shard, band, tallies)

	// Planning.
	t = time.Now()
	plan := p.planner.Plan(p.shard, p.cache.Capacity())
	r.Planning = time.Since(t)
	r.GlobalBand = plan.GlobalBand

	if len(plan.Slots) == 0 {
		p.push(r, nil, NoBand)
		return
	}

	// Fetching.
	t = time.Now()
	steps, err := p.source.FetchSteps(ctx, p.shard, plan.GlobalBand, plan.Keys, plan.PerKeyCap)
	r.Fetching = time.Since(t)
	if err != nil {
		r.Err = errors.New("fetching", err)
		return
	}

	// Pushing.
	p.push(r, assemble(plan.Slots, steps, p.shard), plan.GlobalBand)
}

// push hands a batch to the cache and records what it cost.
func (p *Pipeline) push(r *Result, batch []candidates.Job, floor int) {
	t := time.Now()
	r.Discarded = p.cache.Refill(p.shard, batch, floor)
	r.Pushing = time.Since(t)
	r.Selected = len(batch)
}

// assemble turns the plan's slots into candidates by walking them in order and taking each key's next
// fetched step. Walking the slots rather than grouping by key is what preserves the plan's fairness
// interleave in the batch the workers pop from.
//
// A key that comes up short - its steps claimed or completed between the fetch and now - simply
// contributes fewer candidates. The batch runs short for one cycle and the next one re-selects; there is
// nothing to reconcile.
func assemble(slots []string, steps map[string][]int, shard int) []candidates.Job {
	batch := make([]candidates.Job, 0, len(slots))
	taken := make(map[string]int, len(steps))
	for _, key := range slots {
		list := steps[key]
		i := taken[key]
		if i >= len(list) {
			continue
		}
		batch = append(batch, candidates.Job{StepID: list[i], Shard: shard})
		taken[key]++
	}
	return batch
}
