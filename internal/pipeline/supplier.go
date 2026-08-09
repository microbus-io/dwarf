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

package pipeline

import (
	"context"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/errors"
)

// StepSource is the Supplier's database side - the one query it makes, read-only.
type StepSource interface {
	// FetchSteps returns up to perKey step ids for each of keys at the given band, OLDEST FIRST within
	// each key. The ordering is the StepSource's responsibility - the cycle consumes the lists in order
	// and does not re-sort them. A key may come back short or missing.
	FetchSteps(ctx context.Context, shard, band int, keys []string, perKey int) (map[string][]int, error)
}

// SupplyResult is one supply cycle's outcome, for logging and metrics. Nothing here is a control signal: a
// caller reads it and carries on.
type SupplyResult struct {
	// Slept is the pace this cycle waited out before planning. Total spans planning through pushing and
	// EXCLUDES it, so the period is Slept+Total.
	Slept    time.Duration
	Planning time.Duration
	Fetching time.Duration
	Pushing  time.Duration
	Total    time.Duration

	// GlobalBand is the best band any shard holds, or NoBand when the fleet has nothing due. A shard whose
	// own band is worse than this was outranked and served nothing - the ordinary strict-priority case,
	// not a fault.
	GlobalBand int

	// Selected is how many candidates were pushed, Discarded how many un-popped ones that replaced.
	// Discarded rising toward Selected means the cycle is turning faster than the workers drain.
	Selected  int
	Discarded int

	// Reconciled is whether the cycle reached its push - i.e. whether this shard's partition now reflects
	// the plan. It is NOT the same as Err==nil: the two hold-everything paths (a fetch failure, an
	// untallied shard) both leave the partition exactly as they found it, and only one of them is an
	// error. A caller waiting on the partition being in a known state waits on this.
	Reconciled bool

	// Err is set when the fetch failed. The cycle has already dealt with it; this is for the log line.
	Err error
}

// Supplier turns the planner's verdict into candidates in one shard's cache partition.
//
// Cycle is NOT safe for concurrent use - one Supplier per shard, driven by one goroutine, is the whole
// intended shape, and the cadence timestamps are unsynchronized on that basis. The cadence setters are the
// deliberate exception and may be called from anywhere.
type Supplier struct {
	pacer

	shard   int
	source  StepSource
	planner *planner.Planner
	cache   *candidates.Cache
}

// NewSupplier returns a Supplier for one shard, paced at DefaultInterval and DefaultMinGap until told
// otherwise. It is where a wiring mistake is caught, which is why Cycle itself has no error return to spend
// on one.
func NewSupplier(shard int, source StepSource, plan *planner.Planner, cache *candidates.Cache) (*Supplier, error) {
	if shard < 1 {
		return nil, errors.New("shard must be positive, got %d", shard)
	}
	if source == nil {
		return nil, errors.New("source is required")
	}
	if plan == nil {
		return nil, errors.New("planner is required")
	}
	if cache == nil {
		return nil, errors.New("cache is required")
	}
	s := &Supplier{shard: shard, source: source, planner: plan, cache: cache}
	s.init()
	return s, nil
}

// Shard is the shard this Supplier fills for.
func (s *Supplier) Shard() int { return s.shard }

// Cycle plans, fetches and pushes - sleeping first for whatever the cadence still owes - and reports what
// happened. It never returns an error; see the package doc.
func (s *Supplier) Cycle(ctx context.Context) SupplyResult {
	r := SupplyResult{GlobalBand: NoBand}
	r.Slept = s.wait(ctx)
	// A cancellation during the sleep ends the cycle before it touches anything. The caller's own loop sees
	// the same cancellation and stops; nothing here needs to signal it.
	if err := ctx.Err(); err != nil {
		r.Err = errors.Trace(err)
		return r
	}
	start := s.begin()
	s.run(ctx, &r)
	r.Total = s.end(start)
	return r
}

// run executes planning through pushing, filling r as it goes.
//
// Three outcomes, and the differences between them are the part most likely to be "simplified" into a bug:
//
//   - An UNTALLIED shard touches nothing. The Tallier either has not reported yet or cleared the shard
//     because its scan failed, and a cleared shard means UNKNOWN, not "nothing is due" - so
//     wholesale-replacing a healthy partition with nothing because the database blipped would idle this
//     shard's workers for a cycle, on the last good information anyone had. The scan-error policy is split
//     across the two loops: the Tallier owns clearing the shard from planning, this owns sparing the
//     cache.
//   - A failed FETCH also touches nothing. The tally the plan was built on succeeded and is still true, so
//     this must NOT clear the shard from planning either - that would drop a valid band claim and let peers
//     serve worse work for no reason. It just pushes nothing this cycle.
//   - An EMPTY plan from a TALLIED shard is not a failure at all, and is the one case that DOES clear the
//     partition: it is a positive statement that nothing here is dispatchable, so every cached candidate is
//     a dead hint a worker would pop and burn a claim round-trip on.
func (s *Supplier) run(ctx context.Context, r *SupplyResult) {
	// Planning.
	t := time.Now()
	plan := s.planner.Plan(s.shard, s.cache.Capacity())
	r.Planning = time.Since(t)
	r.GlobalBand = plan.GlobalBand

	if !plan.Tallied {
		return
	}
	if len(plan.Slots) == 0 {
		s.push(r, nil, NoBand)
		return
	}

	// Fetching.
	t = time.Now()
	steps, err := s.source.FetchSteps(ctx, s.shard, plan.GlobalBand, plan.Keys, plan.PerKeyCap)
	r.Fetching = time.Since(t)
	if err != nil {
		r.Err = errors.New("fetching", err)
		return
	}

	// Pushing.
	s.push(r, assemble(plan.Slots, steps, s.shard), plan.GlobalBand)
}

// push hands a batch to the cache and records what it cost.
func (s *Supplier) push(r *SupplyResult, batch []candidates.Job, floor int) {
	t := time.Now()
	r.Discarded = s.cache.Refill(s.shard, batch, floor)
	r.Pushing = time.Since(t)
	r.Selected = len(batch)
	r.Reconciled = true
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
