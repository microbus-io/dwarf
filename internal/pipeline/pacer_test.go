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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/testarossa"
)

// Only the database is faked. The planner and the cache are in-memory and are used for real, so a fake
// can never drift from their semantics - which matters most for Refill, whose wholesale-REPLACE behaviour
// is exactly what the error policy turns on.
type fakeBandSource struct {
	band    int
	tallies []planner.Tally
	scanErr error

	scans int
}

func (f *fakeBandSource) ScanBand(ctx context.Context, shard int) (int, []planner.Tally, error) {
	f.scans++
	if f.scanErr != nil {
		return 0, nil, f.scanErr
	}
	// A fresh slice per call, because Tally HANDS OVER the one it is given and normalizes it in place.
	out := make([]planner.Tally, len(f.tallies))
	copy(out, f.tallies)
	return f.band, out, nil
}

type fakeStepSource struct {
	steps    map[string][]int
	fetchErr error

	fetches   int
	gotBand   int
	gotKeys   []string
	gotPerKey int
}

func (f *fakeStepSource) FetchSteps(ctx context.Context, shard, band int, keys []string, perKey int) (map[string][]int, error) {
	f.fetches++
	f.gotBand, f.gotKeys, f.gotPerKey = band, keys, perKey
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	return f.steps, nil
}

// rig is one shard's pair of loops over a faked database, a real planner and a real cache of capacity 8,
// with pacing turned OFF so a behavioural test drives cycles back to back. The cadence tests set their own.
type rig struct {
	tallier  *Tallier
	supplier *Supplier
	band     *fakeBandSource
	steps    *fakeStepSource
	cache    *candidates.Cache
	planner  *planner.Planner
}

func newRig(t *testing.T, tallies []planner.Tally, band int) *rig {
	t.Helper()
	bs := &fakeBandSource{band: band, tallies: tallies}
	ss := &fakeStepSource{}
	cache := &candidates.Cache{}
	cache.Init(4) // capacity is twice the worker count
	t.Cleanup(cache.Close)
	pl := planner.New()

	tallier, err := NewTallier(1, bs, pl)
	if err != nil {
		t.Fatal(err)
	}
	supplier, err := NewSupplier(1, ss, pl, cache)
	if err != nil {
		t.Fatal(err)
	}
	tallier.SetInterval(0)
	tallier.SetMinGap(0)
	supplier.SetInterval(0)
	supplier.SetMinGap(0)
	return &rig{tallier: tallier, supplier: supplier, band: bs, steps: ss, cache: cache, planner: pl}
}

// cycle drives one full round trip - tally then supply - the way a hand-driven caller does.
func (r *rig) cycle(ctx context.Context) (TallyResult, SupplyResult) {
	return r.tallier.Cycle(ctx), r.supplier.Cycle(ctx)
}

// seed fills the shard's partition so a test can tell "the cache was left alone" from "the cache was
// cleared" - the distinction the whole error policy rests on, and one an empty starting cache hides.
func seed(c *candidates.Cache, ids ...int) {
	batch := make([]candidates.Job, 0, len(ids))
	for _, id := range ids {
		batch = append(batch, candidates.Job{StepID: id, Shard: 1})
	}
	c.Refill(1, batch, 100)
}

// drain pops exactly what the cache holds. Pop blocks on an empty cache, so the count is read first.
func drain(c *candidates.Cache) []candidates.Job {
	out := make([]candidates.Job, 0, c.Len())
	for c.Len() > 0 {
		j, ok, _ := c.Pop()
		if !ok {
			break
		}
		out = append(out, j)
	}
	return out
}

func stepIDs(batch []candidates.Job) []int {
	out := make([]int, 0, len(batch))
	for _, j := range batch {
		out = append(out, j.StepID)
	}
	return out
}

// filterRange returns the ids within [lo, hi], preserving order.
func filterRange(ids []int, lo, hi int) []int {
	var out []int
	for _, id := range ids {
		if id >= lo && id <= hi {
			out = append(out, id)
		}
	}
	return out
}

// TestPacer_PacesFromCycleStart pins the cadence both loops share: cycles are spaced start-to-start by the
// interval, so a cycle that itself took time waits only the remainder rather than stacking its duration on
// top.
func TestPacer_PacesFromCycleStart(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const interval = 120 * time.Millisecond
	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	r.tallier.SetInterval(interval)

	assert.Equal(time.Duration(0), r.tallier.Cycle(ctx).Slept, "a starting loop looks immediately")

	// Burn a known slice of the interval before asking for the next cycle; the wait must shrink by it.
	const spent = 40 * time.Millisecond
	time.Sleep(spent)
	slept := r.tallier.Cycle(ctx).Slept
	assert.True(slept > 0, "the second cycle waits out the rest of the interval")
	assert.True(slept < interval-spent/2,
		"the wait is measured from the cycle start, so elapsed time counts against it (slept %v)", slept)
}

// TestPacer_MinGapGuardsBackToBackCycles pins the fuse the interval alone cannot provide: with no interval
// at all, consecutive cycles must still leave a quiet gap rather than running continuously.
func TestPacer_MinGapGuardsBackToBackCycles(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const gap = 60 * time.Millisecond
	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	r.tallier.SetInterval(0)
	r.tallier.SetMinGap(gap)

	assert.Equal(time.Duration(0), r.tallier.Cycle(ctx).Slept)
	slept := r.tallier.Cycle(ctx).Slept
	assert.True(slept >= gap/2, "an interval of zero must still leave the min gap (slept %v)", slept)
}

// TestPacer_SetIntervalTakesEffectLive pins that the interval is read per cycle, not captured. A fleet
// change re-derives it, and a captured value would leave the loop on yesterday's cadence forever - silent,
// and invisible to any test that does not change it mid-run.
func TestPacer_SetIntervalTakesEffectLive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)

	r.tallier.SetInterval(200 * time.Millisecond)
	assert.Equal(200*time.Millisecond, r.tallier.Interval())
	r.tallier.Cycle(ctx) // first cycle: no wait, but it anchors the cadence

	// Shrink the interval before the paced cycle runs; it must honour the NEW value.
	r.tallier.SetInterval(20 * time.Millisecond)
	slept := r.tallier.Cycle(ctx).Slept
	assert.True(slept < 100*time.Millisecond, "a shortened interval applies to the very next cycle (slept %v)", slept)

	// And a zero interval paces nothing at all.
	r.tallier.SetInterval(0)
	assert.Equal(time.Duration(0), r.tallier.Cycle(ctx).Slept)
}

// TestPacer_TheTwoLoopsPaceIndependently pins the whole point of the split: the loops share a pacer TYPE,
// never a pacer. A long-running Tallier must not hold up the Supplier, which is what the fused cycle did
// and what made a deep backlog starve the workers exactly when supply mattered most.
func TestPacer_TheTwoLoopsPaceIndependently(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	r.tallier.SetInterval(30 * time.Second)
	r.supplier.SetInterval(0)
	r.supplier.SetMinGap(0)

	assert.Equal(30*time.Second, r.tallier.Interval())
	assert.Equal(time.Duration(0), r.supplier.Interval(), "setting one loop's interval must not touch the other's")

	// The Supplier turns freely while the Tallier is parked on its own 30s interval.
	ctx := context.Background()
	r.tallier.Cycle(ctx) // anchors the Tallier's cadence; the next one would sleep 30s
	start := time.Now()
	for range 5 {
		assert.NoError(r.supplier.Cycle(ctx).Err)
	}
	assert.True(time.Since(start) < 5*time.Second, "the Supplier is not paced by the Tallier")
}

// TestPacer_NegativeTimingsClampToZero pins that a meaningless cadence is clamped rather than rejected: a
// negative duration is nonsense, not dangerous, and failing on it would push a pointless error path into
// wiring code.
func TestPacer_NegativeTimingsClampToZero(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	r := newRig(t, nil, NoBand)

	r.supplier.SetInterval(-time.Second)
	r.supplier.SetMinGap(-time.Second)
	assert.Equal(time.Duration(0), r.supplier.Interval())
	assert.Equal(time.Duration(0), r.supplier.MinGap())
}
