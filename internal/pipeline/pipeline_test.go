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

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// Only the database is faked. The planner and the cache are in-memory and are used for real, so a fake
// can never drift from their semantics - which matters most for Refill, whose wholesale-REPLACE behaviour
// is exactly what the error policy below turns on.
type fakeSource struct {
	band     int
	tallies  []planner.Tally
	scanErr  error
	steps    map[string][]int
	fetchErr error

	scans, fetches int
	gotBand        int
	gotKeys        []string
	gotPerKey      int
}

func (f *fakeSource) ScanBand(ctx context.Context, shard int) (int, []planner.Tally, error) {
	f.scans++
	if f.scanErr != nil {
		return 0, nil, f.scanErr
	}
	return f.band, f.tallies, nil
}

func (f *fakeSource) FetchSteps(ctx context.Context, shard, band int, keys []string, perKey int) (map[string][]int, error) {
	f.fetches++
	f.gotBand, f.gotKeys, f.gotPerKey = band, keys, perKey
	if f.fetchErr != nil {
		return nil, f.fetchErr
	}
	return f.steps, nil
}

// newHarness wires a pipeline over a faked database, a real planner and a real cache of capacity 8, with
// pacing turned OFF so a behavioural test drives cycles back to back. The cadence tests set their own.
func newHarness(t *testing.T, tallies []planner.Tally, band int) (*Pipeline, *fakeSource, *candidatecache.Cache, *planner.Planner) {
	t.Helper()
	src := &fakeSource{band: band, tallies: tallies}
	cache := &candidatecache.Cache{}
	cache.Init(4) // capacity is twice the worker count
	t.Cleanup(cache.Close)
	pl := planner.New()
	p, err := New(1, src, pl, cache)
	if err != nil {
		t.Fatal(err)
	}
	p.SetInterval(0)
	p.SetMinGap(0)
	return p, src, cache, pl
}

// seed fills the shard's partition so a test can tell "the cache was left alone" from "the cache was
// cleared" - the distinction the whole error policy rests on, and one an empty starting cache hides.
func seed(c *candidatecache.Cache, ids ...int) {
	batch := make([]candidatecache.Job, 0, len(ids))
	for _, id := range ids {
		batch = append(batch, candidatecache.Job{StepID: id, Shard: 1})
	}
	c.Refill(1, batch, 100)
}

// drain pops exactly what the cache holds. Pop blocks on an empty cache, so the count is read first.
func drain(c *candidatecache.Cache) []candidatecache.Job {
	out := make([]candidatecache.Job, 0, c.Len())
	for c.Len() > 0 {
		j, ok, _ := c.Pop()
		if !ok {
			break
		}
		out = append(out, j)
	}
	return out
}

func stepIDs(batch []candidatecache.Job) []int {
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

// TestPipeline_HappyPathPushesThePlannedBatch walks a whole cycle and pins that what reaches the cache is
// the plan's slots resolved to steps, in the plan's order - the fairness interleave must survive the
// assembly, not be regrouped by key.
func TestPipeline_HappyPathPushesThePlannedBatch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, cache, _ := newHarness(t, []planner.Tally{
		{Key: "a", Weight: 1, AgeMs: 100, Count: 3},
		{Key: "b", Weight: 1, AgeMs: 100, Count: 3},
	}, 5)
	src.steps = map[string][]int{"a": {11, 12, 13}, "b": {21, 22, 23}}

	r := p.Cycle(ctx)
	assert.NoError(r.Err)
	assert.Equal(5, r.Band)
	assert.Equal(5, r.GlobalBand)
	assert.Equal(1, src.scans)
	assert.Equal(1, src.fetches)

	// Six steps are due and capacity is 8, so the whole band fits and every step is pushed exactly once.
	assert.Equal(6, r.Selected)
	assert.Equal(6, cache.Len())
	batch := drain(cache)
	seen := map[int]int{}
	for _, j := range batch {
		seen[j.StepID]++
		assert.Equal(1, j.Shard, "every candidate carries its own shard")
		assert.Equal(5, j.Priority, "the cache stamps the band the batch was pushed at")
	}
	for _, id := range []int{11, 12, 13, 21, 22, 23} {
		assert.Equal(1, seen[id], "step %d pushed exactly once", id)
	}
	// Within a key, steps are taken in the order the Source returned them - oldest first.
	ids := stepIDs(batch)
	assert.Equal([]int{11, 12, 13}, filterRange(ids, 11, 13), "a key's steps keep the Source's oldest-first order")
	assert.Equal([]int{21, 22, 23}, filterRange(ids, 21, 23))
}

// TestPipeline_ScanFailureClearsAndSparesTheCache pins the first half of the error policy. A shard that
// could not look must leave planning - a stale claim on the best band makes every peer find no keys of
// its own there and dispatch nothing - but must NOT have its partition replaced, because the failure
// means "unknown", not "nothing is due".
func TestPipeline_ScanFailureClearsAndSparesTheCache(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, cache, pl := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 1)

	// A good cycle first, so the shard is in the planner holding band 1 and the partition is populated.
	src.steps = map[string][]int{"a": {11}}
	assert.NoError(p.Cycle(ctx).Err)
	assert.Equal(1, cache.Len())
	assert.Equal(1, pl.Plan(2, 8).GlobalBand, "a peer sees this shard's band")

	// Now the scan fails.
	src.scanErr = errors.New("database is down")
	r := p.Cycle(ctx)
	assert.Error(r.Err)
	assert.Contains(r.Err.Error(), "tallying", "the error names the phase that failed")
	assert.Equal(NoBand, r.Band)
	assert.Equal(1, cache.Len(), "a failed scan must leave the partition intact")
	assert.Equal(11, drain(cache)[0].StepID, "and intact means the same candidates, not a fresh batch")
	assert.Equal(NoBand, pl.Plan(2, 8).GlobalBand, "the failed shard is cleared from planning")

	// And it comes back on the next good cycle, with no cooldown.
	src.scanErr = nil
	assert.NoError(p.Cycle(ctx).Err)
	assert.Equal(1, pl.Plan(2, 8).GlobalBand, "recovery costs exactly one cycle")
}

// TestPipeline_FetchFailureKeepsTheTally pins the other half, and the asymmetry that makes it easy to get
// wrong: the tally already succeeded and is still TRUE, so a fetch failure must not clear the shard.
// Clearing would drop a valid band claim and let peers serve worse work for no reason.
func TestPipeline_FetchFailureKeepsTheTally(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, cache, pl := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 3)
	src.fetchErr = errors.New("connection reset")
	seed(cache, 99)

	r := p.Cycle(ctx)
	assert.Error(r.Err)
	assert.Contains(r.Err.Error(), "fetching", "the error names the phase that failed")
	assert.Equal(3, r.Band, "the scan succeeded, so its band is reported")
	assert.Equal(1, cache.Len(), "a failed fetch touches neither the cache...")
	assert.Equal(99, drain(cache)[0].StepID)
	assert.Equal(3, pl.Plan(2, 8).GlobalBand, "...nor the tally it already published")
}

// TestPipeline_NothingDueClearsThePartition pins the one case that DOES empty the partition. An empty
// plan is a positive statement, not an error: nothing here is dispatchable, so every cached candidate is
// a dead hint a worker would pop and burn a claim round-trip on.
func TestPipeline_NothingDueClearsThePartition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, _, cache, _ := newHarness(t, nil, NoBand)
	seed(cache, 1, 2, 3)

	r := p.Cycle(ctx)
	assert.NoError(r.Err)
	assert.Equal(NoBand, r.Band)
	assert.Equal(NoBand, r.GlobalBand)
	assert.Equal(0, cache.Len(), "the partition is cleared, not left holding dead hints")
	assert.Equal(0, r.Selected)
	assert.Equal(3, r.Discarded, "and the hints it dropped are reported")
}

// TestPipeline_AboveBandClearsAndFetchesNothing pins strict cross-shard priority as the pipeline sees it:
// a shard with real due work, outranked by a peer holding a better band, serves nothing and does not even
// pay for a fetch. Its partition is cleared too - those hints are for a band it no longer serves.
func TestPipeline_AboveBandClearsAndFetchesNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, cache, pl := newHarness(t, []planner.Tally{{Key: "mine", Weight: 1, AgeMs: 1, Count: 5}}, 9)
	src.steps = map[string][]int{"mine": {1, 2, 3, 4, 5}}
	seed(cache, 7, 8)
	// A peer holds a strictly better band.
	pl.Tally(2, 1, []planner.Tally{{Key: "theirs", Weight: 1, AgeMs: 1, Count: 5}})

	r := p.Cycle(ctx)
	assert.NoError(r.Err, "being outranked is the ordinary case, never a fault")
	assert.Equal(9, r.Band)
	assert.Equal(1, r.GlobalBand)
	assert.True(r.Band > r.GlobalBand, "Band > GlobalBand is how a caller logs 'outranked'")
	assert.Equal(0, src.fetches, "an outranked shard pays for no fetch")
	assert.Equal(0, cache.Len(), "and holds no hints for a band it cannot serve")
}

// TestPipeline_ShortFetchRunsShort pins that a key whose steps were claimed between the fetch and the
// assembly simply contributes fewer candidates. The batch runs short for one cycle; nothing misaligns,
// and no other key's steps shift into its slots.
func TestPipeline_ShortFetchRunsShort(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, cache, _ := newHarness(t, []planner.Tally{
		{Key: "a", Weight: 1, AgeMs: 100, Count: 3},
		{Key: "b", Weight: 1, AgeMs: 100, Count: 3},
	}, 5)
	// "a" was planned for three slots but only one step survives; "b" is intact.
	src.steps = map[string][]int{"a": {11}, "b": {21, 22, 23}}

	r := p.Cycle(ctx)
	assert.NoError(r.Err)
	assert.Equal(4, r.Selected, "one step for the short key, three for the intact one")
	ids := stepIDs(drain(cache))
	assert.Equal([]int{11}, filterRange(ids, 11, 13))
	assert.Equal([]int{21, 22, 23}, filterRange(ids, 21, 23))
}

// TestPipeline_MissingKeyIsSkipped is the degenerate case of the above - a planned key the fetch returned
// nothing at all for must be skipped, not panic on an empty list.
func TestPipeline_MissingKeyIsSkipped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, cache, _ := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 2}}, 5)
	src.steps = map[string][]int{} // nothing came back
	seed(cache, 5)

	r := p.Cycle(ctx)
	assert.NoError(r.Err)
	assert.Equal(0, r.Selected)
	assert.Equal(0, cache.Len(), "an empty batch still replaces the partition")
}

// TestPipeline_FetchArgumentsComeFromThePlan pins that the fetch asks for exactly what the plan chose -
// the plan's band, its distinct keys, and its per-key cap - rather than re-deriving any of them.
func TestPipeline_FetchArgumentsComeFromThePlan(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, _, _ := newHarness(t, []planner.Tally{{Key: "solo", Weight: 1, AgeMs: 1, Count: 10}}, 7)
	src.steps = map[string][]int{"solo": {1, 2, 3, 4, 5, 6, 7, 8}}

	r := p.Cycle(ctx)
	assert.NoError(r.Err)
	assert.Equal(7, src.gotBand, "the fetch binds the planned band, never a freshly-mined one")
	assert.Equal([]string{"solo"}, src.gotKeys)
	assert.Equal(8, src.gotPerKey, "one key at capacity 8 wins all eight slots")
	assert.Equal(8, r.Selected)
}

// TestPipeline_ReportsPhasesAndDiscards pins that a Result carries what the metrics need: a duration per
// phase, the count of un-popped candidates the push replaced, and a Total that excludes the sleep (the
// cycle's cost, not its period).
func TestPipeline_ReportsPhasesAndDiscards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, cache, _ := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	src.steps = map[string][]int{"a": {11}}
	seed(cache, 71, 72, 73) // three un-popped candidates the push will replace

	r := p.Cycle(ctx)
	assert.NoError(r.Err)
	assert.Equal(1, r.Selected)
	assert.Equal(3, r.Discarded, "the push reports what it threw away un-popped")
	assert.Equal(time.Duration(0), r.Slept, "the first cycle never waits")
	assert.True(r.Total > 0)
	assert.True(r.Tallying >= 0 && r.Planning >= 0 && r.Fetching >= 0 && r.Pushing >= 0)
	assert.True(r.Total >= r.Tallying+r.Fetching, "Total spans the phases it contains")
}

// TestPipeline_PacesFromTallyStart pins the cadence: cycles are spaced start-to-start by the interval, so
// a cycle that itself took time waits only the remainder rather than stacking its duration on top.
func TestPipeline_PacesFromTallyStart(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const interval = 120 * time.Millisecond
	p, src, _, _ := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	src.steps = map[string][]int{"a": {11}}
	p.SetInterval(interval)

	assert.Equal(time.Duration(0), p.Cycle(ctx).Slept, "a starting shard looks immediately")

	// Burn a known slice of the interval before asking for the next cycle; the wait must shrink by it.
	const spent = 40 * time.Millisecond
	time.Sleep(spent)
	slept := p.Cycle(ctx).Slept
	assert.True(slept > 0, "the second cycle waits out the rest of the interval")
	assert.True(slept < interval-spent/2,
		"the wait is measured from the tally start, so elapsed time counts against it (slept %v)", slept)
}

// TestPipeline_MinGapGuardsBackToBackCycles pins the fuse the interval alone cannot provide: with no
// interval at all, consecutive cycles must still leave a quiet gap rather than scanning continuously.
func TestPipeline_MinGapGuardsBackToBackCycles(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const gap = 60 * time.Millisecond
	p, src, _, _ := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	src.steps = map[string][]int{"a": {11}}
	p.SetInterval(0)
	p.SetMinGap(gap)

	assert.Equal(time.Duration(0), p.Cycle(ctx).Slept)
	slept := p.Cycle(ctx).Slept
	assert.True(slept >= gap/2, "an interval of zero must still leave the min gap (slept %v)", slept)
}

// TestPipeline_SetIntervalTakesEffectLive pins that the interval is read per cycle, not captured. A fleet
// change re-derives it, and a captured value would leave the shard on yesterday's cadence forever -
// silent, and invisible to any test that does not change it mid-run.
func TestPipeline_SetIntervalTakesEffectLive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	p, src, _, _ := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	src.steps = map[string][]int{"a": {11}}

	p.SetInterval(200 * time.Millisecond)
	assert.Equal(200*time.Millisecond, p.Interval())
	p.Cycle(ctx) // first cycle: no wait, but it anchors the cadence

	// Shrink the interval before the paced cycle runs; it must honour the NEW value.
	p.SetInterval(20 * time.Millisecond)
	slept := p.Cycle(ctx).Slept
	assert.True(slept < 100*time.Millisecond, "a shortened interval applies to the very next cycle (slept %v)", slept)

	// And a zero interval paces nothing at all.
	p.SetInterval(0)
	assert.Equal(time.Duration(0), p.Cycle(ctx).Slept)
}

// TestPipeline_CancelDuringSleepEndsTheCycle pins that a cancelled context cuts the wait short and ends
// the cycle before it touches the planner or the cache - which is what lets a caller stop promptly with
// no second signal, since both queries are read-only and safe to abandon.
func TestPipeline_CancelDuringSleepEndsTheCycle(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p, src, cache, _ := newHarness(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	src.steps = map[string][]int{"a": {11}}
	p.SetInterval(30 * time.Second) // long enough that the test would hang if cancellation were ignored

	ctx, cancel := context.WithCancel(context.Background())
	p.Cycle(ctx) // anchor the cadence
	scansBefore, heldBefore := src.scans, cache.Len()

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	r := p.Cycle(ctx)
	assert.True(time.Since(start) < 5*time.Second, "cancellation must cut the wait short")
	assert.Error(r.Err)
	assert.Equal(scansBefore, src.scans, "a cancelled cycle never scans")
	assert.Equal(heldBefore, cache.Len(), "nor pushes")
}

// TestPipeline_NewValidates pins that a wiring mistake is caught at construction - which is the whole
// reason Cycle has no error return to spend on one - and that a fresh pipeline is paced rather than
// running flat out until someone remembers to configure it.
func TestPipeline_NewValidates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	src, pl := &fakeSource{}, planner.New()
	cache := &candidatecache.Cache{}
	cache.Init(4)
	t.Cleanup(cache.Close)

	p, err := New(1, src, pl, cache)
	assert.NoError(err)
	assert.Equal(1, p.Shard())
	assert.Equal(DefaultInterval, p.Interval(), "a fresh pipeline is paced by default")
	assert.Equal(DefaultMinGap, p.MinGap())

	_, err = New(0, src, pl, cache)
	assert.Error(err, "shard must be positive")
	_, err = New(-1, src, pl, cache)
	assert.Error(err)
	_, err = New(1, nil, pl, cache)
	assert.Error(err, "source is required")
	_, err = New(1, src, nil, cache)
	assert.Error(err, "planner is required")
	_, err = New(1, src, pl, nil)
	assert.Error(err, "cache is required")
}

// TestPipeline_NegativeTimingsClampToZero pins that a meaningless cadence is clamped rather than
// rejected: a negative duration is nonsense, not dangerous, and failing on it would push a pointless
// error path into wiring code.
func TestPipeline_NegativeTimingsClampToZero(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p, _, _, _ := newHarness(t, nil, NoBand)

	p.SetInterval(-time.Second)
	p.SetMinGap(-time.Second)
	assert.Equal(time.Duration(0), p.Interval())
	assert.Equal(time.Duration(0), p.MinGap())
}
