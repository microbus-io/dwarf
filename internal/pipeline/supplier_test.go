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

	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSupplier_PushesThePlannedBatch walks a whole round trip and pins that what reaches the cache is the
// plan's slots resolved to steps, in the plan's order - the fairness interleave must survive the assembly,
// not be regrouped by key.
func TestSupplier_PushesThePlannedBatch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{
		{Key: "a", Weight: 1, AgeMs: 100, Count: 3},
		{Key: "b", Weight: 1, AgeMs: 100, Count: 3},
	}, 5)
	r.steps.steps = map[string][]int{"a": {11, 12, 13}, "b": {21, 22, 23}}

	_, res := r.cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(5, res.GlobalBand)
	assert.True(res.Reconciled)
	assert.Equal(1, r.steps.fetches)

	// Six steps are due and capacity is 8, so the whole band fits and every step is pushed exactly once.
	assert.Equal(6, res.Selected)
	assert.Equal(6, r.cache.Len())
	batch := drain(r.cache)
	seen := map[int]int{}
	for _, j := range batch {
		seen[j.StepID]++
		assert.Equal(1, j.Shard, "every candidate carries its own shard")
		assert.Equal(5, j.Priority, "the cache stamps the band the batch was pushed at")
	}
	for _, id := range []int{11, 12, 13, 21, 22, 23} {
		assert.Equal(1, seen[id], "step %d pushed exactly once", id)
	}
	// Within a key, steps are taken in the order the source returned them - oldest first.
	ids := stepIDs(batch)
	assert.Equal([]int{11, 12, 13}, filterRange(ids, 11, 13), "a key's steps keep the source's oldest-first order")
	assert.Equal([]int{21, 22, 23}, filterRange(ids, 21, 23))
}

// TestSupplier_UntalliedShardSparesTheCache pins the Supplier's half of the scan-error policy. A shard the
// Tallier cleared - because its scan failed - is UNKNOWN, not "nothing is due", so the Supplier must leave
// its partition exactly as it found it. Clearing it would idle this shard's workers for a cycle on the
// strength of one database blip, throwing away the last good information anyone had.
//
// The Supplier turns on its own cadence now, so this is reachable in a way it never was when one cycle both
// scanned and pushed: nothing stops the Supplier running several times while the scan is still failing.
func TestSupplier_UntalliedShardSparesTheCache(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 1)
	r.steps.steps = map[string][]int{"a": {11}}

	// A good round trip first, so the partition is populated by the plan rather than by a test fixture.
	_, res := r.cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(1, r.cache.Len())

	// Now the scan fails, and keeps failing, while the Supplier turns several times beside it.
	r.band.scanErr = errors.New("database is down")
	assert.Error(r.tallier.Cycle(ctx).Err)
	for range 3 {
		res = r.supplier.Cycle(ctx)
		assert.NoError(res.Err, "an untallied shard is not an error - there is simply nothing to say")
		assert.False(res.Reconciled, "and nothing was reconciled, so no waiter may be told otherwise")
		assert.Equal(0, res.Selected)
		assert.Equal(0, res.Discarded, "a spared partition discards nothing")
	}
	assert.Equal(1, r.cache.Len(), "an untallied shard must leave the partition intact")
	assert.Equal(11, drain(r.cache)[0].StepID, "and intact means the same candidates, not a fresh batch")
	assert.Equal(0, r.steps.fetches-1, "nor does it pay for a fetch")
}

// TestSupplier_FetchFailureKeepsTheTally pins the asymmetry that makes this easy to get wrong: the tally the
// plan was built on already succeeded and is still TRUE, so a fetch failure must not clear the shard.
// Clearing would drop a valid band claim and let peers serve worse work for no reason.
func TestSupplier_FetchFailureKeepsTheTally(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 3)
	r.steps.fetchErr = errors.New("connection reset")
	seed(r.cache, 99)

	_, res := r.cycle(ctx)
	assert.Error(res.Err)
	assert.Contains(res.Err.Error(), "fetching", "the error names the phase that failed")
	assert.False(res.Reconciled)
	assert.Equal(1, r.cache.Len(), "a failed fetch touches neither the cache...")
	assert.Equal(99, drain(r.cache)[0].StepID)
	assert.Equal(3, r.planner.Plan(2, 8).GlobalBand, "...nor the tally it already published")
}

// TestSupplier_NothingDueClearsThePartition pins the one case that DOES empty the partition. An empty plan
// from a shard that REPORTED is a positive statement, not an error: nothing here is dispatchable, so every
// cached candidate is a dead hint a worker would pop and burn a claim round-trip on.
func TestSupplier_NothingDueClearsThePartition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, nil, NoBand)
	seed(r.cache, 1, 2, 3)

	_, res := r.cycle(ctx)
	assert.NoError(res.Err)
	assert.True(res.Reconciled, "an empty plan reconciles the partition just as a full one does")
	assert.Equal(NoBand, res.GlobalBand)
	assert.Equal(0, r.cache.Len(), "the partition is cleared, not left holding dead hints")
	assert.Equal(0, res.Selected)
	assert.Equal(3, res.Discarded, "and the hints it dropped are reported")
}

// TestSupplier_AboveBandClearsAndFetchesNothing pins strict cross-shard priority as the Supplier sees it: a
// shard with real due work, outranked by a peer holding a better band, serves nothing and does not even pay
// for a fetch. Its partition is cleared too - those hints are for a band it no longer serves.
func TestSupplier_AboveBandClearsAndFetchesNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "mine", Weight: 1, AgeMs: 1, Count: 5}}, 9)
	r.steps.steps = map[string][]int{"mine": {1, 2, 3, 4, 5}}
	seed(r.cache, 7, 8)
	// A peer holds a strictly better band.
	r.planner.Tally(2, 1, []planner.Tally{{Key: "theirs", Weight: 1, AgeMs: 1, Count: 5}})

	tr, res := r.cycle(ctx)
	assert.NoError(res.Err, "being outranked is the ordinary case, never a fault")
	assert.Equal(9, tr.Band)
	assert.Equal(1, res.GlobalBand)
	assert.True(tr.Band > res.GlobalBand, "Band > GlobalBand is how a caller logs 'outranked'")
	assert.Equal(0, r.steps.fetches, "an outranked shard pays for no fetch")
	assert.Equal(0, r.cache.Len(), "and holds no hints for a band it cannot serve")
}

// TestSupplier_ShortFetchRunsShort pins that a key whose steps were claimed between the fetch and the
// assembly simply contributes fewer candidates. The batch runs short for one cycle; nothing misaligns, and
// no other key's steps shift into its slots.
func TestSupplier_ShortFetchRunsShort(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{
		{Key: "a", Weight: 1, AgeMs: 100, Count: 3},
		{Key: "b", Weight: 1, AgeMs: 100, Count: 3},
	}, 5)
	// "a" was planned for three slots but only one step survives; "b" is intact.
	r.steps.steps = map[string][]int{"a": {11}, "b": {21, 22, 23}}

	_, res := r.cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(4, res.Selected, "one step for the short key, three for the intact one")
	ids := stepIDs(drain(r.cache))
	assert.Equal([]int{11}, filterRange(ids, 11, 13))
	assert.Equal([]int{21, 22, 23}, filterRange(ids, 21, 23))
}

// TestSupplier_MissingKeyIsSkipped is the degenerate case of the above - a planned key the fetch returned
// nothing at all for must be skipped, not panic on an empty list.
func TestSupplier_MissingKeyIsSkipped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 2}}, 5)
	r.steps.steps = map[string][]int{} // nothing came back
	seed(r.cache, 5)

	_, res := r.cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(0, res.Selected)
	assert.Equal(0, r.cache.Len(), "an empty batch still replaces the partition")
}

// TestSupplier_FetchArgumentsComeFromThePlan pins that the fetch asks for exactly what the plan chose - the
// plan's band, its distinct keys, and its per-key cap - rather than re-deriving any of them.
func TestSupplier_FetchArgumentsComeFromThePlan(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "solo", Weight: 1, AgeMs: 1, Count: 10}}, 7)
	r.steps.steps = map[string][]int{"solo": {1, 2, 3, 4, 5, 6, 7, 8}}

	_, res := r.cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(7, r.steps.gotBand, "the fetch binds the planned band, never a freshly-mined one")
	assert.Equal([]string{"solo"}, r.steps.gotKeys)
	assert.Equal(8, r.steps.gotPerKey, "one key at capacity 8 wins all eight slots")
	assert.Equal(8, res.Selected)
}

// TestSupplier_ReportsPhasesAndDiscards pins that a SupplyResult carries what the metrics need: a duration
// per phase, the count of un-popped candidates the push replaced, and a Total that excludes the sleep (the
// cycle's cost, not its period).
func TestSupplier_ReportsPhasesAndDiscards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	r.steps.steps = map[string][]int{"a": {11}}
	seed(r.cache, 71, 72, 73) // three un-popped candidates the push will replace

	_, res := r.cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(1, res.Selected)
	assert.Equal(3, res.Discarded, "the push reports what it threw away un-popped")
	assert.Equal(time.Duration(0), res.Slept, "the first cycle never waits")
	assert.True(res.Total > 0)
	assert.True(res.Planning >= 0 && res.Fetching >= 0 && res.Pushing >= 0)
	assert.True(res.Total >= res.Planning+res.Fetching, "Total spans the phases it contains")
}

// TestSupplier_CancelDuringSleepEndsTheCycle pins prompt shutdown on this loop too: a cancelled context cuts
// the wait short and ends the cycle before it plans or pushes.
func TestSupplier_CancelDuringSleepEndsTheCycle(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	r.steps.steps = map[string][]int{"a": {11}}
	r.supplier.SetInterval(30 * time.Second) // would hang if cancellation were ignored

	ctx, cancel := context.WithCancel(context.Background())
	r.cycle(ctx) // anchor the cadence
	heldBefore, fetchesBefore := r.cache.Len(), r.steps.fetches

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	res := r.supplier.Cycle(ctx)
	assert.True(time.Since(start) < 5*time.Second, "cancellation must cut the wait short")
	assert.Error(res.Err)
	assert.Equal(fetchesBefore, r.steps.fetches, "a cancelled cycle never fetches")
	assert.Equal(heldBefore, r.cache.Len(), "nor pushes")
}

// TestSupplier_NewValidates pins that a wiring mistake is caught at construction, and that a fresh Supplier
// is paced rather than running flat out.
func TestSupplier_NewValidates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	r := newRig(t, nil, NoBand)
	pl := planner.New()
	src := &fakeStepSource{}

	s, err := NewSupplier(1, src, pl, r.cache)
	assert.NoError(err)
	assert.Equal(1, s.Shard())
	assert.Equal(DefaultInterval, s.Interval(), "a fresh Supplier is paced by default")
	assert.Equal(DefaultMinGap, s.MinGap())

	_, err = NewSupplier(0, src, pl, r.cache)
	assert.Error(err, "shard must be positive")
	_, err = NewSupplier(1, nil, pl, r.cache)
	assert.Error(err, "source is required")
	_, err = NewSupplier(1, src, nil, r.cache)
	assert.Error(err, "planner is required")
	_, err = NewSupplier(1, src, pl, nil)
	assert.Error(err, "cache is required")
}
