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

package engine

import (
	"context"
	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	"math"
	"testing"
	"time"
)

// eventuallyCacheLen waits briefly for the cache to hold want candidates. It exists because a test
// driving runShardRefill by hand races the engine's own background refiller for the same shard, which
// computes the same result from the same census - so the value converges and only the ordering is racy.
func eventuallyCacheLen(e *Engine, want int) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if e.cache.Len() == want {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return e.cache.Len() == want
}

// mkCensusEntry fabricates one shard's census entry for slice/merge unit tests.
func mkCensusEntry(shard, band int, at time.Time, rows ...censusRow) censusShard {
	sc := &shardCensus{band: band, rows: rows, at: at}
	if len(rows) > 0 {
		sc.byKey = make(map[string]int, len(rows))
		for i, r := range rows {
			sc.byKey[r.key] = i
		}
	}
	return censusShard{shard: shard, census: sc}
}

// TestRefillSlice_FirstSlotToOldestThenProportional pins the slice rule: a key's first plan slot goes
// to the shard holding its OLDEST step (preserving globally-oldest-first dispatch, and preventing the
// starvation where a purely proportional split rounds a quiet shard's lone old step to zero forever);
// the remaining slots split proportional to per-shard counts with deterministic largest-remainder
// rounding. Shards at a worse band are invisible to the split - they are not competing at this band.
func TestRefillSlice_FirstSlotToOldestThenProportional(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	now := time.Now()

	entries := []censusShard{
		mkCensusEntry(1, 3, now, censusRow{key: "t", weight: 1, ageMs: 500, count: 3}),
		mkCensusEntry(2, 3, now, censusRow{key: "t", weight: 1, ageMs: 900, count: 6}),
		mkCensusEntry(3, 5, now, censusRow{key: "t", weight: 1, ageMs: 9999, count: 50}), // worse band: excluded
	}
	plan := []string{"t", "t", "t", "t"}

	// Shard 2 holds the oldest step at the band, so it gets the first slot; the remaining 3 split over
	// avail (shard1: 3, shard2: 5) by largest remainder -> shard1: 1, shard2: 2 more.
	q1, max1, assign1 := sliceDemand(plan, entries, 3, 1)
	q2, max2, assign2 := sliceDemand(plan, entries, 3, 2)
	q3, _, _ := sliceDemand(plan, entries, 3, 3)

	assert.Equal(1, q1["t"])
	assert.Equal(3, q2["t"])
	assert.Equal(0, q3["t"], "a shard above the band gets no slots regardless of its backlog")
	assert.Equal(1, max1)
	assert.Equal(3, max2)
	assert.Equal(2, assign1["t"][0], "the first slot names the oldest holder")

	// The assignment is a pure function of (plan, census): every shard replaying it agrees on every
	// slot, and repeated evaluation is identical (determinism - map iteration must not leak in).
	assert.Equal(assign1["t"], assign2["t"])
	for range 10 {
		qr, _, ar := sliceDemand(plan, entries, 3, 1)
		assert.Equal(q1["t"], qr["t"])
		assert.Equal(assign1["t"], ar["t"])
	}

	// Total slots across shards equal the plan's demand.
	assert.Equal(len(plan), q1["t"]+q2["t"])
}

// TestRefillSlice_StarvationGuard pins the exact starvation case the first-slot rule exists for: a key
// with ONE old step on a quiet shard and a deep, constantly-replenished backlog on a busy shard. A
// purely proportional split of a small demand rounds the quiet shard to zero pass after pass; the
// first-slot rule hands it the head slot because it holds the key's oldest step.
func TestRefillSlice_StarvationGuard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	now := time.Now()

	entries := []censusShard{
		mkCensusEntry(1, 3, now, censusRow{key: "t", weight: 1, ageMs: 60000, count: 1}), // one OLD step
		mkCensusEntry(2, 3, now, censusRow{key: "t", weight: 1, ageMs: 800, count: 1000}),
	}
	q1, _, _ := sliceDemand([]string{"t", "t"}, entries, 3, 1)
	assert.Equal(1, q1["t"], "the quiet shard's old step must get the first slot, not be rounded away")
}

// TestRefillCensus_MergeMatchesBarrierSemantics pins that mergeCensus computes exactly what the
// barriered cross-shard merge computed: the global minimum band; per-key counts summed across shards;
// the globally-oldest step's weight winning a key's weight (so a tenant cannot self-promote with newer
// high-weight tasks); and worse-band shards contributing nothing.
func TestRefillCensus_MergeMatchesBarrierSemantics(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	now := time.Now()

	entries := []censusShard{
		mkCensusEntry(1, 3, now,
			censusRow{key: "a", weight: 2, ageMs: 500, count: 3},
			censusRow{key: "b", weight: 1, ageMs: 100, count: 1},
		),
		mkCensusEntry(2, 3, now,
			censusRow{key: "a", weight: 7, ageMs: 900, count: 6}, // older: its weight wins
		),
		mkCensusEntry(3, 9, now, censusRow{key: "z", weight: 1, ageMs: 1, count: 1}), // worse band
		mkCensusEntry(4, math.MaxInt, now),                                           // nothing due
	}
	band, keys := mergeCensus(entries)
	assert.Equal(3, band)
	assert.Equal(2, len(keys))
	assert.Equal("a", keys[0].key)
	assert.Equal(9, keys[0].count)
	assert.Equal(7.0, keys[0].weight, "the globally-oldest step's weight wins the merge")
	assert.Equal("b", keys[1].key)

	// All-empty census: no band.
	band, keys = mergeCensus([]censusShard{mkCensusEntry(1, math.MaxInt, now)})
	assert.Equal(math.MaxInt, band)
	assert.Equal(0, len(keys))
}

// TestRefillCensus_TTLDropsDeadShard pins the dead-shard containment: a shard whose census entry has
// aged past the TTL is dropped from snapshots entirely, so its stale band claim cannot pin the global
// band (the fleet-wedge failure the barrier made impossible and the decoupling introduced), and its
// keys cannot keep winning plan slots into a slice nobody fetches.
func TestRefillCensus_TTLDropsDeadShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	e.census = map[int]*shardCensus{}
	e.publishCensus(1, 5, []censusRow{{key: "live", weight: 1, ageMs: 100, count: 2}})
	e.publishCensus(2, 1, []censusRow{{key: "dead", weight: 1, ageMs: 100, count: 2}})
	// Age shard 2's entry past the TTL floor (no passes recorded, so the TTL is the 5s floor).
	e.censusLock.Lock()
	e.census[2].at = time.Now().Add(-6 * time.Second)
	e.censusLock.Unlock()

	entries := e.censusSnapshot()
	assert.Equal(1, len(entries))
	assert.Equal(1, entries[0].shard)
	band, keys := mergeCensus(entries)
	assert.Equal(5, band, "the dead shard's band-1 claim must not survive its TTL")
	assert.Equal(1, len(keys))
	assert.Equal("live", keys[0].key)
}

// TestRefillOutcome_AboveBandVsNothingDue pins the two distinct empty-slice outcomes and the strict
// cross-shard priority they implement:
//   - A shard with due work ABOVE the global band reports refillAboveBand (its refiller backs off and
//     watches the census; parking on the doorbell would spin the scan at full rate producing nothing),
//     and its partition is cleared - strict priority means it dispatches nothing while a better band is
//     live anywhere.
//   - A shard with NOTHING due reports refillIdle and parks on the doorbell as always, even when other
//     shards hold work.
func TestRefillOutcome_AboveBandVsNothingDue(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("A", "aboveband/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("aboveband/g", g)
	proxy.HandleTask("aboveband/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetWorkers(0)) // nothing dispatches; refills are driven by hand
	assert.NoError(e.Startup(t.Context()))

	// Nothing due anywhere: refillIdle.
	assert.Equal(refillIdle, e.runShardRefill(ctx, 1))

	// Nothing due HERE while a (fabricated) peer shard holds band-1 work: still refillIdle - there is
	// nothing on this shard to dispatch at any band, so it parks on the doorbell, not the back-off.
	e.publishCensus(99, 1, []censusRow{{key: "urgent", weight: 1, ageMs: 100, count: 1}})
	assert.Equal(refillIdle, e.runShardRefill(ctx, 1))

	// Due work here at the default priority (100), while the peer holds band 1: the global band is 1,
	// this shard's slice is empty by strict priority, the outcome is the back-off, and any cached
	// candidates are cleared (their band is not live for this shard anymore).
	//
	// The cache assertions poll rather than read once: Startup leaves this shard's BACKGROUND
	// refiller running (SetWorkers(0) stops workers, not refillers), so a pass triggered by the Create
	// doorbell can rewrite the partition around these explicit calls. It computes the same outcome
	// from the same census, so the value converges - only the ordering is racy.
	_, err := e.Create(ctx, "aboveband/g", nil, nil)
	assert.NoError(err)
	e.cache.Refill(1, []candidatecache.Job{{StepID: 12345, Shard: 1}}, 100)
	assert.Equal(refillAboveBand, e.runShardRefill(ctx, 1))
	assert.True(eventuallyCacheLen(e, 0), "an above-band shard's partition holds no dead hints (got %d)", e.cache.Len())

	// The peer's band-1 claim expires (TTL) or drains: the next pass serves this shard's own band.
	e.censusLock.Lock()
	e.census[99].at = time.Now().Add(-6 * time.Second)
	e.censusLock.Unlock()
	assert.Equal(refillIdle, e.runShardRefill(ctx, 1))
	assert.True(eventuallyCacheLen(e, 1), "released from the band, the shard's work is selected again (got %d)", e.cache.Len())
}

// TestRefillOutcome_StarvedNeverParksOnTheDoorbell pins the outcome for a shard that is AT the global
// band with due work but wins no slots, because a capacity-bound plan gave them all to shards holding
// more (or older) of the planned keys. It must report refillStarved - which routes to a self-arm of
// this shard's own trigger - and never refillIdle, which parks on a trigger nothing will ring.
//
// The bug this pins: a starved shard has an empty partition and nothing in flight, so it produces no
// pops, no completed steps and no doorbells - it arms nothing. Parked, it would sleep out the whole
// refillIdleInterval tick (and, before that tick existed, until pollPendingSteps' fleet-wide backstop
// MINUTES later) with its due work sitting there; once the competitors drained, every worker would
// block in Pop while those steps waited. The tick is the outer bound, far too coarse to serve here.
// Worse, a parked shard stops refreshing its census entry, so past the TTL its keys vanish from peers'
// plans and it cannot win slots even in principle - the starvation becomes self-sustaining.
func TestRefillOutcome_StarvedNeverParksOnTheDoorbell(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("A", "starved/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("starved/g", g)
	proxy.HandleTask("starved/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetWorkers(0)) // capacity 1: any competitor holding the key's oldest step takes the slot
	assert.NoError(e.Startup(t.Context()))

	// Real due work on this shard, at the default band.
	_, err := e.Create(ctx, "starved/g", nil, &workflow.FlowOptions{FairnessKey: "t"})
	assert.NoError(err)

	// A peer shard at the SAME band holding far more of the same key, and its oldest step. The slice
	// rule hands the first (only) slot to that shard, leaving this one at the band with zero quota.
	e.publishCensus(99, int(e.defaultPriority.Load()), []censusRow{{key: "t", weight: 1, ageMs: 99999, count: 100}})

	assert.Equal(refillStarved, e.runShardRefill(ctx, 1),
		"an at-band shard that wins no slots must retry, not park on a doorbell nothing will ring")
}

// TestRefillDecoupled_MultiShardDrains is the end-to-end sanity of the decoupled shape: two shards,
// two per-shard refillers planning globally over the census and fetching only their own slices, a
// backlog spread across both by placement - everything completes.
func TestRefillDecoupled_MultiShardDrains(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Chain")
	g.SetEndpoint("A", "msd/nop")
	g.SetEndpoint("B", "msd/nop")
	g.AddTransitionChain("A", "B", workflow.END)
	proxy.HandleGraph("msd/g", g)
	proxy.HandleTask("msd/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2}))
	assert.NoError(e.Startup(t.Context()))

	keys := make([]string, 24)
	for i := range keys {
		k, err := e.Create(ctx, "msd/g", nil, &workflow.FlowOptions{FairnessKey: "tenant"})
		assert.NoError(err)
		keys[i] = k
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	shardsSeen := map[string]bool{}
	for _, k := range keys {
		outcome, err := e.Await(deadline, k)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
		shardsSeen[k[:2]] = true
	}
	assert.Equal(2, len(shardsSeen), "placement should have used both shards (key prefix is the shard)")
}

// TestRefillScanFloor_OverridePinsAndRestores pins the benchmarking override (SetRefillScanFloor): a
// positive value pins every shard's floor, and <=0 restores derivation. It exists so a scan-rate sweep
// can hold the floor at a series of fixed values; the derived value is otherwise not externally settable.
func TestRefillScanFloor_OverridePinsAndRestores(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	e.refillState = map[int]*shardRefillState{1: {}}
	e.census = map[int]*shardCensus{}
	e.cache.Init(96) // capacity 192 -> share 192 at 1 shard
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))

	// Derived by default.
	e.recomputeScanFloors()
	derived := e.refillState[1].wait(time.Now())
	assert.True(derived > 0)

	// Pinned to an explicit value.
	assert.NoError(e.SetRefillScanFloor(42 * time.Millisecond))
	e.recomputeScanFloors()
	pinned := e.refillState[1].wait(time.Now())
	assert.True(pinned > 40*time.Millisecond && pinned <= 42*time.Millisecond, "got %v", pinned)

	// Back to derived.
	assert.NoError(e.SetRefillScanFloor(0))
	e.recomputeScanFloors()
	assert.Equal(derived.Round(time.Millisecond), e.refillState[1].wait(time.Now()).Round(time.Millisecond))
}

// TestRefillScanFloor_TriggerCoalescesAndMeasuresFromPassStart pins the two mechanics that make the
// scan floor a debounce rather than a policy timer: the trigger coalesces, and the wait is measured
// from the pass START so a slow pass pays for itself instead of stacking its duration on top.
//
// The floor exists because the trigger is re-armed by every completed step, which ran the refillers at
// a 100% duty cycle and cost 8% candidate churn plus a 77% worse p99 while buying no throughput. Every
// nudge is floor-gated - there is no bypass class, so nothing can drive the loop back toward that hot
// regime. A derived/adaptive floor was tried instead and measured WORSE than a fixed one (same scan
// count and batch size, ~1,000x the discard, 2.4x the p99): setting supply from observed consumption
// oscillates, because consumption is min(demand, supply) and the actuation contaminates its own
// measurement.
func TestRefillScanFloor_TriggerCoalescesAndMeasuresFromPassStart(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	e.refillTriggers = map[int]chan struct{}{1: make(chan struct{}, 1)}

	drain := func() bool {
		select {
		case <-e.refillTriggers[1]:
			return true
		default:
			return false
		}
	}

	e.requestRefill(1)
	assert.True(drain(), "a nudge must arm the scan")

	// Non-blocking and coalescing: a burst cannot wedge a caller or queue up scans.
	for range 100 {
		e.requestRefill(1)
	}
	assert.True(drain())
	assert.False(drain(), "single-slot: 100 nudges coalesce into one pending scan")

	// An unknown shard (a peer signal naming a shard this replica does not have) is a silent no-op.
	e.requestRefill(99)

	// The floor itself: measured from the pass start, so a slow pass pays for itself rather than
	// stacking its own duration on top.
	var st shardRefillState
	st.setFloor(150 * time.Millisecond)
	assert.True(st.wait(time.Now()) > 100*time.Millisecond)
	assert.True(st.wait(time.Now().Add(-200*time.Millisecond)) <= 0,
		"a pass that already outlasted the floor waits no longer")
	var unset shardRefillState
	assert.Equal(time.Duration(0), unset.wait(time.Now()), "an underived floor never holds the refiller")
}

// TestRefillScanFloor_DerivedFromStaticConfig pins the floor's derivation. It is arithmetic over values
// known at Startup - capacity, declared vCPUs, observed replicas - and reads NO observed rate, which is
// the distinction that matters: the version that set the interval from measured consumption oscillated
// (same scan count and batch as a fixed floor, ~1,000x the discard, 2.4x the p99) because consumption is
// min(demand, supply). Static arithmetic cannot do that.
//
// Deriving rather than hardcoding is what keeps the floor correct when the sizing it depends on moves:
// capacity is 2 x workersPerConnBudget x conns, so a change there silently rescales the buffer this
// floor is measured against. Campaign 11 nearly made exactly that change.
func TestRefillScanFloor_DerivedFromStaticConfig(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// The DERIVED path: a shard's pool is connsPerVCPU*vCPUs/R, so the two drain channels agree and the
	// min is a no-op. The measured rig: 8 vCPUs, R=1 -> a 48-conn pool and a 768-candidate partition
	// share. The configuration terms cancel (bufferShare is 96*vCPUs/R and the sustained drain
	// 720*vCPUs/R), so the derived floor is 96/(2*720) ~= 67ms and is the SAME at any vCPU or replica
	// count. 67ms sits inside the measured-good band (10-80ms on the 2026-07-22 rig M-sweep); the earlier
	// 340 constant put it at 141ms, the worst point in that band.
	rig := deriveScanFloor(768, 8, 48, 1)
	assert.True(rig > 60*time.Millisecond && rig < 74*time.Millisecond,
		"expected ~67ms at the measured configuration, got %v", rig)
	// Doubling vCPUs doubles the buffer share, the pool, AND the drain, so the floor is unchanged.
	assert.Equal(rig, deriveScanFloor(1536, 16, 96, 1))
	// Same for replicas: R halves the share, the pool, and the per-replica drain together.
	assert.Equal(rig, deriveScanFloor(384, 8, 24, 2))

	// A bigger buffer covers a longer gap; a faster shard (bigger pool) needs more frequent scans.
	assert.True(deriveScanFloor(1536, 8, 48, 1) > rig)
	assert.True(deriveScanFloor(768, 32, 192, 1) < rig)

	// The FOOTGUN: an operator pins a large pool with SetMaxOpenConns but leaves VirtualCPUs undeclared.
	// The buffer is sized off the big pool; the drain must follow the SAME pool. Deriving it from the
	// default 2 vCPUs instead (the old bug) overshot the floor to the 1s cap and starved the refiller -
	// the rig's 20-80s fan-out latency. With the pool driving the drain, the floor stays at the optimum.
	assert.True(deriveScanFloor(3072, 0, 192, 1) > 60*time.Millisecond && deriveScanFloor(3072, 0, 192, 1) < 74*time.Millisecond,
		"a big pinned pool with undeclared vCPUs must derive drain from the pool, not clamp to the cap")
	// The SAME buffer on a genuinely slow 2-vCPU shard (small pool to match) correctly caps - it really
	// is that slow. The fix distinguishes "big pool, vCPUs just unset" from "actually a 2-vCPU shard".
	assert.Equal(refillScanFloorCap, deriveScanFloor(3072, 2, 12, 1))

	// Both provided, connection-constrained (32-vCPU DB behind a 48-conn pooler): the drain follows the
	// tighter CONNECTION channel, identical to leaving the vCPUs unset.
	assert.Equal(deriveScanFloor(768, 0, 48, 1), deriveScanFloor(768, 32, 48, 1))
	// Both provided, CPU-constrained (4-vCPU DB with an over-provisioned 192-conn pool): the drain
	// follows the tighter CPU channel, so the extra connections do not shorten the floor.
	assert.Equal(deriveScanFloor(768, 4, 24, 1), deriveScanFloor(768, 4, 192, 1))

	// The cap governs where the cancellation breaks - workersDispatch's max(64, ...) floor at small or
	// high-R configurations - and degenerate inputs fall back to it rather than to zero, which would
	// restore the 100%-duty-cycle hot loop.
	assert.Equal(refillScanFloorCap, deriveScanFloor(4096, 2, 2, 8))
	assert.Equal(refillScanFloorCap, deriveScanFloor(0, 8, 48, 1))
	assert.Equal(refillScanFloorCap, deriveScanFloor(768, 0, 0, 1), "zero pool -> zero drain falls back to the cap, never a 0 divide")
	assert.True(deriveScanFloor(1, 64, 384, 1) >= refillScanFloorMin)
}

// TestRefillFloor_DeepBacklogLiveness pins that rate-limiting the scan cannot wedge a deep backlog:
// with a single worker (cache capacity 2) and a backlog of flows several times the capacity, everything
// still completes under a floor set well into the over-limiting regime - the armed trigger resumes the
// refiller the moment the floor elapses, and the floor only bounds scan frequency, never delivery.
func TestRefillFloor_DeepBacklogLiveness(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("Do", "refillfloorliveness.verify:428/nop")
	g.AddTransition("Do", workflow.END)
	proxy.HandleGraph("refillfloorliveness.verify:428/one", g)
	proxy.HandleTask("refillfloorliveness.verify:428/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	e.SetWorkers(1)                                             // capacity 2, against a backlog of 10
	assert.NoError(e.SetRefillScanFloor(50 * time.Millisecond)) // the over-limiting regime; must still drain, just slower
	assert.NoError(e.Startup(t.Context()))

	keys := make([]string, 10)
	for i := range keys {
		k, err := e.Create(ctx, "refillfloorliveness.verify:428/one", nil, nil)
		assert.NoError(err)
		keys[i] = k
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, k := range keys {
		outcome, err := e.Await(deadline, k)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
}

// TestRefillScan_BoundedPerFairnessKey pins that the refiller's band scan cost scales with fairness-key
// CARDINALITY, not with the backlog. Phase 1 (scanBandKeys) collapses each key to a single aggregate row
// server-side, so a deep backlog on N tenants returns N rows, not N*capacity. Phase 3 (fetchBandSteps)
// then reads only the selected steps - at most perKey OLDEST per chosen key. The old single-query scan
// cut each key at the cache capacity and streamed up to capacity rows PER KEY across the wire, only to
// discard all but `capacity` of them total - so under a deep backlog with high key cardinality it
// materialized hundreds of thousands of rows on every pass.
//
// The fetch keeps every key oldest-first because the picker dispatches oldest-first within a key; a bound
// that kept the newest would starve the head of every queue. This test checks all of it: one aggregate
// row per key (not per step), the per-key fetch cut, and that the fetched steps are the oldest.
func TestRefillScan_BoundedPerFairnessKey(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Scan")
	g.SetEndpoint("A", "scan/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("scan/g", g)
	proxy.HandleTask("scan/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// SetWorkers(0): nothing dispatches, so every created flow's entry step stays `pending` and the scan
	// sees a real backlog.
	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetWorkers(0))
	assert.NoError(e.Startup(t.Context()))

	// A deep backlog on two tenants: 40 steps each, far past any per-key limit used below.
	const perTenant = 40
	tenants := []string{"tenant-a", "tenant-b"}
	for _, tenant := range tenants {
		for range perTenant {
			_, err := e.Create(ctx, "scan/g", nil, &workflow.FlowOptions{FairnessKey: tenant})
			assert.NoError(err)
		}
	}

	// Phase 1: one aggregate row per key regardless of backlog depth, carrying that key's due count
	// CAPPED at the cache capacity (min(due, capacity)) - the scan stops counting a key past capacity,
	// which is all planBatch can ever consume from one key, so the cap is lossless. SetWorkers(0) makes
	// this fixture's capacity small, so the assertion is the capped value, not the raw backlog depth.
	band, rows, err := e.scanShardBandKeys(ctx, 1)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(2, len(rows), "one aggregate row per tenant, not per step in the %d-step backlog", 2*perTenant)
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.key] = r.count
	}
	capped := min(perTenant, e.cache.Capacity())
	assert.Equal(capped, counts["tenant-a"])
	assert.Equal(capped, counts["tenant-b"])

	// Phase 3: the fetch cuts each key at perKey - not the backlog - and every key still reaches the batch.
	for _, perKey := range []int{1, 3, 10} {
		byKey, err := e.fetchShardBandSteps(ctx, 1, band, tenants, perKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(2, len(byKey), "both tenants are represented (fairness is not sacrificed to the bound)")
		for key, list := range byKey {
			assert.Equal(perKey, len(list), "key %q is cut at perKey", key)
		}
	}

	// And the fetch keeps the OLDEST step of each key: the single row for a key at perKey=1 is its oldest
	// (smallest step_id here, since steps are created in age order).
	one, err := e.fetchShardBandSteps(ctx, 1, band, tenants, 1)
	if !assert.NoError(err) {
		return
	}
	all, err := e.fetchShardBandSteps(ctx, 1, band, tenants, perTenant)
	if !assert.NoError(err) {
		return
	}
	for key, list := range all {
		oldest := list[0].stepID
		for _, fs := range list {
			if fs.stepID < oldest {
				oldest = fs.stepID
			}
		}
		if assert.Equal(1, len(one[key])) {
			assert.Equal(oldest, one[key][0].stepID, "the single fetched row for %q is its oldest step", key)
		}
	}
}

// TestRefillIdleTick_ScansWithNoTrigger pins the refiller's capped park: a shard whose trigger is
// never armed keeps rescanning on refillIdleInterval instead of parking indefinitely.
//
// This is the property that lets the cross-replica enqueue doorbell be dropped. Every trigger-arming
// site is shard-LOCAL (a pop, a completed step, a doorbell naming a step on this shard), so an idle
// replica holding a step no peer announced to it arms nothing and generates nothing: parked, it would
// sleep until pollPendingSteps' fleet-wide backstop, whose resting cadence is maxPollInterval (5 MINUTES)
// now that the due-backlog existence probe and its 1-minute cap are gone.
//
// It counts PASSES on a fully idle engine rather than timing an undoorbelled flow's completion, because
// that end-to-end shape is not sensitive: Startup arms every trigger twice (once directly, once from the
// timer's first poll), so the residual pending trigger dispatches the step one scan floor later and the
// test passes with the tick disabled. Pass count separates the two cleanly - unbounded with the tick,
// zero-or-one without - and needs no flow at all.
func TestRefillIdleTick_ScansWithNoTrigger(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Two independent solo engines (separate test databases so they do not count each other as peers):
	// one with the tick, one with it disabled, so the assertion below is sensitive to the mechanism and
	// not to whatever else happens to touch the trigger.
	ticking := NewEngineUnderTest(t)
	assert.NoError(ticking.SetHost(NewTestProxy()))
	ticking.refillIdleInterval = 50 * time.Millisecond
	startSolo(t, ticking, "ticking")

	parked := NewEngineUnderTest(t)
	assert.NoError(parked.SetHost(NewTestProxy()))
	parked.refillIdleInterval = 0 // the pre-change behavior: park on the trigger, forever
	startSolo(t, parked, "parked")

	// Let Startup's own trigger arms (requestRefillAll, plus the timer's first poll) work through before
	// counting, so what is measured is the steady state rather than startup residue.
	settle := 4 * refillScanFloorCap
	time.Sleep(settle)

	const window = time.Second
	var ticked, stalled int
	done := make(chan struct{})
	go func() {
		stalled = countRefillPasses(parked, 1, window)
		close(done)
	}()
	ticked = countRefillPasses(ticking, 1, window)
	<-done

	// With a 50ms tick against a ~67ms scan floor the pass rate is floor-bound, so ~8/s is the ceiling;
	// assert well under it so a loaded CI machine cannot flake.
	assert.True(ticked >= 3, "an idle refiller must keep rescanning on its idle tick, got %d passes in %v", ticked, window)
	// Parked, the only passes possible are leftover startup arms the settle above should already have
	// absorbed. Allow one for slack; the point is that it does not scale with the window.
	assert.True(stalled <= 1, "with the idle tick disabled an idle refiller must park, got %d passes in %v", stalled, window)
	assert.True(ticked > stalled, "the idle tick must be what drives the rescans (%d vs %d)", ticked, stalled)
}

// countRefillPasses reports how many phase-1 scans a shard's refiller completes within the window, by
// sampling the census entry it republishes on every pass. It is the cheapest in-process observation of
// "the refiller ran", needing no metric reader and no flow.
func countRefillPasses(e *Engine, shard int, window time.Duration) int {
	at := func() time.Time {
		e.censusLock.Lock()
		defer e.censusLock.Unlock()
		if sc := e.census[shard]; sc != nil {
			return sc.at
		}
		return time.Time{}
	}
	last := at()
	passes := 0
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
		if cur := at(); cur.After(last) {
			passes++
			last = cur
		}
	}
	return passes
}
