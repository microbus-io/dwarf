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
	"math"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
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
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("A", "aboveband/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("aboveband/g", g)
	proxy.HandleTask("aboveband/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetWorkers(0)) // nothing dispatches; refills are driven by hand
	e.RunInTest(t)

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
	// The cache assertions poll rather than read once: RunInTest leaves this shard's BACKGROUND
	// refiller running (SetWorkers(0) stops workers, not refillers), so a pass triggered by the Create
	// doorbell can rewrite the partition around these explicit calls. It computes the same outcome
	// from the same census, so the value converges - only the ordering is racy.
	_, err := e.Create(ctx, "aboveband/g", nil, nil)
	assert.NoError(err)
	e.cache.Refill(1, []candidatecache.Job{{StepID: 12345, Shard: 1}}, 100)
	assert.Equal(refillAboveBand, e.runShardRefill(ctx, 1))
	assert.True(eventuallyCacheLen(e, 0), "an above-band shard's partition holds no dead hints (got %d)", e.cache.Len())

	// The peer's band-1 claim expires (TTL) or drains: the next pass serves this shard's own band.
	// (SetWorkers(0) makes the cache capacity 1, so the one-step plan is capacity-bound: refillFull.)
	e.censusLock.Lock()
	e.census[99].at = time.Now().Add(-6 * time.Second)
	e.censusLock.Unlock()
	assert.Equal(refillFull, e.runShardRefill(ctx, 1))
	assert.True(eventuallyCacheLen(e, 1), "released from the band, the shard's work is selected again (got %d)", e.cache.Len())
}

// TestRefillOutcome_StarvedNeverParksOnTheDoorbell pins the outcome for a shard that is AT the global
// band with due work but wins no slots, because a capacity-bound plan gave them all to shards holding
// more (or older) of the planned keys. It must report refillStarved - which routes to a short timed
// retry - and never refillIdle/refillFull, which park on a trigger only this shard's own activity can
// arm.
//
// The bug this pins: a starved shard has an empty partition and nothing in flight, so it produces no
// pops, no completed steps and no doorbells - it arms nothing. Parked, it would sleep until
// pollPendingSteps' fleet-wide backstop, up to backlogPollInterval (1 MINUTE), with its due work
// sitting there; once the competitors drained, every worker would block in Pop while those steps
// waited. Worse, a parked shard stops refreshing its census entry, so past the TTL its keys vanish
// from peers' plans and it cannot win slots even in principle - the starvation becomes self-sustaining.
func TestRefillOutcome_StarvedNeverParksOnTheDoorbell(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("A", "starved/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("starved/g", g)
	proxy.HandleTask("starved/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetWorkers(0)) // capacity 1: any competitor holding the key's oldest step takes the slot
	e.RunInTest(t)

	// Real due work on this shard, at the default band.
	_, err := e.Create(ctx, "starved/g", nil, &workflow.FlowOptions{FairnessKey: "t"})
	assert.NoError(err)

	// A peer shard at the SAME band holding far more of the same key, and its oldest step. The slice
	// rule hands the first (only) slot to that shard, leaving this one at the band with zero quota.
	e.publishCensus(99, int(e.defaultPriority.Load()), []censusRow{{key: "t", weight: 1, ageMs: 99999, count: 100}})

	assert.Equal(refillStarved, e.runShardRefill(ctx, 1),
		"an at-band shard that wins no slots must retry, not park on a doorbell nothing will ring")

	// And the retry releases promptly - bounded by the pace, not by the 1-minute poll backstop.
	start := time.Now()
	assert.True(e.awaitStarvedRetry(e.refillTriggers[1]), "the retry must release, not report shutdown")
	assert.True(time.Since(start) < 5*time.Second, "the starved retry must be short (took %v)", time.Since(start))
}

// TestRefillDecoupled_MultiShardDrains is the end-to-end sanity of the decoupled shape: two shards,
// two per-shard refillers planning globally over the census and fetching only their own slices, a
// backlog spread across both by placement - everything completes.
func TestRefillDecoupled_MultiShardDrains(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Chain")
	g.SetEndpoint("A", "msd/nop")
	g.SetEndpoint("B", "msd/nop")
	g.AddTransitionChain("A", "B", workflow.END)
	proxy.HandleGraph("msd/g", g)
	proxy.HandleTask("msd/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2}))
	e.RunInTest(t)

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

// TestRouteRefill_EscalationBypassesFloorSameBandDoesNot pins the doorbell routing that reconciles two
// requirements the empty-partition case put in tension: strict CROSS-SHARD priority needs a
// genuinely-higher-priority arrival to publish its new band fast (bypass the floor), while a deep
// backlog needs same-band successors NOT to bypass (or a drained partition re-scans every completion and
// the floor is defeated). The rule is priority STRICTLY BETTER than the current global band = escalation
// = bypass; at or below = ordinary backlog = floor-gated.
//
// The regression this guards: with a blanket empty-partition bypass, TestShardedflow's priority-1 holder
// arrived at an empty partition but its band-1 was not published before peer shards planned, so they
// dispatched lower-priority work first ([p2 holder ...] instead of [holder p2 ...]). With no bypass at
// all, the holder was never escalated and the same break occurred. Only the band-aware rule fixes both.
func TestRouteRefill_EscalationBypassesFloorSameBandDoesNot(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	e.refillTriggers = map[int]chan struct{}{1: make(chan struct{}, 1)}
	e.refillDemand = map[int]chan struct{}{1: make(chan struct{}, 1)}
	drainDemand := func() bool {
		select {
		case <-e.refillDemand[1]:
			return true
		default:
			return false
		}
	}

	// Global band is 5 (something at priority 5 is due cluster-wide).
	e.lastGlobalBand.Store(5)

	// A step STRICTLY better than the band (priority 2 < 5) is an escalation: bypass the floor.
	e.routeRefill(1, 2, false)
	assert.True(drainDemand(), "priority 2 beats global band 5: a genuine escalation must bypass the floor")

	// A step AT the band (priority 5) is ordinary backlog: floor-gated, no bypass.
	e.routeRefill(1, 5, false)
	assert.False(drainDemand(), "priority 5 == global band: same-band backlog must respect the floor (no hot loop)")

	// A step BELOW the band (priority 9 > 5) is likewise floor-gated.
	e.routeRefill(1, 9, false)
	assert.False(drainDemand(), "priority 9 below the band must respect the floor")

	// urgent (a head-insert into a non-empty partition) always bypasses, regardless of the global band.
	e.routeRefill(1, 9, true)
	assert.True(drainDemand(), "an urgent head-insert always bypasses (local preemption)")

	// Nothing due (band MaxInt): the first arrival at any priority is an escalation from nothing.
	e.lastGlobalBand.Store(int64(math.MaxInt))
	e.routeRefill(1, 100, false)
	assert.True(drainDemand(), "into an idle cluster, any due step is an escalation and bypasses")
}

// TestRefillScanFloor_OverridePinsAndRestores// TestRefillScanFloor_OverridePinsAndRestores pins the benchmarking override (SetRefillScanFloor): a
// positive value pins every shard's floor, and <=0 restores derivation. It exists so a scan-rate sweep
// can hold the floor at a series of fixed values; the derived value is otherwise not externally settable.
func TestRefillScanFloor_OverridePinsAndRestores(t *testing.T) {
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

// TestRefillScanFloor_UrgentNudgesBypassRoutineOnesDoNot pins the split that lets the scan floor be a
// debounce instead of a policy timer.
//
// The floor exists because the trigger is re-armed by every completed step, which ran the refillers at
// a 100% duty cycle and cost 8% candidate churn plus a 77% worse p99 while buying no throughput. But a
// floor that ALSO delays urgent work would have to be short enough to cover priority latency, which is
// what forces it back down toward the hot loop. So the two nudge classes are separated:
//
//   - Urgent (Offer head-inserted a strictly-better band - genuinely higher-priority work): rings the
//     trigger AND the demand channel, which cuts the floor short.
//   - Routine (post-processStep, low-water, AND an empty partition): rings the trigger only, and waits
//     out the floor. An empty partition is deliberately routine, not urgent: it is the ordinary
//     deep-backlog drain signal (enqueueStepDue hits it on every completed step once the partition
//     drains), so bypassing the floor on it re-creates the exact 100%-duty-cycle hot loop - measured,
//     it defeated the floor entirely (a pinned 110ms floor ran at ~19ms effective). The floor adds no
//     latency in light load regardless, since it is measured from the last pass.
//
// A derived/adaptive floor was tried instead and measured WORSE than a fixed one (same scan count and
// batch size, ~1,000x the discard, 2.4x the p99): setting supply from observed consumption oscillates,
// because consumption is min(demand, supply) and the actuation contaminates its own measurement.
func TestRefillScanFloor_UrgentNudgesBypassRoutineOnesDoNot(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	e.refillTriggers = map[int]chan struct{}{1: make(chan struct{}, 1)}
	e.refillDemand = map[int]chan struct{}{1: make(chan struct{}, 1)}

	drain := func() (trigger, demand bool) {
		select {
		case <-e.refillTriggers[1]:
			trigger = true
		default:
		}
		select {
		case <-e.refillDemand[1]:
			demand = true
		default:
		}
		return
	}

	// Routine: the refiller is asked to scan, but not before its floor elapses.
	e.requestRefill(1)
	tr, dm := drain()
	assert.True(tr, "a routine nudge must still arm the scan")
	assert.False(dm, "a routine nudge must NOT cut the floor short - that is the hot loop")

	// Urgent: both, so a refiller parked on the floor wakes immediately.
	e.requestRefillDemand(1)
	tr, dm = drain()
	assert.True(tr)
	assert.True(dm, "an empty partition or a better band must wake the refiller now")

	// Both are non-blocking and coalescing: a burst cannot wedge a caller or queue up scans.
	for range 100 {
		e.requestRefillDemand(1)
	}
	tr, dm = drain()
	assert.True(tr)
	assert.True(dm)
	tr, dm = drain()
	assert.False(tr, "single-slot: 100 nudges coalesce into one pending scan")
	assert.False(dm)

	// An unknown shard (a peer signal naming a shard this replica does not have) is a silent no-op.
	e.requestRefill(99)
	e.requestRefillDemand(99)

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
	assert := testarossa.For(t)

	// The measured rig: 8 vCPUs, R=1 -> a 768-candidate partition share. The configuration terms cancel
	// (bufferShare is 96*vCPUs/R and the sustained drain 340*vCPUs/R), so the derived floor is
	// 96/(2*340) ~= 141ms and is the SAME at any vCPU or replica count. That is the measured throughput
	// optimum (headroom 2.0): a ~2x supply buffer absorbs drain-rate jitter that a tighter one stalls on.
	rig := deriveScanFloor(768, 8, 1)
	assert.True(rig > 130*time.Millisecond && rig < 152*time.Millisecond,
		"expected ~141ms at the measured configuration, got %v", rig)
	assert.Equal(rig, deriveScanFloor(768*4/4, 8, 1))
	// Doubling vCPUs doubles BOTH the buffer share and the drain, so the floor is unchanged.
	assert.Equal(rig, deriveScanFloor(1536, 16, 1))
	// Same for replicas: R halves the share and the per-replica drain together.
	assert.Equal(rig, deriveScanFloor(384, 8, 2))

	// A bigger buffer covers a longer gap; a faster shard needs more frequent scans.
	assert.True(deriveScanFloor(1536, 8, 1) > rig)
	assert.True(deriveScanFloor(768, 32, 1) < rig)

	// The cap governs where the cancellation breaks - workersDispatch's max(64, ...) floor at small or
	// high-R configurations - and degenerate inputs fall back to it rather than to zero, which would
	// restore the 100%-duty-cycle hot loop.
	assert.Equal(refillScanFloorCap, deriveScanFloor(4096, 2, 8))
	assert.Equal(refillScanFloorCap, deriveScanFloor(0, 8, 1))
	assert.Equal(refillScanFloorCap, deriveScanFloor(768, 0, 1), "zero drain (0 vCPUs) falls back to the cap, never a 0 divide")
	assert.True(deriveScanFloor(1, 64, 1) >= refillScanFloorMin)
}
