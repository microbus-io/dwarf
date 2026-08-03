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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// The selection ALGORITHM is no longer tested here. The cross-shard merge, the fairness lottery, the
// slice rule and its starvation guard live in internal/planner and are pinned by its own tests; the
// cycle's error policy lives in internal/pipeline; the two queries live in internal/piston. What remains
// here is what only a whole engine can show: that the pistons drain a real fleet, that the cycle period
// is derived and pushed, and that a rate limit cannot wedge a backlog.

// TestRefillDecoupled_MultiShardDrains pins the fleet end to end: one piston per shard, each cycling on
// its own clock with no barrier against its peers, drains work placed across both shards.
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

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
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

// TestRefillInterval_OneCyclePerPistonNoTrigger pins the shape that replaced the single-slot trigger: a
// piston cycles UNCONDITIONALLY on its period, so a step no local activity ever announced is still picked
// up. Nothing arms anything - that is the point. Under the old design an idle shard parked on a trigger
// only its own activity could arm, which is why a cross-replica doorbell had to exist and then why an
// idle tick had to replace it; an unconditional cycle subsumes both.
func TestRefillInterval_OneCyclePerPistonNoTrigger(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("Do", "notrigger/nop")
	g.AddTransition("Do", workflow.END)
	proxy.HandleGraph("notrigger/g", g)
	proxy.HandleTask("notrigger/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.Startup(t.Context()))

	// FaultDropDoorbell removes the local Offer entirely, so nothing but a cycle can discover this step.
	e.seams.InjectN(FaultDropDoorbell, 1<<20)
	k, err := e.Create(ctx, "notrigger/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	outcome, err := e.Await(deadline, k)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, "a cycle must find work no doorbell announced")
	}
}

// TestRefillInterval_OverridePinsAndRestores pins the benchmarking override (SetRefillInterval): a
// positive value pins every piston's period, and <=0 restores derivation. It exists so a scan-rate sweep
// can hold the period at a series of fixed values; the derived value is otherwise not externally settable.
func TestRefillInterval_OverridePinsAndRestores(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(NewTestProxy()))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	derived := e.pistons[1].Interval()
	assert.True(derived > 0, "a started engine derives and pushes a period")

	assert.NoError(e.SetRefillInterval(42 * time.Millisecond))
	assert.Equal(42*time.Millisecond, e.pistons[1].Interval(), "the override pins it live")

	assert.NoError(e.SetRefillInterval(0))
	assert.Equal(derived, e.pistons[1].Interval(), "<=0 restores derivation")
}

// TestRefillInterval_DerivedFromStaticConfig pins the period's derivation. It is arithmetic over values
// known at Startup - capacity, declared vCPUs, observed replicas - and reads NO observed rate, which is
// the distinction that matters: the version that set the interval from measured consumption oscillated
// (same scan count and batch as a fixed period, ~1,000x the discard, 2.4x the p99) because consumption is
// min(demand, supply). Static arithmetic cannot do that.
//
// Deriving rather than hardcoding is what keeps the period correct when the sizing it depends on moves:
// capacity is 2 x workersPerConnBudget x conns, so a change there silently rescales the buffer this
// period is measured against. Campaign 11 nearly made exactly that change.
func TestRefillInterval_DerivedFromStaticConfig(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// The DERIVED path: a shard's pool is connsPerVCPU*vCPUs/R, so the two drain channels agree and the
	// min is a no-op. The measured rig: 8 vCPUs, R=1 -> a 48-conn pool and a 768-candidate partition
	// share. The configuration terms cancel (bufferShare is 96*vCPUs/R and the sustained drain
	// 720*vCPUs/R), so the derived period is 96/(2*720) ~= 67ms and is the SAME at any vCPU or replica
	// count. 67ms sits inside the measured-good band (10-80ms on the 2026-07-22 rig M-sweep); the earlier
	// 340 constant put it at 141ms, the worst point in that band.
	rig := deriveRefillInterval(768, 8, 48, 1)
	assert.True(rig > 60*time.Millisecond && rig < 74*time.Millisecond,
		"expected ~67ms at the measured configuration, got %v", rig)
	// Doubling vCPUs doubles the buffer share, the pool, AND the drain, so the period is unchanged.
	assert.Equal(rig, deriveRefillInterval(1536, 16, 96, 1))
	// Same for replicas: R halves the share, the pool, and the per-replica drain together.
	assert.Equal(rig, deriveRefillInterval(384, 8, 24, 2))

	// A bigger buffer covers a longer gap; a faster shard (bigger pool) needs more frequent scans.
	assert.True(deriveRefillInterval(1536, 8, 48, 1) > rig)
	assert.True(deriveRefillInterval(768, 32, 192, 1) < rig)

	// The FOOTGUN: an operator pins a large pool with SetMaxOpenConns but leaves VirtualCPUs undeclared.
	// The buffer is sized off the big pool; the drain must follow the SAME pool. Deriving it from the
	// default 2 vCPUs instead (the old bug) overshot the period to the 1s cap and starved the refiller -
	// the rig's 20-80s fan-out latency. With the pool driving the drain, the period stays at the optimum.
	assert.True(deriveRefillInterval(3072, 0, 192, 1) > 60*time.Millisecond && deriveRefillInterval(3072, 0, 192, 1) < 74*time.Millisecond,
		"a big pinned pool with undeclared vCPUs must derive drain from the pool, not clamp to the cap")
	// The SAME buffer on a genuinely slow 2-vCPU shard (small pool to match) correctly caps - it really
	// is that slow. The fix distinguishes "big pool, vCPUs just unset" from "actually a 2-vCPU shard".
	assert.Equal(refillIntervalCap, deriveRefillInterval(3072, 2, 12, 1))

	// Both provided, connection-constrained (32-vCPU DB behind a 48-conn pooler): the drain follows the
	// tighter CONNECTION channel, identical to leaving the vCPUs unset.
	assert.Equal(deriveRefillInterval(768, 0, 48, 1), deriveRefillInterval(768, 32, 48, 1))
	// Both provided, CPU-constrained (4-vCPU DB with an over-provisioned 192-conn pool): the drain
	// follows the tighter CPU channel, so the extra connections do not shorten the period.
	assert.Equal(deriveRefillInterval(768, 4, 24, 1), deriveRefillInterval(768, 4, 192, 1))

	// The cap governs where the cancellation breaks - workersDispatch's max(64, ...) floor at small or
	// high-R configurations - and degenerate inputs fall back to it rather than to zero, which would
	// restore the 100%-duty-cycle hot loop.
	assert.Equal(refillIntervalCap, deriveRefillInterval(4096, 2, 2, 8))
	assert.Equal(refillIntervalCap, deriveRefillInterval(0, 8, 48, 1))
	assert.Equal(refillIntervalCap, deriveRefillInterval(768, 0, 0, 1), "zero pool -> zero drain falls back to the cap, never a 0 divide")

	// There is deliberately NO minimum: a degenerate-small buffer derives a sub-millisecond period, and
	// what keeps that from becoming a 100%-duty-cycle scan loop is the pipeline's MinGap, which bounds the
	// quiet time between cycles rather than their start-to-start period. A period-side floor could not -
	// it cannot bound a cycle that outruns it, which is exactly the deep-backlog case the fuse is for.
	assert.True(deriveRefillInterval(1, 64, 384, 1) < time.Millisecond)
}

// TestRefillInterval_DeepBacklogLiveness pins that rate-limiting the scan cannot wedge a deep backlog:
// with a single worker (cache capacity 2) and a backlog of flows several times the capacity, everything
// still completes under a period set well into the over-limiting regime. The period bounds scan
// FREQUENCY, never delivery.
func TestRefillInterval_DeepBacklogLiveness(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("Do", "refillfloorliveness.verify:428/nop")
	g.AddTransition("Do", workflow.END)
	proxy.HandleGraph("refillfloorliveness.verify:428/one", g)
	proxy.HandleTask("refillfloorliveness.verify:428/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	e.SetWorkers(1)                                            // capacity 2, against a backlog of 10
	assert.NoError(e.SetRefillInterval(50 * time.Millisecond)) // the over-limiting regime; must still drain, just slower
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

// TestRefillScan_BoundedPerFairnessKey pins that the band scan's cost scales with fairness-key
// CARDINALITY, not with the backlog, THROUGH THE ENGINE'S OWN WIRING - the piston tests cover the two
// queries in isolation, but only this one proves the engine hands its piston the capacity that bounds
// them. Phase 1 collapses each key to a single aggregate row server-side, so a deep backlog on N tenants
// returns N rows, not N*capacity; phase 3 then reads only the selected steps, at most perKey OLDEST per
// chosen key.
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
	// sees a real backlog. It also idles the pistons, so nothing races these hand-driven queries.
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
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
	// which is all the plan can ever consume from one key, so the cap is lossless. SetWorkers(0) makes
	// this fixture's capacity small, so the assertion is the capped value, not the raw backlog depth.
	band, tallies, err := e.pistons[1].ScanBand(ctx, 1)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(2, len(tallies), "one aggregate row per tenant, not per step in the %d-step backlog", 2*perTenant)
	counts := map[string]int{}
	for _, tl := range tallies {
		counts[tl.Key] = tl.Count
	}
	capped := min(perTenant, e.cache.Capacity())
	assert.Equal(capped, counts["tenant-a"])
	assert.Equal(capped, counts["tenant-b"])

	// Phase 3: the fetch cuts each key at perKey - not the backlog - and every key still reaches the batch.
	for _, perKey := range []int{1, 3, 10} {
		byKey, err := e.pistons[1].FetchSteps(ctx, 1, band, tenants, perKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(2, len(byKey), "both tenants are represented (fairness is not sacrificed to the bound)")
		for key, list := range byKey {
			assert.Equal(perKey, len(list), "key %q is cut at perKey", key)
		}
	}

	// And the fetch keeps the OLDEST step of each key: the single row for a key at perKey=1 is its oldest
	// (smallest step_id here, since steps are created in age order). A bound that kept the newest would
	// starve the head of every queue.
	one, err := e.pistons[1].FetchSteps(ctx, 1, band, tenants, 1)
	if !assert.NoError(err) {
		return
	}
	all, err := e.pistons[1].FetchSteps(ctx, 1, band, tenants, perTenant)
	if !assert.NoError(err) {
		return
	}
	for key, list := range all {
		oldest := list[0]
		for _, id := range list {
			oldest = min(oldest, id)
		}
		if assert.Equal(1, len(one[key])) {
			assert.Equal(oldest, one[key][0], "the single fetched row for %q is its oldest step", key)
		}
	}
}
