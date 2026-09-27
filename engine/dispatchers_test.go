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

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// fakeFleet is six peer ids and the two engine ids whose rendezvous ranks against them on shard 1 are
// known: three of the peers outrank readerID and one outranks dispatcherID. A test that needs this replica on one side of
// the dispatcher cut sets its id to one of them, and checks the rank before relying on it.
var fakeFleet = []int64{1001, 1002, 1003, 1004, 1005, 1006}

const (
	readerID     = 7004
	dispatcherID = 7006
)

// joinFakeFleet registers fakeFleet on every shard, waits for the engine to count all seven, and applies
// the roles that count implies.
func joinFakeFleet(t *testing.T, e *Engine) {
	t.Helper()
	err := e.db.OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		for _, id := range fakeFleet {
			_, err := db.ExecContext(ctx,
				"INSERT INTO dwarf_peers (engine_id, seen_at, dispatched_at) VALUES (?, NOW_UTC(), NOW_UTC())", id)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("insert fake fleet: %v", err)
	}
	awaitPeerCount(t, e, 1, len(fakeFleet)+1)
	e.recomputePools()
}

// leaveFakeFleet removes fakeFleet from every shard and applies the solo role that follows.
func leaveFakeFleet(t *testing.T, e *Engine) {
	t.Helper()
	err := e.db.OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		for _, id := range fakeFleet {
			if _, err := db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", id); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("delete fake fleet: %v", err)
	}
	awaitPeerCount(t, e, 1, 1)
	e.recomputePools()
}

// TestDispatchers_Slots pins how many replicas dispatch a shard. The count comes from the shard's budget
// at the REFERENCE distance, never the probed one, so every replica derives the same number; it never
// exceeds the replicas there are, and it is at least two once there are two.
func TestDispatchers_Slots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	cases := []struct {
		vcpus, replicas, want int
	}{
		{2, 1, 1},   // a lone replica is its own dispatcher
		{2, 2, 2},   // a small shard still takes two once there are two, so one death does not stop it
		{2, 7, 2},   // ...and no more: the budget cannot feed a third
		{0, 7, 2},   // undeclared vCPUs size as the default 2
		{8, 2, 2},   // never more dispatchers than replicas
		{8, 100, 4}, // an 8-vCPU budget of 53 at the reference distance, /12
		{64, 100, 16},
		{64, 3, 3},
	}
	for _, c := range cases {
		assert.Equal(c.want, dispatcherSlots(ShardSpec{VirtualCPUs: c.vcpus}, c.replicas), "%+v", c)
	}
	// Agreement: the answer is independent of the RTT this replica happened to probe.
	assert.Equal(shardBudget(8, defaultRTTMs), shardBudget(8, defaultRTTMs))
}

// TestDispatchers_ReaderReplicaHoldsTheReaderPoolAndRunsNothing pins the whole role on a replica ranked
// below its shard's dispatcher cut: it holds the reader pool, its piston idles, and it does not ring its
// own doorbell - so a flow it creates is left to the shard's dispatchers rather than run through the reader
// pool. When the fleet leaves and it is promoted, the flow runs.
func TestDispatchers_ReaderReplicaHoldsTheReaderPoolAndRunsNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Reader")
	g.SetEndpoint("A", "reader/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("reader/g", g)
	proxy.HandleTask("reader/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.testConnCap = 0 // assert the real derived pool sizes, not the test-mode connection cap
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetEngineID(readerID))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 2}))
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)
	assert.True(e.dispatchesOn(1), "alone, the replica dispatches")

	joinFakeFleet(t, e)
	if rank, _ := e.sonarFor(1).Rank(); !assert.Equal(3, rank, "fixture: readerID must rank below a 2-slot cut") {
		return
	}
	assert.Equal(0, e.slotsOn(1, ShardSpec{Index: 1, VirtualCPUs: 2}))
	assert.False(e.dispatchesOn(1))
	assert.True(e.pistons[1].Idle(), "a reader's piston idles")
	assert.Equal(readerOpenConns, db.DB.Stats().MaxOpenConnections, "a reader holds the reader pool")

	fk, err := e.Create(ctx, "reader/g", nil, nil)
	assert.NoError(err)
	assert.Equal(0, e.cache.Len(), "a reader does not offer the step to its own cache")
	time.Sleep(300 * time.Millisecond)
	snap, err := e.Snapshot(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusRunning, snap.Status, "nobody real dispatches the shard, so nothing ran")

	leaveFakeFleet(t, e)
	assert.True(e.dispatchesOn(1), "promoted once the fleet is gone")
	assert.False(e.pistons[1].Idle())
	outcome, err := e.Await(ctx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, "the promoted dispatcher's scan picks the step up")
	}
}

// TestDispatchers_RankedReplicaSplitsTheBudget is the other side of the cut: a replica ranked inside it
// dispatches, with the shard's budget divided by the slot count rather than by the whole fleet.
func TestDispatchers_RankedReplicaSplitsTheBudget(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.testConnCap = 0
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetEngineID(dispatcherID))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)

	joinFakeFleet(t, e)
	slots := dispatcherSlots(ShardSpec{VirtualCPUs: 8}, len(fakeFleet)+1)
	if rank, _ := e.sonarFor(1).Rank(); !assert.Equal(1, rank, "fixture: dispatcherID must rank inside a %d-slot cut", slots) {
		return
	}
	assert.True(e.dispatchesOn(1))
	_, open := shardPool(ShardSpec{VirtualCPUs: 8}, 0, slots, probedRTT(e, 1))
	assert.Equal(open, db.DB.Stats().MaxOpenConnections, "the budget divides by %d dispatchers, not 7 replicas", slots)
}

// TestDispatchers_OverrideDispatchesEveryReplica pins the pooler path: with SetMaxOpenConns pinning the
// pools there is no budget to divide, so a replica ranked below the cut dispatches anyway - including one
// that was already a reader when the override landed.
func TestDispatchers_OverrideDispatchesEveryReplica(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.testConnCap = 0
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetEngineID(readerID))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 2}))
	assert.NoError(e.Startup(t.Context()))

	joinFakeFleet(t, e)
	assert.False(e.dispatchesOn(1))

	assert.NoError(e.SetMaxOpenConns(9))
	e.recomputePools()
	assert.True(e.dispatchesOn(1), "an override makes every replica a dispatcher")
	assert.False(e.pistons[1].Idle())
	db, err := e.db.Shard(1)
	assert.NoError(err)
	assert.Equal(9, db.DB.Stats().MaxOpenConnections, "and the role change leaves the pinned pool alone")
}

// TestDispatchers_ZeroWorkersReplicaRegistersButNeverRanks pins how an await-only replica takes part: it
// registers - so the fleet counts the connections it holds, and an operator can see it with its RTT - but
// flagged, so no shard's ranking can hand it a dispatcher slot it would never serve.
func TestDispatchers_ZeroWorkersReplicaRegistersButNeverRanks(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.testConnCap = 0
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetWorkers(0))
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	assert.NoError(err)
	var zeroWorkers int
	var rttUs int64
	assert.NoError(db.QueryRowContext(t.Context(),
		"SELECT zero_workers, rtt_us FROM dwarf_peers WHERE engine_id=?", e.engineID).Scan(&zeroWorkers, &rttUs))
	assert.Equal(1, zeroWorkers, "registered, and flagged")
	assert.Equal(time.Duration(probedRTT(e, 1)*float64(time.Millisecond)).Microseconds(), rttUs,
		"with the RTT it probed, in microseconds")
	assert.False(e.dispatchesOn(1))
	assert.Equal(readerOpenConns, db.DB.Stats().MaxOpenConnections, "holding the reader pool")
}

// TestDispatchers_ZeroWorkersPeersDoNotDivideTheBudget pins why the slots come from the candidates rather than
// from every registered replica: a lone worker beside await-only frontends is the shard's only possible
// dispatcher, and must hold the whole budget rather than a share sized for replicas that will never use it.
func TestDispatchers_ZeroWorkersPeersDoNotDivideTheBudget(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.testConnCap = 0
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)

	for _, id := range []int64{3001, 3002, 3003} {
		_, err := db.ExecContext(t.Context(),
			"INSERT INTO dwarf_peers (engine_id, seen_at, zero_workers) VALUES (?, NOW_UTC(), 1)", id)
		assert.NoError(err)
	}
	awaitPeerCount(t, e, 1, 4)
	e.recomputePools()
	_, candidates := e.sonarFor(1).Rank()
	assert.Equal(1, candidates, "three await-only peers, one candidate")
	assert.True(e.dispatchesOn(1))
	assert.Equal(budget8(e, 1), db.DB.Stats().MaxOpenConnections, "the lone worker keeps the whole budget")
}

// TestDispatchers_DoorbellRingsOnlyForDispatchedShards pins the gate at its source. The cache admits a
// step for any shard, and a worker that pops one executes the whole chain behind it, so the gate has to sit
// in front of the offer.
func TestDispatchers_DoorbellRingsOnlyForDispatchedShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetWorkers(0)) // no crew draining the cache while the test looks at it
	assert.NoError(e.Startup(t.Context()))

	e.dispatching[1].Store(false)
	e.enqueueStepDue(t.Context(), 1, 999, 100)
	assert.Equal(0, e.cache.Len(), "a shard this replica only reads is never offered")

	e.dispatching[1].Store(true)
	e.enqueueStepDue(t.Context(), 1, 999, 100)
	assert.Equal(1, e.cache.Len(), "a dispatched shard is")
}

// TestDispatchers_PlacementPrefersDispatchedShards pins pickShard's preference: a new flow lands on a shard
// this replica dispatches, where its own doorbell rings, and falls back to every shard only when it
// dispatches none.
func TestDispatchers_PlacementPrefersDispatchedShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 2}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, VirtualCPUs: 2}))
	assert.NoError(e.Startup(t.Context()))

	e.dispatching[2].Store(false)
	for range 50 {
		shard, err := e.pickShard()
		assert.NoError(err)
		assert.Equal(1, shard, "only the dispatched shard takes new flows")
	}

	e.dispatching[1].Store(false)
	seen := map[int]bool{}
	for range 200 {
		shard, err := e.pickShard()
		assert.NoError(err)
		seen[shard] = true
	}
	assert.True(seen[1] && seen[2], "a replica dispatching nothing places across every shard")
}

// TestDispatchers_FleetLimitWarnsOnTheEdge pins the advisory check: it flips when the fleet's connections
// cross the server's limit and flips back when they return, and a server that cannot report a limit is
// never judged.
func TestDispatchers_FleetLimitWarnsOnTheEdge(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.Startup(t.Context()))
	spec := ShardSpec{Index: 1, VirtualCPUs: 8}

	e.poolsLock.Lock()
	defer e.poolsLock.Unlock()
	e.checkFleetFits(1, spec, defaultRTTMs, 0)
	assert.False(e.fleetOverLimit[1], "no known limit, no verdict")
	e.checkFleetFits(1, spec, defaultRTTMs, 10)
	assert.True(e.fleetOverLimit[1], "a budget of ~54 does not fit in 80% of 10")
	e.checkFleetFits(1, spec, defaultRTTMs, 1000)
	assert.False(e.fleetOverLimit[1], "and fits again under a larger limit")
}

// TestDispatchers_SweepsSkipShardsThisReplicaOnlyReads pins that the background sweeps run where this
// replica dispatches and nowhere else. The reaper stands in for both loops: each shard always has a
// dispatcher to sweep it, and a reader running the same scan through its two-connection pool would only
// duplicate the work, once per replica in the fleet.
func TestDispatchers_SweepsSkipShardsThisReplicaOnlyReads(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()
	g := workflow.NewGraph("Sweep")
	g.SetEndpoint("A", "sweep/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("sweep/g", g)
	proxy.HandleTask("sweep/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	shortenDeletion(e, time.Millisecond, time.Hour) // due immediately; the test drives reaps
	assert.NoError(e.Startup(t.Context()))

	waitDone := awaitNextStop(t, e)
	fk, err := e.Create(ctx, "sweep/g", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	waitDone()
	shard, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	time.Sleep(5 * time.Millisecond) // inherent wall-clock: let the 1ms deletion window elapse

	e.dispatching[shard].Store(false)
	e.reapDueFlows(ctx)
	assert.Equal(1, shardFlowCount(t, e, shard), "a reader leaves the shard to its dispatchers")

	e.dispatching[shard].Store(true)
	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shard), "a dispatcher reaps it")
}
