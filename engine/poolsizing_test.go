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
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestPoolSizing_ShardPool pins the per-shard pool derivation: the explicit override pins the pool
// exactly; VirtualCPUs derives the open ceiling at the measured knee (~6x CPUs) with a warm idle core;
// an undeclared count assumes defaultVirtualCPUs (2). That assumption is bounded, not reckless: 2 is
// the floor of every current-gen AWS RDS class, and on the smaller machines that do exist (Cloud SQL's
// 1-vCPU tier) the resulting pool of 12 still sits under the measured knee (that tier peaked at M=16
// and only collapsed from M=32). The derived budgets are per database, so the observed replica count
// splits them; the override is a per-replica exact number and is never divided.
// startSolo starts e as an ISOLATED single-replica engine, keyed by a unique per-engine test-DB name.
// Several independent engines in one test function otherwise share one registry (NewEngineUnderTest keys
// the test DB by t.Name() alone) and would count each other, dividing the pools they are asserting on. Use this
// when a test spins up multiple engines that are meant to be separate deployments, not one fleet.
func startSolo(t *testing.T, e *Engine, key string) {
	t.Helper()
	assert := testarossa.For(t)
	// e must be a NewEngineUnderTest engine; SetTestName overrides its t.Name() key with a per-engine
	// unique one so these independent engines land in separate databases and never count each other.
	assert.NoError(e.SetTestName(t.Name() + "#" + key))
	assert.NoError(e.Startup(context.Background()))
	// Startup registered the t.Cleanup shutdown (e was built with NewEngineUnderTest).
}

// probedRTT is the round-trip time Startup measured for one shard, read under the lock its writer takes.
// A test asserting on anything derived from the worker ceiling must go through this rather than assume the
// same-zone constant: the probe is a real measurement of a real database, and the ceiling moves with it.
func probedRTT(e *Engine, shard int) float64 {
	e.shardsLock.Lock()
	defer e.shardsLock.Unlock()
	return e.shardRTTMs[shard]
}

// addPeerRow inserts a fake peer with the given engine_id into every shard's registry and waits for the
// engine to see it - the DB-backed stand-in for a peer that came online.
//
// dispatched_at as well as seen_at: the dispatcher count reads that column's freshness (evidence a piston
// actually turned), so a fake peer stamping only seen_at is a live replica that never dispatches - a real
// state, but not the one these fixtures mean.
func addPeerRow(t *testing.T, e *Engine, id int64) {
	t.Helper()
	before := peerCounts(e)
	err := e.db.OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_peers (engine_id, seen_at, dispatched_at) VALUES (?, NOW_UTC(), NOW_UTC())", id)
		return err
	})
	if err != nil {
		t.Fatalf("insert peer %d: %v", id, err)
	}
	awaitPeerChange(t, e, before)
}

// delPeerRow removes a fake peer from every shard's registry and waits for the engine to see it go.
func delPeerRow(t *testing.T, e *Engine, id int64) {
	t.Helper()
	before := peerCounts(e)
	err := e.db.OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		_, err := db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", id)
		return err
	})
	if err != nil {
		t.Fatalf("delete peer %d: %v", id, err)
	}
	awaitPeerChange(t, e, before)
}

// insertPeerRows writes fake peers straight into every shard's registry, without waiting for the engine to
// notice. For a test that needs to control WHEN the change is observed - addPeerRow folds the wait and the
// recompute in, which is what most tests want and exactly what a race test must not have.
func insertPeerRows(t *testing.T, e *Engine, ids ...int64) {
	t.Helper()
	for _, id := range ids {
		err := e.db.OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
			_, err := db.ExecContext(ctx,
				"INSERT INTO dwarf_peers (engine_id, seen_at, dispatched_at) VALUES (?, NOW_UTC(), NOW_UTC())", id)
			return err
		})
		if err != nil {
			t.Fatalf("insert peer %d: %v", id, err)
		}
	}
}

// awaitPeerCount waits until one shard's Sonar reports want, WITHOUT recomputing anything - so a test can
// line up what the next recompute will read.
func awaitPeerCount(t *testing.T, e *Engine, shard, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e.replicasOn(shard) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("shard %d settled at %d replicas, want %d", shard, e.replicasOn(shard), want)
}

// peerCounts is what every shard's Sonar currently reports.
func peerCounts(e *Engine) map[int]int {
	counts := map[int]int{}
	for _, idx := range e.db.Indices() {
		counts[idx] = e.replicasOn(idx)
	}
	return counts
}

// awaitPeerChange waits until every shard's count has moved off `before`, then applies it.
//
// A wait rather than a forced read, because the read is the Sonars' and they own their own cadence -
// there is no nudge entry point, deliberately. Under test that cadence is milliseconds (buildSonars), so
// this settles almost at once. The recompute is then driven directly rather than waiting out a second
// cadence for the reconcile loop, so a test asserting on POOL sizes sees them the moment the count it
// caused has landed.
func awaitPeerChange(t *testing.T, e *Engine, before map[int]int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		moved := true
		for idx, was := range before {
			if e.replicasOn(idx) == was {
				moved = false
			}
		}
		if moved {
			e.recomputePools()
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("peer count never moved off %v (now %v)", before, peerCounts(e))
}

func TestPoolSizing_ShardPool(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	cases := []struct {
		vcpus    int
		override int
		replicas int
		idle     int
		open     int
	}{
		{0, 0, 1, 6, 12},   // undeclared: assume 2 vCPUs -> the 2-vCPU pool
		{1, 0, 1, 3, 6},    // 1 vCPU: knee at 6
		{2, 0, 1, 6, 12},   // 2 vCPU: knee at 12
		{4, 0, 1, 12, 24},  // 4 vCPU: knee at 24
		{8, 0, 1, 24, 48},  // 8 vCPU: knee at 48
		{8, 30, 1, 30, 30}, // override wins over derived, pinned exactly
		{0, 5, 1, 5, 5},    // override beats the assumed default too
		{8, 0, 2, 12, 24},  // replicas split the derived budget: each takes its 1/R share of the knee
		{8, 0, 3, 8, 16},
		{1, 0, 4, 2, 2},    // floor: even many replicas keep a usable minimum pool
		{0, 0, 2, 3, 6},    // the assumed default splits across replicas too
		{8, 30, 4, 30, 30}, // the override is per replica and is never divided
	}
	for _, c := range cases {
		idle, open := shardPool(ShardSpec{VirtualCPUs: c.vcpus}, c.override, c.replicas)
		assert.Equal(c.idle, idle, "idle for %+v", c)
		assert.Equal(c.open, open, "open for %+v", c)
	}
}

// TestPoolSizing_ObservedReplicasLive pins the observed-R path end to end through the registry: a peer
// row joining shrinks the derived pool to the 1/R share on the recount; a second peer shrinks it
// further; a departure regrows it; a duplicate recount is a no-op; and the SetMaxOpenConns override,
// once set, is never divided by fleet changes.
func TestPoolSizing_ObservedReplicasLive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	e.testConnCap = 0 // assert the real derived pool sizes, not the test-mode connection cap
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	assert.NoError(err)
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "full budget while alone (R=1)")
	assert.Equal(1, e.replicasOn(1))

	addPeerRow(t, e, 1001)
	assert.Equal(2, e.replicasOn(1))
	assert.Equal(24, db.DB.Stats().MaxOpenConnections, "1/2 share once the peer registered")

	addPeerRow(t, e, 1002)
	assert.Equal(3, e.replicasOn(1))
	assert.Equal(16, db.DB.Stats().MaxOpenConnections, "1/3 share at three replicas")

	e.recomputePools() // recompute with no fleet change: dedupes, so the pools are untouched
	assert.Equal(16, db.DB.Stats().MaxOpenConnections)

	delPeerRow(t, e, 1001)
	delPeerRow(t, e, 1002)
	assert.Equal(1, e.replicasOn(1))
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "departures restore the full budget")

	// The pinned override wins over any fleet change.
	assert.NoError(e.SetMaxOpenConns(11))
	addPeerRow(t, e, 1003)
	assert.Equal(11, db.DB.Stats().MaxOpenConnections, "the pinned override is never divided")
}

// TestPoolSizing_IdleCoreTracksTheDerivedPool pins the half of shardPool nothing else witnesses. database/sql
// reports the configured max OPEN size through DBStats.MaxOpenConnections, but never the configured max idle
// (DBStats.Idle counts connections currently idle - traffic, not configuration), so the derived idle core is
// read back through the VariablePoolIdle recording the sizing sites make.
//
// It is half the open size, floored at 2, and it must follow the open size down as the fleet grows: an idle
// core left at the R=1 width would hold connections a departing replica's share no longer entitles it to.
func TestPoolSizing_IdleCoreTracksTheDerivedPool(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	e.testConnCap = 0 // assert the real derived sizes, not the test-mode connection cap
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	idle := func() int {
		t.Helper()
		v, ok := e.seams.Capture(seamsJoin(VariablePoolIdle, "1"))
		if !ok {
			t.Fatal("the sizing path never recorded a derived idle size for shard 1")
		}
		return v.(int)
	}

	db, err := e.db.Shard(1)
	assert.NoError(err)
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "full budget while alone (R=1)")
	assert.Equal(24, idle(), "idle core is half the open pool at R=1")

	addPeerRow(t, e, 2001)
	assert.Equal(24, db.DB.Stats().MaxOpenConnections)
	assert.Equal(12, idle(), "the idle core follows the pool down to the 1/2 share")

	addPeerRow(t, e, 2002)
	assert.Equal(16, db.DB.Stats().MaxOpenConnections)
	assert.Equal(8, idle(), "and down again to the 1/3 share")

	delPeerRow(t, e, 2001)
	delPeerRow(t, e, 2002)
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "departures restore the full budget")
	assert.Equal(24, idle(), "and restore the idle core with it")
}

// TestPoolSizing_PeerExpiry pins the crashed-peer path: a peer that stops heartbeating (its row goes stale,
// and no goodbye ever comes) drops out of the count once its row ages past the freshness window, and the
// pool regrows on its own. No signal is involved, which is exactly what makes a vanished peer recoverable
// without cooperation.
//
// The row is AGED rather than waited out. The window is tens of seconds by design - it is the conservative
// one, because under-counting over-sizes every pool derived from it - so a test that waited would be
// trading half a minute for a fact the row's own timestamp states directly. What the wait would add is the
// Sonar's read cadence, and that is milliseconds here.
func TestPoolSizing_PeerExpiry(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	assert.NoError(err)

	// A peer registers a fresh row but then never heartbeats again (crashed).
	addPeerRow(t, e, 2001)
	assert.Equal(24, db.DB.Stats().MaxOpenConnections, "1/2 share while the peer's row is fresh")

	// Its row ages out. Written with the database's own clock, never a bound Go time, so this is stale by
	// exactly the measure the Sonar applies.
	shardDB, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = shardDB.ExecContext(t.Context(),
		"UPDATE dwarf_peers SET seen_at=DATE_ADD_MILLIS(NOW_UTC(), ?), dispatched_at=DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" WHERE engine_id=?", -600000, -600000, 2001)
	assert.NoError(err)

	// Nothing forces a recount: the Sonar re-reads on its own cadence and the reconcile loop re-derives.
	// The bound is a "did it hang" ceiling, not a timing contract.
	deadline := time.Now().Add(30 * time.Second)
	for db.DB.Stats().MaxOpenConnections != 48 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	assert.Equal(1, e.replicasOn(1), "crashed peer aged out of the count")
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "full budget restored with no signal and no forcing")
}

// TestPoolSizing_PoolGrowsForLongTasks pins the grow-on-demand pool: with every worker parked in a
// slow ExecuteTask and more work waiting, the pool spawns beyond its resident set (up to the ceiling),
// so long tasks are not capped by the connection-derived dispatch count. This is the whole point of
// deriving the worker maximum from the lease margin rather than from the connection budget.
func TestPoolSizing_PoolGrowsForLongTasks(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	release := make(chan struct{})
	var running atomic.Int32
	g := workflow.NewGraph("Grow")
	g.SetEndpoint("Slow", "grow/slow")
	g.AddTransition("Slow", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("grow/g", g)
	proxy.HandleTask("grow/slow", func(ctx context.Context, f *workflow.Flow) error {
		running.Add(1)
		<-release // park: hold the worker exactly as a minutes-long LLM call would
		return nil
	})

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.Startup(t.Context()))
	resident := int32(e.crew.Resident())
	assert.Equal(int32(96), resident, "resident set is the connection-derived dispatch count")

	// Start more flows than the resident set can hold concurrently.
	const flows = 130
	for range flows {
		_, err := e.Create(ctx, "grow/g", nil, nil)
		assert.NoError(err)
	}
	// The pool must grow past its resident set to run them all at once.
	deadline := time.Now().Add(10 * time.Second)
	for running.Load() < flows && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(int32(flows), running.Load(), "every parked task got a worker")
	assert.True(int32(e.crew.Resident()) > resident, "the pool grew beyond its resident set (got %d, was %d)",
		int32(e.crew.Resident()), resident)
	assert.True(e.crew.Resident() <= int(e.workers.Load()), "growth stays under the ceiling")
	close(release)
}

// TestPoolSizing_CeilingFollowsLivePoolChange pins that the worker ceiling tracks a LIVE pool change.
// The ceiling bounds how fast a completion storm drains through M connections, so it is a function of
// the pool: a SetMaxOpenConns that shrinks the pool must shrink it too. Leaving it stale would keep a
// bound many times too permissive on exactly the storm it exists to contain - and the override path is
// the only one that can re-derive it, because recomputePools (the fleet-change path) early-returns while
// an override pins the pools.
func TestPoolSizing_CeilingFollowsLivePoolChange(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.Startup(t.Context()))

	derived := int(e.workers.Load()) // the ceiling for the derived pool of 48
	assert.True(derived > 1000, "the derived ceiling is large (got %d)", derived)

	// Shrink the pool 12x (the external-pooler case): the ceiling must shrink with it.
	assert.NoError(e.SetMaxOpenConns(4))
	shrunk := int(e.workers.Load())
	assert.True(shrunk < derived/8, "the ceiling followed the pool down (was %d, now %d)", derived, shrunk)

	// And back up: a larger pool drains a storm faster, so it permits more workers.
	assert.NoError(e.SetMaxOpenConns(96))
	grown := int(e.workers.Load())
	assert.True(grown > derived, "the ceiling followed the pool up (derived %d, now %d)", derived, grown)
}

// TestPoolSizing_SaturationDoesNotGrowThePool pins the direction that was missing, and whose absence let
// a runaway ship: a DB-BOUND backlog must NOT grow the pool. The spawn trigger counts workers parked
// inside ExecuteTask (holding no connection); a worker queued on the connection pool is contending, not
// parked, and spawning a peer for it only adds contention. Counting time inside processStep instead -
// which includes that queueing - made "every worker busy" mean "saturated", so any backlog grew the pool
// toward the ceiling: measured at ~20% throughput loss and a ~1,300-worker pool where ~512 sufficed.
func TestPoolSizing_SaturationDoesNotGrowThePool(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Sat")
	g.SetEndpoint("A", "sat/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("sat/g", g)
	proxy.HandleTask("sat/a", func(ctx context.Context, f *workflow.Flow) error {
		return nil // instant: the task never parks, so the pool has no reason to grow
	})

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	// A deliberately tiny pool makes the connection the binding resource: workers WILL queue on it.
	assert.NoError(e.SetMaxOpenConns(2))
	assert.NoError(e.Startup(t.Context()))
	resident := int32(e.crew.Resident())

	// A deep backlog of DB-bound steps: every worker is contending for 2 connections, continuously.
	var done atomic.Int32
	for range 400 {
		go func() {
			_, _, err := e.Run(ctx, "sat/g", nil, nil)
			if err == nil {
				done.Add(1)
			}
		}()
	}
	// The drain is a PRECONDITION for the two assertions below, not a throughput contract, so the bound is a
	// "did it hang" ceiling and is sized for the slowest database the suite runs against. 400 flows through a
	// deliberately 2-connection pool is throughput-bound by construction: in-memory SQLite drains it in about a
	// second and leaves immediately, while a loaded remote SQL Server was measured at 3.5-6.5 flows/s - so a 30s
	// bound cut the drain off at 105 and 196 of 400 on two separate runs. Do NOT shrink the backlog to fit a
	// tighter bound: the 400 concurrent Runs ARE the saturation this test needs every worker queued behind.
	deadline := time.Now().Add(3 * time.Minute)
	for done.Load() < 400 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(int32(400), done.Load(), "the backlog drained")
	assert.Equal(resident, int32(e.crew.Resident()),
		"saturation must not grow the pool (was %d, now %d)", resident, int32(e.crew.Resident()))
	assert.Equal(e.crew.Resident(), e.crew.Idle(), "every worker is idle once the backlog drains")
}

// TestPoolSizing_CapacityWeight pins the placement-weight curve: flat up to 2 vCPUs (the measured
// 1- and 2-vCPU tiers ceiling at the same ~745 steps/s), then ~450 steps/s per vCPU. An undeclared
// count weighs as the assumed default (2 vCPUs), so every shard carries a positive weight.
func TestPoolSizing_CapacityWeight(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	assert.Equal(745, capacityWeight(0)) // undeclared: assume 2 vCPUs
	assert.Equal(745, capacityWeight(1))
	assert.Equal(745, capacityWeight(2))
	assert.Equal(1800, capacityWeight(4))
	assert.Equal(3600, capacityWeight(8))
}

// TestPoolSizing_PickShard pins the weighted placement: cordoned shards are never picked, weights
// follow the capacity curve (an 8-vCPU shard drawing ~4.8x a 1-vCPU shard's flows), and a shard that
// declares no CPUs is placed as the assumed 2-vCPU default.
func TestPoolSizing_PickShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, VirtualCPUs: 8}))
	assert.NoError(e.SetShard(ShardSpec{Index: 3, VirtualCPUs: 4, Cordoned: true}))
	assert.NoError(e.SetShard(ShardSpec{Index: 4})) // undeclared -> the 2-vCPU weight (745)

	counts := map[int]int{}
	for range 10000 {
		idx, err := e.pickShard()
		assert.NoError(err)
		counts[idx]++
	}
	assert.Equal(0, counts[3], "cordoned shard must never be picked")
	// Expected proportions: 745 : 3600 : 745 (total 5090). Allow generous slack for randomness.
	assert.True(counts[2] > counts[1]*3, "8-vCPU shard should draw ~4.8x the 1-vCPU shard (got %d vs %d)", counts[2], counts[1])
	assert.True(counts[1] > 800 && counts[4] > 800, "low-weight shards still receive flows (got %d, %d)", counts[1], counts[4])
}

// TestPoolSizing_AllCordoned pins the loud failure: when every shard is cordoned there is nowhere to
// place a new flow, and pickShard errors rather than silently violating the cordon.
func TestPoolSizing_AllCordoned(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngine()
	assert.NoError(e.SetShard(ShardSpec{Index: 1, Cordoned: true}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, Cordoned: true}))
	_, err := e.pickShard()
	assert.Error(err)
}

// TestPoolSizing_ConcurrentRecomputeAppliesLatestR pins the ORDERING of pool application, which the
// lastAppliedR dedupe does not give. Two peers saying hello microseconds apart during a rolling deploy each
// run a recompute: one reads R=2, the other R=3. Nothing ordered their pushes, so the R=2 sizes could land
// AFTER the R=3 sizes and leave the replica holding a half-of-the-budget pool against a fleet of three -
// over-connecting the shard's server, and sticky until the next fleet change (the dedupe sees R unchanged
// and skips). poolsLock now spans read-of-R through push, so the last writer wins with the value it read.
//
// The interleaving is forced, not raced: a fault freezes the first recompute in exactly that window. With
// the lock, the second recompute blocks on it and applies AFTER - which is the fix working. The two counts
// the race needs are staged through the registry itself, waiting for each to be OBSERVED before the
// recompute that must read it.
func TestPoolSizing_ConcurrentRecomputeAppliesLatestR(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8})) // budget 48
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "full budget while alone (R=1)")

	// Stall the first recompute (one-shot) after it has read a fleet of 2, before it pushes 48/2=24.
	insertPeerRows(t, e, 1001)
	awaitPeerCount(t, e, 1, 2)
	e.seams.InjectN(FaultSlowPoolPush, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		e.recomputePools()
	})
	time.Sleep(slowPoolPushDelay / 10) // the first recompute is now inside the window, holding a stale 2

	// A second recompute runs while the first is stalled, and is NOT stalled: it sees 3 and wants 48/3=16.
	// Unserialized, it pushes 16 immediately and the first then overwrites it with the stale 24.
	// Serialized, it cannot start until the first's push is done, so 16 lands last - the correct value.
	insertPeerRows(t, e, 1002)
	awaitPeerCount(t, e, 1, 3)
	wg.Go(func() {
		e.recomputePools()
	})
	wg.Wait()

	// The fleet is 3, so the pool must be the R=3 share - not the R=2 share applied last.
	assert.Equal(3, e.replicasOn(1))
	assert.Equal(16, db.DB.Stats().MaxOpenConnections,
		"the pool must reflect the LATEST replica count (48/3), not a stale recompute that pushed last")
}

// TestPoolSizing_ConcurrentRecomputeDoesNotClobberOverride is the same race against the OTHER writer of pool
// sizes, and the damaging half: recomputePools reads maxOpenConns and only THEN pushes, so a SetMaxOpenConns
// landing in that window had its pinned pools silently overwritten by derived ones. The operator's explicit
// override - the external-pooler / benchmark path - just evaporated, and stayed gone until the next fleet
// change. poolsLock spans the read through the push, so whichever of the two goes second sees a settled world:
// the override applies last, or the recompute early-returns because the override is already set. Either way
// the pin stands.
func TestPoolSizing_ConcurrentRecomputeDoesNotClobberOverride(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	e.testConnCap = 0 // assert the real derived pool sizes, not the test-mode connection cap
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8})) // derived budget 48
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}

	// A recompute reads a fleet of 2 (derived 24) and stalls before pushing.
	insertPeerRows(t, e, 1001)
	awaitPeerCount(t, e, 1, 2)
	e.seams.InjectN(FaultSlowPoolPush, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		e.recomputePools()
	})
	time.Sleep(slowPoolPushDelay / 10)

	// The operator pins the pool while that recompute is mid-flight.
	assert.NoError(e.SetMaxOpenConns(7))
	wg.Wait()

	assert.Equal(7, db.DB.Stats().MaxOpenConnections,
		"the explicit override must survive a concurrent derived recompute, not be overwritten by its 48/2")
}

// TestPoolSizing_NoOpenShardsIs503 pins that pickShard returns an error instead of panicking when no shard
// is open. It cannot mean "a shardless engine" - Startup always opens at least the default shard - so it
// means the engine is not live: never started, or already SHUT DOWN (ShardSet.Close nils the indices).
// The second is the one that matters: a host still serving while it tears the engine down, or a Create in
// flight when Shutdown lands, is an ordinary race, and indexing the empty index slice (rand.IntN(0)) took
// the host's whole process down with it. Create must surface a 503, not a panic.
func TestPoolSizing_NoOpenShardsIs503(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// (1) Never started. No SetShard either, so this is the default-shard path - the one that indexed the
	// empty slice.
	e := NewEngine()
	e.SetHost(NewTestProxy())
	_, err := e.pickShard()
	if assert.Error(err) {
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err))
	}
	_, err = e.Create(ctx, "unstarted/g", nil, nil) // and Create surfaces it rather than dying
	assert.Error(err)

	// (2) Started, then shut down - the shutdown race, reached through the public API.
	proxy := NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "shutdownrace/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("shutdownrace/g", g)
	proxy.HandleTask("shutdownrace/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e2 := NewEngineUnderTest(t)
	e2.SetHost(proxy)
	// Startup registers a t.Cleanup Shutdown; shutting down early here is idempotent.
	assert.NoError(e2.Startup(t.Context()))
	flowKey, err := e2.Create(ctx, "shutdownrace/g", nil, nil)
	assert.NoError(err) // sanity: it works while live
	assert.NoError(e2.Shutdown(ctx))

	_, err = e2.pickShard()
	if assert.Error(err) {
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err))
	}

	// Every public operation must now say 503 - "the engine is not available" - and NOT the lies it used to
	// tell. The key-addressed ops all route through ShardSet.Shard, which answers "flow not found" (404) when
	// no shard is open: a stopped engine told the caller its flow did not exist. The cross-shard ops were
	// worse - List/Purge/ShardInfo fanned out over an empty index set and returned SUCCESS with an empty
	// result ("you have no flows"). Both are indistinguishable from the truth, and a caller can act on them.
	is503 := func(label string, err error) {
		if assert.Error(err, "%s must fail on a stopped engine", label) {
			assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err), "%s", label)
		}
	}
	_, err = e2.Create(ctx, "shutdownrace/g", nil, nil)
	is503("Create", err)
	_, err = e2.Snapshot(ctx, flowKey)
	is503("Snapshot", err)
	_, err = e2.Fork(ctx, flowKey, nil)
	is503("Fork", err)
	_, err = e2.Continue(ctx, flowKey, nil)
	is503("Continue", err)
	is503("Resume", e2.Resume(ctx, flowKey, nil))
	is503("Cancel", e2.Cancel(ctx, flowKey, "x"))
	is503("Delete", e2.Delete(ctx, flowKey))
	_, err = e2.History(ctx, flowKey)
	is503("History", err)
	_, err = e2.Await(ctx, flowKey)
	is503("Await", err)
	_, _, err = e2.List(ctx, workflow.Query{}) // used to return an EMPTY LIST and no error
	is503("List", err)
	_, err = e2.Purge(ctx, workflow.Query{}) // used to return 0 marked and no error
	is503("Purge", err)
	_, err = e2.ShardInfo(ctx)
	is503("ShardInfo", err)
}

// TestPoolSizing_LiveOverride pins that SetMaxOpenConns pushes the pinned pool to every live shard
// immediately (the expert/benchmark path).
func TestPoolSizing_LiveOverride(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	e.testConnCap = 0 // assert the real derived pool sizes, not the test-mode connection cap
	assert.NoError(e.SetHost(noopHost{}))
	for i := 1; i <= 2; i++ {
		assert.NoError(e.SetShard(ShardSpec{Index: i, VirtualCPUs: 1}))
	}
	assert.NoError(e.Startup(t.Context()))

	// Derived from VirtualCPUs=1: open = 6.
	for i := 1; i <= 2; i++ {
		db, err := e.db.Shard(i)
		assert.NoError(err)
		assert.Equal(6, db.DB.Stats().MaxOpenConnections, "shard %d derived pool", i)
	}

	// Live override pins exactly.
	assert.NoError(e.SetMaxOpenConns(11))
	for i := 1; i <= 2; i++ {
		db, _ := e.db.Shard(i)
		assert.Equal(11, db.DB.Stats().MaxOpenConnections, "shard %d after override", i)
	}

	assert.Error(e.SetMaxOpenConns(0)) // must be >= 1
}

// TestPoolSizing_WorkerCeiling pins the lease-margin ceiling: N_max = M x margin / txTime x safety,
// with txTime = 7 x RTT + 3ms. Every input is engine-visible (the pool, the engine's own 30s margin,
// and the RTT probed at Startup) - no task duration appears, which is the property that makes it a
// derivable default rather than a guess.
func TestPoolSizing_WorkerCeiling(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Same-zone (RTT 0.3ms): txTime = 7*0.3 + 3 = 5.1ms. M=48 -> 48 * 30000/5.1 * 0.25 = 70,588.
	assert.Equal(70588, workerCeiling(48, 0.3))
	// The small-tier pool scales the ceiling down proportionally: M=6 -> 8,823.
	assert.Equal(8823, workerCeiling(6, 0.3))
	// Cross-zone (RTT 1.1ms): txTime = 10.7ms - roughly halves the ceiling at the same pool.
	assert.Equal(33644, workerCeiling(48, 1.1))
	// A high-latency link is dominated by the round trips: RTT 5ms -> txTime 38ms.
	assert.Equal(9473, workerCeiling(48, 5))
	// Degenerate inputs never yield a zero-worker pool.
	assert.True(workerCeiling(1, 0) >= 1)
}

// TestPoolSizing_DerivedWorkers pins the derived worker default (the lease-margin ceiling, so it holds
// for any task duration) and the resident/dispatch split: the eagerly-spawned set and the candidate
// cache stay sized by the connection budget (8x conns, floor 64), because a worker parked in a long
// ExecuteTask holds no connection and must not inflate the refill scan.
func TestPoolSizing_DerivedWorkers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Zero-config: one default shard (undeclared CPUs -> the assumed 2-vCPU pool of 12). The max is the
	// ceiling (large - set by the 30s margin, not by the task); the resident set and cache follow the
	// connection budget: max(64, 8 x 12) = 96.
	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	startSolo(t, e, "e")
	assert.Equal(96, e.workersDispatch, "resident/dispatch set stays connection-derived")
	assert.Equal(192, e.cache.Capacity(), "cache is 2x the dispatch count, never 2x the ceiling")
	// The max is the ceiling DERIVED from this shard's own probed RTT, not a fixed large number. A constant
	// floor here silently reads the probe: the ceiling is 90,000/(7 x RTTms + 3), so a `SELECT 1` measured at
	// 12.4ms - which a loaded parallel -race suite can produce - brings it under a hardcoded 1,000 and fails
	// a test about wiring on an environment measurement. The formula itself is pinned separately by
	// TestPoolSizing_WorkerCeiling; what this pins is that the derived max follows it and is not the
	// dispatch count.
	assert.Equal(workerCeiling(12, probedRTT(e, 1)), int(e.workers.Load()), "the worker max is the lease-margin ceiling")
	assert.True(int(e.workers.Load()) > e.workersDispatch, "the max is the ceiling, not the dispatch count")
	assert.Equal(int32(96), int32(e.crew.Resident()), "only the resident set is spawned eagerly")

	// An 8-vCPU shard (pool 48) + a 2-vCPU shard (pool 12): dispatch = max(64, 8*60) = 480, and the
	// ceiling is keyed on the WORST shard (the 2-vCPU pool of 12), never the aggregate.
	e2 := NewEngineUnderTest(t)
	assert.NoError(e2.SetHost(noopHost{}))
	assert.NoError(e2.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e2.SetShard(ShardSpec{Index: 2, VirtualCPUs: 2}))
	startSolo(t, e2, "e2")
	assert.Equal(480, e2.workersDispatch)
	assert.True(int(e2.workers.Load()) < workerCeiling(48, 0.3), "the smallest pool sets the ceiling")

	// An explicit SetWorkers is spawned IN FULL, even above the connection-derived dispatch count: the
	// operator asked for that many workers, and no-op tasks never park, so growth would never take the
	// pool there on its own. (The derived maximum - the lease-margin ceiling - is the opposite: never
	// spawned up front, filled on demand.)
	eBig := NewEngineUnderTest(t)
	assert.NoError(eBig.SetHost(noopHost{}))
	assert.NoError(eBig.SetWorkers(300)) // > the zero-config dispatch count of 96
	startSolo(t, eBig, "eBig")
	assert.Equal(int32(300), int32(eBig.crew.Resident()), "an explicit SetWorkers is spawned in full")
	assert.Equal(192, eBig.cache.Capacity(), "but the cache still follows the dispatch count")

	// Explicit SetWorkers pins, regardless of shards.
	e3 := NewEngineUnderTest(t)
	assert.NoError(e3.SetHost(noopHost{}))
	assert.NoError(e3.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e3.SetWorkers(2))
	startSolo(t, e3, "e3")
	assert.Equal(2, int(e3.workers.Load()))
}

// TestPoolSizing_CacheFollowsTheReplicaSplit pins that the candidate cache follows the connection budget down
// when the fleet grows - the same "every path that changes a pool must re-derive what depends on it" rule the
// worker ceiling obeys.
//
// Startup derives the dispatch count with R=1, because peer discovery has not run yet, so it is the FULL
// per-database budget. When the replicas find each other, each shard's pool is correctly re-divided by R - but
// the cache used to stay pinned at its R=1 size for the life of the process. A replica in a fleet of 8 then held
// a cache sized for 8x the connections it could actually use, and the refiller (which scans up to the cache's
// capacity per fairness key and wholesale-replaces it) handed it far more candidates than it could ever claim:
// stale hints whose claim CAS loses to a peer, and wasted round-trips, exactly when the fleet is busiest.
//
// Severity is nil at R=1 - where the two numbers agree by construction - which is why this escaped every test and
// every single-replica deployment.
func TestPoolSizing_CacheFollowsTheReplicaSplit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8})) // open = 6 x 8 = 48 at R=1
	assert.NoError(e.Startup(t.Context()))

	assert.Equal(1, e.replicasOn(1))
	solo := e.cache.Capacity()
	assert.True(solo > 100, "a solo replica caches against the whole budget (got %d)", solo)

	// Seven peers register: the pool is re-divided by 8, and the cache must come down with it.
	for id := int64(3001); id <= 3007; id++ {
		addPeerRow(t, e, id)
	}
	assert.Equal(8, e.replicasOn(1))

	split := e.cache.Capacity()
	assert.True(split < solo, "the cache must follow the pool split down (was %d, still %d)", solo, split)

	// And back up as peers leave, so a shrinking fleet is not left under-fed.
	for id := int64(3001); id <= 3007; id++ {
		delPeerRow(t, e, id)
	}
	assert.Equal(1, e.replicasOn(1))
	assert.Equal(solo, e.cache.Capacity(), "the cache follows the pool back up when the fleet shrinks")
}

// TestPoolSizing_StartupSizesSoloFull pins that a solo replica takes its full derived budget as soon as
// Startup returns - R is read from the registry synchronously during Startup (this replica's own row is
// the only one), so there is no async grace window and no cap-then-release: the derived 48 is in place
// before any worker dispatches.
func TestPoolSizing_StartupSizesSoloFull(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8})) // derives 48 at R=1
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	assert.NoError(err)
	assert.Equal(1, e.replicasOn(1), "solo replica reads R=1 from the registry at startup")
	assert.Equal(48, db.DB.Stats().MaxOpenConnections, "the full derived pool is in place the moment Startup returns")
}

// TestPoolSizing_StartupSizesFromRegisteredFleet is the real replacement for the old grace window: a
// replica joining an already-registered fleet sizes for the full R at Startup, so it NEVER over-connects
// on a partial count. eng2 reads eng1's row from the shared registry during its own Startup and opens at
// the R=2 share immediately - the property the async grace window used to approximate, now exact because
// the count comes from the database, not from waiting on signals. It also pins that the count propagates
// with no signal wiring at all (noopHost): eng1 converges to R=2 purely via its heartbeat recount.
func TestPoolSizing_StartupSizesFromRegisteredFleet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	// eng1 starts solo and registers itself. Small heartbeat so its recount converges within the test.
	eng1 := NewEngineUnderTest(t)
	assert.NoError(eng1.SetHost(noopHost{}))
	assert.NoError(eng1.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(eng1.Startup(ctx))
	t.Cleanup(func() { eng1.Shutdown(ctx) })

	db1, err := eng1.db.Shard(1)
	assert.NoError(err)
	assert.Equal(48, db1.DB.Stats().MaxOpenConnections, "solo eng1 holds the full budget")

	// eng2 starts against the SAME databases (shared test-DB key, both keyed by t.Name()). At its Startup
	// it reads eng1's row, so R=2 is known BEFORE it dispatches - it opens at 24, never over-connecting on
	// a partial count.
	eng2 := NewEngineUnderTest(t)
	assert.NoError(eng2.SetHost(noopHost{}))
	assert.NoError(eng2.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(eng2.Startup(ctx))
	t.Cleanup(func() { eng2.Shutdown(ctx) })

	db2, err := eng2.db.Shard(1)
	assert.NoError(err)
	assert.Equal(2, eng2.replicasOn(1), "eng2 sees the registered fleet at startup")
	assert.Equal(24, db2.DB.Stats().MaxOpenConnections, "eng2 sizes for R=2 immediately - no partial over-connect")

	// eng1 converges to the R=2 share on its own reading, with no signal wiring.
	deadline := time.Now().Add(5 * time.Second)
	for db1.DB.Stats().MaxOpenConnections != 24 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(24, db1.DB.Stats().MaxOpenConnections, "eng1 converges to R=2 by reading the registry")
}

// TestPoolSizing_StartupOverridePins pins that SetMaxOpenConns - the expert / external-pooler path -
// pins the pool exactly at Startup and is not divided by R (the discovery still runs so peers count this
// replica, but the override is never second-guessed).
func TestPoolSizing_StartupOverridePins(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, VirtualCPUs: 8}))
	assert.NoError(e.SetMaxOpenConns(40))
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	assert.NoError(err)
	assert.Equal(40, db.DB.Stats().MaxOpenConnections, "an explicit override pins the pool, ignoring R")
}
