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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// fakeFleet is six peer ids, and readerID and dispatcherID are the two engine ids whose places against them in
// the shared order are known: joinFakeFleet registers three of the peers as having joined before readerID and
// one before dispatcherID, and the rest after. Any other engine id joined first and ranks first. A test that needs this replica on one side of
// the dispatcher cut sets its id to one of them, and checks the rank before relying on it.
var fakeFleet = []int64{1001, 1002, 1003, 1004, 1005, 1006}

const (
	readerID     = 7000
	dispatcherID = 7669
)

// joinFakeFleet registers fakeFleet on every shard, waits for the engine to count all seven, and applies
// the roles that count implies.
func joinFakeFleet(t *testing.T, e *Engine) {
	t.Helper()
	ahead := map[int64]int{readerID: 3, dispatcherID: 1}[e.engineID]
	err := e.db.OnEach(context.Background(), func(ctx context.Context, db *sequel.DB, shard int) error {
		for i, id := range fakeFleet {
			// The peers placed ahead joined an hour ago; the rest join now, after the engine under test.
			joined := 0
			if i < ahead {
				joined = -3600000
			}
			_, err := db.ExecContext(ctx,
				"INSERT INTO dwarf_peers (engine_id, seen_at, dispatched_at, ordering_seq) VALUES (?, NOW_UTC(), NOW_UTC(), ?)",
				id, time.Now().UnixMilli()+int64(joined))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("insert fake fleet: %v", err)
	}
	for _, idx := range e.db.Indices() {
		awaitPeerCount(t, e, idx, len(fakeFleet)+1)
	}
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

// TestDispatchers_WindowsBalanceTheRoles pins the staggered cut's arithmetic. The shard at place p takes the
// slots positions starting at p*slots in the shared order, so (1) every shard has exactly slots distinct
// dispatchers, and (2) every replica carries the floor or the ceiling of shards*slots/candidates roles - the
// balance an independent per-shard draw does not give.
func TestDispatchers_WindowsBalanceTheRoles(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for shards := 1; shards <= 8; shards++ {
		for candidates := 1; candidates <= 20; candidates++ {
			slots := dispatcherSlots(candidates)
			roles := make([]int, candidates)
			for o := range shards {
				n := 0
				for rank := range candidates {
					if inWindow(rank, candidates, o, slots) {
						roles[rank]++
						n++
					}
				}
				assert.Equal(slots, n, "shards %d candidates %d: shard %d has %d dispatchers", shards, candidates, o, n)
			}
			lo, hi := shards*slots/candidates, (shards*slots+candidates-1)/candidates
			for rank, r := range roles {
				assert.True(r >= lo && r <= hi, "shards %d candidates %d: rank %d carries %d roles, want %d..%d",
					shards, candidates, rank, r, lo, hi)
			}
		}
	}
}

// TestDispatchers_WindowsMoveOneDispatcherPerShard pins the churn bound when the order has room for every
// window (candidates >= shards*slots): a replica joining at any place shifts the later places by one, so a
// shard's window slides by at most one replica - it swaps at most one of its dispatchers. With fewer
// candidates the windows wrap and a join can move more; that is the small-fleet case, accepted.
func TestDispatchers_WindowsMoveOneDispatcherPerShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for shards := 1; shards <= 6; shards++ {
		for candidates := shards * 2; candidates <= 16; candidates++ {
			// Replicas are named by their place before the join; the joiner takes place p and is named -1.
			for p := 0; p <= candidates; p++ {
				for o := range shards {
					before, after := map[int]bool{}, map[int]bool{}
					for rank := range candidates {
						if inWindow(rank, candidates, o, dispatcherSlots(candidates)) {
							before[rank] = true
						}
					}
					for rank := range candidates + 1 {
						name := rank
						if rank == p {
							name = -1
						} else if rank > p {
							name = rank - 1
						}
						if inWindow(rank, candidates+1, o, dispatcherSlots(candidates+1)) {
							after[name] = true
						}
					}
					gone := 0
					for name := range before {
						if !after[name] {
							gone++
						}
					}
					assert.True(gone <= 1, "shards %d candidates %d join at %d: shard %d lost %d dispatchers",
						shards, candidates, p, o, gone)
				}
			}
		}
	}
}

// TestDispatchers_ShardsStaggerTheirCuts pins the wiring end to end: on two shards sharing one fleet, each
// shard's cut sits at its own place in the order - by the shard's place among the indices, not the index, so
// the shards are 1 and 3. Among seven candidates, shard 1 takes ranks 0-1 and shard 3 takes ranks 2-3, so
// readerID (rank 3) dispatches shard 3 alone and dispatcherID (rank 1) shard 1 alone - where one shared top-two
// would have given both shards to the same replicas, and an index-anchored window would have put shard 3 at
// ranks 4-5.
func TestDispatchers_ShardsStaggerTheirCuts(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, c := range []struct {
		id       int64
		on1, on3 bool
	}{
		{readerID, false, true},
		{dispatcherID, true, false},
	} {
		e := NewEngineUnderTest(t.Name() + "#" + strconv.FormatInt(c.id, 10))
		assert.NoError(e.SetHost(noopHost{}))
		assert.NoError(e.SetEngineID(c.id))
		assert.NoError(e.SetShard(ShardSpec{Index: 1}))
		assert.NoError(e.SetShard(ShardSpec{Index: 3}))
		assert.Must(assert.NoError(e.Startup(t.Context())))
		joinFakeFleet(t, e)
		assert.Equal(c.on1, e.dispatchesOn(1), "id %d on shard 1", c.id)
		assert.Equal(c.on3, e.dispatchesOn(3), "id %d on shard 3", c.id)
		e.Shutdown(t.Context())
	}
}

// TestDispatchers_EveryShardSharesTheOrder pins that a replica takes the same place in the order on every shard.
// The windows tile one order; a key each shard's database stamped on its own would order replicas that start
// together differently per shard, leaving one with no role while another holds two.
func TestDispatchers_EveryShardSharesTheOrder(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	assert.NoError(e.SetHost(noopHost{}))
	for _, idx := range []int{1, 2, 3, 4} {
		assert.NoError(e.SetShard(ShardSpec{Index: idx}))
	}
	assert.Must(assert.NoError(e.Startup(t.Context())))
	defer e.Shutdown(t.Context())

	var mu sync.Mutex
	seqs := map[int64]bool{}
	err := e.db.OnEach(t.Context(), func(ctx context.Context, db *sequel.DB, shard int) error {
		var seq int64
		err := db.QueryRowContext(ctx, "SELECT ordering_seq FROM dwarf_peers WHERE engine_id=?", e.engineID).Scan(&seq)
		mu.Lock()
		seqs[seq] = true
		mu.Unlock()
		return err
	})
	assert.NoError(err)
	assert.Len(seqs, 1, "one ordering sequence on every shard")
	for seq := range seqs {
		assert.True(seq > 0)
	}
}

// TestDispatchers_PinnedRestartKeepsOneOrder pins that a replica restarting under a pinned engine id takes one
// ordering sequence on every shard, the oldest any of its surviving rows held. Its rows can be pruned on some
// shards and not others while it is down; the survivors kept the old value and the re-created rows took the new
// start time, so the replica ranked differently per shard and the windows stopped tiling.
func TestDispatchers_PinnedRestartKeepsOneOrder(t *testing.T) {
	t.Parallel()
	const pinned = int64(777)
	for _, c := range []struct {
		name   string
		stored map[int]int64 // the rows the last run left, by shard
		want   int64         // 0: any one value, the same on both shards
	}{
		{"pruned_on_one_shard", map[int]int64{1: 100}, 100},
		{"diverged_rows", map[int]int64{1: 100, 2: 300}, 100},
		{"no_rows_left", map[int]int64{}, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			// A peer holds the shards' databases open while the pinned replica is down.
			keeper := NewEngineUnderTest(t.Name())
			assert.NoError(keeper.SetHost(noopHost{}))
			assert.NoError(keeper.SetWorkers(0))
			assert.NoError(keeper.SetShard(ShardSpec{Index: 1}))
			assert.NoError(keeper.SetShard(ShardSpec{Index: 2}))
			assert.Must(assert.NoError(keeper.Startup(t.Context())))
			defer keeper.Shutdown(t.Context())
			for shard, seq := range c.stored {
				sh, err := keeper.db.Shard(shard)
				assert.Must(assert.NoError(err))
				db := sh.DB()
				_, err = db.ExecContext(t.Context(),
					"INSERT INTO dwarf_peers (engine_id, seen_at, ordering_seq, working, draining, rtt_us) VALUES (?, NOW_UTC(), ?, 1, 0, 0)",
					pinned, seq)
				assert.Must(assert.NoError(err))
			}

			e := NewEngineUnderTest(t.Name())
			assert.NoError(e.SetHost(noopHost{}))
			assert.NoError(e.SetEngineID(pinned))
			assert.NoError(e.SetShard(ShardSpec{Index: 1}))
			assert.NoError(e.SetShard(ShardSpec{Index: 2}))
			assert.Must(assert.NoError(e.Startup(t.Context())))
			defer e.Shutdown(t.Context())

			seqs := map[int]int64{}
			for _, shard := range []int{1, 2} {
				sh, err := e.db.Shard(shard)
				assert.Must(assert.NoError(err))
				db := sh.DB()
				var seq int64
				assert.NoError(db.QueryRowContext(t.Context(), "SELECT ordering_seq FROM dwarf_peers WHERE engine_id=?", pinned).Scan(&seq))
				seqs[shard] = seq
			}
			assert.Equal(seqs[1], seqs[2], "one ordering sequence on every shard")
			if c.want != 0 {
				assert.Equal(c.want, seqs[1], "the oldest surviving value")
			}
		})
	}
}

// TestDispatchers_Slots pins how many replicas dispatch a shard: maxDispatchers, never more than the
// replicas there are, and never zero.
func TestDispatchers_Slots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	cases := []struct {
		replicas, want int
	}{
		{0, 1},   // an empty reading still names a dispatcher: this replica, about to register
		{1, 1},   // a lone replica is its own dispatcher
		{2, 2},   // two once there are two, so one death does not stop the shard
		{100, 2}, // and no more: every dispatcher's piston walks the whole band
	}
	for _, c := range cases {
		assert.Equal(c.want, dispatcherSlots(c.replicas), "%+v", c)
	}
}

// TestDispatchers_ReaderReplicaHoldsTheReaderPoolAndRunsNothing pins the whole role on a replica ranked
// below its shard's dispatcher cut: it holds the reader pool, its piston idles, and it does not ring its
// own doorbell - so a flow it creates is left to the shard's dispatchers rather than run through the reader
// pool. When the fleet leaves and it is promoted, the flow runs.
func TestDispatchers_ReaderReplicaHoldsTheReaderPoolAndRunsNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Reader")
	g.SetURL("A", "reader/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("reader/g", g)
	mux.HandleTask("reader/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	reader := sdkmetric.NewManualReader()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.testConnCap = 0 // assert the real derived pool sizes, not the test-mode connection cap
	assert.NoError(e.SetHost(mux))
	assert.NoError(e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	assert.NoError(e.SetEngineID(readerID))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, MaxConnections: 24}))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	assert.True(e.dispatchesOn(1), "alone, the replica dispatches")

	joinFakeFleet(t, e)
	if rank, _ := e.sonarFor(1).Rank(); !assert.Equal(3, rank, "fixture: readerID must rank below a 2-slot cut") {
		return
	}
	assert.Equal(0, e.slotsOn(1))
	assert.False(e.dispatchesOn(1))
	assert.True(e.pistons[1].Idle(), "a reader's piston idles")
	assert.Equal(readerOpenConns, db.DB.Stats().MaxOpenConnections, "a reader holds the reader pool")

	// Create rings the doorbell before it returns, and with the piston idle the doorbell is the only way this
	// replica could run the step. So the offer count, not a wait, is what shows nothing will run here.
	fk, err := e.Create(ctx, "reader/g", nil, nil)
	assert.NoError(err)
	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	offered, _ := sumCounter(rm, "dwarf_steps_offered", "", "")
	assert.Equal(int64(0), offered, "a reader does not offer the step to its own cache")
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

// TestDispatchers_PromotedReaderTakesADispatcherSizedCache pins that a reader promoted to dispatcher gets the
// cache its new pool calls for. A reader's worker maximum is the fallback 64, and the cache is capped by the
// worker maximum, so a recompute that resizes the cache before re-deriving the maximum leaves the promoted
// replica on a reader-sized cache - and no later recompute corrects it, since the role no longer changes.
func TestDispatchers_PromotedReaderTakesADispatcherSizedCache(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.testConnCap = 0
	assert.NoError(e.SetHost(hosts.NewMux()))
	assert.NoError(e.SetEngineID(readerID))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, MaxConnections: 24}))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	solo := e.cache.Capacity()

	joinFakeFleet(t, e)
	if !assert.False(e.dispatchesOn(1), "fixture: readerID must rank below the cut") {
		return
	}
	assert.True(e.cache.Capacity() < solo, "a reader's cache is smaller than a dispatcher's")

	leaveFakeFleet(t, e)
	assert.True(e.dispatchesOn(1))
	assert.Equal(solo, e.cache.Capacity(), "promoted alone, the cache is sized as it was solo")
	e.recomputePools()
	assert.Equal(solo, e.cache.Capacity())
}

// TestDispatchers_DemotedReplicaHoldsItsPoolUntilInFlightStepsFinish pins the demotion path for a replica
// with work under way: it stops dispatching at once, but keeps a dispatcher's share of the pool and
// turnstile - the budget divided among the dispatchers and itself - until the steps it already claimed have
// finished, so their completions do not queue on the reader pool. It states it is draining in its registry
// row throughout, for peers to divide by. Once the steps finish it shrinks to the reader pool and clears
// the statement on its own, and no step runs twice.
func TestDispatchers_DemotedReplicaHoldsItsPoolUntilInFlightStepsFinish(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const flows = 5
	release := make(chan struct{})
	var runs atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("Demote")
	g.SetURL("Slow", "demote/slow")
	g.AddTransition("Slow", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("demote/g", g)
	mux.HandleTask("demote/slow", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.testConnCap = 0 // assert the real derived pool sizes, not the test-mode connection cap
	assert.NoError(e.SetHost(mux))
	assert.NoError(e.SetEngineID(readerID))
	spec := ShardSpec{Index: 1, MaxConnections: 24} // two dispatcher slots, and a budget whose 3-way share beats the reader pool
	assert.NoError(e.SetShard(spec))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	assert.True(e.dispatchesOn(1), "alone, the replica dispatches")
	// Held, it drains alongside a 2-slot cut, so the budget divides three ways.
	_, heldOpen := shardPool(spec.MaxConnections, 3)
	if !assert.True(heldOpen > readerOpenConns, "fixture: the held pool must exceed the reader pool") {
		return
	}

	var keys []string
	for range flows {
		fk, err := e.Create(ctx, "demote/g", nil, nil)
		assert.NoError(err)
		keys = append(keys, fk)
	}
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for runs.Load() < flows && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !assert.Equal(int32(flows), runs.Load(), "every task is parked in flight") {
		close(release)
		return
	}

	joinFakeFleet(t, e)
	if rank, _ := e.sonarFor(1).Rank(); !assert.Equal(3, rank, "fixture: readerID must rank below a 2-slot cut") {
		close(release)
		return
	}
	assert.False(e.dispatchesOn(1), "demoted: it claims nothing new")
	assert.True(e.pistons[1].Idle())
	assert.Equal(int64(flows), e.inFlightOn(1))
	assert.Equal(heldOpen, db.DB.Stats().MaxOpenConnections, "the pool is held while steps are in flight")
	assert.Equal(turnstilePassesPerConn*heldOpen, turnLimit(t, e, 1), "and so is the turnstile")
	assert.Equal(1, awaitDrainingRow(t, db, readerID, 1), "the drain is stated for peers to count")

	// Another replica starts draining: the held share narrows with every other pool on the shard.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_peers SET draining=1 WHERE engine_id=1006")
	assert.NoError(err)
	_, narrower := shardPool(spec.MaxConnections, 4)
	assert.Equal(narrower, awaitPoolSize(t, db, narrower), "a held pool follows the draining count")

	close(release)
	for _, fk := range keys {
		outcome, err := e.Await(ctx, fk)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
	}
	// The reconcile loop releases the pool on its own once the count reaches zero.
	assert.Equal(readerOpenConns, awaitPoolSize(t, db, readerOpenConns), "the pool shrinks once the steps finish")
	assert.Equal(int64(0), e.inFlightOn(1))
	assert.Equal(0, awaitDrainingRow(t, db, readerID, 0), "the statement is cleared once the pool is released")
	// Read after the statement clears: the push resizes the turnstile after the pool, before the statement.
	assert.Equal(turnstilePassesPerConn*readerOpenConns, turnLimit(t, e, 1))
	assert.Equal(int32(flows), runs.Load(), "no step ran twice")
}

// TestDispatchers_PromotionWhileHeldClearsTheHold pins the other exit from a held pool: a replica demoted
// with steps in flight and promoted again before they finish takes the dispatcher's pool back directly and
// stops stating it is draining.
func TestDispatchers_PromotionWhileHeldClearsTheHold(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	release := make(chan struct{})
	started := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("Repromote")
	g.SetURL("Slow", "repromote/slow")
	g.AddTransition("Slow", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("repromote/g", g)
	mux.HandleTask("repromote/slow", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.testConnCap = 0
	assert.NoError(e.SetHost(mux))
	assert.NoError(e.SetEngineID(readerID))
	spec := ShardSpec{Index: 1, MaxConnections: 24} // two dispatcher slots, and a budget whose 3-way share beats the reader pool
	assert.NoError(e.SetShard(spec))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, soloOpen := shardPool(spec.MaxConnections, 1)
	_, heldOpen := shardPool(spec.MaxConnections, 3)

	fk, err := e.Create(ctx, "repromote/g", nil, nil)
	assert.NoError(err)
	<-started
	joinFakeFleet(t, e)
	assert.False(e.dispatchesOn(1))
	assert.Equal(heldOpen, db.DB.Stats().MaxOpenConnections, "held while the step is in flight")
	assert.Equal(1, awaitDrainingRow(t, db, readerID, 1))

	// Promoted with the step still in flight: the dispatcher's pool, not the held share, and no drain stated.
	leaveFakeFleet(t, e)
	assert.True(e.dispatchesOn(1), "promoted again")
	assert.Equal(int64(1), e.inFlightOn(1), "fixture: the step is still in flight")
	assert.Equal(soloOpen, db.DB.Stats().MaxOpenConnections, "the promotion takes the dispatcher pool, not the held share")
	e.poolsLock.Lock()
	_, pending := e.heldDivisors[1]
	e.poolsLock.Unlock()
	assert.False(pending, "the promotion clears the hold")
	assert.Equal(0, awaitDrainingRow(t, db, readerID, 0), "and lowers the statement")

	close(release)
	outcome, err := e.Await(ctx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
}

// awaitDrainingRow polls one replica's registry row until its draining column reads want, or 10s pass, and
// returns the last value read. The statement reaches the row on the Sonar's next pass, not synchronously.
func awaitDrainingRow(t *testing.T, db *sequel.DB, id int64, want int) int {
	t.Helper()
	var got int
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for {
		if err := db.QueryRowContext(t.Context(), "SELECT draining FROM dwarf_peers WHERE engine_id=?", id).Scan(&got); err != nil {
			t.Errorf("read draining for %d: %v", id, err)
			return got
		}
		if got == want || time.Now().After(deadline) {
			return got
		}
		time.Sleep(5 * time.Millisecond)
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
	assert.NoError(e.SetShard(ShardSpec{Index: 1, MaxConnections: 48}))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	joinFakeFleet(t, e)
	slots := dispatcherSlots(len(fakeFleet) + 1)
	if rank, _ := e.sonarFor(1).Rank(); !assert.Equal(1, rank, "fixture: dispatcherID must rank inside a %d-slot cut", slots) {
		return
	}
	assert.True(e.dispatchesOn(1))
	_, open := shardPool(48, slots)
	assert.Equal(open, db.DB.Stats().MaxOpenConnections, "the budget divides by %d dispatchers, not 7 replicas", slots)
}

// TestDispatchers_DrainingReplicasDivideTheBudget pins the peer side of a held pool: replicas outside the
// dispatcher cut that state they are draining a share are added to the divisor, so a demoted or withdrawn
// replica finishing its steps shrinks every dispatcher's pool rather than putting the shard over its budget.
// When the statements clear, the pools grow back to the dispatcher split.
func TestDispatchers_DrainingReplicasDivideTheBudget(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.testConnCap = 0
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetEngineID(dispatcherID))
	spec := ShardSpec{Index: 1, MaxConnections: 48}
	assert.NoError(e.SetShard(spec))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	joinFakeFleet(t, e)
	slots := dispatcherSlots(len(fakeFleet) + 1)
	if !assert.True(e.dispatchesOn(1), "fixture: dispatcherID must rank inside the cut") {
		return
	}
	_, split := shardPool(spec.MaxConnections, slots)
	assert.Equal(split, db.DB.Stats().MaxOpenConnections)

	setDraining := func(flag int) {
		_, err := db.ExecContext(t.Context(), "UPDATE dwarf_peers SET draining=? WHERE engine_id IN (1005, 1006)", flag)
		assert.NoError(err)
	}
	setDraining(1)
	_, wide := shardPool(spec.MaxConnections, slots+2)
	assert.Equal(wide, awaitPoolSize(t, db, wide), "two draining replicas divide the budget with the dispatchers")

	setDraining(0)
	assert.Equal(split, awaitPoolSize(t, db, split), "cleared statements return the pool to the dispatcher split")
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
	assert.Must(assert.NoError(e.Startup(t.Context())))

	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var working int
	var rttUs int64
	assert.NoError(db.QueryRowContext(t.Context(),
		"SELECT working, rtt_us FROM dwarf_peers WHERE engine_id=?", e.engineID).Scan(&working, &rttUs))
	assert.Equal(0, working, "registered, as not working")
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
	assert.NoError(e.SetShard(ShardSpec{Index: 1, MaxConnections: 48}))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	for _, id := range []int64{3001, 3002, 3003} {
		_, err := db.ExecContext(t.Context(),
			"INSERT INTO dwarf_peers (engine_id, seen_at, working, ordering_seq) VALUES (?, NOW_UTC(), 0, ?)", id, time.Now().UnixMilli())
		assert.NoError(err)
	}
	awaitPeerCount(t, e, 1, 4)
	e.recomputePools()
	_, candidates := e.sonarFor(1).Rank()
	assert.Equal(1, candidates, "three await-only peers, one candidate")
	assert.True(e.dispatchesOn(1))
	assert.Equal(declaredBudget, db.DB.Stats().MaxOpenConnections, "the lone worker keeps the whole budget")
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
	assert.Must(assert.NoError(e.Startup(t.Context())))

	e.dispatchFlag(1).Store(false)
	e.enqueueStepDue(t.Context(), 1, 999, 100)
	assert.Equal(0, e.cache.Len(), "a shard this replica only reads is never offered")

	e.dispatchFlag(1).Store(true)
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
	assert.NoError(e.SetShard(ShardSpec{Index: 1, MaxConnections: 24}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2, MaxConnections: 24}))
	assert.Must(assert.NoError(e.Startup(t.Context())))

	e.dispatchFlag(2).Store(false)
	for range 50 {
		shard, err := e.pickShard()
		assert.NoError(err)
		assert.Equal(1, shard, "only the dispatched shard takes new flows")
	}

	e.dispatchFlag(1).Store(false)
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
	assert.Must(assert.NoError(e.Startup(t.Context())))
	spec := ShardSpec{Index: 1, MaxConnections: 48}

	e.poolsLock.Lock()
	defer e.poolsLock.Unlock()
	e.checkFleetFits(1, shardSizing{budget: spec.MaxConnections, serverMax: 0})
	assert.False(e.fleetOverLimit[1], "no known limit, no verdict")
	e.checkFleetFits(1, shardSizing{budget: spec.MaxConnections, serverMax: 10})
	assert.True(e.fleetOverLimit[1], "a budget of 48 does not fit in 80% of 10")
	e.checkFleetFits(1, shardSizing{budget: spec.MaxConnections, serverMax: 1000})
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
	mux := hosts.NewMux()
	g := workflow.NewGraph("Sweep")
	g.SetURL("A", "sweep/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("sweep/g", g)
	mux.HandleTask("sweep/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	shortenDeletion(e, time.Millisecond, time.Hour) // due immediately; the test drives reaps
	assert.Must(assert.NoError(e.Startup(t.Context())))

	waitDone := awaitNextStop(t, e)
	fk, err := e.Create(ctx, "sweep/g", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	waitDone()
	shard, _, _, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	time.Sleep(5 * time.Millisecond) // inherent wall-clock: let the 1ms deletion window elapse

	setRole(e, shard, false)
	e.reapDueFlows(ctx)
	assert.Equal(1, shardFlowCount(t, e, shard), "a reader leaves the shard to its dispatchers")

	setRole(e, shard, true)
	e.reapDueFlows(ctx)
	assert.Equal(0, shardFlowCount(t, e, shard), "a dispatcher reaps it")
}

// TestDispatchers_DrainGivesUpTheSlotBeforeWaiting pins that a shutting-down replica hands its dispatcher
// slots on at the START of its drain. It takes on no new work from that moment, but its row stays fresh
// until it leaves at the end, and a fresh row keeps its rank - so a shard whose dispatchers all drained at
// once would otherwise run nothing until the slowest in-flight task finished. The same write states it is
// draining, so the replica taking the slot divides the budget with it rather than growing into its share.
func TestDispatchers_DrainGivesUpTheSlotBeforeWaiting(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("Drain")
	g.SetURL("A", "drain/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("drain/g", g)
	mux.HandleTask("drain/a", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, err = e.Create(ctx, "drain/g", nil, nil)
	assert.NoError(err)
	<-started

	var wg sync.WaitGroup
	wg.Go(func() { e.Shutdown(ctx) })
	defer func() {
		close(release)
		wg.Wait()
	}()
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for time.Now().Before(deadline) {
		var working, draining int
		err := db.QueryRowContext(ctx, "SELECT working, draining FROM dwarf_peers WHERE engine_id=?", e.engineID).Scan(&working, &draining)
		if err == nil && working == 0 {
			assert.Equal(1, draining, "withdrawn from a shard it dispatched, it states the share it still holds")
			return // withdrawn while the task is still holding the drain open
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the draining replica never withdrew from the ranking while its task was in flight")
}

// TestDispatchers_DrainStopsClaimingDispatch pins that a draining replica stops proving it dispatches, so its
// peers stop counting it in the work divisor and take over its residue class, rather than leaving that class's
// steps to wait out the steal grace for as long as an in-flight task holds the drain open. Its pistons idle at
// the start of the drain - nothing they push can reach a worker any more - and the beat then stops stamping
// dispatched_at, which ages past a couple of beats while the task is still running.
func TestDispatchers_DrainStopsClaimingDispatch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("Drain")
	g.SetURL("A", "drainidle/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("drainidle/g", g)
	mux.HandleTask("drainidle/a", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, err = e.Create(ctx, "drainidle/g", nil, nil)
	assert.NoError(err)
	<-started
	dispatchAge := func() float64 {
		var ms float64
		if err := db.QueryRowContext(ctx, "SELECT DATE_DIFF_MILLIS(NOW_UTC(), dispatched_at) FROM dwarf_peers WHERE engine_id=?", e.engineID).Scan(&ms); err != nil {
			return -1
		}
		return ms
	}
	// The beat stamps dispatched_at every second while the replica dispatches; wait for the first stamp.
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for age := dispatchAge(); age < 0 || age > 1500; age = dispatchAge() {
		if time.Now().After(deadline) {
			t.Fatal("the replica never stamped dispatched_at")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var wg sync.WaitGroup
	wg.Go(func() { e.Shutdown(ctx) })
	defer func() {
		close(release)
		wg.Wait()
	}()
	deadline = time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for time.Now().Before(deadline) {
		if dispatchAge() > 2500 {
			for _, p := range e.pistons {
				assert.True(p.Idle())
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the draining replica kept stamping dispatched_at while its task was in flight")
}

// TestDispatchers_DrainingReplicasClassPassesToAPeer pins the other half of the drain's handoff: while an
// in-flight task holds a dispatcher's drain open, its peer takes over its residue class through the partition
// alone, not by stealing. The peer's steal is switched off, so work in the drainer's class completes only if the
// drainer has dropped out of the work divisor; a drainer that kept proving it dispatches would leave that class
// unserved until its task finished. The work is created on an await-only replica, so no doorbell carries it to
// a dispatcher and the partition is genuinely in the path.
func TestDispatchers_DrainingReplicasClassPassesToAPeer(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	mk := func(workers int) *Engine {
		mux := hosts.NewMux()
		g := workflow.NewGraph("DrainHandoff")
		g.SetURL("W", "drainhandoff/w")
		g.AddTransition("W", workflow.End)
		mux.HandleGraph("drainhandoff/g", g)
		mux.HandleTask("drainhandoff/w", func(ctx context.Context, f *workflow.Flow) error {
			if f.GetString("hold") == "yes" {
				close(started)
				<-release
			}
			return nil
		})
		e := NewEngineUnderTest(t.Name())
		assert.NoError(e.SetHost(mux))
		if workers >= 0 {
			assert.NoError(e.SetWorkers(workers))
		}
		return e
	}
	drainer, peer, creator := mk(-1), mk(-1), mk(0)
	defer creator.Shutdown(ctx)
	defer peer.Shutdown(ctx)
	assert.Must(assert.NoError(drainer.Startup(t.Context())))
	assert.Must(assert.NoError(peer.Startup(t.Context())))
	assert.Must(assert.NoError(creator.Startup(t.Context())))
	for _, p := range peer.pistons {
		p.SetStealAfter(0) // the class can reach the peer only through the partition
	}

	// Both dispatchers partition the shard between them before the drain starts, or the peer would be serving
	// every class already and the test would prove nothing.
	awaitPartition(t, drainer, 1, 2, -1)
	awaitPartition(t, peer, 1, 2, -1)
	_, drainerOrd, ok := drainer.partitionOn(1)
	if !ok {
		t.Fatal("the drainer stopped partitioning the shard")
	}

	heldKey, err := drainer.Create(ctx, "drainhandoff/g", map[string]any{"hold": "yes"}, nil)
	assert.NoError(err)
	_, _, heldFlowID, _, err := keys.ParseFlowKey(heldKey)
	assert.NoError(err)
	<-started
	sh, err := creator.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	// The held task must be the drainer's, or nothing holds its drain open and the handoff is never exercised.
	var claimer int64
	assert.NoError(db.QueryRowContext(ctx, "SELECT engine_id FROM dwarf_steps WHERE flow_id=?", heldFlowID).Scan(&claimer))
	if claimer != drainer.engineID {
		t.Fatal("the held task ran on the peer, not the drainer")
	}
	var wg sync.WaitGroup
	wg.Go(func() { drainer.Shutdown(ctx) })
	defer func() {
		rel()
		wg.Wait()
	}()

	var flowKeys []string
	for range 16 {
		fk, err := creator.Create(ctx, "drainhandoff/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
		flowKeys = append(flowKeys, fk)
	}
	// One deadline for all of them: a stranded class fails at the bound, not after one bound per flow.
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	for _, fk := range flowKeys {
		out, err := creator.Await(awaitCtx, fk)
		if !assert.NoError(err, "a flow waited past the bound while the drain was held open") || !assert.NotNil(out) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	// The handoff was in the path only if some of that work fell in the drainer's class.
	var inDrainerClass int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_steps WHERE task_name='W' AND step_id % 2 = ? AND flow_id <> ?",
		drainerOrd, heldFlowID).Scan(&inDrainerClass))
	assert.True(inDrainerClass > 0, "none of the work fell in the drainer's residue class")
}

// TestDispatchers_RoleFlagsSurviveARestartUnderLoad pins that the role flags are published, not assigned in
// place: a host still serving through a Shutdown/Startup restart reads them from Create while Startup
// builds the next run's set, and an in-place map assignment under that read is a fatal throw. Meaningful
// under -race.
func TestDispatchers_RoleFlagsSurviveARestartUnderLoad(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	assert.NoError(e.SetShard(ShardSpec{Index: 1, MaxConnections: 24}))
	assert.Must(assert.NoError(e.Startup(t.Context())))

	var stop atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		for !stop.Load() {
			e.dispatchesOn(1)
			e.pickShard()
		}
	})
	for range 3 {
		assert.NoError(e.Shutdown(t.Context()))
		assert.Must(assert.NoError(e.Startup(t.Context())))
	}
	stop.Store(true)
	wg.Wait()
	assert.True(e.dispatchesOn(1))
}

// twoReplicaFleet stands up a dispatcher (dispatcherID) and a reader (readerID) on one two-dispatcher shard, beside
// one fake working peer (1002) that dispatches nothing. Among the three candidates 1002 ranks first,
// dispatcherID second and readerID third, so with two slots the dispatcher dispatches and the reader reads -
// and once the dispatcher stops working, the reader moves inside the cut. Each engine runs on its own host,
// so a test can tell which replica ran a task.
func twoReplicaFleet(t *testing.T, dispHost, readHost Host) (disp, reader *Engine) {
	t.Helper()
	assert := testarossa.For(t)
	mk := func(id int64, h Host) *Engine {
		e := NewEngineUnderTest(t.Name())
		e.testConnCap = 0
		assert.NoError(e.SetHost(h))
		assert.NoError(e.SetEngineID(id))
		assert.NoError(e.SetShard(ShardSpec{Index: 1, MaxConnections: 24}))
		return e
	}
	disp = mk(dispatcherID, dispHost)
	assert.Must(assert.NoError(disp.Startup(t.Context())))
	sh, err := disp.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, err = db.ExecContext(t.Context(), "INSERT INTO dwarf_peers (engine_id, seen_at, ordering_seq) VALUES (1002, NOW_UTC(), ?)", time.Now().UnixMilli()-3600000)
	assert.NoError(err)
	reader = mk(readerID, readHost)
	assert.Must(assert.NoError(reader.Startup(t.Context())))
	for _, e := range []*Engine{disp, reader} {
		awaitPeerCount(t, e, 1, 3)
		e.recomputePools()
	}
	if !assert.True(disp.dispatchesOn(1), "fixture: the dispatcher must rank inside the cut") ||
		!assert.False(reader.dispatchesOn(1), "fixture: the reader must rank outside it") {
		t.FailNow()
	}
	return disp, reader
}

// TestDispatchers_ResumeOnAReaderRunsOnADispatcher pins the cross-replica path the doorbell gate creates. A
// Resume issued on a replica that does not dispatch the flow's shard rings no bell, so the step is left to a
// dispatcher's scan - and it must run there, not through the reader's two-connection pool.
func TestDispatchers_ResumeOnAReaderRunsOnADispatcher(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	var dispRan, readRan atomic.Int32
	build := func(ran *atomic.Int32) *hosts.Mux {
		p := hosts.NewMux()
		g := workflow.NewGraph("ResumeOnReader")
		g.SetURL("Gate", "ronr/gate")
		g.SetURL("B", "ronr/b")
		g.AddTransition("Gate", "B")
		g.AddTransition("B", workflow.End)
		p.HandleGraph("ronr/g", g)
		p.HandleTask("ronr/gate", func(ctx context.Context, f *workflow.Flow) error {
			_, err := f.Interrupt(nil, nil)
			return err
		})
		p.HandleTask("ronr/b", func(ctx context.Context, f *workflow.Flow) error { ran.Add(1); return nil })
		return p
	}
	disp, reader := twoReplicaFleet(t, build(&dispRan), build(&readRan))
	defer disp.Shutdown(ctx)
	defer reader.Shutdown(ctx)

	fk, err := disp.Create(ctx, "ronr/g", nil, nil)
	assert.NoError(err)
	outcome, err := disp.Await(ctx, fk)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status) {
		return
	}

	assert.NoError(reader.Resume(ctx, fk, nil))
	assert.Equal(0, reader.cache.Len(), "the reader rang no bell")
	outcome, err = reader.Await(ctx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, "a dispatcher's scan picked the resumed step up")
	}
	assert.Equal(int32(1), dispRan.Load(), "and ran the rest of the flow")
	assert.Equal(int32(0), readRan.Load(), "the reader ran nothing")
}

// TestDispatchers_DrainingDispatcherHandsItsSlotToAPeer pins what Withdraw is for, end to end: while a
// dispatcher drains a long in-flight task at shutdown, a peer takes over its slot - not after the drain.
func TestDispatchers_DrainingDispatcherHandsItsSlotToAPeer(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	started, release := make(chan struct{}), make(chan struct{})
	build := func() *hosts.Mux {
		p := hosts.NewMux()
		g := workflow.NewGraph("Drain")
		g.SetURL("A", "dslot/a")
		g.AddTransition("A", workflow.End)
		p.HandleGraph("dslot/g", g)
		p.HandleTask("dslot/a", func(ctx context.Context, f *workflow.Flow) error {
			close(started)
			<-release
			return nil
		})
		return p
	}
	disp, reader := twoReplicaFleet(t, build(), build())
	defer reader.Shutdown(ctx)

	_, err := disp.Create(ctx, "dslot/g", nil, nil)
	assert.NoError(err)
	<-started

	var wg sync.WaitGroup
	wg.Go(func() { disp.Shutdown(ctx) })
	defer func() {
		close(release)
		wg.Wait()
	}()
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for time.Now().Before(deadline) {
		if reader.dispatchesOn(1) {
			return // promoted while the dispatcher's task is still holding its drain open
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the peer was never promoted while the dispatcher drained")
}

// TestDispatchers_RecoverySweepSkipsShardsThisReplicaOnlyReads pins the recovery half of the sweep rule,
// through the lease reset: an expired lease on a shard this replica only reads is left to the shard's
// dispatchers, and reset once this replica dispatches it.
func TestDispatchers_RecoverySweepSkipsShardsThisReplicaOnlyReads(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	started, release := make(chan struct{}), make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("Lease")
	g.SetURL("A", "rsweep/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("rsweep/g", g)
	mux.HandleTask("rsweep/a", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer close(release) // below the Shutdown defer, so it unwinds first and frees the drain
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	_, err := e.Create(ctx, "rsweep/g", nil, nil)
	assert.NoError(err)
	<-started
	// The startup pass runs the same lease reset as a dispatcher: one still in flight resets the lease
	// forged below before the reader half can observe it.
	awaitStartupRecoverySweep(t, e)

	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE status='"+workflow.StatusRunning+"'")
	assert.NoError(err)
	expired := func() int {
		var n int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE status='"+workflow.StatusRunning+"' AND lease_expires<=NOW_UTC()").Scan(&n))
		return n
	}

	e.dispatchFlag(1).Store(false)
	e.recoverExpiredLeases(ctx)
	assert.Equal(1, expired(), "a reader leaves the expired lease to the shard's dispatchers")

	e.dispatchFlag(1).Store(true)
	e.recoverExpiredLeases(ctx)
	assert.Equal(0, expired(), "a dispatcher resets it")
}

// TestDispatchers_WeightsOverrideTheOwnShardPreference pins placement for a replica that dispatches a subset of
// the shards. With every weight at 1 it places only on its own shards, so the first step rings its doorbell.
// Any other weight drops that preference: placing only on its own shards would hand each shard its own
// dispatchers' creates whatever the weights say, so it places by weight over every non-cordoned shard.
func TestDispatchers_WeightsOverrideTheOwnShardPreference(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, c := range []struct {
		name     string
		w1, w2   float64
		wantOnly bool
	}{
		{"equal", 0, 0, true},
		{"weighted", 1, 3, false},
	} {
		e := NewEngineUnderTest(t.Name() + "#" + c.name)
		assert.NoError(e.SetHost(noopHost{}))
		assert.NoError(e.SetEngineID(dispatcherID))
		assert.NoError(e.SetShard(ShardSpec{Index: 1, Weight: c.w1}))
		assert.NoError(e.SetShard(ShardSpec{Index: 2, Weight: c.w2}))
		assert.Must(assert.NoError(e.Startup(t.Context())))
		joinFakeFleet(t, e)
		if !assert.True(e.dispatchesOn(1) && !e.dispatchesOn(2), "fixture: dispatcherID dispatches shard 1 alone") {
			e.Shutdown(t.Context())
			continue
		}
		counts := map[int]int{}
		for range 4000 {
			idx, err := e.pickShard()
			assert.NoError(err)
			counts[idx]++
		}
		if c.wantOnly {
			assert.Equal(4000, counts[1], "%s: equal weights keep placement on the replica's own shard", c.name)
		} else {
			assert.True(counts[2] > 2*counts[1], "%s: a weight of 3 against 1 draws ~3x (got %d vs %d)", c.name, counts[2], counts[1])
		}
		e.Shutdown(t.Context())
	}
}

// turnLimit is how many turns shard admits at once.
func turnLimit(t *testing.T, e *Engine, shard int) int {
	t.Helper()
	sh, err := e.db.Shard(shard)
	if err != nil {
		t.Fatal(err)
	}
	return sh.TurnLimit()
}
