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

package fixtures

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// These pin the failure the residue partition has no other answer for: a replica that is SLOW rather than
// DEAD. A dead one stops advancing its dispatch evidence and is evicted from the divisor within the dispatch
// window, at which point its class is redistributed. A slow one keeps beating, keeps its class, and cannot
// serve it - and nothing else in the fleet will look at those steps.
//
// TWO INDEPENDENT CRIPPLINGS, because the coupling is a property of the PARTITION rather than of how a
// replica goes slow, and one mechanism alone cannot show that:
//
//   - CAPACITY (SetWorkers(1)): the replica can hold one step in flight at a time. Pins the maximum and
//     disables grow-on-demand, so it cannot heal itself.
//   - LATENCY (SimulateRTT): every operation it makes - scan, claim, write - pays a round trip, so its
//     piston also cycles slower. This is the cross-region replica, and it is the more realistic shape.
//
// Measured on a three-replica bench fleet, both produced the same fleet-wide cap: throughput fell to
// roughly a third of what the SAME fleet did with the crippled replica removed from the divisor entirely,
// with its class aging past 30s while its healthy peers sat at a third of a core.
//
// The flows are created by the await-only replica in every case, and that is load-bearing: a dispatcher
// that creates a step offers it straight into its own cache through the doorbell, which is deliberately not
// partitioned, so such a chain never consults a residue class at all. Only work created by a replica that
// cannot dispatch is discovered by SCANNING, which is the path the partition governs.

// stealFleet stands up creator + healthy + crippled over one shared database, with the crippled replica's
// worker count under the caller's control. Returns them in that order, plus per-replica execution counts.
// crippledDelay makes the crippled replica's own tasks slow, which is what gives "one worker" teeth. The
// steal fires only once a foreign step has been DUE longer than the grace (four cycle periods, ~533ms at
// the derived test interval), and a single worker chewing through 40 no-op steps against in-memory SQLite
// finishes well inside that - so with instant tasks the crippled class never ages and the test is a
// coin-flip on scheduling. A delay makes the backlog build for a reason the test controls.
func stealFleet(t *testing.T, name string, crippledWorkers int, crippledDelay time.Duration) (creator, healthy, crippled *engine.Engine, ran map[string]*atomic.Int64) {
	t.Helper()
	assert := testarossa.For(t)
	ran = map[string]*atomic.Int64{"creator": {}, "healthy": {}, "crippled": {}}

	build := func(replica string) *engine.TestProxy {
		p := engine.NewTestProxy()
		g := workflow.NewGraph("Chain")
		g.SetEndpoint("A", name+"/a")
		g.SetEndpoint("B", name+"/b")
		g.AddTransition("A", "B")
		g.AddTransition("B", workflow.END)
		assert.NoError(g.Validate())
		p.HandleGraph(name+"/g", g)
		for _, url := range []string{name + "/a", name + "/b"} {
			p.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error {
				if replica == "crippled" && crippledDelay > 0 {
					select {
					case <-time.After(crippledDelay):
					case <-ctx.Done():
					}
				}
				ran[replica].Add(1)
				return nil
			})
		}
		return p
	}

	healthy = engine.NewEngineUnderTest(t)
	healthy.SetHost(build("healthy"))
	assert.NoError(healthy.SetWorkers(4))
	crippled = engine.NewEngineUnderTest(t)
	crippled.SetHost(build("crippled"))
	assert.NoError(crippled.SetWorkers(crippledWorkers))
	// Await-only: it holds connections and divides the pools, but claims no work and so earns no residue
	// class. Its creations are the fleet's only scan-discovered work.
	creator = engine.NewEngineUnderTest(t)
	creator.SetHost(build("creator"))
	assert.NoError(creator.SetWorkers(0))

	ctx := context.Background()
	for _, e := range []*engine.Engine{healthy, crippled, creator} {
		assert.NoError(e.Startup(ctx))
		t.Cleanup(func() { e.Shutdown(ctx) })
	}
	return creator, healthy, crippled, ran
}

// awaitSteal reports whether a replica took work from outside its own residue class, arming the waiter
// BEFORE the trigger and checking Visits after - so a steal that lands between the two lines is caught by
// the channel and one before them by the count.
//
// It is a rendezvous rather than a poll because no duration is the right one to wait: the steal fires on
// the first cycle after the gate arms and the grace elapses, which is a function of the pipeline's cadence,
// the peer's degradation and the backlog, none of which the test controls.
func awaitSteal(t *testing.T, e *engine.Engine, work func()) bool {
	t.Helper()
	ch := e.Seams().Waiter(engine.CheckpointRefillStole)
	before := e.Seams().Visits(engine.CheckpointRefillStole)
	work()
	if e.Seams().Visits(engine.CheckpointRefillStole) != before {
		return true
	}
	select {
	case <-ch:
		return true
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		return false
	}
}

// TestStealCapacityPoisonflow pins the capacity mode: the crippled replica can hold exactly one step in
// flight, so its residue class backs up while it stays perfectly alive in the registry.
func TestStealCapacityPoisonflow(t *testing.T) {
	t.Parallel()
	const name = "stealcap.verify:428"
	assert := testarossa.For(t)
	creator, healthy, _, ran := stealFleet(t, name, 1, 150*time.Millisecond)

	// Warm both dispatchers so the crippled one genuinely holds a class before the load arrives.
	drainFlows(t, 30*time.Second, creator, name, 4)

	before := ran["healthy"].Load() + ran["crippled"].Load()
	stole := awaitSteal(t, healthy, func() {
		drainFlows(t, 60*time.Second, creator, name, 20)
	})

	assert.True(stole, "the healthy replica must take the crippled one's class rather than leave it to back up")
	assert.Equal(before+40, ran["healthy"].Load()+ran["crippled"].Load(),
		"every step of every flow ran exactly once")
	assert.Equal(int64(0), ran["creator"].Load(), "and the await-only replica ran none of them")
}

// TestStealLatencyPoisonflow pins the latency mode. The crippled replica keeps ALL of its workers - nothing
// about its capacity changes - but every round trip it makes costs 25ms, so each step it dispatches takes
// roughly an order of magnitude longer and its piston cycles slower with it.
//
// Each engine opens its own pool (sequel.Open, not OpenSingleton), so the injection reaches this replica
// alone. 25ms is far below every registry window - the beat is 1s and the dispatch window 5s - so the
// replica is NOT evicted: it keeps its class throughout, which is the state under test.
func TestStealLatencyPoisonflow(t *testing.T) {
	t.Parallel()
	const name = "stealrtt.verify:428"
	assert := testarossa.For(t)
	creator, healthy, crippled, ran := stealFleet(t, name, 4, 0)

	drainFlows(t, 30*time.Second, creator, name, 4)

	// Slow every operation this replica makes, from here on.
	for _, idx := range crippled.DB().Indices() {
		db, err := crippled.DB().Shard(idx)
		if !assert.NoError(err) {
			return
		}
		db.SimulateRTT(25 * time.Millisecond)
	}

	before := ran["healthy"].Load() + ran["crippled"].Load()
	stole := awaitSteal(t, healthy, func() {
		drainFlows(t, 60*time.Second, creator, name, 20)
	})

	assert.True(stole, "a latency-crippled peer must have its class taken, exactly as a capacity-crippled one does")
	assert.Equal(before+40, ran["healthy"].Load()+ran["crippled"].Load(),
		"every step of every flow ran exactly once")
}

// TestStealHoldsOffAHealthyFleetflow is the other half, and the one that decides whether stealing is a fuse
// or a regression: a healthy fleet must keep dispatching from its OWN classes, or the residue partition
// stops excluding anything and every replica is back to racing its peers for the same rows.
//
// It is measured over the window where the gate's condition is unambiguously met - a backlog deep enough
// that BOTH classes can fill their batch - and over CYCLES rather than a duration, so a loaded suite makes
// the backlog deeper rather than the measurement noisier. That framing is the whole point of the shape
// below, and the reason for each half:
//
//   - DEEP, because at MODERATE load the gate legitimately opens with no replica at fault: classes empty
//     between arrivals while everything ages past the grace, so a light workload measures the grace rather
//     than the gate. Under -race, which slows execution ~10x, that regime stole on up to 17 cycles of a
//     31-cycle drain - i.e. the light-load number carries no signal about the gate at all.
//   - CYCLES, because CheckpointRefillStole fires once per FETCH that took a foreign row, and a pending
//     step is re-fetched every cycle until someone claims it. The raw count is therefore bounded by the
//     LENGTH of the drain, not by the steps in it, and a loaded suite lengthens the drain: an absolute
//     bound on it reported the suite's load (measured 22-23 against a bound of 20 in a parallel -race run
//     with nothing wrong).
//
// The residual is not zero and is not expected to be: the fleet still passes through moderate load on its
// way in and out of the deep phase, and the count also picks up cycles where this replica's partition pair
// was momentarily unavailable, which makes every row it fetched read as foreign. Both are bounded, and a
// build whose gate never held off would steal on essentially every cycle that fetched anything - which is
// the regression this guards. The gate itself is pinned deterministically in internal/piston, where the
// armed flag is directly observable rather than inferred.
func TestStealHoldsOffAHealthyFleetflow(t *testing.T) {
	t.Parallel()
	const name = "stealhealthy.verify:428"
	assert := testarossa.For(t)
	creator, healthy, crippled, ran := stealFleet(t, name, 4, 0)

	// The cache holds 2 x 4 workers, so ~100 due steps per residue class is far past "this class can fill
	// its own batch". Created up front, and not awaited until the measurement is over.
	keys := createFlows(t, creator, name, 200)

	// Two cycles of settling before the window opens: the gate arms from the PREVIOUS cycle's tally, and
	// the cycle before this work landed saw an empty shard, so the first one after it is legitimately still
	// armed.
	awaitShardCycles(t, healthy, 1, 2)
	awaitShardCycles(t, crippled, 1, 2)
	stoleBefore := healthy.Seams().Visits(engine.CheckpointRefillStole) +
		crippled.Seams().Visits(engine.CheckpointRefillStole)
	cyclesBefore := healthy.Seams().Visits(engine.CheckpointRefillCycleDone) +
		crippled.Seams().Visits(engine.CheckpointRefillCycleDone)

	awaitShardCycles(t, healthy, 1, 10)
	awaitShardCycles(t, crippled, 1, 10)
	stole := healthy.Seams().Visits(engine.CheckpointRefillStole) +
		crippled.Seams().Visits(engine.CheckpointRefillStole) - stoleBefore
	cycles := healthy.Seams().Visits(engine.CheckpointRefillCycleDone) +
		crippled.Seams().Visits(engine.CheckpointRefillCycleDone) - cyclesBefore
	assert.True(2*stole < cycles,
		"a fleet whose every class can fill its own batch must mostly not steal (stole on %d of %d cycles)",
		stole, cycles)

	awaitFlows(t, 120*time.Second, creator, keys)
	assert.Equal(int64(400), ran["healthy"].Load()+ran["crippled"].Load(), "and the work still drains exactly once")
	assert.Equal(int64(0), ran["creator"].Load())
}

// TestStealTwoBadApplesflow pins the second tier, which exists for exactly one reason: to stop the first
// one from stranding work.
//
// The first tier gives each class a single designated stealer - its neighbour - so the common case (one bad
// apple in a working fleet) is covered with no two replicas ever eligible for the same step. That is a real
// contention saving, and it is also a trap: with TWO consecutive degraded replicas, the far one's class has
// no working stealer at all, because its designated one is itself broken. Its work then gets ZERO service
// rather than slow service, which is qualitatively worse than contention - flows landing there never finish.
//
// So past twice the grace the class opens to everyone. This fixture is the case that would hang against a
// neighbour-only build: TWO of the three dispatchers are crippled, so the single healthy replica must reach
// past its own neighbour to the far class. It cannot serve all three classes at full rate - nothing could -
// but every flow must still complete, which is the difference between slow and stranded.
func TestStealTwoBadApplesflow(t *testing.T) {
	t.Parallel()
	const name = "steal2bad.verify:428"
	assert := testarossa.For(t)
	ctx := context.Background()

	// Four replicas: an await-only creator plus three dispatchers, two of them crippled. The creator's work
	// is reachable only by scanning, which is what puts the residue class in the path.
	ran := map[string]*atomic.Int64{"creator": {}, "healthy": {}, "bad1": {}, "bad2": {}}
	build := func(replica string, delay time.Duration) *engine.TestProxy {
		p := engine.NewTestProxy()
		g := workflow.NewGraph("Chain")
		g.SetEndpoint("A", name+"/a")
		g.SetEndpoint("B", name+"/b")
		g.AddTransition("A", "B")
		g.AddTransition("B", workflow.END)
		assert.NoError(g.Validate())
		p.HandleGraph(name+"/g", g)
		for _, url := range []string{name + "/a", name + "/b"} {
			p.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error {
				if delay > 0 {
					select {
					case <-time.After(delay):
					case <-ctx.Done():
					}
				}
				ran[replica].Add(1)
				return nil
			})
		}
		return p
	}
	mk := func(replica string, workers int, delay time.Duration) *engine.Engine {
		e := engine.NewEngineUnderTest(t)
		e.SetHost(build(replica, delay))
		assert.NoError(e.SetWorkers(workers))
		return e
	}
	healthy := mk("healthy", 4, 0)
	bad1 := mk("bad1", 1, 150*time.Millisecond)
	bad2 := mk("bad2", 1, 150*time.Millisecond)
	creator := mk("creator", 0, 0)
	for _, e := range []*engine.Engine{healthy, bad1, bad2, creator} {
		assert.NoError(e.Startup(ctx))
		t.Cleanup(func() { e.Shutdown(ctx) })
	}

	// Warm the fleet so all three dispatchers genuinely hold classes before the load arrives.
	drainFlows(t, 30*time.Second, creator, name, 4)

	before := ran["healthy"].Load() + ran["bad1"].Load() + ran["bad2"].Load()
	stole := awaitSteal(t, healthy, func() {
		drainFlows(t, 60*time.Second, creator, name, 20)
	})

	assert.True(stole, "the one healthy replica must reach past its neighbour into the far crippled class")
	assert.Equal(before+40, ran["healthy"].Load()+ran["bad1"].Load()+ran["bad2"].Load(),
		"every step of every flow ran exactly once - stranded is worse than slow")
	assert.Equal(int64(0), ran["creator"].Load(), "and the await-only replica ran none of them")
}
