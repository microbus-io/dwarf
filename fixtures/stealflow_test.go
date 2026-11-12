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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/diagnostics"
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
// coin-flip on scheduling. A delay makes the backlog build for a reason the test controls. A non-nil gate
// holds every task on both dispatchers until it is closed, which the caller must do before the returned
// shutdown, which shuts all three down.
func stealFleet(t *testing.T, name string, crippledWorkers int, crippledDelay time.Duration, gate <-chan struct{}) (creator, healthy, crippled *engine.Engine, ran map[string]*atomic.Int64, shutdown func()) {
	t.Helper()
	assert := testarossa.For(t)
	ran = map[string]*atomic.Int64{"creator": {}, "healthy": {}, "crippled": {}}

	build := func(replica string) *hosts.Mux {
		p := hosts.NewMux()
		g := workflow.NewGraph("Chain")
		g.SetURL("A", name+"/a")
		g.SetURL("B", name+"/b")
		g.AddTransition("A", "B")
		g.AddTransition("B", workflow.End)
		assert.NoError(g.Validate())
		p.HandleGraph(name+"/g", g)
		for _, url := range []string{name + "/a", name + "/b"} {
			p.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error {
				if gate != nil {
					select {
					case <-gate:
					case <-ctx.Done():
					}
				}
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

	healthy = engine.NewEngineUnderTest(t.Name())
	healthy.SetHost(build("healthy"))
	assert.NoError(healthy.SetWorkers(4))
	crippled = engine.NewEngineUnderTest(t.Name())
	crippled.SetHost(build("crippled"))
	assert.NoError(crippled.SetWorkers(crippledWorkers))
	// Await-only: it holds a reader pool, is never ranked as a dispatcher and claims no work, so it earns no
	// residue class. Its creations are the fleet's only scan-discovered work.
	creator = engine.NewEngineUnderTest(t.Name())
	creator.SetHost(build("creator"))
	assert.NoError(creator.SetWorkers(0))

	ctx := context.Background()
	fleet := []*engine.Engine{healthy, crippled, creator}
	shutdown = func() {
		for _, e := range fleet {
			e.Shutdown(ctx)
		}
	}
	for _, e := range fleet {
		assert.Must(assert.NoError(e.Startup(ctx)))
	}
	return creator, healthy, crippled, ran, shutdown
}

// awaitSteal reports whether a replica took work from outside its own residue class, arming the waiter
// BEFORE the trigger and checking Visits after - so a steal that lands between the two lines is caught by
// the channel and one before them by the count.
//
// It is a rendezvous rather than a poll because no duration is the right one to wait: the steal fires on
// the first cycle where the grace has elapsed and this replica's own class ran short of the batch, which is
// a function of the pipeline's cadence, the peer's degradation and the backlog, none of which the test
// controls.
func awaitSteal(t *testing.T, e *engine.Engine, work func()) bool {
	t.Helper()
	ch := accessors.Seams(e).Waiter(diagnostics.CheckpointRefillStole)
	before := accessors.Seams(e).Visits(diagnostics.CheckpointRefillStole)
	work()
	if accessors.Seams(e).Visits(diagnostics.CheckpointRefillStole) != before {
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
	creator, healthy, _, ran, shutdown := stealFleet(t, name, 1, 150*time.Millisecond, nil)
	defer shutdown()

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
	creator, healthy, crippled, ran, shutdown := stealFleet(t, name, 4, 0, nil)
	defer shutdown()

	drainFlows(t, 30*time.Second, creator, name, 4)

	// Slow every operation this replica makes, from here on.
	for _, idx := range accessors.DB(crippled).Indices() {
		sh, err := accessors.DB(crippled).Shard(idx)
		if !assert.NoError(err) {
			return
		}
		db := sh.DB()
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
// It is measured only while a healthy fleet's own classes are unambiguously full, and the shape below is
// what makes that true rather than hoped for:
//
//   - HELD, because creating the backlog does not build one: Create is one transaction per flow, and two
//     four-worker dispatchers drain the chain as fast as the creator makes it, so an unheld fleet has run
//     every step before a window opens and the assertion measures an empty shard. Under a loaded suite the
//     drain merely lags, and the window lands on its tail instead - the moderate-load regime below. The gate
//     holds every task until the whole backlog is due, which also puts every foreign step past the grace:
//     the fill order alone is under test, which is the saturation case it exists for.
//   - DEEP, because at MODERATE load a replica legitimately reaches outside its class with nobody at fault:
//     classes empty between arrivals while everything ages past the grace, so a light workload measures the
//     grace rather than the fill order. Under -race, which slows execution ~10x, that regime stole on up to
//     17 cycles of a 31-cycle drain - i.e. the light-load number carries no signal at all.
//   - BOUNDED BY THE BACKLOG, not by a cycle count or a duration, because either one lets a slow suite or a
//     fast drain carry the window past the deep phase. It closes once either dispatcher has run half of what
//     a class holds, which leaves each class still several batches deep.
//   - Counted per CYCLE, because CheckpointRefillStole fires once per FETCH that took a foreign row, and a
//     pending step is re-fetched every cycle until someone claims it, so a raw count reports the length of
//     the window rather than the steps in it.
//
// The residual is not zero and is not expected to be. It is NOT inflated by an unavailable partition pair -
// that resolves to a zero pair, the ranking is skipped and nothing is counted stolen (pinned by
// TestPiston_PartitionPairIsValidated). A build that reached outside its class whenever it merely COULD
// would steal on essentially every cycle that fetched anything, which is the regression this guards. The
// mechanism itself is pinned deterministically in internal/piston against real rows
// (TestPiston_FetchPrefersItsOwnClass), where the ranking is observable rather than inferred from counts.
func TestStealHoldsOffAHealthyFleetflow(t *testing.T) {
	t.Parallel()
	const name = "stealhealthy.verify:428"
	const flows = 200
	assert := testarossa.For(t)
	gate := make(chan struct{})
	var opened sync.Once
	release := func() { opened.Do(func() { close(gate) }) }
	creator, healthy, crippled, ran, shutdown := stealFleet(t, name, 4, 0, gate)
	defer shutdown()
	defer release() // below the shutdown, so a failed assertion cannot leave the drain waiting on the gate

	// The cache holds 2 x 4 workers, so ~100 due steps per residue class is far past "this class can fill
	// its own batch".
	keys := createFlows(t, creator, name, flows)

	// A scan must have seen the backlog before the window opens, or the first fetch plans from a tally of
	// an empty shard.
	enginetest.AwaitShardCycles(t, healthy, 2)
	enginetest.AwaitShardCycles(t, crippled, 2)
	visits := func(checkpoint string) int {
		return accessors.Seams(healthy).Visits(checkpoint) + accessors.Seams(crippled).Visits(checkpoint)
	}
	stoleBefore := visits(diagnostics.CheckpointRefillStole)
	cyclesBefore := visits(diagnostics.CheckpointRefillCycleDone)
	release()

	// Each dispatcher runs its own class's A steps and the B each one chains into its own cache, so a
	// replica that has run `flows/2` tasks is half-way through a class of ~flows/2 A steps.
	deadline := time.After(60 * time.Second * enginetest.TimeoutScale())
	for {
		cycled := accessors.Seams(healthy).Waiter(diagnostics.CheckpointRefillCycleDone)
		if max(ran["healthy"].Load(), ran["crippled"].Load()) >= flows/2 {
			break
		}
		select {
		case <-cycled:
		case <-deadline:
			t.Fatalf("the held backlog never drained half a class (ran %d and %d)", ran["healthy"].Load(), ran["crippled"].Load())
		}
	}
	stole := visits(diagnostics.CheckpointRefillStole) - stoleBefore
	cycles := visits(diagnostics.CheckpointRefillCycleDone) - cyclesBefore
	assert.True(2*stole < cycles,
		"a fleet whose every class can fill its own batch must mostly not steal (stole on %d of %d cycles)",
		stole, cycles)

	awaitFlows(t, 120*time.Second, creator, keys)
	assert.Equal(int64(2*flows), ran["healthy"].Load()+ran["crippled"].Load(), "and the work still drains exactly once")
	assert.Equal(int64(0), ran["creator"].Load())
}
