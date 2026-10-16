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
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// peerFleet stands up three replicas over one shared database - which is all a fleet is: two that dispatch,
// and one await-only CREATOR that cannot.
//
// The creator is what makes these tests test anything. A replica that creates a step offers it straight
// into its own candidate cache, and the doorbell is deliberately not partitioned, so a chain created on a
// working replica walks hop by hop on that replica and never consults a residue class at all. Work created
// by a replica with no workers can only be found by SCANNING, which is the path the partition governs.
//
// The caller defers the returned shutdown, which shuts all three down.
func peerFleet(t *testing.T, name string) (creator, eng1, eng2 *engine.Engine, ran map[string]*atomic.Int64, shutdown func()) {
	t.Helper()
	assert := testarossa.For(t)
	ran = map[string]*atomic.Int64{"0": {}, "1": {}, "2": {}}

	buildMux := func(replica string) *hosts.Mux {
		p := hosts.NewMux()
		g := workflow.NewGraph("Pair")
		g.SetURL("A", name+"/a")
		g.SetURL("B", name+"/b")
		g.AddTransition("A", "B")
		g.AddTransition("B", workflow.End)
		assert.NoError(g.Validate())
		p.HandleGraph(name+"/g", g)
		for _, url := range []string{name + "/a", name + "/b"} {
			p.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error {
				ran[replica].Add(1)
				return nil
			})
		}
		return p
	}

	eng1 = engine.NewEngineUnderTest(t.Name())
	eng1.SetHost(buildMux("1"))
	assert.NoError(eng1.SetWorkers(4))
	eng2 = engine.NewEngineUnderTest(t.Name())
	eng2.SetHost(buildMux("2"))
	assert.NoError(eng2.SetWorkers(4))
	// Await-only: it holds connections and counts toward the pool divisor, but claims no work, so it never
	// earns a residue class. Its creations are therefore the fleet's only scan-discovered work.
	creator = engine.NewEngineUnderTest(t.Name())
	creator.SetHost(buildMux("0"))
	assert.NoError(creator.SetWorkers(0))

	ctx := context.Background()
	fleet := []*engine.Engine{eng1, eng2, creator}
	shutdown = func() {
		for _, e := range fleet {
			e.Shutdown(ctx)
		}
	}
	for _, e := range fleet {
		assert.NoError(e.Startup(ctx))
	}
	return creator, eng1, eng2, ran, shutdown
}

// createFlows creates n flows on one replica and returns their keys, without waiting for any of them.
// Split from the await half so a test can observe what the fleet does with the work while it is still in
// flight - see the takeover timing in TestPeerStalledDispatcherflow.
func createFlows(t *testing.T, from *engine.Engine, name string, n int) []string {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	keys := make([]string, 0, n)
	for range n {
		k, err := from.Create(ctx, name+"/g", nil, nil)
		if !assert.NoError(err) {
			return keys
		}
		keys = append(keys, k)
	}
	return keys
}

// awaitFlows awaits every key on one replica, failing on the first that does not complete inside the
// budget. The budget is a "did it hang" ceiling, never a timing contract: it covers the dispatch of every
// step of every flow across the fleet, which a loaded suite makes unbounded.
func awaitFlows(t *testing.T, budget time.Duration, from *engine.Engine, keys []string) {
	t.Helper()
	assert := testarossa.For(t)
	awaitCtx, cancel := context.WithTimeout(context.Background(), budget*enginetest.TimeoutScale())
	defer cancel()
	for _, k := range keys {
		out, err := from.Await(awaitCtx, k)
		if !assert.NoError(err, "flow %s never stopped", k) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

// drainFlows creates n flows on one replica and awaits them all there.
func drainFlows(t *testing.T, budget time.Duration, from *engine.Engine, name string, n int) {
	t.Helper()
	awaitFlows(t, budget, from, createFlows(t, from, name, n))
}

// TestPeerRegistryBlindflow pins the property an operator depends on: the peer registry going bad must not
// stop workflows from running. It holds the engine's own bookkeeping - what divides the connection pools
// and the residue classes - not workflow data, so losing sight of it must degrade tuning rather than
// execution.
//
// The whole fleet goes blind at once, which is the correlated-stall shape: one database hiccup stalls every
// replica's reading, not one. Each then holds its last good view and stops partitioning, and the work still
// drains exactly once. Note what this does NOT distinguish: a blind replica holding a STALE partition would
// also drain this workload, because the two classes remain complementary while both replicas are alive.
// What it pins is that a registry that stops answering wedges nothing and duplicates nothing - the
// fail-open pair itself is pinned in engine/peerfault_test.go, where the divisor is visible.
//
// Not reachable without the read seam: a registry that answers cannot be made to stop.
func TestPeerRegistryBlindflow(t *testing.T) {
	t.Parallel()
	const name = "peerblind.verify:428"
	creator, eng1, eng2, ran, shutdown := peerFleet(t, name)
	defer shutdown()
	assert := testarossa.For(t)

	// Warm the fleet so both dispatchers have a good reading behind them, then take the registry away from
	// all three - reads only. Their beats still land, so this is a fleet that cannot LOOK, not one that has
	// stopped existing.
	drainFlows(t, 30*time.Second, creator, name, 4)
	for _, e := range []*engine.Engine{creator, eng1, eng2} {
		engineseams.Seams(e).InjectN(engineseams.FaultPeerReadErr, 1<<20)
	}

	before := ran["1"].Load() + ran["2"].Load()
	drainFlows(t, 60*time.Second, creator, name, 20)
	assert.Equal(before+40, ran["1"].Load()+ran["2"].Load(), "every step of every flow ran exactly once")
	assert.Equal(int64(0), ran["0"].Load(), "and the await-only replica ran none of them")
}

// TestPeerStalledDispatcherflow pins the takeover, which is the failure the work divisor exists to survive:
// one replica stops serving while its row goes on saying it is alive, and the work in ITS residue class
// must not strand.
//
// The stalled replica keeps beating - it is alive, reading, holding connections - but its supply cycles
// fail, so it stops advancing the evidence that it DISPATCHES. Its peer must drop it from the work divisor
// within the dispatch window and then stop partitioning, since one dispatcher divides nothing, picking up
// the half it had been excluding by residue class.
//
// The flows are created by the await-only replica precisely so this is load-bearing: work a dispatcher
// created itself would ride its own doorbell into its own cache and never consult a class at all. Here the
// only way to the stalled replica's half is for the survivor to stop excluding it, so without the eviction
// this test hangs on roughly half its flows.
// Deliberately NOT parallel. The takeover ceiling below is an upper-bound timing assertion, and the
// window it is measured against - the 5s dispatch freshness window - is a fixed property of the peer
// registry that no test knob shortens, so the ceiling cannot be stretched to absorb CPU oversubscription
// from co-running fixtures. An outcome assertion tolerates parallelism; this one does not.
func TestPeerStalledDispatcherflow(t *testing.T) {
	const name = "peerstall.verify:428"
	creator, eng1, eng2, ran, shutdown := peerFleet(t, name)
	defer shutdown()
	assert := testarossa.For(t)

	// Both dispatching first, so eng2 really is holding a residue class before it stalls.
	drainFlows(t, 30*time.Second, creator, name, 4)

	// eng2's supply cycle fails from here on. It goes on beating - so it stays in the POOL divisor, which is
	// correct, it still holds connections - but publishes no dispatch evidence.
	//
	// Armed with eng2's PIPELINE quiesced, or eng2 can still reach a few of the flows below, through either
	// loop - and the hints it pushes survive every later scan failure, since a failed scan spares the cache:
	//
	//   - The tally loop consults the fault before its scan waits for a turn and a connection, so a scan that
	//     passed the consult just before the arming can run its SQL after the flows commit. Freezing the loop
	//     just after a completed scan means the next scan consults the armed fault, and the frozen tally was
	//     read after the warmup drained, so it holds nothing.
	//   - The supply loop fetches CURRENT rows for whatever its plan named, so a cycle that planned from a
	//     tally taken while the warmup was still pending, then waited for a turn, fetches the new flows'
	//     steps. Two pushes while the tally is frozen empty retire any such cycle and leave the partition
	//     empty: the first may be the in-flight one, the second planned from the empty tally.
	//
	// Shard 1 is the fleet's only shard.
	tallied := engineseams.Join(engineseams.CheckpointRefillTallyDone, "1")
	engineseams.Seams(eng2).Break(tallied)
	<-engineseams.Seams(eng2).Waiter(tallied)
	enginetest.AwaitVisits(t, engineseams.Seams(eng2), 2, 30*time.Second, engineseams.Join(engineseams.CheckpointRefillCycleDone, "1"))
	engineseams.Seams(eng2).InjectN(engineseams.FaultRefillScanErr, 1<<20)
	engineseams.Seams(eng2).Resume(tallied)

	// Arm BEFORE the work exists, then check Visits after: a steal landing between the two lines is caught by
	// the channel, one before them by the count. The reverse order reintroduces the race the split-arm Waiter
	// exists to remove.
	stole := engineseams.Seams(eng1).Waiter(engineseams.CheckpointRefillStole)
	visitsBefore := engineseams.Seams(eng1).Visits(engineseams.CheckpointRefillStole)

	before := ran["1"].Load()
	keys := createFlows(t, creator, name, 20)

	// WHICH mechanism carried it is the whole question, and the outcome alone cannot answer it: the flows
	// drain either way. Two routes exist - the survivor STEALS the stalled replica's class while it still
	// holds it, or it waits out the 5s dispatch window until the stalled replica is evicted from the divisor
	// and partitioning switches off.
	//
	// One bounded wait settles both, because a steal CANNOT fire after the eviction: with one dispatcher
	// left the partition pair fails open, the fetch runs unpartitioned, and no row is outside a class to be
	// counted as stolen. So a steal seen inside the window IS the takeover, and the wait is the window.
	//
	// It is timed on the takeover, not on the drain. Timing the drain instead folds in the dispatch of 40
	// steps across three replicas sharing one database, which a loaded suite makes unbounded - measured at
	// 4.3s, 6.7s and 31.2s on runs where this steal assertion passed, so the clock was reporting the suite's
	// load and not the mechanism. The takeover itself is one grace (4 cycle periods, ~100ms here) behind the
	// first Create. Measured ~150ms.
	stolen := visitsBefore != engineseams.Seams(eng1).Visits(engineseams.CheckpointRefillStole)
	if !stolen {
		select {
		case <-stole:
			stolen = true
		case <-time.After(3 * time.Second):
		}
	}
	assert.True(stolen,
		"the survivor must TAKE the stalled replica's class within the dispatch window, not wait for it to be evicted")

	awaitFlows(t, 60*time.Second, creator, keys)
	assert.Equal(before+40, ran["1"].Load(), "the surviving replica ran every step of every flow")
}
