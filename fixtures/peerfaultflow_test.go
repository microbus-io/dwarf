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
func peerFleet(t *testing.T, name string) (creator, eng1, eng2 *engine.Engine, ran map[string]*atomic.Int64) {
	t.Helper()
	assert := testarossa.For(t)
	ran = map[string]*atomic.Int64{"0": {}, "1": {}, "2": {}}

	buildProxy := func(replica string) *engine.TestProxy {
		p := engine.NewTestProxy()
		g := workflow.NewGraph("Pair")
		g.SetEndpoint("A", name+"/a")
		g.SetEndpoint("B", name+"/b")
		g.AddTransition("A", "B")
		g.AddTransition("B", workflow.END)
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

	eng1 = engine.NewEngineUnderTest(t)
	eng1.SetHost(buildProxy("1"))
	assert.NoError(eng1.SetWorkers(4))
	eng2 = engine.NewEngineUnderTest(t)
	eng2.SetHost(buildProxy("2"))
	assert.NoError(eng2.SetWorkers(4))
	// Await-only: it holds connections and counts toward the pool divisor, but claims no work, so it never
	// earns a residue class. Its creations are therefore the fleet's only scan-discovered work.
	creator = engine.NewEngineUnderTest(t)
	creator.SetHost(buildProxy("0"))
	assert.NoError(creator.SetWorkers(0))

	ctx := context.Background()
	for _, e := range []*engine.Engine{eng1, eng2, creator} {
		assert.NoError(e.Startup(ctx))
		t.Cleanup(func() { e.Shutdown(ctx) })
	}
	return creator, eng1, eng2, ran
}

// drainFlows creates n flows on one replica and awaits them all there, failing on the first that does not
// complete inside the budget.
func drainFlows(t *testing.T, budget time.Duration, from *engine.Engine, name string, n int) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	keys := make([]string, 0, n)
	for range n {
		k, err := from.Create(ctx, name+"/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
		keys = append(keys, k)
	}
	awaitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	for _, k := range keys {
		out, err := from.Await(awaitCtx, k)
		if !assert.NoError(err, "flow %s never stopped", k) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
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
	creator, eng1, eng2, ran := peerFleet(t, name)
	assert := testarossa.For(t)

	// Warm the fleet so both dispatchers have a good reading behind them, then take the registry away from
	// all three - reads only. Their beats still land, so this is a fleet that cannot LOOK, not one that has
	// stopped existing.
	drainFlows(t, 30*time.Second, creator, name, 4)
	for _, e := range []*engine.Engine{creator, eng1, eng2} {
		e.Seams().InjectN(1<<20, engine.FaultPeerReadErr)
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
func TestPeerStalledDispatcherflow(t *testing.T) {
	t.Parallel()
	const name = "peerstall.verify:428"
	creator, _, eng2, ran := peerFleet(t, name)
	assert := testarossa.For(t)

	// Both dispatching first, so eng2 really is holding a residue class before it stalls.
	drainFlows(t, 30*time.Second, creator, name, 4)

	// eng2's supply cycle fails from here on. It goes on beating - so it stays in the POOL divisor, which is
	// correct, it still holds connections - but publishes no dispatch evidence.
	eng2.Seams().InjectN(1<<20, engine.FaultRefillScanErr)

	before := ran["1"].Load()
	started := time.Now()
	drainFlows(t, 60*time.Second, creator, name, 20)
	assert.Equal(before+40, ran["1"].Load(), "the surviving replica ran every step of every flow")

	// It cannot have been quick: the survivor had to watch the stalled replica's evidence age out first, and
	// that window is deliberately several beats wide. A drain that finished immediately would mean the
	// partition was never in force and the test proved nothing.
	assert.True(time.Since(started) > time.Second,
		"the takeover waits out the dispatch window (took %s)", time.Since(started))
}
