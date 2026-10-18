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

/*
Two-replica contention. Two engines share one database (a shared in-memory
SQLite DSN), each with its own hosts.Mux and workers, wired as peers both ways. This pins the
claim-CAS exactly-once guarantee under real cross-replica contention: every step of every flow runs
exactly once no matter which replica claims it. A second subtest proves the cross-replica Await wake is
prompt - within the latch detector's cadence - rather than dragging out to the caller's own deadline.
*/
package fixtures

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// NOT t.Parallel: asserts an upper-bound reaction latency (cross-replica wake via the latch detector's poll < 2s), which CPU oversubscription
// from co-running parallel tests can inflate past the bound.
func TestTwoReplicaflow(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	// Shared per-task execution counters, incremented by whichever replica runs the step.
	counters := map[string]*atomic.Int64{
		"A": {}, "B": {}, "C": {},
	}

	// buildMux registers the same 3-step graph and handlers (closing over the shared counters) on a
	// mux, so either replica executes identically.
	buildMux := func() *hosts.Mux {
		p := hosts.NewMux()
		g := workflow.NewGraph("Chain")
		g.SetURL("A", "tworeplica.verify:428/a")
		g.SetURL("B", "tworeplica.verify:428/b")
		g.SetURL("C", "tworeplica.verify:428/c")
		g.AddTransition("A", "B")
		g.AddTransition("B", "C")
		g.AddTransition("C", workflow.End)
		assert.NoError(g.Validate())
		p.HandleGraph("tworeplica.verify:428/chain", g)
		for task, url := range map[string]string{"A": "tworeplica.verify:428/a", "B": "tworeplica.verify:428/b", "C": "tworeplica.verify:428/c"} {
			p.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error {
				counters[task].Add(1)
				return nil
			})
		}
		return p
	}

	mux1 := buildMux()
	mux2 := buildMux()

	// Both replicas share one isolated database via a common test-DB key (each built with
	// NewEngineUnderTest(t.Name()), which keys by that name), so the suite runs this contention on whatever
	// dialect SEQUEL_TESTING_DSN names (in-memory SQLite by default).
	eng1 := engine.NewEngineUnderTest(t.Name())
	defer eng1.Shutdown(ctx)
	eng1.SetHost(mux1)
	assert.NoError(eng1.SetWorkers(4))
	eng2 := engine.NewEngineUnderTest(t.Name())
	defer eng2.Shutdown(ctx)
	eng2.SetHost(mux2)
	assert.NoError(eng2.SetWorkers(4))
	assert.NoError(eng1.Startup(ctx))
	assert.NoError(eng2.Startup(ctx))

	t.Run("exactly_once_under_contention", func(t *testing.T) {
		assert := testarossa.For(t)

		const flows = 40
		engines := []*engine.Engine{eng1, eng2}
		keys := make([]string, 0, flows)
		creators := make([]*engine.Engine, 0, flows)
		for i := range flows {
			e := engines[i%2] // alternate which replica's Create is called
			k, err := e.Create(ctx, "tworeplica.verify:428/chain", nil, nil)
			if !assert.NoError(err) {
				return
			}
			keys = append(keys, k)
			creators = append(creators, e)
		}

		// Await each flow (on its creating replica); either replica may have executed any given step.
		for i, k := range keys {
			awaitCtx, cancel := context.WithTimeout(ctx, 60*time.Second*enginetest.TimeoutScale())
			out, err := creators[i].Await(awaitCtx, k)
			cancel()
			if assert.NoError(err) {
				assert.Equal(workflow.StatusCompleted, out.Status)
			}
		}

		// The CAS proof: each of the 3 tasks executed exactly once per flow - never twice, despite both
		// replicas racing to claim every step.
		for _, name := range []string{"A", "B", "C"} {
			assert.Equal(int64(flows), counters[name].Load(),
				"task %s should have run exactly once per flow", name)
		}
	})

	t.Run("cross_replica_await_wakes_promptly", func(t *testing.T) {
		assert := testarossa.For(t)

		// A dedicated pair on their own shared DB: `awaiter` has ZERO workers (so it can never claim the
		// step - forcing the cross-replica path), `worker` executes it. Nothing crosses between the two, so
		// `awaiter` can only learn of the stop by reading the flow row - and the detector is the only thing
		// that reads it on the awaiter's behalf, so a sub-2s return is a direct measurement of the detector
		// working. Nothing else could return this Await short of its own 30s deadline.
		pa := hosts.NewMux() // pure awaiter
		pb := hosts.NewMux() // executor
		wg := workflow.NewGraph("Wake")
		wg.SetURL("W", "tworeplicawake.verify:428/w")
		wg.AddTransition("W", workflow.End)
		assert.NoError(wg.Validate())
		pa.HandleGraph("tworeplicawake.verify:428/g", wg)
		pb.HandleGraph("tworeplicawake.verify:428/g", wg)
		pa.HandleTask("tworeplicawake.verify:428/w", func(ctx context.Context, f *workflow.Flow) error {
			t.Error("the 0-worker awaiter must never execute the task")
			return nil
		})
		var ranOnWorker atomic.Bool
		pb.HandleTask("tworeplicawake.verify:428/w", func(ctx context.Context, f *workflow.Flow) error {
			ranOnWorker.Store(true)
			return nil
		})

		// This pair shares its own isolated database, distinct from the outer pair's, via the subtest's
		// t.Name() key (NewEngineUnderTest keys by it) - so it too runs on the SEQUEL_TESTING_DSN dialect,
		// not SQLite only.
		awaiter := engine.NewEngineUnderTest(t.Name())
		defer awaiter.Shutdown(ctx)
		awaiter.SetHost(pa)
		assert.NoError(awaiter.SetWorkers(0))
		worker := engine.NewEngineUnderTest(t.Name())
		defer worker.Shutdown(ctx)
		worker.SetHost(pb)
		assert.NoError(worker.SetWorkers(2))
		assert.NoError(awaiter.Startup(ctx))
		assert.NoError(worker.Startup(ctx))

		k, err := awaiter.Create(ctx, "tworeplicawake.verify:428/g", nil, nil)
		if !assert.NoError(err) {
			return
		}

		start := time.Now()
		awaitCtx, cancel := context.WithTimeout(ctx, 15*time.Second*enginetest.TimeoutScale())
		out, err := awaiter.Await(awaitCtx, k)
		cancel()
		elapsed := time.Since(start)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, out.Status)
		}
		assert.True(ranOnWorker.Load(), "the worker replica should have executed the step")
		assert.True(elapsed < 2*time.Second, "cross-replica Await should wake via the latch detector's poll (<2s), took %s", elapsed)
	})
}
