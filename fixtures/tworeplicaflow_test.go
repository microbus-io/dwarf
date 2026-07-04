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
SQLite DSN), each with its own TestProxy and workers, wired as peers both ways. This pins the
claim-CAS exactly-once guarantee under real cross-replica contention: every step of every flow runs
exactly once no matter which replica claims it. A second subtest proves the cross-replica Await wake
fires via the peer statusChange signal (fast), not the 5s backstop poll.
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

func TestTwoReplicaflow(t *testing.T) {
	ctx := context.Background()

	// Shared per-task execution counters, incremented by whichever replica runs the step.
	counters := map[string]*atomic.Int64{
		"A": {}, "B": {}, "C": {},
	}

	// buildProxy registers the same 3-step graph and handlers (closing over the shared counters) on a
	// proxy, so either replica executes identically.
	buildProxy := func() *engine.TestProxy {
		p := engine.NewTestProxy()
		g := workflow.NewGraph("Chain")
		g.SetEndpoint("A", "tworeplica.verify:428/a")
		g.SetEndpoint("B", "tworeplica.verify:428/b")
		g.SetEndpoint("C", "tworeplica.verify:428/c")
		g.AddTransition("A", "B")
		g.AddTransition("B", "C")
		g.AddTransition("C", workflow.END)
		testarossa.NoError(t, g.Validate())
		p.HandleGraph("tworeplica.verify:428/chain", g)
		for task, url := range map[string]string{"A": "tworeplica.verify:428/a", "B": "tworeplica.verify:428/b", "C": "tworeplica.verify:428/c"} {
			p.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error {
				counters[task].Add(1)
				return nil
			})
		}
		return p
	}

	dsn := "file:tworeplica%d?mode=memory&cache=shared"
	proxy1 := buildProxy()
	proxy2 := buildProxy()

	eng1 := engine.NewEngine()
	eng1.SetHost(proxy1)
	eng1.SetDSN(dsn)
	testarossa.NoError(t, eng1.SetWorkers(4))
	eng2 := engine.NewEngine()
	eng2.SetHost(proxy2)
	eng2.SetDSN(dsn)
	testarossa.NoError(t, eng2.SetWorkers(4))
	proxy1.AddPeer(eng2)
	proxy2.AddPeer(eng1)

	assert := testarossa.For(t)
	assert.NoError(eng1.Startup(ctx))
	t.Cleanup(func() { eng1.Shutdown(ctx) })
	assert.NoError(eng2.Startup(ctx))
	t.Cleanup(func() { eng2.Shutdown(ctx) })

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
			awaitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
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

	t.Run("cross_replica_await_wakes_via_signal_not_poll", func(t *testing.T) {
		assert := testarossa.For(t)

		// A dedicated pair on their own shared DB: `awaiter` has ZERO workers (so it can never claim the
		// step - forcing the cross-replica path), `worker` executes it. Awaiting on `awaiter` can only
		// return via `worker`'s peer statusChange signal; the backstop poll is 5s, so a sub-2s return proves
		// the signal (not the poll) woke the waiter. (Mirrors subgrapherrorwaitflow_test.go's timing pin.)
		wdsn := "file:tworeplicawake%d?mode=memory&cache=shared"
		pa := engine.NewTestProxy() // pure awaiter
		pb := engine.NewTestProxy() // executor
		wg := workflow.NewGraph("Wake")
		wg.SetEndpoint("W", "tworeplicawake.verify:428/w")
		wg.AddTransition("W", workflow.END)
		testarossa.NoError(t, wg.Validate())
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

		awaiter := engine.NewEngine()
		awaiter.SetHost(pa)
		awaiter.SetDSN(wdsn)
		assert.NoError(awaiter.SetWorkers(0))
		worker := engine.NewEngine()
		worker.SetHost(pb)
		worker.SetDSN(wdsn)
		assert.NoError(worker.SetWorkers(2))
		pa.AddPeer(worker)
		pb.AddPeer(awaiter)
		assert.NoError(awaiter.Startup(ctx))
		t.Cleanup(func() { awaiter.Shutdown(ctx) })
		assert.NoError(worker.Startup(ctx))
		t.Cleanup(func() { worker.Shutdown(ctx) })

		k, err := awaiter.Create(ctx, "tworeplicawake.verify:428/g", nil, nil)
		if !assert.NoError(err) {
			return
		}

		start := time.Now()
		awaitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		out, err := awaiter.Await(awaitCtx, k)
		cancel()
		elapsed := time.Since(start)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, out.Status)
		}
		assert.True(ranOnWorker.Load(), "the worker replica should have executed the step")
		assert.True(elapsed < 2*time.Second, "cross-replica Await should wake via peer signal (<2s), took %s", elapsed)
	})
}
