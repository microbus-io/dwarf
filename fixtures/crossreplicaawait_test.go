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

// TestCrossReplicaAwait verifies that an Await on the replica that did NOT run a flow's final step still
// wakes when a peer replica completes it. eng1 is a pure awaiter (zero workers, so it can never claim or
// execute a step); eng2 does all the work. proxy2's SignalPeers relays the status-change signal from eng2 to eng1, so
// the only path by which eng1's Await can return is the cross-replica notification. Without that wiring
// this test would block until its context deadline.
func TestCrossReplicaAwait(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	graph := workflow.NewGraph("Flow", "crossreplica.verify:428/flow")
	graph.AddTask("Work", "crossreplica.verify:428/work")
	graph.AddTransition("Work", workflow.END)

	// eng1: pure awaiter. Its task handler must never run (zero workers).
	proxy1 := engine.NewTestProxy()
	proxy1.HandleGraph("crossreplica.verify:428/flow", graph)
	proxy1.HandleTask("crossreplica.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		t.Error("eng1 has zero workers and must never execute the task")
		return nil
	})

	// eng2: the executor. Records that it ran the step and writes the result.
	var ranOnEng2 atomic.Bool
	proxy2 := engine.NewTestProxy()
	proxy2.HandleGraph("crossreplica.verify:428/flow", graph)
	proxy2.HandleTask("crossreplica.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		ranOnEng2.Store(true)
		f.SetString("result", "done-by-eng2")
		return nil
	})

	dsn := "file:xrepl%d?mode=memory&cache=shared"
	eng1 := engine.NewEngine()
	eng1.SetHost(proxy1)
	eng1.SetDSN(dsn)
	eng1.SetWorkers(0)
	eng2 := engine.NewEngine()
	eng2.SetHost(proxy2)
	eng2.SetDSN(dsn)
	eng2.SetWorkers(2)
	proxy1.AddPeer(eng2)
	proxy2.AddPeer(eng1)

	err := eng1.Startup(ctx)
	assert.NoError(err)
	t.Cleanup(func() { eng1.Shutdown(ctx) })
	err = eng2.Startup(ctx)
	assert.NoError(err)
	t.Cleanup(func() { eng2.Shutdown(ctx) })

	// Create and start on the awaiter replica. eng1 has no workers, so eng2 (reached via the relayed
	// Enqueue doorbell, or its own poll) is the only replica that can run the step.
	flowKey, err := eng1.Create(ctx, "crossreplica.verify:428/flow", nil, nil)
	if !assert.NoError(err) {
		return
	}
	err = eng1.Start(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}

	// Await on eng1. eng1 fires no local status-change notification for this flow (it never runs it), so
	// returning here requires eng2's cross-replica NotifyStatusChange to have woken the waiter.
	timeoutCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	outcome, err := eng1.Await(timeoutCtx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("done-by-eng2", outcome.State["result"])
	assert.True(ranOnEng2.Load())
}
