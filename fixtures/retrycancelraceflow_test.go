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

// TestRetryCancelRaceflow pins the retry rewind's status guard. A Cancel landing mid-task terminalizes the
// running step (cancelled); if the task then arms flow.Retry, an unguarded rewind would flip the immutable
// cancelled step back to `pending` (with a backoff not_before minutes out) and reap the now-terminal tree's
// subgraph children - a transient zombie plus an immutability violation. With the guard, the rewind is a
// no-op against the cancelled step: it stays cancelled and is never re-dispatched.
func TestRetryCancelRaceflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var invocations atomic.Int32

	graph := workflow.NewGraph("RetryCancelRace")
	graph.SetEndpoint("Work", "retrycancelraceflow.verify:428/work")
	graph.AddTransitionChain("Work", workflow.END)
	proxy.HandleGraph("retrycancelraceflow.verify:428/retry-cancel-race", graph)

	proxy.HandleTask("retrycancelraceflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		if invocations.Add(1) != 1 {
			// A revived zombie dispatch would land here; the guard means it never does.
			return nil
		}
		started <- struct{}{}
		<-release // hold the step in `running` until the test has cancelled the flow
		// Arm a retry with a long backoff: without the guard the cancelled step would be flipped to
		// `pending` with a not_before ~10s out, which the assertion window below would observe.
		f.Retry(10*time.Second, 2, 30*time.Second, time.Hour)
		return nil
	})

	assert := testarossa.For(t)

	flowKey, err := eng.Create(ctx, "retrycancelraceflow.verify:428/retry-cancel-race", nil, nil)
	if !assert.NoError(err) {
		return
	}

	// Wait until the task is executing, so the step is definitively `running`.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task never started")
	}

	// Cancel mid-task: the running step and the flow go cancelled.
	if !assert.NoError(eng.Cancel(ctx, flowKey, "abort mid-task")) {
		return
	}

	// Release the task; it now arms flow.Retry, and the worker runs the retry-rewind branch.
	close(release)

	outcome, err := eng.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCancelled, outcome.Status)

	// Invariant: the cancelled step is never revived. With the guard it stays `cancelled` (attempt 0)
	// throughout; without it the step would flip to `pending` (attempt 1) and persist there for the ~10s
	// backoff, which this loop would catch.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := stepRecordByTask(t, eng, flowKey, "Work")
		if !assert.NotEqual(workflow.StatusPending, st.Status) {
			return
		}
		if !assert.Equal(0, st.Attempt) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	final := stepRecordByTask(t, eng, flowKey, "Work")
	assert.Equal(workflow.StatusCancelled, final.Status)
	assert.Equal(0, final.Attempt)
	// The zombie re-dispatch never re-ran the task.
	assert.Equal(int32(1), invocations.Load())
}

// stepRecordByTask returns the full history record of the first step whose task matches taskName.
func stepRecordByTask(t *testing.T, eng *engine.Engine, flowKey, taskName string) workflow.FlowStep {
	t.Helper()
	hist, err := eng.History(context.Background(), flowKey)
	testarossa.For(t).NoError(err)
	for _, s := range hist {
		if s.TaskName == taskName {
			return s
		}
	}
	return workflow.FlowStep{}
}
