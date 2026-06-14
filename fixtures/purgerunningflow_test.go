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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestPurgerunningflow verifies that Purge never deletes a running flow. A flow whose task is still
// in flight must survive a Purge whose query matches it; purging in-flight work would corrupt active
// executions. The non-running guard lives inside the DELETE, so the running flow is skipped.
func TestPurgerunningflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Flow", "purgerunningflow.verify:428/flow")
	graph.AddTask("Work", "purgerunningflow.verify:428/work")
	graph.AddTransition("Work", workflow.END)
	proxy.HandleGraph("purgerunningflow.verify:428/flow", graph)

	// The task blocks until the test releases it, so the flow stays running across the Purge call.
	release := make(chan struct{})
	running := make(chan struct{}, 1)
	proxy.HandleTask("purgerunningflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case running <- struct{}{}:
		default:
		}
		<-release
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy).
		WithWorkers(2)
	eng.RunInTest(t)

	flowKey, err := eng.Create(ctx, "purgerunningflow.verify:428/flow", nil, nil)
	if !assert.NoError(err) {
		return
	}
	if !assert.NoError(eng.Start(ctx, flowKey)) {
		return
	}

	// Wait until the task is actually running.
	select {
	case <-running:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("task never started")
	}

	// Purge everything for this workflow. The running flow must be skipped.
	deleted, err := eng.Purge(ctx, workflow.Query{WorkflowURL: "purgerunningflow.verify:428/flow"})
	assert.NoError(err)
	assert.Equal(0, deleted)

	// The running flow must still exist.
	snap, err := eng.Snapshot(ctx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusRunning, snap.Status)
	}

	// Release the task so the flow completes and the engine can drain on cleanup.
	close(release)
	timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	outcome, err := eng.Await(timeoutCtx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
}
