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
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestDeleteRunningflow pins the Delete retention guard: Delete refuses a running flow with 409 (the caller
// must Cancel first), and succeeds once the flow is terminal. The flow is held reliably in `running` by a
// task that signals it has started and then blocks until the test releases it.
func TestDeleteRunningflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	started := make(chan struct{}, 1)
	release := make(chan struct{})

	graph := workflow.NewGraph("DeleteRunning")
	graph.SetEndpoint("Block", "deleterunningflow.verify:428/block")
	graph.AddTransitionChain("Block", workflow.END)
	proxy.HandleGraph("deleterunningflow.verify:428/delete-running", graph)

	proxy.HandleTask("deleterunningflow.verify:428/block", func(ctx context.Context, f *workflow.Flow) error {
		started <- struct{}{}
		<-release // hold the step (and thus the flow) in `running` until the test releases it
		return nil
	})

	assert := testarossa.For(t)

	flowKey, err := eng.Create(ctx, "deleterunningflow.verify:428/delete-running", nil, nil)
	assert.NoError(err)

	// Wait until the task is executing, so the flow is definitively `running`.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("task never started")
	}

	// Delete must refuse a running flow with 409.
	err = eng.Delete(ctx, flowKey)
	assert.Error(err)
	assert.Equal(409, errors.StatusCode(err))

	// Release the task and let the flow reach terminal.
	close(release)
	outcome, err := eng.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Now that the flow is terminal, Delete succeeds and the flow is gone.
	err = eng.Delete(ctx, flowKey)
	assert.NoError(err)

	_, err = eng.Snapshot(ctx, flowKey)
	assert.Error(err)
	assert.Equal(404, errors.StatusCode(err))
}
