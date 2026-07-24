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

// TestRuntimeoutcancelflow pins that when Run's context expires before the flow stops, Run does NOT tear the
// flow down. Run awaits on the caller's ctx; when that ctx times out, await returns a 408, and Run leaves the
// durable flow running (it is not bound to this call) and returns its flowKey with the error - so the caller
// keeps a handle. Cancelling a healthy durable flow just because the caller stopped waiting is an availability
// footgun (and the earlier "cancel on the caller's behalf" both never ran - the await ctx was already expired -
// and was the wrong intent). Teardown-on-timeout is the caller's explicit choice: it Cancels via the returned key.
func TestRuntimeoutcancelflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// A blocked task holds the flow running past Run's short deadline. Released at test teardown before the
	// engine drains (registered after Startup, so LIFO ordering runs it before the engine's shutdown cleanup).
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })

	graph := workflow.NewGraph("RunTimeoutCancel")
	graph.SetEndpoint("Block", "runtimeoutcancelflow.verify:428/block")
	graph.AddTransitionChain("Block", workflow.END)
	proxy.HandleGraph("runtimeoutcancelflow.verify:428/run-timeout-cancel", graph)

	proxy.HandleTask("runtimeoutcancelflow.verify:428/block", func(taskCtx context.Context, f *workflow.Flow) error {
		select {
		case <-block:
		case <-taskCtx.Done(): // time-budget safety net
		}
		return nil
	})

	// Run with a deadline far shorter than the (never-arriving) completion, so await times out.
	tctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	flowKey, outcome, err := eng.Run(tctx, "runtimeoutcancelflow.verify:428/run-timeout-cancel", nil, nil)
	assert.Error(err)            // 408 from the expired await
	assert.NotEqual("", flowKey) // the flow's handle is returned so the caller can recover it
	assert.Nil(outcome)

	// Run left the flow running - it never cancels on the caller's behalf.
	flows, _, listErr := eng.List(ctx, workflow.Query{WorkflowURL: "runtimeoutcancelflow.verify:428/run-timeout-cancel"})
	assert.NoError(listErr)
	if assert.Equal(1, len(flows)) {
		assert.Equal(workflow.StatusRunning, flows[0].Status)
	}

	// The returned key is a live handle: the caller tears the flow down explicitly (the supported
	// teardown-on-timeout path). Cancel commits synchronously, so the flow is cancelled right after.
	assert.NoError(eng.Cancel(ctx, flowKey, "caller gave up waiting"))
	var status string
	for range 100 {
		flows, _, listErr := eng.List(ctx, workflow.Query{WorkflowURL: "runtimeoutcancelflow.verify:428/run-timeout-cancel"})
		assert.NoError(listErr)
		if len(flows) == 1 {
			status = flows[0].Status
			if status == workflow.StatusCancelled {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(workflow.StatusCancelled, status)
}
