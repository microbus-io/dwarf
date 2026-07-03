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

// TestRuntimeoutcancelflow pins that when Run's context expires before the flow stops, the cleanup cancel
// still runs. Run awaits on the caller's ctx; when that ctx times out, await returns a 408 and Run tears the
// just-started flow down. Before the fix Run passed the already-expired ctx to cancel, so every cancel DB op
// failed immediately and the flow was left running - a silent leak only the log-only orphan detector noticed.
// The fix cancels on a detached ctx (context.WithoutCancel), so the flow ends cancelled.
func TestRuntimeoutcancelflow(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	// A blocked task holds the flow running past Run's short deadline. Released at test teardown before the
	// engine drains (registered after RunInTest, so it runs before RunInTest's shutdown cleanup).
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
	assert.Error(err) // 408 from the expired await
	assert.Equal("", flowKey)
	assert.Nil(outcome)

	// The cleanup cancel must have marked the running flow cancelled. Poll briefly on a fresh ctx: the cancel
	// commits synchronously inside Run, so this resolves immediately; the loop only guards CI jitter.
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
	assert.Equal(workflow.StatusCancelled, status) // not left running
}
