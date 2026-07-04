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

// TestAwaitShutdownflow pins the shutdown sentinel for Await. A waiter blocked on a flow that is
// still running when the engine shuts down must be released with an error, not left spinning: drainRuntime
// wakes each waiter once, and before the fix it sent an empty string that Await treats as "re-snapshot", so a
// non-stopped flow's waiter re-blocked on a channel no goroutine would ever signal again and only escaped when
// its own ctx expired (or, up to a full poll interval later, when a ticker re-snapshot happened to hit the
// now-closed database). The sentinel makes Await return promptly with a shutting-down error instead.
func TestAwaitShutdownflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	graph := workflow.NewGraph("AwaitShutdown")
	graph.SetEndpoint("Gate", "awaitshutdownflow.verify:428/gate")
	graph.SetEndpoint("After", "awaitshutdownflow.verify:428/after")
	graph.AddTransitionChain("Gate", "After", workflow.END)
	proxy.HandleGraph("awaitshutdownflow.verify:428/await-shutdown", graph)

	// Gate arms a long sleep, deferring its successor's not_before ~1h out. The flow therefore rests
	// `running` indefinitely (never reaching a stopped status) without occupying a worker, so Shutdown drains
	// cleanly and the only thing that can release a blocked Await is the shutdown sentinel.
	proxy.HandleTask("awaitshutdownflow.verify:428/gate", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(time.Hour)
		return nil
	})
	proxy.HandleTask("awaitshutdownflow.verify:428/after", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	assert := testarossa.For(t)

	flowKey, err := eng.Create(ctx, "awaitshutdownflow.verify:428/await-shutdown", nil, nil)
	if !assert.NoError(err) {
		return
	}

	// Block an Await on the still-running flow. The ctx is deliberately generous (well past the 5s poll
	// interval) so that a prompt return proves the sentinel released the waiter, not the ctx deadline and not
	// a ticker re-snapshot stumbling onto the closed database.
	type awaitResult struct {
		out *workflow.FlowOutcome
		err error
	}
	done := make(chan awaitResult, 1)
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 30*time.Second)
	defer cancelAwait()
	go func() {
		out, aerr := eng.Await(awaitCtx, flowKey)
		done <- awaitResult{out, aerr}
	}()

	// Give the Await goroutine time to register its waiter channel and settle into the snapshot/select loop
	// before Shutdown fans the sentinel out - a waiter registered after the wake loop would never see it.
	time.Sleep(300 * time.Millisecond)

	shutStart := time.Now()
	if !assert.NoError(eng.Shutdown(ctx)) {
		return
	}

	select {
	case res := <-done:
		// The waiter must be released with an error (not a nil-error stopped outcome), and specifically the
		// shutdown-sentinel error - the reliable discriminator. Without the fix the ignored empty wake makes
		// Await re-snapshot and either re-block until the 5s ticker or stumble onto the closing database,
		// surfacing a ctx-timeout or DB-closed error rather than this message.
		assert.Nil(res.out)
		if assert.Error(res.err) {
			assert.Contains(res.err.Error(), "shutting down")
		}
		assert.True(time.Since(shutStart) < 2*time.Second, "Await did not return promptly after Shutdown: %v", time.Since(shutStart))
	case <-time.After(10 * time.Second):
		t.Fatal("Await never returned after Shutdown")
	}
}
