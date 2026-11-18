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
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestAwaitShutdownflow pins that Shutdown releases an Await blocked on a flow that is still running, with a
// shutting-down error, and promptly. Shutdown ends every call it admitted before it drains anything; without that the
// caller escapes solely on its own ctx, which is the hang this test exists to catch.
// NOT t.Parallel: asserts an upper-bound reaction latency (Await returns < 2s after Shutdown), which CPU oversubscription
// from co-running parallel tests can inflate past the bound.
func TestAwaitShutdownflow(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	graph := workflow.NewGraph("AwaitShutdown")
	graph.SetURL("Gate", "awaitshutdownflow.verify:428/gate")
	graph.SetURL("After", "awaitshutdownflow.verify:428/after")
	graph.AddTransitionChain("Gate", "After", workflow.End)
	mux.HandleGraph("awaitshutdownflow.verify:428/await-shutdown", graph)

	// Gate arms a long sleep, deferring its successor's not_before ~1h out. The flow therefore rests
	// `running` indefinitely (never reaching a stopped status) without occupying a worker, so Shutdown drains
	// cleanly and the only thing that can release a blocked Await is the shutdown sentinel.
	mux.HandleTask("awaitshutdownflow.verify:428/gate", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(time.Hour)
		return nil
	})
	mux.HandleTask("awaitshutdownflow.verify:428/after", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	flowKey, err := eng.Create(ctx, "awaitshutdownflow.verify:428/await-shutdown", nil, nil)
	if !assert.NoError(err) {
		return
	}

	// Block an Await on the still-running flow. The ctx is deliberately generous so that a prompt return
	// proves the board's closure released the waiter, rather than the ctx deadline expiring.
	type awaitResult struct {
		out *workflow.FlowOutcome
		err error
	}
	done := make(chan awaitResult, 1)
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancelAwait()
	// Armed BEFORE the goroutine that triggers it, which is the only ordering that works here: the waiter
	// has to be registered before the statement that drives the engine to the checkpoint.
	parked := accessors.Seams(eng).Waiter(diagnostics.Join(diagnostics.CheckpointAwaitParked, flowKey))
	go func() {
		out, aerr := eng.Await(awaitCtx, flowKey)
		done <- awaitResult{out, aerr}
	}()

	// The Await must be blocked before Shutdown lands, or the test times a caller that was never released at all.
	// Rendezvous, not a sleep: a sleep long enough to make this likely still degrades into its opposite on a slow
	// machine.
	select {
	case <-parked:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "the Await never parked on the board, so there was no blocked waiter to release")
		return
	}

	if !assert.NoError(eng.Shutdown(ctx)) {
		return
	}
	// Time the wait from when Shutdown RETURNS, not from before it. Shutdown ends the Await before it drains,
	// so a healthy waiter is already back before Shutdown returns and this reads ~0. Timing across Shutdown
	// instead folds in Shutdown's own duration - draining workers, pistons and connections against the
	// database - which is unbounded on a loaded server (measured 3.09s on SQL Server, tripping a 2s bound) and
	// is not what this test pins. The non-parallel note above addresses CPU oversubscription; it does not
	// protect against server latency, which is why the start point has to move.
	shutDone := time.Now()

	select {
	case res := <-done:
		// The waiter must be released with an error (not a nil-error stopped outcome), and specifically the
		// shutting-down one - the reliable discriminator: a waiter that escaped some other way surfaces a ctx
		// timeout or a DB-closed error instead.
		assert.Nil(res.out)
		if assert.Error(res.err) {
			assert.Contains(res.err.Error(), "shutting down")
		}
		assert.True(time.Since(shutDone) < 2*time.Second, "Await did not return promptly after Shutdown: %v", time.Since(shutDone))
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await never returned after Shutdown")
		return
	}
}
