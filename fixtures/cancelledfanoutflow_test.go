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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestCancelledfanoutflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("CancelledFanOut")
	graph.SetEndpoint("Source", "cancelledfanoutflow.verify:428/source")
	graph.SetEndpoint("A", "cancelledfanoutflow.verify:428/a")
	graph.SetEndpoint("B", "cancelledfanoutflow.verify:428/b")
	graph.SetEndpoint("C", "cancelledfanoutflow.verify:428/c")
	graph.SetEndpoint("J", "cancelledfanoutflow.verify:428/j")
	graph.SetFanIn("J")
	graph.SetReducer("executed", workflow.ReducerAdd)
	graph.AddTransition("Source", "A")
	graph.AddTransition("Source", "B")
	graph.AddTransition("Source", "C")
	graph.AddTransition("A", "J")
	graph.AddTransition("B", "J")
	graph.AddTransitionChain("C", "J", workflow.END)
	proxy.HandleGraph("cancelledfanoutflow.verify:428/cancelled-fan-out", graph)

	var executed atomic.Int32

	// A branch reports that it is in flight and then HOLDS the single worker until the test lets go. Both
	// halves replace a duration: the report is what the cancel is sequenced against (a sleep long enough to
	// cover dispatch on a loaded suite is a guess, and one that reads as "no branch ever ran" when it is
	// short), and the hold is what keeps a second branch from starting behind the first (a fixed branch
	// duration has to outlast the Cancel round trip, which is equally unbounded). With the worker held and
	// growth disabled by SetWorkers(1), exactly one branch can ever have run when the cancel lands.
	// Released with a defer rather than a t.Cleanup: deferred funcs run BEFORE any registered cleanup, and
	// Startup registers the engine's Shutdown as one. A cleanup here would run AFTER that Shutdown (they
	// unwind LIFO and Startup's is registered later), so the drain would wait forever on the very worker
	// this is holding.
	var executedOnce sync.Once
	inFlight := make(chan struct{})
	release := make(chan struct{})
	defer close(release)

	branch := func(ctx context.Context, f *workflow.Flow) error {
		executed.Add(1)
		executedOnce.Do(func() { close(inFlight) })
		select {
		case <-release:
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}
		f.SetInt("executed", 1)
		return nil
	}

	proxy.HandleTask("cancelledfanoutflow.verify:428/source", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("cancelledfanoutflow.verify:428/a", branch)
	proxy.HandleTask("cancelledfanoutflow.verify:428/b", branch)
	proxy.HandleTask("cancelledfanoutflow.verify:428/c", branch)
	proxy.HandleTask("cancelledfanoutflow.verify:428/j", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	eng.SetWorkers(1)
	assert.NoError(eng.Startup(t.Context()))

	t.Run("cancel_mid_fan_out", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "cancelledfanoutflow.verify:428/cancelled-fan-out", nil, nil)
		if !assert.NoError(err) {
			return
		}
		select {
		case <-inFlight:
		case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
			t.Fatal("no branch of the fan-out ever started, so there was nothing to cancel mid-flight")
		}
		err = eng.Cancel(ctx, flowKey, "")
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal(1, int(executed.Load()))
	})
}
