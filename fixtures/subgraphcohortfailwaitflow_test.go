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
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphCohortFailWaitflow is the completing-last-arriver companion of TestSubgraphErrorWaitflow: a
// subgraph child with an internal fan-out where one branch fails first and the OTHER branch's completion
// resolves the cohort (cohort_failures > 0), failing the child via the transition path rather than failStep.
// That path too must signalStop the child key, so an Await already blocked on the (read-only) child key is
// woken by the signal - not left to the awaitPollInterval lost-wake backstop.
func TestSubgraphCohortFailWaitflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("RunInner", "subgraphcohortfailwait.verify:0/run-inner")
	parent.AddTransition("RunInner", workflow.END)
	proxy.HandleGraph("subgraphcohortfailwait.verify:0/parent", parent)

	// Inner: Split fans out to {Boom, Slow}, both converging on Join. Boom fails immediately (no onError);
	// Slow blocks until released, so its completion is the last arrival that resolves the failed cohort.
	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("Split", "subgraphcohortfailwait.verify:0/split")
	inner.SetEndpoint("Boom", "subgraphcohortfailwait.verify:0/boom")
	inner.SetEndpoint("Slow", "subgraphcohortfailwait.verify:0/slow")
	inner.SetEndpoint("Join", "subgraphcohortfailwait.verify:0/join")
	inner.AddTransition("Split", "Boom")
	inner.AddTransition("Split", "Slow")
	inner.AddTransition("Boom", "Join")
	inner.AddTransition("Slow", "Join")
	inner.AddTransition("Join", workflow.END)
	inner.SetFanIn("Join")
	proxy.HandleGraph("subgraphcohortfailwait.verify:0/inner", inner)

	var mu sync.Mutex
	var childKey string
	release := make(chan struct{})
	proxy.HandleTask("subgraphcohortfailwait.verify:0/split", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		childKey = f.FlowKey() // a subgraph child sees its own (child) flow key
		mu.Unlock()
		return nil
	})
	proxy.HandleTask("subgraphcohortfailwait.verify:0/boom", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("branch boom", http.StatusInternalServerError)
	})
	proxy.HandleTask("subgraphcohortfailwait.verify:0/slow", func(ctx context.Context, f *workflow.Flow) error {
		<-release // completes last, resolving the cohort with cohort_failures>0 (the transition path)
		return nil
	})
	proxy.HandleTask("subgraphcohortfailwait.verify:0/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil // never reached: the cohort fails
	})
	proxy.HandleTask("subgraphcohortfailwait.verify:0/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("subgraphcohortfailwait.verify:0/inner", map[string]any{}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// Run the parent in the background; Run blocks until the (root) flow stops.
	type outcome struct {
		out *workflow.FlowOutcome
		err error
	}
	rootCh := make(chan outcome, 1)
	go func() {
		_, o, e := eng.Run(ctx, "subgraphcohortfailwait.verify:0/parent", map[string]any{}, nil)
		rootCh <- outcome{o, e}
	}()

	// Wait until the Boom branch has FAILED in the child's history (its failStep committed without failing
	// the flow - the cohort is not fully arrived while Slow blocks). Only then does releasing Slow make its
	// completion the last arrival, deterministically driving the transition-path cohort failure under test.
	var child string
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		child = childKey
		mu.Unlock()
		if child != "" {
			steps, err := eng.History(ctx, child)
			assert.NoError(err)
			boomFailed := false
			for _, s := range steps {
				if s.TaskName == "Boom" && s.Status == workflow.StatusFailed {
					boomFailed = true
				}
			}
			if boomFailed {
				break
			}
		}
		if !assert.True(time.Now().Before(deadline), "Boom branch never failed") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Register an Await on the child key while the child is still running (Slow is blocked).
	awaitCtx, cancelAwait := context.WithTimeout(ctx, 30*time.Second)
	defer cancelAwait()
	awaitCh := make(chan outcome, 1)
	go func() {
		o, e := eng.Await(awaitCtx, child)
		awaitCh <- outcome{o, e}
	}()

	// Give the Await goroutine time to register its waiter and block on the (still-running) child.
	time.Sleep(100 * time.Millisecond)
	close(release) // Slow completes last; the cohort resolves with failures via the transition path

	// The wake bound is deliberately well below awaitPollInterval (5s): signalStop returns in ms, so this
	// proves the *signal* woke Await, not the 5s lost-wake poll backstop that would mask a missing signalStop.
	select {
	case aw := <-awaitCh:
		assert.NoError(aw.err, "Await(childKey) must be woken by the cohort failure's signalStop")
		if assert.NotNil(aw.out) {
			assert.Equal(workflow.StatusFailed, aw.out.Status)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Await(childKey) was not woken by the child's cohort failure")
	}

	// The parent observes the child failure through its flow.Subgraph call and fails in turn.
	select {
	case ro := <-rootCh:
		assert.NoError(ro.err)
		if assert.NotNil(ro.out) {
			assert.Equal(workflow.StatusFailed, ro.out.Status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("parent flow did not stop")
	}
}
