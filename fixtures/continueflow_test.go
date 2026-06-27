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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestContinueflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Counting")
	graph.SetEndpoint("Increment", "continueflow.verify:428/increment")
	graph.AddTransition("Increment", workflow.END)
	proxy.HandleGraph("continueflow.verify:428/counting", graph)

	proxy.HandleTask("continueflow.verify:428/increment", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("counter", f.GetInt("counter")+1)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("counter_persists_across_continue_turns", func(t *testing.T) {
		assert := testarossa.For(t)

		// Turn 1: create (auto-starts) a flow starting from counter=0.
		flowKey, err := eng.Create(ctx, "continueflow.verify:428/counting", map[string]any{"counter": 0}, nil)
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(1.0, outcome.State["counter"]) // JSON round-trip: int -> float64

		// Turn 2: continue from the thread, no additional state.
		flowKey2, err := eng.Continue(ctx, flowKey, map[string]any{})
		if !assert.NoError(err) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey2)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(2.0, outcome.State["counter"])
	})
}

// TestContinueInheritsThreadPolicy verifies a Continue turn (which no longer takes FlowOptions) inherits
// the thread's policy: scheduling priority (not reset to the engine default) and notify-on-stop.
func TestContinueInheritsThreadPolicy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	var mu sync.Mutex
	stopped := map[string]bool{}
	proxy.OnFlowStopped(func(ctx context.Context, flowKey string, outcome *workflow.FlowOutcome) {
		mu.Lock()
		stopped[flowKey] = true
		mu.Unlock()
	})

	g := workflow.NewGraph("Policy")
	g.SetEndpoint("T", "continuepolicy.verify:428/t")
	g.AddTransition("T", workflow.END)
	proxy.HandleGraph("continuepolicy.verify:428/g", g)
	proxy.HandleTask("continuepolicy.verify:428/t", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	// Turn 1 with a distinctive priority (7, not the engine default of 100) and notify-on-stop.
	turn1, err := eng.Create(ctx, "continuepolicy.verify:428/g", map[string]any{},
		&workflow.FlowOptions{Priority: 7, NotifyOnStop: true})
	assert.NoError(err)
	_, err = eng.Await(ctx, turn1)
	assert.NoError(err)

	// Turn 2 via Continue (no opts) inherits the thread's policy.
	turn2, err := eng.Continue(ctx, turn1, map[string]any{})
	assert.NoError(err)
	_, err = eng.Await(ctx, turn2)
	assert.NoError(err)

	// Priority inherited (7), not reset to the engine default.
	summaries, _, err := eng.List(ctx, workflow.Query{ThreadKey: turn1})
	assert.NoError(err)
	var found bool
	for _, s := range summaries {
		if s.FlowKey == turn2 {
			found = true
			assert.Equal(7, s.Priority)
		}
	}
	assert.True(found)

	// Notify-on-stop inherited: FlowStopped fired for turn 2.
	deadline := time.Now().Add(2 * time.Second)
	var got bool
	for time.Now().Before(deadline) {
		mu.Lock()
		got = stopped[turn2]
		mu.Unlock()
		if got {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(got)
}

// TestCreateWithThreadKeyJoinsThread verifies FlowOptions.ThreadKey places a new flow into an existing
// thread (the explicit-policy counterpart to Continue), and that a bad key is rejected.
func TestCreateWithThreadKeyJoinsThread(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	g := workflow.NewGraph("Join")
	g.SetEndpoint("T", "threadjoin.verify:428/t")
	g.AddTransition("T", workflow.END)
	proxy.HandleGraph("threadjoin.verify:428/g", g)
	proxy.HandleTask("threadjoin.verify:428/t", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)
	assert := testarossa.For(t)

	// Flow A starts its own thread.
	a, _, err := eng.Run(ctx, "threadjoin.verify:428/g", map[string]any{}, nil)
	assert.NoError(err)

	// Flow B joins A's thread via FlowOptions.ThreadKey.
	b, _, err := eng.Run(ctx, "threadjoin.verify:428/g", map[string]any{}, &workflow.FlowOptions{ThreadKey: a})
	assert.NoError(err)
	assert.NotEqual(a, b)

	// Both flows are grouped in A's thread.
	summaries, _, err := eng.List(ctx, workflow.Query{ThreadKey: a})
	assert.NoError(err)
	keys := map[string]bool{}
	for _, s := range summaries {
		keys[s.FlowKey] = true
	}
	assert.True(keys[a])
	assert.True(keys[b])
	assert.Equal(2, len(summaries))

	// A non-existent ThreadKey is rejected (404).
	_, _, err = eng.Run(ctx, "threadjoin.verify:428/g", map[string]any{},
		&workflow.FlowOptions{ThreadKey: "1-99999-0000000000000000"})
	assert.Error(err)
}
