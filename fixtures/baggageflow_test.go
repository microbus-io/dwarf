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

/*
The opaque baggage set in FlowOptions.Baggage at Create is stored on the flow row and delivered, via
the dispatch context, to every GraphLoader and TaskExecutor call for the flow's lifetime - including
subgraph flows, which inherit the parent's baggage, and Continue turns, which inherit the thread's.
The engine never interprets it; the host reads it with workflow.BaggageFrom(ctx). This wires capturing
loader/executor shims to assert baggage reaches every call site identically, across a subgraph boundary
and a Continue.
*/
package fixtures

import (
	"context"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// baggageRecordingHost wraps a TestProxy and records the baggage seen on ctx at each LoadGraph and
// ExecuteTask call, then delegates to the proxy.
type baggageRecordingHost struct {
	*engine.TestProxy
	onLoad func(ctx context.Context, name string)
	onTask func(ctx context.Context, taskName string)
}

func (h *baggageRecordingHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) {
	h.onLoad(ctx, name)
	return h.TestProxy.LoadGraph(ctx, name)
}

func (h *baggageRecordingHost) ExecuteTask(ctx context.Context, taskName string, f *workflow.Flow) error {
	h.onTask(ctx, taskName)
	return h.TestProxy.ExecuteTask(ctx, taskName, f)
}

func TestBaggageflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Parent: A -> runInner -> END; Inner: X -> END.
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("TaskA", "baggageflow.verify:428/task-a")
	parent.SetEndpoint("RunInner", "baggageflow.verify:428/run-inner")
	parent.AddTransitionChain("TaskA", "RunInner", workflow.END)
	proxy.HandleGraph("baggageflow.verify:428/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("TaskX", "baggageflow.verify:428/task-x")
	inner.AddTransition("TaskX", workflow.END)
	proxy.HandleGraph("baggageflow.verify:428/inner", inner)

	proxy.HandleTask("baggageflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("baggageflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("baggageflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("baggageflow.verify:428/inner", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// Capturing shims record the baggage seen on the context at each loader/executor call.
	var mu sync.Mutex
	seenLoad := map[string]map[string]any{}
	seenTask := map[string]map[string]any{}
	bagOf := func(ctx context.Context) map[string]any {
		m, _ := workflow.BaggageFrom(ctx).(map[string]any)
		return m
	}
	host := &baggageRecordingHost{
		TestProxy: proxy,
		onLoad: func(ctx context.Context, name string) {
			mu.Lock()
			seenLoad[name] = bagOf(ctx)
			mu.Unlock()
		},
		onTask: func(ctx context.Context, taskName string) {
			mu.Lock()
			seenTask[taskName] = bagOf(ctx)
			mu.Unlock()
		},
	}

	eng := engine.NewEngine()
	eng.SetHost(host)
	eng.RunInTest(t)

	t.Run("baggage_reaches_loader_and_every_task_including_subgraph", func(t *testing.T) {
		assert := testarossa.For(t)

		// "n" is an int on the way in; it must arrive JSON-decoded (float64) everywhere, including the
		// create-time loader — proving baggage is normalized through JSON, not handed over raw.
		md := map[string]any{"actor": "alice", "scope": "s1", "n": 5}
		flowKey, err := eng.Create(ctx, "baggageflow.verify:428/parent", nil, &workflow.FlowOptions{Baggage: md})
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(eng.Start(ctx, flowKey)) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		mu.Lock()
		defer mu.Unlock()

		// Loader saw baggage for the parent graph and the subgraph.
		assert.Equal("alice", seenLoad["baggageflow.verify:428/parent"]["actor"])
		assert.Equal("alice", seenLoad["baggageflow.verify:428/inner"]["actor"])
		// The create-time loader saw the JSON-decoded form (float64), identical to dispatch.
		assert.Equal(float64(5), seenLoad["baggageflow.verify:428/parent"]["n"])

		// Every task — parent tasks and the inherited subgraph task — saw it.
		for _, task := range []string{
			"baggageflow.verify:428/task-a",
			"baggageflow.verify:428/run-inner",
			"baggageflow.verify:428/task-x",
		} {
			got := seenTask[task]
			if !assert.NotNil(got, "task %s never received baggage", task) {
				continue
			}
			assert.Equal("alice", got["actor"], "task %s", task)
			assert.Equal("s1", got["scope"], "task %s", task)
		}
	})

	t.Run("baggage_inherited_across_continue", func(t *testing.T) {
		assert := testarossa.For(t)

		// Turn 1 carries the caller's baggage.
		md := map[string]any{"actor": "bob", "scope": "s2"}
		flowKey, err := eng.Create(ctx, "baggageflow.verify:428/parent", nil, &workflow.FlowOptions{Baggage: md})
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(eng.Start(ctx, flowKey)) {
			return
		}
		if _, err = eng.Await(ctx, flowKey); !assert.NoError(err) {
			return
		}

		// Clear observations so what we assert below comes only from the continued turn.
		mu.Lock()
		seenLoad = map[string]map[string]any{}
		seenTask = map[string]map[string]any{}
		mu.Unlock()

		// Turn 2 (Continue) must inherit turn 1's baggage even though the caller passes none.
		nextKey, err := eng.Continue(ctx, flowKey, nil, nil)
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(eng.Start(ctx, nextKey)) {
			return
		}
		outcome, err := eng.Await(ctx, nextKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		mu.Lock()
		defer mu.Unlock()

		// The continued flow reuses the frozen parent graph (loader not re-invoked for it), but its
		// subgraph is still loaded — with the inherited baggage.
		assert.Equal("bob", seenLoad["baggageflow.verify:428/inner"]["actor"])

		// Every task in the continued turn saw the inherited baggage.
		for _, task := range []string{
			"baggageflow.verify:428/task-a",
			"baggageflow.verify:428/run-inner",
			"baggageflow.verify:428/task-x",
		} {
			got := seenTask[task]
			if !assert.NotNil(got, "continued task %s never received baggage", task) {
				continue
			}
			assert.Equal("bob", got["actor"], "continued task %s", task)
			assert.Equal("s2", got["scope"], "continued task %s", task)
		}
	})

	t.Run("nil_baggage_is_safe", func(t *testing.T) {
		assert := testarossa.For(t)

		mu.Lock()
		seenTask = map[string]map[string]any{}
		mu.Unlock()

		// No baggage set: the callbacks observe a nil map (BaggageFrom returns nil) — reads are safe,
		// so the flow completes without panicking.
		flowKey, err := eng.Create(ctx, "baggageflow.verify:428/parent", nil, nil)
		if !assert.NoError(err) {
			return
		}
		if !assert.NoError(eng.Start(ctx, flowKey)) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		mu.Lock()
		defer mu.Unlock()
		_, ok := seenTask["baggageflow.verify:428/task-a"]
		assert.True(ok, "task-a should have been dispatched")
		assert.Equal(0, len(seenTask["baggageflow.verify:428/task-a"]), "nil baggage should arrive as an empty/nil map")
	})
}
