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
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// visitCounter is a concurrency-safe per-task visit tally.
type visitCounter struct {
	mu sync.Mutex
	m  map[string]int
}

func (v *visitCounter) inc(name string) {
	v.mu.Lock()
	v.m[name]++
	v.mu.Unlock()
}

func (v *visitCounter) get(name string) int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.m[name]
}

func superflowSetup(t *testing.T, numShards int) (*engine.Engine, *engine.TestProxy, *visitCounter) {
	t.Helper()

	proxy := engine.NewTestProxy()

	// Main graph: A -> B -> forEach(items) -> C -> {onError -> ErrorHandler} -> D -> {when} -> superSubCall or E -> {goto} -> Z
	superGraph := workflow.NewGraph("superflow.verify:428/super")
	superGraph.AddTask("taskA", "superflow.verify:428/task-a")
	superGraph.AddTask("taskB", "superflow.verify:428/task-b")
	superGraph.AddTask("taskC", "superflow.verify:428/task-c")
	superGraph.AddTask("errorHandler", "superflow.verify:428/error-handler")
	superGraph.AddTask("taskD", "superflow.verify:428/task-d")
	superGraph.AddTask("superSubCall", "superflow.verify:428/super-sub-call")
	superGraph.AddTask("taskE", "superflow.verify:428/task-e")
	superGraph.AddTask("taskZ", "superflow.verify:428/task-z")
	superGraph.SetFanIn("taskD")
	superGraph.SetFanIn("taskE")
	superGraph.AddTransition("taskA", "taskB")
	superGraph.AddTransitionForEach("taskB", "taskC", "items", "item")
	superGraph.AddTransitionOnError("taskC", "errorHandler")
	superGraph.AddTransition("taskC", "taskD")
	superGraph.AddTransition("errorHandler", "taskD")
	superGraph.AddTransitionWhen("taskD", "superSubCall", "useSubgraph == true")
	superGraph.AddTransitionWhen("taskD", "taskE", "useSubgraph != true")
	superGraph.AddTransition("superSubCall", "taskE")
	superGraph.AddTransitionGoto("taskE", "taskZ")
	superGraph.AddTransition("taskE", workflow.END)
	superGraph.AddTransition("taskZ", workflow.END)
	proxy.HandleGraph("superflow.verify:428/super", superGraph)

	// Sub graph: SubTaskA -> SubTaskB -> END
	subGraph := workflow.NewGraph("superflow.verify:428/super-sub")
	subGraph.AddTask("subTaskA", "superflow.verify:428/sub-task-a")
	subGraph.AddTask("subTaskB", "superflow.verify:428/sub-task-b")
	subGraph.AddTransition("subTaskA", "subTaskB")
	subGraph.AddTransition("subTaskB", workflow.END)
	proxy.HandleGraph("superflow.verify:428/super-sub", subGraph)

	// Per-task visit counters. Fan-out branches (e.g. taskC over a forEach) run concurrently across
	// workers and shards, so the counter must be safe for concurrent increment.
	visits := &visitCounter{m: map[string]int{}}

	step := func(ctx context.Context, f *workflow.Flow, taskName string) error {
		visits.inc(taskName)

		// Behavior injection from state.
		var behaviors map[string]map[string]any
		f.Get("behaviors", &behaviors)
		b, ok := behaviors[taskName]
		if !ok {
			return nil
		}
		if sleepMs, ok := b["SleepMs"].(float64); ok && sleepMs > 0 {
			time.Sleep(time.Duration(sleepMs) * time.Millisecond)
		}
		if gotoTarget, ok := b["Goto"].(string); ok && gotoTarget != "" {
			f.Goto(gotoTarget)
		}
		if errStatus, ok := b["ErrorStatus"].(float64); ok && errStatus != 0 {
			return errors.New("injected error from "+taskName, int(errStatus))
		}
		return nil
	}

	for _, name := range []string{"taskA", "taskB", "taskC", "taskD", "taskE", "taskZ", "errorHandler", "subTaskA", "subTaskB"} {
		taskName := name
		proxy.HandleTask("superflow.verify:428/"+kebab(taskName), func(ctx context.Context, f *workflow.Flow, baggage any) error {
			return step(ctx, f, taskName)
		})
	}
	proxy.HandleTask("superflow.verify:428/super-sub-call", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		_, yield, err := f.Subgraph("superflow.verify:428/super-sub", nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithNumShards(numShards)
	eng.RunInTest(t)

	return eng, proxy, visits
}

func kebab(camel string) string {
	var out []byte
	for i, c := range camel {
		if c >= 'A' && c <= 'Z' {
			if i > 0 {
				out = append(out, '-')
			}
			out = append(out, byte(c)+32)
		} else {
			out = append(out, byte(c))
		}
	}
	return string(out)
}

func TestSuperflow_Sequential(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, shards := range []int{1, 4} {
		t.Run("happy_path_"+itoa(shards)+"shard", func(t *testing.T) {
			assert := testarossa.For(t)
			eng, _, visits := superflowSetup(t, shards)

			state := map[string]any{"items": []string{"x", "y", "z"}, "behaviors": map[string]any{}}
			outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil, nil)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
			assert.Equal(1, visits.get("taskA"))
			assert.Equal(1, visits.get("taskB"))
			assert.Equal(3, visits.get("taskC"))
			assert.Equal(1, visits.get("taskD"))
			assert.Equal(1, visits.get("taskE"))
			assert.Equal(0, visits.get("taskZ"))
			assert.Equal(0, visits.get("errorHandler"))
		})
	}
}

func TestSuperflow_Subgraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, shards := range []int{1, 4} {
		t.Run("subgraph_branch_"+itoa(shards)+"shard", func(t *testing.T) {
			assert := testarossa.For(t)
			eng, _, visits := superflowSetup(t, shards)

			state := map[string]any{"items": []string{"x"}, "useSubgraph": true, "behaviors": map[string]any{}}
			outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil, nil)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
			assert.Equal(1, visits.get("subTaskA"))
			assert.Equal(1, visits.get("subTaskB"))
			assert.Equal(1, visits.get("taskE"))
		})
	}
}

func TestSuperflow_Goto(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	eng, _, visits := superflowSetup(t, 1)

	t.Run("goto_to_taskZ", func(t *testing.T) {
		assert := testarossa.For(t)

		state := map[string]any{
			"items":     []string{"x"},
			"behaviors": map[string]any{"taskE": map[string]any{"Goto": "taskZ"}},
		}
		outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(1, visits.get("taskE"))
		assert.Equal(1, visits.get("taskZ"))
	})
}

func TestSuperflow_OnError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for _, shards := range []int{1, 4} {
		t.Run("forEach_branch_errors_"+itoa(shards)+"shard", func(t *testing.T) {
			assert := testarossa.For(t)
			eng, _, visits := superflowSetup(t, shards)

			state := map[string]any{
				"items":     []string{"x", "y"},
				"behaviors": map[string]any{"taskC": map[string]any{"ErrorStatus": 500.0}},
			}
			outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil, nil)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
			assert.True(visits.get("errorHandler") >= 1)
			assert.Equal(1, visits.get("taskD"))
			assert.Equal(1, visits.get("taskE"))
		})
	}
}

func TestSuperflow_Sleep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	eng, _, visits := superflowSetup(t, 1)

	t.Run("sleep_in_forEach_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		state := map[string]any{
			"items":     []string{"x", "y", "z"},
			"behaviors": map[string]any{"taskC": map[string]any{"SleepMs": 50.0}},
		}
		outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3, visits.get("taskC"))
		assert.Equal(1, visits.get("taskD"))
	})
}

func itoa(n int) string {
	if n == 1 {
		return "1"
	}
	return string(rune('0' + n))
}
