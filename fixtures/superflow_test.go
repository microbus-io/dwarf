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
	superGraph := workflow.NewGraph("Super", "superflow.verify:428/super")
	superGraph.AddTask("TaskA", "superflow.verify:428/task-a")
	superGraph.AddTask("TaskB", "superflow.verify:428/task-b")
	superGraph.AddTask("TaskC", "superflow.verify:428/task-c")
	superGraph.AddTask("ErrorHandler", "superflow.verify:428/error-handler")
	superGraph.AddTask("TaskD", "superflow.verify:428/task-d")
	superGraph.AddTask("SuperSubCall", "superflow.verify:428/super-sub-call")
	superGraph.AddTask("TaskE", "superflow.verify:428/task-e")
	superGraph.AddTask("TaskZ", "superflow.verify:428/task-z")
	superGraph.SetFanIn("TaskD")
	superGraph.SetFanIn("TaskE")
	superGraph.AddTransition("TaskA", "TaskB")
	superGraph.AddTransitionForEach("TaskB", "TaskC", "items", "item")
	superGraph.AddTransitionOnError("TaskC", "ErrorHandler")
	superGraph.AddTransition("TaskC", "TaskD")
	superGraph.AddTransition("ErrorHandler", "TaskD")
	superGraph.AddTransitionWhen("TaskD", "SuperSubCall", "useSubgraph == true")
	superGraph.AddTransitionWhen("TaskD", "TaskE", "useSubgraph != true")
	superGraph.AddTransition("SuperSubCall", "TaskE")
	superGraph.AddTransitionGoto("TaskE", "TaskZ")
	superGraph.AddTransition("TaskE", workflow.END)
	superGraph.AddTransition("TaskZ", workflow.END)
	proxy.HandleGraph("superflow.verify:428/super", superGraph)

	// Sub graph: SubTaskA -> SubTaskB -> END
	subGraph := workflow.NewGraph("SuperSub", "superflow.verify:428/super-sub")
	subGraph.AddTask("SubTaskA", "superflow.verify:428/sub-task-a")
	subGraph.AddTask("SubTaskB", "superflow.verify:428/sub-task-b")
	subGraph.AddTransition("SubTaskA", "SubTaskB")
	subGraph.AddTransition("SubTaskB", workflow.END)
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

	for _, name := range []string{"TaskA", "TaskB", "TaskC", "TaskD", "TaskE", "TaskZ", "ErrorHandler", "SubTaskA", "SubTaskB"} {
		taskName := name
		proxy.HandleTask("superflow.verify:428/"+kebab(taskName), func(ctx context.Context, f *workflow.Flow) error {
			return step(ctx, f, taskName)
		})
	}
	proxy.HandleTask("superflow.verify:428/super-sub-call", func(ctx context.Context, f *workflow.Flow) error {
		_, yield, err := f.Subgraph("superflow.verify:428/super-sub", nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy).
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
			outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
			assert.Equal(1, visits.get("TaskA"))
			assert.Equal(1, visits.get("TaskB"))
			assert.Equal(3, visits.get("TaskC"))
			assert.Equal(1, visits.get("TaskD"))
			assert.Equal(1, visits.get("TaskE"))
			assert.Equal(0, visits.get("TaskZ"))
			assert.Equal(0, visits.get("ErrorHandler"))
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
			outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
			assert.Equal(1, visits.get("SubTaskA"))
			assert.Equal(1, visits.get("SubTaskB"))
			assert.Equal(1, visits.get("TaskE"))
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
			"behaviors": map[string]any{"TaskE": map[string]any{"Goto": "TaskZ"}},
		}
		outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(1, visits.get("TaskE"))
		assert.Equal(1, visits.get("TaskZ"))
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
				"behaviors": map[string]any{"TaskC": map[string]any{"ErrorStatus": 500.0}},
			}
			outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
			assert.True(visits.get("ErrorHandler") >= 1)
			assert.Equal(1, visits.get("TaskD"))
			assert.Equal(1, visits.get("TaskE"))
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
			"behaviors": map[string]any{"TaskC": map[string]any{"SleepMs": 50.0}},
		}
		outcome, err := eng.Run(ctx, "superflow.verify:428/super", state, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(3, visits.get("TaskC"))
		assert.Equal(1, visits.get("TaskD"))
	})
}

func itoa(n int) string {
	if n == 1 {
		return "1"
	}
	return string(rune('0' + n))
}
