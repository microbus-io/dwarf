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

func TestNestedfailfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// 3x3 nested forEach: taskA -> forEach(outers) -> taskO -> forEach(inners) -> taskI -> joinI -> joinO -> END
	graph := workflow.NewGraph("Nested", "nestedfailfanoutflow.verify:428/nested")
	graph.AddTask("TaskA", "nestedfailfanoutflow.verify:428/task-a")
	graph.AddTask("TaskO", "nestedfailfanoutflow.verify:428/task-o")
	graph.AddTask("TaskI", "nestedfailfanoutflow.verify:428/task-i")
	graph.AddTask("JoinI", "nestedfailfanoutflow.verify:428/join-i")
	graph.AddTask("JoinO", "nestedfailfanoutflow.verify:428/join-o")
	graph.SetFanIn("JoinI")
	graph.SetFanIn("JoinO")
	graph.AddTransitionForEach("TaskA", "TaskO", "outers", "outerItem")
	graph.AddTransitionForEach("TaskO", "TaskI", "inners", "innerItem")
	graph.AddTransition("TaskI", "JoinI")
	graph.AddTransition("JoinI", "JoinO")
	graph.AddTransition("JoinO", workflow.END)
	proxy.HandleGraph("nestedfailfanoutflow.verify:428/nested", graph)

	// Shared test state
	var mu sync.Mutex
	var innerStarts, innerCompleted, joinIRuns, joinORuns int
	gate := make(chan struct{})

	proxy.HandleTask("nestedfailfanoutflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("outers", []int{0, 1, 2})
		return nil
	})
	proxy.HandleTask("nestedfailfanoutflow.verify:428/task-o", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("inners", []int{0, 1, 2})
		f.SetInt("currentOuter", f.GetInt("outerItem"))
		return nil
	})
	proxy.HandleTask("nestedfailfanoutflow.verify:428/task-i", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		innerStarts++
		mu.Unlock()

		if f.GetInt("currentOuter") == 1 && f.GetInt("innerItem") == 1 {
			return errors.New("simulated failure at outer=1 inner=1")
		}

		select {
		case <-gate:
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}

		mu.Lock()
		innerCompleted++
		mu.Unlock()
		return nil
	})
	proxy.HandleTask("nestedfailfanoutflow.verify:428/join-i", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		joinIRuns++
		mu.Unlock()
		return nil
	})
	proxy.HandleTask("nestedfailfanoutflow.verify:428/join-o", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		joinORuns++
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	flowKey, err := eng.Create(ctx, "nestedfailfanoutflow.verify:428/nested", nil, nil)
	if !testarossa.For(t).NoError(err) {
		return
	}
	err = eng.Start(ctx, flowKey)
	if !testarossa.For(t).NoError(err) {
		return
	}

	// Wait until all 9 inner cells have started.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		s := innerStarts
		mu.Unlock()
		if s >= 9 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Run("flow_running_while_siblings_still_in_flight", func(t *testing.T) {
		assert := testarossa.For(t)
		mu.Lock()
		s, c := innerStarts, innerCompleted
		mu.Unlock()
		assert.Equal(9, s)
		assert.Equal(0, c)
		outcome, err := eng.Snapshot(ctx, flowKey)
		if assert.NoError(err) && assert.NotNil(outcome) {
			assert.Equal(workflow.StatusRunning, outcome.Status)
		}
	})

	close(gate)
	outcome, err := eng.Await(ctx, flowKey)

	t.Run("flow_failed_after_full_resolution", func(t *testing.T) {
		assert := testarossa.For(t)
		assert.NoError(err)
		if !assert.NotNil(outcome) {
			return
		}
		assert.Equal(workflow.StatusFailed, outcome.Status)
	})

	t.Run("eight_inners_completed_and_two_joinI_fired", func(t *testing.T) {
		assert := testarossa.For(t)
		mu.Lock()
		s, c, ji, jo := innerStarts, innerCompleted, joinIRuns, joinORuns
		mu.Unlock()
		assert.Equal(9, s)
		assert.Equal(8, c)
		assert.Equal(2, ji)
		assert.Equal(0, jo)
	})

	// RestartFrom the failed cell with overrides so it succeeds.
	steps, err := eng.History(ctx, flowKey)
	if !testarossa.For(t).NoError(err) {
		return
	}
	var failedStepKey string
	for _, s := range steps {
		if s.Status == workflow.StatusFailed {
			failedStepKey = s.StepKey
			break
		}
	}
	if !testarossa.For(t).NotEqual("", failedStepKey) {
		return
	}

	err = eng.RestartFrom(ctx, failedStepKey, map[string]any{"currentOuter": 2})
	if !testarossa.For(t).NoError(err) {
		return
	}
	restartOutcome, err := eng.Await(ctx, flowKey)

	t.Run("restart_flips_to_completed", func(t *testing.T) {
		assert := testarossa.For(t)
		assert.NoError(err)
		if !assert.NotNil(restartOutcome) {
			return
		}
		assert.Equal(workflow.StatusCompleted, restartOutcome.Status)
	})

	t.Run("only_failed_cell_re_executed", func(t *testing.T) {
		assert := testarossa.For(t)
		mu.Lock()
		s, c, ji, jo := innerStarts, innerCompleted, joinIRuns, joinORuns
		mu.Unlock()
		assert.Equal(10, s)
		assert.Equal(9, c)
		assert.Equal(3, ji)
		assert.Equal(1, jo)
	})
}
