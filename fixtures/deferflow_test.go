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
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestDeferflow pins flow.Defer end to end: a deferred step comes back as it stands - its resume data, its
// subgraph's child and result, its writes and its attempt kept - and Deferrals counts the deferrals since the step
// last started over, which a Retry resets and Fork starts again.
func TestDeferflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// stepOf returns the history record of the flow's step that ran task.
	stepOf := func(t *testing.T, flowKey, task string) workflow.FlowStep {
		t.Helper()
		steps, err := eng.History(ctx, flowKey)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range steps {
			if s.TaskName == task {
				return s
			}
		}
		t.Fatalf("no %s step in the history of %s", task, flowKey)
		return workflow.FlowStep{}
	}

	t.Run("keeps_the_resume_data", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("Approve")
		g.SetURL("Ask", "deferflow.verify:428/ask")
		g.AddTransition("Ask", workflow.End)
		g.SetReducer("runs", workflow.ReducerAdd)
		mux.HandleGraph("deferflow.verify:428/approve", g)
		var asked atomic.Int32
		mux.HandleTask("deferflow.verify:428/ask", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("runs", 1)
			var answer struct {
				Approved bool `json:"approved"`
			}
			yield, err := f.Interrupt(map[string]any{"ask": "approve?"}, &answer)
			if err != nil {
				return err
			}
			if yield {
				asked.Add(1)
				return nil
			}
			// The dependency the answer is for is busy twice; a Retry here would ask again.
			if f.Deferrals() < 2 {
				if yield, err := f.Defer(10*time.Millisecond, 1, 0, time.Minute); yield || err != nil {
					return err
				}
			}
			f.SetBool("approved", answer.Approved)
			return nil
		})

		flowKey, err := eng.Create(ctx, "deferflow.verify:428/approve", nil, nil)
		assert.Must(assert.NoError(err))
		outcome, err := eng.Await(ctx, flowKey)
		assert.Must(assert.NoError(err))
		assert.Must(assert.Equal(workflow.StatusInterrupted, outcome.Status))
		assert.Must(assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"approved": true})))
		outcome, err = eng.Await(ctx, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCompleted, outcome.Status, "one Resume answers the step")
		assert.Equal(int32(1), asked.Load(), "the deferred runs did not ask again")
		assert.True(outcome.State.GetBool("approved"))
		assert.Equal(4, outcome.State.GetInt("runs"), "every run's writes are kept: the ask, two deferrals, the last")
		ask := stepOf(t, flowKey, "Ask")
		assert.Equal(0, ask.Attempt)
		assert.Equal(2, ask.Deferrals)
		enginetest.AssertInvariants(t, eng)
	})

	t.Run("keeps_the_subgraph_result", func(t *testing.T) {
		assert := testarossa.For(t)
		child := workflow.NewGraph("Score")
		child.SetURL("Score", "deferflow.verify:428/score")
		child.AddTransition("Score", workflow.End)
		mux.HandleGraph("deferflow.verify:428/child", child)
		var scored atomic.Int32
		mux.HandleTask("deferflow.verify:428/score", func(ctx context.Context, f *workflow.Flow) error {
			scored.Add(1)
			f.SetInt("score", 9)
			return nil
		})
		g := workflow.NewGraph("Caller")
		g.SetURL("Call", "deferflow.verify:428/call")
		g.AddTransition("Call", workflow.End)
		mux.HandleGraph("deferflow.verify:428/caller", g)
		mux.HandleTask("deferflow.verify:428/call", func(ctx context.Context, f *workflow.Flow) error {
			var out struct {
				Score int `json:"score"`
			}
			yield, err := f.Subgraph("deferflow.verify:428/child", nil, &out)
			if yield || err != nil {
				return err
			}
			if f.Deferrals() < 2 {
				if yield, err := f.Defer(10*time.Millisecond, 1, 0, time.Minute); yield || err != nil {
					return err
				}
			}
			f.SetInt("score", out.Score)
			return nil
		})

		flowKey, outcome, err := eng.Run(ctx, "deferflow.verify:428/caller", nil, nil)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(9, outcome.State.GetInt("score"))
		assert.Equal(int32(1), scored.Load(), "the child ran once: a deferral keeps it")
		call := stepOf(t, flowKey, "Call")
		assert.Equal(2, call.Deferrals)
		if assert.NotEqual("", call.SubgraphKey) {
			childOutcome, err := eng.Snapshot(ctx, call.SubgraphKey)
			if assert.NoError(err) {
				assert.Equal(workflow.StatusCompleted, childOutcome.Status)
			}
		}
		enginetest.AssertInvariants(t, eng)
	})

	t.Run("counts_apart_from_the_attempt", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("Count")
		g.SetURL("Count", "deferflow.verify:428/count")
		g.AddTransition("Count", workflow.End)
		mux.HandleGraph("deferflow.verify:428/count-graph", g)
		type seen struct{ attempt, deferrals int }
		var mu sync.Mutex
		var runs []seen
		mux.HandleTask("deferflow.verify:428/count", func(ctx context.Context, f *workflow.Flow) error {
			mu.Lock()
			runs = append(runs, seen{f.Attempt(), f.Deferrals()})
			n := len(runs)
			mu.Unlock()
			switch n {
			case 1, 2, 4:
				f.Defer(time.Millisecond, 1, 0, 0)
			case 3:
				f.Retry(time.Millisecond, 1, 0, 0)
			}
			return nil
		})

		flowKey, outcome, err := eng.Run(ctx, "deferflow.verify:428/count-graph", nil, nil)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		mu.Lock()
		assert.Equal([]seen{{0, 0}, {0, 1}, {0, 2}, {1, 0}, {1, 1}}, runs, "a Defer leaves the attempt; a Retry resets the deferrals")
		mu.Unlock()
		count := stepOf(t, flowKey, "Count")
		assert.Equal(1, count.Attempt)
		assert.Equal(1, count.Deferrals)
	})

	t.Run("gives_up_on_its_own_count", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("Wait")
		g.SetURL("Wait", "deferflow.verify:428/wait")
		g.AddTransition("Wait", workflow.End)
		mux.HandleGraph("deferflow.verify:428/wait-graph", g)
		var forkedRun atomic.Value
		mux.HandleTask("deferflow.verify:428/wait", func(ctx context.Context, f *workflow.Flow) error {
			if f.GetBool("forked") {
				forkedRun.CompareAndSwap(nil, [2]int{f.Attempt(), f.Deferrals()})
				return nil
			}
			if f.Attempt() < 3 {
				f.Retry(time.Millisecond, 1, 0, 0)
				return nil
			}
			// 40ms, then 80ms: the second would take the total to 120ms, past the bound. Counted by Attempt, the first
			// would already be 40+80+160+320ms.
			if yield, err := f.Defer(40*time.Millisecond, 2, 0, 100*time.Millisecond); yield || err != nil {
				return err
			}
			return errors.New("gave up waiting")
		})

		flowKey, outcome, err := eng.Run(ctx, "deferflow.verify:428/wait-graph", nil, nil)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "gave up waiting")
		wait := stepOf(t, flowKey, "Wait")
		assert.Equal(3, wait.Attempt)
		assert.Equal(1, wait.Deferrals, "the task's retries spent none of the bound")

		// A fork re-runs the step from the start: both counts start over.
		forkKey, err := eng.Fork(ctx, wait.StepKey, map[string]any{"forked": true})
		assert.Must(assert.NoError(err))
		outcome, err = eng.Await(ctx, forkKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([2]int{0, 0}, forkedRun.Load())
	})

	t.Run("a_resolved_park_starts_the_count_over", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("Reset")
		g.SetURL("Gate", "deferflow.verify:428/gate")
		g.AddTransition("Gate", workflow.End)
		mux.HandleGraph("deferflow.verify:428/reset", g)
		var mu sync.Mutex
		var seen []int
		mux.HandleTask("deferflow.verify:428/gate", func(ctx context.Context, f *workflow.Flow) error {
			mu.Lock()
			seen = append(seen, f.Deferrals())
			mu.Unlock()
			// Two waits before each call to Interrupt: before it asks, and again after the answer arrives.
			if f.Deferrals() < 2 {
				if yield, err := f.Defer(time.Millisecond, 1, 0, 0); yield || err != nil {
					return err
				}
			}
			yield, err := f.Interrupt(nil, nil)
			if yield || err != nil {
				return err
			}
			return nil
		})

		flowKey, err := eng.Create(ctx, "deferflow.verify:428/reset", nil, nil)
		assert.Must(assert.NoError(err))
		outcome, err := eng.Await(ctx, flowKey)
		assert.Must(assert.NoError(err))
		assert.Must(assert.Equal(workflow.StatusInterrupted, outcome.Status))
		assert.Must(assert.NoError(eng.Resume(ctx, flowKey, nil)))
		outcome, err = eng.Await(ctx, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		mu.Lock()
		assert.Equal([]int{0, 1, 2, 0, 1, 2}, seen, "the Resume set the count back to 0")
		mu.Unlock()
	})

	// Each write that resolves a subgraph park starts the count over: the revive of a child that completed, the
	// delivery of one that failed, and the delivery of a spawn that never made a child.
	for _, how := range []string{"child_completes", "child_fails", "spawn_fails"} {
		t.Run("a_returned_subgraph_starts_the_count_over/"+how, func(t *testing.T) {
			assert := testarossa.For(t)
			childURL := "deferflow.verify:428/reset-child-" + how
			if how != "spawn_fails" {
				child := workflow.NewGraph("Child")
				child.SetURL("Child", "deferflow.verify:428/reset-work-"+how)
				child.AddTransition("Child", workflow.End)
				mux.HandleGraph(childURL, child)
				mux.HandleTask("deferflow.verify:428/reset-work-"+how, func(ctx context.Context, f *workflow.Flow) error {
					if how == "child_fails" {
						return errors.New("the child failed")
					}
					return nil
				})
			}
			g := workflow.NewGraph("Caller")
			g.SetURL("Call", "deferflow.verify:428/reset-call-"+how)
			g.AddTransition("Call", workflow.End)
			mux.HandleGraph("deferflow.verify:428/reset-caller-"+how, g)
			var mu sync.Mutex
			var seen []int
			mux.HandleTask("deferflow.verify:428/reset-call-"+how, func(ctx context.Context, f *workflow.Flow) error {
				mu.Lock()
				seen = append(seen, f.Deferrals())
				mu.Unlock()
				// Two waits before each call to Subgraph: before it spawns, and again once the child's outcome is in.
				if f.Deferrals() < 2 {
					if yield, err := f.Defer(time.Millisecond, 1, 0, 0); yield || err != nil {
						return err
					}
				}
				if yield, _ := f.Subgraph(childURL, nil, nil); yield {
					return nil
				}
				return nil // the child's outcome, a failure included, is beside the point
			})

			_, outcome, err := eng.Run(ctx, "deferflow.verify:428/reset-caller-"+how, nil, nil)
			assert.Must(assert.NoError(err))
			assert.Equal(workflow.StatusCompleted, outcome.Status)
			mu.Lock()
			assert.Equal([]int{0, 1, 2, 0, 1, 2}, seen, "the subgraph's outcome set the count back to 0")
			mu.Unlock()
		})
	}

	for _, mode := range []string{"returns_the_cancellation", "ignores_the_cancellation", "routes_the_cancellation"} {
		t.Run("a_cancelled_caller_stops_deferring/"+mode, func(t *testing.T) {
			assert := testarossa.For(t)
			child := workflow.NewGraph("Quick")
			child.SetURL("Quick", "deferflow.verify:428/quick-"+mode)
			child.AddTransition("Quick", workflow.End)
			mux.HandleGraph("deferflow.verify:428/quick-child-"+mode, child)
			mux.HandleTask("deferflow.verify:428/quick-"+mode, func(ctx context.Context, f *workflow.Flow) error { return nil })
			g := workflow.NewGraph("Loop")
			g.SetURL("Loop", "deferflow.verify:428/loop-"+mode)
			g.AddTransition("Loop", workflow.End)
			if mode == "routes_the_cancellation" {
				g.SetURL("Recover", "deferflow.verify:428/recover-"+mode)
				g.AddTransitionOnError("Loop", "Recover")
				g.AddTransition("Recover", workflow.End)
				mux.HandleTask("deferflow.verify:428/recover-"+mode, func(ctx context.Context, f *workflow.Flow) error {
					var onErr errors.TracedError
					_, _ = f.Get("onErr", &onErr)
					f.SetBool("recovered", workflow.IsCancelled(&onErr))
					return nil
				})
			}
			mux.HandleGraph("deferflow.verify:428/loop-graph-"+mode, g)
			var runs atomic.Int32
			var sawCancel atomic.Bool
			mux.HandleTask("deferflow.verify:428/loop-"+mode, func(ctx context.Context, f *workflow.Flow) error {
				yield, err := f.Subgraph("deferflow.verify:428/quick-child-"+mode, nil, nil)
				if yield || err != nil {
					return err
				}
				// The child returned: wait without a bound, which only the cancellation can end.
				runs.Add(1)
				yield, err = f.Defer(10*time.Millisecond, 1, 0, 0)
				if workflow.IsCancelled(err) {
					sawCancel.Store(true)
				}
				if yield {
					return nil
				}
				if mode == "ignores_the_cancellation" {
					return nil
				}
				return err
			})

			flowKey, err := eng.Create(ctx, "deferflow.verify:428/loop-graph-"+mode, nil, nil)
			assert.Must(assert.NoError(err))
			assert.Must(assert.True(enginetest.WaitUntil(t, time.Minute, func() bool { return runs.Load() >= 2 }), "the caller deferred"))
			assert.Must(assert.NoError(eng.Cancel(ctx, flowKey, "stop")))
			atCancel := runs.Load()
			outcome, err := enginetest.BoundedAwait(t, eng, flowKey) // a caller that kept deferring would never stop
			assert.Must(assert.NoError(err))
			if mode == "routes_the_cancellation" {
				// The caller's onError handler receives the cancellation and carries the flow on.
				assert.Equal(workflow.StatusCompleted, outcome.Status)
				assert.True(outcome.State.GetBool("recovered"))
			} else {
				assert.Equal(workflow.StatusCancelled, outcome.Status)
				assert.Equal("stop", outcome.CancelReason)
			}
			assert.True(sawCancel.Load(), "Defer returned the cancellation")
			assert.True(runs.Load()-atCancel <= 2, "at most one run past the claim the Cancel caught: %d", runs.Load()-atCancel)
			enginetest.AssertInvariants(t, eng)
		})
	}

	t.Run("a_reply_that_defers_past_the_cancellation", func(t *testing.T) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		child := workflow.NewGraph("Quick")
		child.SetURL("Quick", "deferflow.verify:428/forged-quick")
		child.AddTransition("Quick", workflow.End)
		mux.HandleGraph("deferflow.verify:428/forged-child", child)
		mux.HandleTask("deferflow.verify:428/forged-quick", func(ctx context.Context, f *workflow.Flow) error { return nil })
		g := workflow.NewGraph("Loop")
		g.SetURL("Loop", "deferflow.verify:428/forged-loop")
		g.AddTransition("Loop", workflow.End)
		mux.HandleGraph("deferflow.verify:428/forged-graph", g)
		mux.HandleTask("deferflow.verify:428/forged-loop", func(ctx context.Context, f *workflow.Flow) error {
			_, err := f.Subgraph("deferflow.verify:428/forged-child", nil, nil)
			return err
		})
		var forged atomic.Int32
		e := engine.NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		e.SetHost(forgingHost{mux: mux, url: "deferflow.verify:428/forged-loop", forged: &forged})
		assert.Must(assert.NoError(e.Startup(t.Context())))

		flowKey, err := e.Create(ctx, "deferflow.verify:428/forged-graph", nil, nil)
		assert.Must(assert.NoError(err))
		assert.Must(assert.True(enginetest.WaitUntil(t, time.Minute, func() bool { return forged.Load() >= 2 }), "the reply deferred"))
		assert.Must(assert.NoError(e.Cancel(ctx, flowKey, "stop")))
		atCancel := forged.Load()
		outcome, err := enginetest.BoundedAwait(t, e, flowKey)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusCancelled, outcome.Status, "the engine takes the Defer as the cancellation")
		assert.True(forged.Load()-atCancel <= 2, "at most one run past the claim the Cancel caught: %d", forged.Load()-atCancel)
		enginetest.AssertInvariants(t, e)
	})

	t.Run("competes_with_retry", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("Both")
		g.SetURL("Both", "deferflow.verify:428/both")
		g.AddTransition("Both", workflow.End)
		mux.HandleGraph("deferflow.verify:428/both-graph", g)
		mux.HandleTask("deferflow.verify:428/both", func(ctx context.Context, f *workflow.Flow) error {
			f.Retry(time.Millisecond, 1, 0, 0)
			f.Defer(time.Millisecond, 1, 0, 0)
			return nil
		})

		_, outcome, err := eng.Run(ctx, "deferflow.verify:428/both-graph", nil, nil)
		assert.Must(assert.NoError(err))
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "competing control signals")
	})
}

// forgingHost serves every URL through mux, except that once the caller at url has its child's result it answers
// with a Defer it builds itself, as a task server that ignores its request's cancellation would.
type forgingHost struct {
	mux    *hosts.Mux
	url    string
	forged *atomic.Int32
}

func (h forgingHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	return h.mux.LoadGraph(ctx, workflowURL)
}

func (h forgingHost) ExecuteTask(ctx context.Context, taskURL string, f *workflow.Flow) error {
	if taskURL == h.url {
		request, err := json.Marshal(workflow.RequestCarrier(f))
		if err != nil {
			return err
		}
		var seen struct {
			SubgraphDone bool `json:"subgraphDone"`
		}
		if err := json.Unmarshal(request, &seen); err != nil {
			return err
		}
		if seen.SubgraphDone {
			h.forged.Add(1)
			reply := `{"v":1,"defer":true,"backoffInitialDelayMillis":10,"backoffDelayMultiplier":1}`
			return json.Unmarshal([]byte(reply), workflow.ResultCarrier(f))
		}
	}
	return h.mux.ExecuteTask(ctx, taskURL, f)
}
