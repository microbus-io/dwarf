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

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestReducehandoffflow pins what a write to a reduced field means outside a fan-in: a delta, folded
// through the field's reducer wherever a step hands its state on, so a field reads as its accumulated value
// everywhere. Each subtest uses `messages` on the append reducer.
func TestReducehandoffflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const url = "reducehandoffflow.verify:428/"

	mux := engine.NewHostMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	// handle registers a graph chaining the named tasks to END, every task bound to its own handler. The
	// first task in the chain is the entry point.
	handle := func(name string, tasks map[string]func(f *workflow.Flow) error, chain ...string) {
		g := workflow.NewGraph(name)
		for _, task := range chain {
			h := tasks[task]
			g.SetURL(task, url+name+"/"+task)
			mux.HandleTask(url+name+"/"+task, func(ctx context.Context, f *workflow.Flow) error { return h(f) })
		}
		g.SetReducer("messages", workflow.ReducerAppend)
		g.AddTransitionChain(append(chain, workflow.End)...)
		mux.HandleGraph(url+name, g)
	}
	appendMessage := func(f *workflow.Flow, m string) { f.Set("messages", []string{m}) }
	run := func(name string, initial map[string]any) (string, *workflow.FlowOutcome) {
		key, outcome, err := eng.Run(ctx, url+name, initial, nil)
		if !assert.NoError(err) {
			return "", nil
		}
		return key, outcome
	}

	t.Run("linear_steps_accumulate", func(t *testing.T) {
		assert := testarossa.For(t)
		// An agent loop: each step adds one message and the next step reads the whole conversation. Noop
		// writes nothing and changes nothing.
		handle("Linear", map[string]func(f *workflow.Flow) error{
			"Ask": func(f *workflow.Flow) error { appendMessage(f, "ask"); return nil },
			"Think": func(f *workflow.Flow) error {
				f.Set("thinkSaw", f.GetStrings("messages"))
				appendMessage(f, "think")
				return nil
			},
			"Noop":   func(f *workflow.Flow) error { return nil },
			"Answer": func(f *workflow.Flow) error { appendMessage(f, "answer"); return nil },
		}, "Ask", "Think", "Noop", "Answer")

		_, outcome := run("Linear", map[string]any{"messages": []string{"user"}})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"user", "ask"}, stateVal(outcome.State, "thinkSaw"))
		assert.Equal([]any{"user", "ask", "think", "answer"}, stateVal(outcome.State, "messages"))
	})

	t.Run("when_routes_on_the_folded_value", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("When")
		for _, task := range []string{"Add", "Hit", "Miss"} {
			g.SetURL(task, url+"When/"+task)
		}
		g.SetReducer("total", workflow.ReducerAdd)
		g.AddTransitionSwitch("Add", "Hit", "total == 15")
		g.AddTransitionSwitch("Add", "Miss", "true")
		g.AddTransition("Hit", workflow.End)
		g.AddTransition("Miss", workflow.End)
		mux.HandleGraph(url+"When", g)
		mux.HandleTask(url+"When/Add", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("total", 5)
			return nil
		})
		mux.HandleTask(url+"When/Hit", func(ctx context.Context, f *workflow.Flow) error {
			f.SetString("routed", "hit")
			return nil
		})
		mux.HandleTask(url+"When/Miss", func(ctx context.Context, f *workflow.Flow) error {
			f.SetString("routed", "miss")
			return nil
		})

		_, outcome := run("When", map[string]any{"total": 10})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("hit", stateVal(outcome.State, "routed"))
		assert.Equal(15.0, stateVal(outcome.State, "total"))
	})

	t.Run("retry_runs_compose", func(t *testing.T) {
		assert := testarossa.For(t)
		// A step is one unit of work however many times its task runs, so each run's write adds to the last.
		handle("Retry", map[string]func(f *workflow.Flow) error{
			"Attempt": func(f *workflow.Flow) error {
				appendMessage(f, "attempt")
				if f.Attempt() < 2 {
					f.Retry(0, 0, 0, 0)
				}
				return nil
			},
		}, "Attempt")

		_, outcome := run("Retry", map[string]any{"messages": []string{"user"}})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"user", "attempt", "attempt", "attempt"}, stateVal(outcome.State, "messages"))
	})

	t.Run("interrupt_rerun_composes", func(t *testing.T) {
		assert := testarossa.For(t)
		// The task re-runs from the top when resumed, so a write placed before the Interrupt lands twice.
		handle("Interrupt", map[string]func(f *workflow.Flow) error{
			"Draft": func(f *workflow.Flow) error {
				appendMessage(f, "draft")
				var answer map[string]any
				yield, err := f.Interrupt(map[string]any{"approve": "draft"}, &answer)
				if yield || err != nil {
					return err
				}
				appendMessage(f, "approved")
				return nil
			},
		}, "Draft")

		key, err := eng.Create(ctx, url+"Interrupt", map[string]any{"messages": []string{"user"}}, nil)
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, key)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status) {
			return
		}
		assert.NoError(eng.Resume(ctx, key, map[string]any{"ok": true}))
		outcome, err = eng.Await(ctx, key)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"user", "draft", "draft", "approved"}, stateVal(outcome.State, "messages"))
	})

	t.Run("subgraph_reentry_composes", func(t *testing.T) {
		assert := testarossa.For(t)
		handle("Child", map[string]func(f *workflow.Flow) error{
			"Leaf": func(f *workflow.Flow) error { return nil },
		}, "Leaf")
		handle("Caller", map[string]func(f *workflow.Flow) error{
			"Call": func(f *workflow.Flow) error {
				appendMessage(f, "before")
				yield, err := f.Subgraph(url+"Child", nil, nil)
				if yield || err != nil {
					return err
				}
				appendMessage(f, "after")
				return nil
			},
		}, "Call")

		_, outcome := run("Caller", map[string]any{"messages": []string{"user"}})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"user", "before", "before", "after"}, stateVal(outcome.State, "messages"))
	})

	t.Run("del_then_sets_compose", func(t *testing.T) {
		assert := testarossa.For(t)
		// A Del starts the field fresh; every later Set in the step folds onto the fresh value.
		handle("DelSets", map[string]func(f *workflow.Flow) error{
			"Reset": func(f *workflow.Flow) error {
				f.Del("messages")
				appendMessage(f, "d1")
				appendMessage(f, "d2")
				return nil
			},
		}, "Reset")

		_, outcome := run("DelSets", map[string]any{"messages": []string{"a", "b"}})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"d1", "d2"}, stateVal(outcome.State, "messages"))
	})

	t.Run("bare_nil_is_identity", func(t *testing.T) {
		assert := testarossa.For(t)
		// A task that collected nothing into a nil slice adds nothing; only Del clears a reduced field.
		handle("Nil", map[string]func(f *workflow.Flow) error{
			"Collect": func(f *workflow.Flow) error {
				var none []string
				f.Set("messages", none)
				return nil
			},
		}, "Collect")

		_, outcome := run("Nil", map[string]any{"messages": []string{"a"}})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"a"}, stateVal(outcome.State, "messages"))
	})

	t.Run("overwrite_trims_then_set_appends", func(t *testing.T) {
		assert := testarossa.For(t)
		// The LLM history pattern: keep the last two messages, then add the new one.
		handle("Trim", map[string]func(f *workflow.Flow) error{
			"Trim": func(f *workflow.Flow) error {
				history := f.GetStrings("messages")
				if err := f.Overwrite("messages", history[len(history)-2:]); err != nil {
					return err
				}
				appendMessage(f, "new")
				return nil
			},
			"After": func(f *workflow.Flow) error { appendMessage(f, "after"); return nil },
		}, "Trim", "After")

		_, outcome := run("Trim", map[string]any{"messages": []string{"a", "b", "c", "d"}})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
		assert.Equal([]any{"c", "d", "new", "after"}, stateVal(outcome.State, "messages"))
	})

	t.Run("continue_folds_through_the_reducer", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("Turns")
		g.SetURL("Turn", url+"Turns/Turn")
		g.SetReducer("tags", workflow.ReducerUnion)
		g.AddTransition("Turn", workflow.End)
		mux.HandleGraph(url+"Turns", g)
		mux.HandleTask(url+"Turns/Turn", func(ctx context.Context, f *workflow.Flow) error {
			return f.Set("tags", []string{"turn"})
		})

		key, outcome := run("Turns", map[string]any{"tags": []string{"a"}})
		if outcome == nil {
			return
		}
		assert.Equal([]any{"a", "turn"}, stateVal(outcome.State, "tags"))
		next, err := eng.Continue(ctx, key, map[string]any{"tags": []string{"a", "b"}})
		if !assert.NoError(err) {
			return
		}
		outcome, err = eng.Await(ctx, next)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
			assert.Equal([]any{"a", "turn", "b"}, stateVal(outcome.State, "tags"))
		}
	})

	t.Run("del_then_set_replaces", func(t *testing.T) {
		assert := testarossa.For(t)
		handle("DelSet", map[string]func(f *workflow.Flow) error{
			"Reset": func(f *workflow.Flow) error {
				f.Del("messages")
				appendMessage(f, "x")
				return nil
			},
		}, "Reset")

		_, outcome := run("DelSet", map[string]any{"messages": []string{"a", "b"}})
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"x"}, stateVal(outcome.State, "messages"))
	})

	t.Run("fork_override_replaces", func(t *testing.T) {
		assert := testarossa.For(t)
		// Fork's overrides edit the forked step's input, so they replace a reduced field rather than fold.
		handle("Fork", map[string]func(f *workflow.Flow) error{
			"Start": func(f *workflow.Flow) error { return nil },
			"Read": func(f *workflow.Flow) error {
				f.Set("readSaw", f.GetStrings("messages"))
				return nil
			},
		}, "Start", "Read")

		originKey, outcome := run("Fork", map[string]any{"messages": []string{"a", "b"}})
		if outcome == nil {
			return
		}
		assert.Equal([]any{"a", "b"}, stateVal(outcome.State, "readSaw"))

		readKey := stepKeyByTask(t, eng, originKey, "Read")
		forkKey, err := eng.Fork(ctx, readKey, map[string]any{"messages": []string{"forked"}})
		if !assert.NoError(err) {
			return
		}
		outcome, err = eng.Await(ctx, forkKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal([]any{"forked"}, stateVal(outcome.State, "readSaw"))
	})
}
