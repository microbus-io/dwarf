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
	"errors"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestTerminalReducerFlow pins that a reducer folds only what fan-out branches contribute: a task's own write to a
// reducer-managed field replaces the field - on the way to the next step, as the base its branches fold onto when
// it fans out, and in the flow's final state. Folding a step's write onto its own input would make the outcome
// depend on graph shape: a do-nothing step after the write would change the final state.
func TestTerminalReducerFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := engine.NewHostMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	const base = "terminalreducerflow.verify:428/"
	graph := func(name string, build func(g *workflow.Graph)) string {
		g := workflow.NewGraph(name)
		g.SetReducer("total", workflow.ReducerAdd)
		g.SetReducer("log", workflow.ReducerAppend)
		build(g)
		assert.NoError(g.Validate())
		mux.HandleGraph(base+name, g)
		return base + name
	}
	task := func(name string, fn func(f *workflow.Flow) error) string {
		mux.HandleTask(base+name, func(ctx context.Context, f *workflow.Flow) error { return fn(f) })
		return base + name
	}
	noop := task("noop", func(f *workflow.Flow) error { return nil })
	plus5 := task("plus5", func(f *workflow.Flow) error {
		f.SetInt("total", f.GetInt("total")+5)
		return nil
	})
	run := func(t *testing.T, url string, initial map[string]any) *workflow.FlowOutcome {
		assert := testarossa.For(t)
		_, outcome, err := eng.Run(ctx, url, initial, nil)
		if !assert.NoError(err) || !assert.NotNil(outcome) {
			return &workflow.FlowOutcome{}
		}
		return outcome
	}

	t.Run("padding_does_not_change_the_outcome", func(t *testing.T) {
		assert := testarossa.For(t)
		bare := graph("Bare", func(g *workflow.Graph) {
			g.SetURL("Work", plus5)
			g.AddTransition("Work", workflow.End)
		})
		padded := graph("Padded", func(g *workflow.Graph) {
			g.SetURL("Work", plus5)
			g.SetURL("Noop", noop)
			g.AddTransitionChain("Work", "Noop", workflow.End)
		})
		for _, url := range []string{bare, padded} {
			out := run(t, url, map[string]any{"total": 10})
			assert.Equal(workflow.StatusCompleted, out.Status)
			assert.Equal(15.0, stateVal(out.State, "total"), url)
		}
	})

	t.Run("last_step_delete_removes_the_field", func(t *testing.T) {
		assert := testarossa.For(t)
		url := graph("DeleteLast", func(g *workflow.Graph) {
			g.SetURL("Clear", task("clear", func(f *workflow.Flow) error {
				f.Del("log")
				return nil
			}))
			g.AddTransition("Clear", workflow.End)
		})
		out := run(t, url, map[string]any{"log": []string{"seed"}})
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.False(out.State.Has("log"))
	})

	branch := task("branch", func(f *workflow.Flow) error {
		if f.GetString("item") == "bad" {
			return errors.New("bad item")
		}
		f.SetInt("total", 1)
		f.SetStrings("log", []string{f.GetString("item")})
		return nil
	})
	spread := func(name string, spawn string) string {
		return graph(name, func(g *workflow.Graph) {
			g.SetURL("Split", spawn)
			g.SetURL("Branch", branch)
			g.SetURL("Join", noop)
			g.SetFanIn("Join")
			g.AddTransitionForEach("Split", "Branch", "items", "item")
			g.AddTransitionChain("Branch", "Join", workflow.End)
		})
	}

	t.Run("source_clear_leaves_only_the_branches", func(t *testing.T) {
		assert := testarossa.For(t)
		url := spread("ClearSource", task("clearsource", func(f *workflow.Flow) error {
			f.Del("log")
			return nil
		}))
		out := run(t, url, map[string]any{"log": []string{"seed"}, "items": []string{"a", "b"}})
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal([]string{"a", "b"}, out.State.GetStrings("log"))
	})

	t.Run("failed_fan_out_folds_onto_the_source_value", func(t *testing.T) {
		assert := testarossa.For(t)
		url := spread("FailedSpread", task("set5", func(f *workflow.Flow) error {
			f.SetInt("total", 5)
			return nil
		}))
		out := run(t, url, map[string]any{"total": 10, "items": []string{"a", "bad"}})
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.Equal(6.0, stateVal(out.State, "total"), "the source's 5 plus the completed branch's 1")
	})
}
