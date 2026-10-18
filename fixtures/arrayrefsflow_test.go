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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// arrayMsg is one message a loop step adds; N is its index in the history.
type arrayMsg struct {
	N    int    `json:"n"`
	Text string `json:"text"`
}

// readHistory reads "messages" and fails unless it is messages 0..k-1, in order, each whole.
func readHistory(f *workflow.Flow) ([]arrayMsg, error) {
	var history []arrayMsg
	if _, err := f.Get("messages", &history); err != nil {
		return nil, err
	}
	for i, m := range history {
		if m.N != i || len(m.Text) != 600 {
			return nil, errors.New("%s: message %d of %d is #%d with %d bytes", f.StepKey(), i, len(history), m.N, len(m.Text))
		}
	}
	return history, nil
}

// addMessage contributes the next message to "messages".
func addMessage(f *workflow.Flow, history []arrayMsg) error {
	return f.Set("messages", []arrayMsg{{N: len(history), Text: strings.Repeat("m", 600)}})
}

// TestArrayrefsflow is the end-to-end contract of storing a folded field as a list of the rows holding its
// elements (payload format 2): every task still sees the whole history, in order, through every path that
// re-reads a step's input or its earlier runs' changes - a retry, a resumed interrupt, a subgraph's return - and
// at every flow boundary. The engine is unseeded, so every list is read back from the database.
func TestArrayrefsflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("agent_loop_through_retry_interrupt_and_subgraph", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		eng := unseededEngine(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(mux)
		assert.NoError(eng.Startup(t.Context()))

		const prefix = "arrayrefsflow.loop"
		child := workflow.NewGraph("Child")
		child.SetURL("Reply", prefix+"/reply")
		child.AddTransition("Reply", workflow.End)
		mux.HandleGraph(prefix+"/child", child)
		mux.HandleTask(prefix+"/reply", func(ctx context.Context, f *workflow.Flow) error {
			f.SetString("reply", "from child of "+f.GetString("asked"))
			return nil
		})

		g := workflow.NewGraph("Loop")
		g.SetReducer("messages", workflow.ReducerAppend)
		names := []string{"L0", "Retry", "Wait", "Call", "L4", "L5", "L6", "L7"}
		for _, n := range names {
			g.SetURL(n, prefix+"/"+strings.ToLower(n))
		}
		g.AddTransitionChain(append(names, workflow.End)...)
		mux.HandleGraph(prefix+"/wf", g)
		plain := func(ctx context.Context, f *workflow.Flow) error {
			history, err := readHistory(f)
			if err != nil {
				return err
			}
			return addMessage(f, history)
		}
		for _, n := range []string{"l0", "l4", "l5", "l6", "l7"} {
			mux.HandleTask(prefix+"/"+n, plain)
		}
		// A retry runs the task again on its input, its earlier run's changes folded in.
		mux.HandleTask(prefix+"/retry", func(ctx context.Context, f *workflow.Flow) error {
			history, err := readHistory(f)
			if err != nil {
				return err
			}
			if f.Attempt() == 0 && f.Retry(time.Millisecond, 1, time.Millisecond, 0) {
				return nil
			}
			return addMessage(f, history)
		})
		mux.HandleTask(prefix+"/wait", func(ctx context.Context, f *workflow.Flow) error {
			history, err := readHistory(f)
			if err != nil {
				return err
			}
			if yield, err := f.Interrupt(map[string]any{"waiting": len(history)}, nil); err != nil || yield {
				return err
			}
			return addMessage(f, history)
		})
		mux.HandleTask(prefix+"/call", func(ctx context.Context, f *workflow.Flow) error {
			history, err := readHistory(f)
			if err != nil {
				return err
			}
			var out map[string]any
			if yield, err := f.Subgraph(prefix+"/child", map[string]any{"asked": fmt.Sprint(len(history))}, &out); err != nil || yield {
				return err
			}
			f.SetString("reply", fmt.Sprint(out["reply"]))
			return addMessage(f, history)
		})

		fk, err := eng.Create(ctx, prefix+"/wf", nil, nil)
		assert.NoError(err)
		out, err := eng.Await(ctx, fk)
		assert.NoError(err)
		assert.Equal(workflow.StatusInterrupted, out.Status, out.Error)
		assert.NoError(eng.Resume(ctx, fk, nil))
		out, err = eng.Await(ctx, fk)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
		var history []arrayMsg
		_, err = out.State.Get("messages", &history)
		assert.NoError(err)
		assert.Len(history, len(names))
		assert.Equal("from child of 3", out.State.GetString("reply"))

		// The next turn starts from the whole history.
		next, err := eng.Continue(ctx, fk, nil)
		assert.NoError(err)
		out, err = eng.Await(ctx, next)
		assert.NoError(err)
		assert.Equal(workflow.StatusInterrupted, out.Status, out.Error)
		assert.NoError(eng.Resume(ctx, next, nil))
		out, err = eng.Await(ctx, next)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
		_, err = out.State.Get("messages", &history)
		assert.NoError(err)
		assert.Len(history, 2*len(names))
	})

	t.Run("failed_fan_out_keeps_the_history", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		eng := unseededEngine(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(mux)
		assert.NoError(eng.Startup(t.Context()))

		const prefix = "arrayrefsflow.failed"
		g := workflow.NewGraph("FailedFanOut")
		g.SetReducer("messages", workflow.ReducerAppend)
		g.SetReducer("notes", workflow.ReducerAppend)
		for _, n := range []string{"L0", "L1", "L2"} {
			g.SetURL(n, prefix+"/step")
		}
		g.SetURL("Work", prefix+"/work")
		g.SetURL("Join", prefix+"/join")
		g.SetFanIn("Join")
		g.AddTransitionChain("L0", "L1", "L2")
		g.AddTransitionForEach("L2", "Work", "pages", "page")
		g.AddTransitionChain("Work", "Join", workflow.End)
		assert.NoError(g.Validate())
		mux.HandleGraph(prefix+"/wf", g)
		mux.HandleTask(prefix+"/step", func(ctx context.Context, f *workflow.Flow) error {
			history, err := readHistory(f)
			if err != nil {
				return err
			}
			return addMessage(f, history)
		})
		mux.HandleTask(prefix+"/work", func(ctx context.Context, f *workflow.Flow) error {
			if _, err := readHistory(f); err != nil {
				return err
			}
			if f.GetString("page") == "bad" {
				return errors.New("bad page")
			}
			return f.Set("notes", []string{f.GetString("page") + strings.Repeat("n", 600)})
		})
		mux.HandleTask(prefix+"/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

		_, out, err := eng.Run(ctx, prefix+"/wf", map[string]any{"pages": []string{"a", "bad", "c"}}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, out.Status)
		var history []arrayMsg
		_, err = out.State.Get("messages", &history)
		assert.NoError(err)
		assert.Len(history, 3, "the history before the fan-out survives into the failed flow's final state")
		assert.Len(out.State.GetStrings("notes"), 2, "the completed branches' contributions too")
	})

	t.Run("nested_cohorts_fold_every_entry", func(t *testing.T) {
		t.Parallel()
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		eng := unseededEngine(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(mux)
		assert.NoError(eng.Startup(t.Context()))

		const prefix = "arrayrefsflow.nested"
		g := workflow.NewGraph("Nested")
		g.SetReducer("log", workflow.ReducerAppend)
		for _, n := range []string{"Seed", "Group", "Item", "JoinItems", "JoinGroups", "Tail"} {
			g.SetURL(n, prefix+"/"+strings.ToLower(n))
		}
		g.SetFanIn("JoinItems")
		g.SetFanIn("JoinGroups")
		g.AddTransitionForEach("Seed", "Group", "groups", "group")
		g.AddTransitionForEach("Group", "Item", "items", "item")
		g.AddTransitionChain("Item", "JoinItems", "JoinGroups", "Tail", workflow.End)
		assert.NoError(g.Validate())
		mux.HandleGraph(prefix+"/wf", g)
		entry := func(tag string) []string { return []string{tag + strings.Repeat("e", 600)} }
		mux.HandleTask(prefix+"/seed", func(ctx context.Context, f *workflow.Flow) error { return f.Set("log", entry("seed")) })
		mux.HandleTask(prefix+"/group", func(ctx context.Context, f *workflow.Flow) error {
			return f.Set("log", entry("group"+f.GetString("group")))
		})
		mux.HandleTask(prefix+"/item", func(ctx context.Context, f *workflow.Flow) error {
			return f.Set("log", entry("item"+f.GetString("group")+f.GetString("item")))
		})
		mux.HandleTask(prefix+"/joinitems", func(ctx context.Context, f *workflow.Flow) error { return nil })
		mux.HandleTask(prefix+"/joingroups", func(ctx context.Context, f *workflow.Flow) error { return nil })
		mux.HandleTask(prefix+"/tail", func(ctx context.Context, f *workflow.Flow) error {
			return f.Set("log", entry("tail"))
		})

		_, out, err := eng.Run(ctx, prefix+"/wf", map[string]any{"groups": []string{"A", "B"}, "items": []string{"1", "2", "3"}}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
		log := out.State.GetStrings("log")
		// seed, 2 groups, 6 items, tail - in fold order: each group's own entry before its items.
		assert.Len(log, 1+2+6+1)
		var tags []string
		for _, e := range log {
			tags = append(tags, strings.TrimRight(e, "e"))
		}
		assert.Equal([]string{"seed", "groupA", "itemA1", "itemA2", "itemA3", "groupB", "itemB1", "itemB2", "itemB3", "tail"}, tags)
	})
}
