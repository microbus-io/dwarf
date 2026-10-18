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

package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// loopMsg is one message an agent-loop step adds. N is its index, so a task can check the history it was handed
// is whole and in order.
type loopMsg struct {
	N    int    `json:"n"`
	Text string `json:"text"`
}

// registerLoop registers an agent loop under prefix: steps L0..L(n-1), each adding one ~600-byte message to
// "messages" on the append reducer, after an optional Gate that interrupts once (so a test can change the flow's
// format before any loop step runs). Each step fails if the history it sees is not 0..k-1 in order.
func registerLoop(mux *hosts.Mux, prefix string, steps int, gate bool) {
	g := workflow.NewGraph("Loop")
	g.SetReducer("messages", workflow.ReducerAppend)
	var chain []string
	if gate {
		g.SetURL("Gate", prefix+"/gate")
		chain = append(chain, "Gate")
		mux.HandleTask(prefix+"/gate", func(ctx context.Context, f *workflow.Flow) error {
			_, err := f.Interrupt(nil, nil)
			return err
		})
	}
	for i := range steps {
		n := fmt.Sprintf("L%d", i)
		g.SetURL(n, prefix+"/step")
		chain = append(chain, n)
	}
	g.AddTransitionChain(append(chain, workflow.End)...)
	mux.HandleGraph(prefix+"/wf", g)
	mux.HandleTask(prefix+"/step", func(ctx context.Context, f *workflow.Flow) error {
		var history []loopMsg
		if _, err := f.Get("messages", &history); err != nil {
			return err
		}
		if err := checkLoopHistory(history); err != nil {
			return fmt.Errorf("%s: %w", f.StepKey(), err)
		}
		return f.Set("messages", []loopMsg{{N: len(history), Text: blob(600)}})
	})
}

// checkLoopHistory reports a history that is not messages 0..k-1, in order.
func checkLoopHistory(history []loopMsg) error {
	for i, m := range history {
		if m.N != i {
			return fmt.Errorf("message %d of %d carries index %d", i, len(history), m.N)
		}
	}
	return nil
}

// loopRows reads every step row of a flow: task name, state column and refs.
type loopRow struct {
	stepID    int
	task      string
	state     workflow.State
	refs      staterefs.Refs
	stateSize int
}

func loopRows(t *testing.T, e *Engine, flowKey string) []loopRow {
	t.Helper()
	assert := testarossa.For(t)
	shard, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	rows, err := db.QueryContext(t.Context(), "SELECT step_id, task_name, state, state_refs FROM dwarf_steps WHERE flow_id=? ORDER BY step_id", flowID)
	assert.NoError(err)
	defer rows.Close()
	var out []loopRow
	for rows.Next() {
		var r loopRow
		var stateJSON, refsJSON []byte
		assert.NoError(rows.Scan(&r.stepID, &r.task, &stateJSON, &refsJSON))
		r.state, err = workflow.NewState(stateJSON)
		assert.NoError(err)
		r.refs, err = staterefs.Parse(refsJSON)
		assert.NoError(err)
		r.stateSize = len(stateJSON)
		out = append(out, r)
	}
	assert.NoError(rows.Err())
	return out
}

// awaitLoop waits for a flow and checks it completed holding messages 0..want-1.
func awaitLoop(t *testing.T, e *Engine, flowKey string, want int) {
	t.Helper()
	assert := testarossa.For(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(ctx, flowKey)
	assert.NoError(err)
	if !assert.NotNil(out) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status, out.Error)
	var history []loopMsg
	_, err = out.State.Get("messages", &history)
	assert.NoError(err)
	assert.Len(history, want)
	assert.NoError(checkLoopHistory(history))
}

// TestArrayRefs_AppendLoopStoresEachElementOnce pins what lists buy an agent loop: each step's state stores the
// folded history as a list naming the rows that hold its messages - each in the changes of the step that added it -
// instead of the whole history again.
func TestArrayRefs_AppendLoopStoresEachElementOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const steps = 20
	mux := hosts.NewMux()
	registerLoop(mux, "arrayloop", steps, false)
	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, outcome, err := e.Run(ctx, "arrayloop/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	awaitLoop(t, e, fk, steps)
	rows := loopRows(t, e, fk)

	// The last loop step's input holds the 19 messages before it: a list of 19 entries, none of them a copy.
	last := rows[len(rows)-1]
	assert.Equal(fmt.Sprintf("L%d", steps-1), last.task)
	assert.False(last.state.Has("messages"), "the history is not copied into the step's state")
	if list := last.refs["messages"].List; assert.Len(list, steps-1) {
		for _, entry := range list[1:] {
			assert.Equal(staterefs.ColChanges, entry.Col, "each message is read from the changes of the step that added it")
		}
	}
	// Inline, the 19 messages before the last step would be ~12 KB of its state alone; every step stays small.
	for _, r := range rows {
		assert.True(r.stateSize < 1500, "step %s stores %d bytes of state", r.task, r.stateSize)
	}
}

// TestArrayRefs_ForEachOverAList pins a forEach over an array stored as a list: each branch's element is an item
// ref into the row of the step that added it - never into the list, which would be two hops - and each branch
// sees its own element.
func TestArrayRefs_ForEachOverAList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const steps = 4
	mux := hosts.NewMux()
	registerLoop(mux, "arrayeach-loop", steps, false)
	g := workflow.NewGraph("EachOverList")
	g.SetReducer("messages", workflow.ReducerAppend)
	var chain []string
	for i := range steps {
		n := fmt.Sprintf("L%d", i)
		g.SetURL(n, "arrayeach-loop/step")
		chain = append(chain, n)
	}
	g.SetURL("Fan", "arrayeach/fan")
	g.SetURL("Work", "arrayeach/work")
	g.SetURL("Join", "arrayeach/join")
	g.SetFanIn("Join")
	g.AddTransitionChain(append(chain, "Fan")...)
	g.AddTransitionForEach("Fan", "Work", "messages", "msg")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("arrayeach/wf", g)
	mux.HandleTask("arrayeach/fan", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("arrayeach/work", func(ctx context.Context, f *workflow.Flow) error {
		var msg loopMsg
		if _, err := f.Get("msg", &msg); err != nil {
			return err
		}
		if msg.N != f.GetInt("msgIndex") || len(msg.Text) != 600 {
			return fmt.Errorf("branch %d sees message %d of %d bytes", f.GetInt("msgIndex"), msg.N, len(msg.Text))
		}
		return nil
	})
	mux.HandleTask("arrayeach/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, outcome, err := e.Run(ctx, "arrayeach/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)

	var fan int
	works := 0
	for _, r := range loopRows(t, e, fk) {
		if r.task == "Fan" {
			fan = r.stepID
		}
		if r.task != "Work" {
			continue
		}
		works++
		ref, ok := r.refs["msg"]
		if assert.True(ok, "branch step %d stores its element by ref", r.stepID) {
			assert.Nil(ref.List)
			assert.NotEqual(fan, ref.Anchor, "the element points at the row holding it, not the step carrying the list")
			assert.Len(ref.Path, 1)
		}
	}
	assert.Equal(steps, works)
}

// TestArrayRefs_ForkAndContinueMidLoop pins the two operations that move a list across flows: Fork remaps every
// entry's anchor into the clone, so a fork from the middle of a loop resumes on the history it had there; and
// Continue flattens the final state, so the next turn starts from the whole history.
func TestArrayRefs_ForkAndContinueMidLoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const steps = 10
	mux := hosts.NewMux()
	registerLoop(mux, "arrayfork", steps, false)
	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, outcome, err := e.Run(ctx, "arrayfork/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)

	hist, err := e.History(ctx, fk)
	assert.NoError(err)
	var mid string
	for _, step := range hist {
		if step.TaskName == "L6" {
			mid = step.StepKey
		}
	}
	forked, err := e.Fork(ctx, mid, nil)
	assert.NoError(err)
	awaitLoop(t, e, forked, steps)
	_, forkedFlowID, _, err := keys.ParseFlowKey(forked)
	assert.NoError(err)
	for _, r := range loopRows(t, e, forked) {
		for _, entry := range r.refs["messages"].List {
			if entry.Anchor == 0 {
				continue
			}
			var flowOf int
			db, _ := e.db.Shard(1)
			assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_steps WHERE step_id=?", entry.Anchor).Scan(&flowOf))
			assert.Equal(forkedFlowID, flowOf, "a cloned list names the clone's own rows")
		}
	}

	next, err := e.Continue(ctx, fk, nil)
	assert.NoError(err)
	awaitLoop(t, e, next, 2*steps)
}

// TestArrayRefs_ReplaceLoopStoresOnlyWhatEachStepAdds pins additions: a loop whose field has no reducer, and whose
// task writes the whole history plus one message each step, stores in each step's changes only the message it
// added, and in each successor's state a list - as an append loop does. A branch step never records an addition (it
// is relative to the step's own input, and the fan-in folds a member onto the spawn's), and Step still shows the
// whole value the task wrote.
func TestArrayRefs_ReplaceLoopStoresOnlyWhatEachStepAdds(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const steps = 12
	mux := hosts.NewMux()
	g := workflow.NewGraph("ReplaceLoop")
	var chain []string
	for i := range steps {
		n := fmt.Sprintf("L%d", i)
		g.SetURL(n, "replaceloop/step")
		chain = append(chain, n)
	}
	g.SetURL("Work", "replaceloop/work")
	g.SetURL("Join", "replaceloop/join")
	g.SetFanIn("Join")
	g.AddTransitionChain(chain...)
	g.AddTransitionForEach(chain[len(chain)-1], "Work", "pages", "page")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("replaceloop/wf", g)
	rewrite := func(f *workflow.Flow) error {
		var history []loopMsg
		if _, err := f.Get("messages", &history); err != nil {
			return err
		}
		if err := checkLoopHistory(history); err != nil {
			return fmt.Errorf("%s: %w", f.StepKey(), err)
		}
		return f.Set("messages", append(history, loopMsg{N: len(history), Text: blob(600)}))
	}
	mux.HandleTask("replaceloop/step", func(ctx context.Context, f *workflow.Flow) error { return rewrite(f) })
	mux.HandleTask("replaceloop/work", func(ctx context.Context, f *workflow.Flow) error { return rewrite(f) })
	mux.HandleTask("replaceloop/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "replaceloop/wf", map[string]any{"pages": []string{"a", "b"}}, nil)
	assert.NoError(err)
	// The fan-in's value is one branch's rewrite (the last in fold order): the loop's history plus one.
	awaitLoop(t, e, fk, steps+1)
	rows := loopRows(t, e, fk)
	changesOfStep := func(stepID int) workflow.Changes {
		db, _ := e.db.Shard(1)
		var changesJSON []byte
		assert.NoError(db.QueryRowContext(ctx, "SELECT changes FROM dwarf_steps WHERE step_id=?", stepID).Scan(&changesJSON))
		return changesOf(changesJSON)
	}

	for _, r := range rows {
		c := changesOfStep(r.stepID)
		switch {
		case r.task == "L0":
			assert.True(c.Contributions().Has("messages"), "the first write extends nothing")
		case strings.HasPrefix(r.task, "L"):
			if assert.True(c.Additions().Has("messages"), "%s stores what it added", r.task) {
				n, _ := arrayLen(c.Additions().GetJSON("messages"))
				assert.Equal(1, n, r.task)
			}
		case r.task == "Work":
			assert.False(c.Additions().Has("messages"), "a branch step records no addition")
			assert.True(c.Contributions().Has("messages"))
		}
		if r.task == fmt.Sprintf("L%d", steps-1) {
			assert.False(r.state.Has("messages"), "the history is not copied into the step's state")
			assert.NotNil(r.refs["messages"].List)
		}
	}
	// Step shows the whole value the task wrote, not the addition.
	hist, err := e.History(ctx, fk)
	assert.NoError(err)
	for _, step := range hist {
		if step.TaskName == "L5" {
			full, err := e.Step(ctx, step.StepKey)
			assert.NoError(err)
			var written []loopMsg
			_, err = full.Changes.Get("messages", &written)
			assert.NoError(err)
			assert.Len(written, 6)
		}
	}
}

// TestRecordAdditions pins when a write to a field with no reducer is stored as what it gained: its new value is an
// array that begins with the whole array the field held. Elements compare by bytes, else decoded and re-encoded, so
// a database's reordering and respacing of the stored input still matches; a number spelled two ways does not, and
// that costs only the addition.
func TestRecordAdditions(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	record := func(input, value string, reducers map[string]workflow.Reducer) workflow.Changes {
		in, err := workflow.NewState(json.RawMessage(`{"f":` + input + `}`))
		assert.NoError(err)
		c := workflow.NewChanges()
		c.Contributions().SetJSON("f", json.RawMessage(value))
		recordAdditions(c, in, reducers)
		return c
	}

	c := record(`[{"a":1, "b":"x"},2]`, `[{"b":"x","a":1},2,3,{"c":4}]`, nil)
	assert.True(c.Additions().Has("f"), "a reordered, respaced prefix is still the same prefix")
	assert.False(c.Contributions().Has("f"))
	assert.Equal(`[3,{"c":4}]`, string(c.Additions().GetJSON("f")))

	for _, tc := range []struct{ name, input, value string }{
		{"different prefix", `[1,2]`, `[1,3,4]`},
		{"same length", `[1,2]`, `[1,2]`},
		{"shorter", `[1,2]`, `[1]`},
		{"empty input", `[]`, `[1]`},
		{"input not an array", `"x"`, `["x",1]`},
		{"value not an array", `[1]`, `1`},
		{"a number spelled two ways", `[1]`, `[1.0,2]`},
	} {
		c := record(tc.input, tc.value, nil)
		assert.False(c.Additions().Has("f"), tc.name)
		assert.True(c.Contributions().Has("f"), tc.name)
	}

	c = record(`[1]`, `[1,2]`, map[string]workflow.Reducer{"f": workflow.ReducerAppend})
	assert.False(c.Additions().Has("f"), "a reduced field's write is already a contribution")
}
