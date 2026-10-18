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
	"fmt"
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

// TestArrayRefs_AppendLoopStoresEachElementOnce pins what payload format 2 buys an agent loop: each step's state
// stores the folded history as a list naming the rows that hold its messages - each in the changes of the step
// that added it - instead of the whole history again. A flow created in format 1 keeps storing it inline, and
// writes no list into any row: a format-1 reader would refuse one.
func TestArrayRefs_AppendLoopStoresEachElementOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const steps = 20
	mux := hosts.NewMux()
	registerLoop(mux, "arrayloop", steps, true)
	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	run := func(format int) []loopRow {
		fk, err := e.Create(ctx, "arrayloop/wf", nil, nil)
		assert.NoError(err)
		out, err := e.Await(ctx, fk)
		assert.NoError(err)
		assert.Equal(workflow.StatusInterrupted, out.Status)
		db, flowID, _ := flowRows(t, e, fk)
		setFlowPayloadVer(t, db, flowID, format)
		assert.NoError(e.Resume(ctx, fk, nil))
		awaitLoop(t, e, fk, steps)
		return loopRows(t, e, fk)
	}

	listed := run(payloadVer)
	inline := run(1)

	sizeOf := func(rows []loopRow) int {
		n := 0
		for _, r := range rows {
			n += r.stateSize
		}
		return n
	}
	// The last loop step's input holds the 19 messages before it: a list of 19 entries, none of them a copy.
	last := listed[len(listed)-1]
	assert.Equal(fmt.Sprintf("L%d", steps-1), last.task)
	assert.False(last.state.Has("messages"), "the history is not copied into the step's state")
	if list := last.refs["messages"].List; assert.Len(list, steps-1) {
		for _, entry := range list[1:] {
			assert.Equal(staterefs.ColChanges, entry.Col, "each message is read from the changes of the step that added it")
		}
	}
	assert.True(sizeOf(listed)*10 < sizeOf(inline), "listed %d bytes against %d inline", sizeOf(listed), sizeOf(inline))

	// The format-1 flow stores the history inline, and no row of it holds a list.
	assert.True(inline[len(inline)-1].state.Has("messages"))
	for _, r := range inline {
		for field, ref := range r.refs {
			assert.Nil(ref.List, "step %d (%s) holds a list under %q in a format-1 flow", r.stepID, r.task, field)
		}
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
