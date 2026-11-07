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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// stepColumn reads one column of every step of a task, in step_id order.
func stepColumn(t *testing.T, e *Engine, taskName, column string) []string {
	t.Helper()
	assert := testarossa.For(t)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	var out []string
	for _, id := range stepIDsByTask(t, e, taskName) {
		var v []byte
		assert.NoError(db.QueryRowContext(context.Background(),
			"SELECT "+column+" FROM dwarf_steps WHERE step_id=?", id,
		).Scan(&v))
		out = append(out, string(v))
	}
	return out
}

// decoded is a stored JSON column decoded, so dialects that re-space what they store compare equal.
func decoded(t *testing.T, column string) any {
	t.Helper()
	assert := testarossa.For(t)
	var v any
	assert.NoError(json.Unmarshal([]byte(column), &v))
	return v
}

// overWire makes a task return its carrier as a remote host would: the carrier crosses the wire as JSON, edit
// changes what came across, and the result is decoded over the carrier.
func overWire(f *workflow.Flow, edit func(wire map[string]json.RawMessage)) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	edit(wire)
	data, err = json.Marshal(wire)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, f)
}

// TestChanges_ReducersComeFromTheGraph pins that a task, local or remote, combines its writes through the
// graph's reducers, which the carrier is handed at every dispatch, and that the reducers a remote host returns
// are not read back. It also pins the stored form of a step's changes.
func TestChanges_ReducersComeFromTheGraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := workflow.NewGraph("Reducers")
	g.SetURL("Seed", "changes/seed")
	g.SetURL("Work", "changes/work")
	g.SetURL("Join", "changes/join")
	g.SetURL("Ask", "changes/ask")
	g.SetFanIn("Join")
	g.SetReducer("log", workflow.ReducerAppend)
	g.SetReducer("total", workflow.ReducerAdd)
	g.AddTransitionForEach("Seed", "Work", "items", "item")
	g.AddTransitionChain("Work", "Join", "Ask", workflow.End)
	mux.HandleGraph("changes/wf", g)
	want := `{"log":"append","total":"add"}`

	var mu sync.Mutex
	var seen []string
	see := func(wire map[string]json.RawMessage) {
		mu.Lock()
		seen = append(seen, string(wire["reducers"]))
		mu.Unlock()
	}
	mux.HandleTask("changes/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Del("scratch")
		return nil
	})
	mux.HandleTask("changes/work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("log", []string{f.GetString("item")})
		f.SetInt("total", 1)
		return overWire(f, func(wire map[string]json.RawMessage) {
			see(wire)
			wire["reducers"] = json.RawMessage(`{"log":"union","item":"add"}`)
		})
	})
	mux.HandleTask("changes/join", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("changes/ask", func(ctx context.Context, f *workflow.Flow) error {
		var answer map[string]any
		yield, err := f.Interrupt(map[string]any{"q": 1}, &answer)
		if yield || err != nil {
			return err
		}
		// The re-dispatch after a Resume is handed the reducers too.
		f.Set("log", []string{"resumed"})
		return overWire(f, see)
	})

	flowKey, err := e.Create(ctx, "changes/wf", map[string]any{"items": []string{"a", "b"}, "scratch": 1}, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status, outcome.Error) {
		return
	}
	assert.NoError(e.Resume(ctx, flowKey, map[string]any{"ok": true}))
	outcome, err = e.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal([]string{"a", "b", "resumed"}, outcome.State.GetStrings("log"))
	assert.Equal(2, outcome.State.GetInt("total"))
	assert.False(outcome.State.Has("scratch"))

	mu.Lock()
	assert.Equal([]string{want, want, want}, seen)
	mu.Unlock()

	// A step's changes are stored as one object: a contribution under the field's name, an overwrite under the
	// name with the suffix.
	seedChanges := stepColumn(t, e, "Seed", "changes")
	if assert.Len(seedChanges, 1) {
		assert.Equal(map[string]any{"scratch!": nil}, decoded(t, seedChanges[0]))
	}
	for _, c := range stepColumn(t, e, "Work", "changes") {
		var stored map[string]any
		assert.NoError(json.Unmarshal([]byte(c), &stored))
		assert.Len(stored, 2)
		assert.Equal(1.0, stored["total"])
	}
}

// TestChanges_ReducedStaysWithTheInnerFanIn pins where a cohort's description lives: in the state_reduced
// column of an inner fan-in's row, which an outer fan-in reads, and nowhere else - not on the step after it,
// and not on a fan-in on the trunk, which no outer cohort reads.
func TestChanges_ReducedStaysWithTheInnerFanIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := workflow.NewGraph("Reduced")
	for _, n := range []string{"Seed", "Cell", "Chunk", "Inner", "After", "Outer"} {
		g.SetURL(n, "changesreduced/"+n)
		mux.HandleTask("changesreduced/"+n, func(ctx context.Context, f *workflow.Flow) error {
			if n == "Chunk" {
				f.Set("log", []string{f.GetString("line")})
				// Large enough to be stored by a ref if it were merely carried.
				f.SetString("big", f.GetString("page")+f.GetString("line")+blob(60_000))
			}
			if n == "Cell" {
				f.Set("lines", []string{"x", "y"})
			}
			return nil
		})
	}
	g.SetFanIn("Inner")
	g.SetFanIn("Outer")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Cell", "pages", "page")
	g.AddTransitionForEach("Cell", "Chunk", "lines", "line")
	g.AddTransitionChain("Chunk", "Inner", "After", "Outer", workflow.End)
	mux.HandleGraph("changesreduced/wf", g)

	_, outcome, err := e.Run(ctx, "changesreduced/wf", map[string]any{"pages": []string{"a", "b"}}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal([]string{"x", "y", "x", "y"}, outcome.State.GetStrings("log"))
	assert.Equal("by"+blob(60_000), outcome.State.GetString("big"))

	// Only what the inner cohort wrote: lines is the inner spawn's own write.
	reduced := stepColumn(t, e, "Inner", "state_reduced")
	if assert.Len(reduced, 2) {
		for _, r := range reduced {
			assert.Equal(decoded(t, `{"big":{"keys":[]},"log":{"from":0}}`), decoded(t, r))
		}
	}
	// The outer fold reads the inner fan-in's row as stored, so what the cohort wrote is inline, never a ref.
	for _, s := range stepColumn(t, e, "Inner", "state") {
		st, err := workflow.NewState([]byte(s))
		assert.NoError(err)
		assert.True(st.Has("big"))
	}
	for _, task := range []string{"Seed", "Cell", "Chunk", "After", "Outer"} {
		for _, r := range stepColumn(t, e, task, "state_reduced") {
			assert.Equal(map[string]any{}, decoded(t, r), "task %s", task)
		}
	}
}

// TestChanges_RemoteResultIsChecked pins what the engine accepts in a task's changes when the carrier was
// bypassed: no overwrite or delete of a reduced field inside a fan-out branch, and no field name ending with
// the overwrite suffix. The same delete on the trunk is allowed.
func TestChanges_RemoteResultIsChecked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	var changes string
	returning := func(f *workflow.Flow) error {
		return overWire(f, func(wire map[string]json.RawMessage) {
			wire["changes"] = json.RawMessage(changes)
		})
	}

	g := workflow.NewGraph("Checked")
	g.SetURL("Only", "changeschecked/only")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransition("Only", workflow.End)
	mux.HandleGraph("changeschecked/wf", g)
	mux.HandleTask("changeschecked/only", func(ctx context.Context, f *workflow.Flow) error {
		return returning(f)
	})

	// A name that is not a valid state field name: one ending with a suffix, written either way, and others.
	for _, forged := range []string{`{"x!!":1}`, `{"x!!!":null}`, `{"a.b":1}`, `{"2nd!":1}`, `{"a b+":[1]}`, `{"é":1}`} {
		changes = forged
		_, outcome, err := e.Run(ctx, "changeschecked/wf", nil, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusFailed, outcome.Status, forged)
			assert.Contains(outcome.Error, "invalid state field name", forged)
		}
	}

	// A delete of a reduced field on the trunk.
	changes = `{"log!":null}`
	_, outcome, err := e.Run(ctx, "changeschecked/wf", map[string]any{"log": []string{"a"}}, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
		assert.False(outcome.State.Has("log"))
	}

	// The same delete inside a fan-out branch.
	fan := workflow.NewGraph("CheckedBranch")
	fan.SetURL("Seed", "changeschecked/seed")
	fan.SetURL("Branch", "changeschecked/branch")
	fan.SetURL("Join", "changeschecked/join")
	fan.SetFanIn("Join")
	fan.SetReducer("log", workflow.ReducerAppend)
	fan.AddTransitionForEach("Seed", "Branch", "items", "item")
	fan.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph("changeschecked/fan", fan)
	mux.HandleTask("changeschecked/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("changeschecked/join", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("changeschecked/branch", func(ctx context.Context, f *workflow.Flow) error {
		return returning(f)
	})
	_, outcome, err = e.Run(ctx, "changeschecked/fan", map[string]any{"items": []string{"x"}, "log": []string{"a"}}, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "inside a fan-out branch")
	}

	// An addition the engine could never have recorded: a name ending with the addition suffix, an addition on a
	// reduced field, one whose value is not a list of elements, and one inside a fan-out branch.
	for _, tc := range []struct{ changes, want string }{
		{`{"x++":[1]}`, "invalid state field name"},
		{`{"log+":["b"]}`, "never records there"},
		{`{"x+":"b"}`, "not a list of elements"},
		{`{"x+":null}`, "not a list of elements"},
	} {
		changes = tc.changes
		_, outcome, err := e.Run(ctx, "changeschecked/wf", map[string]any{"log": []string{"a"}, "x": []string{"a"}}, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusFailed, outcome.Status, tc.changes)
			assert.Contains(outcome.Error, tc.want, tc.changes)
		}
	}
	changes = `{"x+":["b"]}`
	_, outcome, err = e.Run(ctx, "changeschecked/wf", map[string]any{"x": []string{"a"}}, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
		assert.Equal([]string{"a", "b"}, outcome.State.GetStrings("x"), "an addition on the trunk extends the field")
	}
	_, outcome, err = e.Run(ctx, "changeschecked/fan", map[string]any{"items": []string{"x"}, "x": []string{"a"}}, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "never records there")
	}
}

// TestChanges_InterruptedSnapshotFoldsThroughTheReducers pins that an interrupted flow's Snapshot shows the
// interrupted step's changes folded onto its state through the graph's reducers, as its successor would see
// them.
func TestChanges_InterruptedSnapshotFoldsThroughTheReducers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := workflow.NewGraph("Snapshot")
	g.SetURL("Ask", "changessnap/ask")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransition("Ask", workflow.End)
	mux.HandleGraph("changessnap/wf", g)
	mux.HandleTask("changessnap/ask", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("log", []string{"b"})
		var answer map[string]any
		_, err := f.Interrupt(map[string]any{"q": 1}, &answer)
		return err
	})

	flowKey, err := e.Create(ctx, "changessnap/wf", map[string]any{"log": []string{"a"}}, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err := e.Await(ctx, flowKey)
	if assert.NoError(err) && assert.Equal(workflow.StatusInterrupted, outcome.Status, outcome.Error) {
		assert.Equal([]string{"a", "b"}, outcome.State.GetStrings("log"))
	}
}

// TestChanges_ErrorRouteComposesThroughTheReducers pins that the onErr an error-routed step records composes
// with what the step's earlier runs wrote through the graph's reducers, like any other write of the step.
func TestChanges_ErrorRouteComposesThroughTheReducers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := workflow.NewGraph("ErrorRoute")
	g.SetURL("Work", "changeserr/work")
	g.SetURL("Handle", "changeserr/handle")
	g.SetReducer("onErr", workflow.ReducerAppend)
	g.AddTransition("Work", workflow.End)
	g.AddTransitionOnError("Work", "Handle")
	g.AddTransition("Handle", workflow.End)
	mux.HandleGraph("changeserr/wf", g)
	mux.HandleTask("changeserr/work", func(ctx context.Context, f *workflow.Flow) error {
		if f.Attempt() == 0 {
			f.Set("onErr", "first")
			f.Retry(time.Millisecond, 1, time.Millisecond, 0)
			return nil
		}
		return errors.New("second")
	})
	var errs []any
	mux.HandleTask("changeserr/handle", func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Get("onErr", &errs)
		return err
	})

	_, outcome, err := e.Run(ctx, "changeserr/wf", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	}
	if assert.Len(errs, 2) {
		assert.Equal("first", errs[0])
	}
}

// TestChanges_UnreadableColumnIsAnErrorNotNoChanges pins that a changes column that does not decode is reported,
// never read as no changes. Read as none, the task's writes vanished silently: Snapshot of an interrupted flow
// returned without them, a Resume re-ran the task without its earlier writes and completed cleanly, and a
// Terminate's final state left them out. Now Snapshot and Step return the error, the resumed dispatch fails the
// step without running the task, and Terminate still stops the flow, with an empty final state.
func TestChanges_UnreadableColumnIsAnErrorNotNoChanges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var bRuns atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("UnreadableChanges")
	g.SetURL("A", "unreadablechanges/a")
	g.SetURL("B", "unreadablechanges/b")
	g.AddTransitionChain("A", "B", workflow.End)
	mux.HandleGraph("unreadablechanges/wf", g)
	mux.HandleTask("unreadablechanges/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("unreadablechanges/b", func(ctx context.Context, f *workflow.Flow) error {
		bRuns.Add(1)
		f.SetInt("x", 42)
		_, err := f.Interrupt(nil, nil)
		return err
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}

	// interrupted runs a flow to B's interrupt, checks the write is visible, then makes B's changes unreadable.
	interrupted := func() (flowKey, stepKey string) {
		flowKey, outcome, err := e.Run(ctx, "unreadablechanges/wf", nil, nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status) {
			t.FailNow()
		}
		assert.Equal(42, outcome.State.GetInt("x"))
		history, err := e.History(ctx, flowKey)
		if !assert.NoError(err) || !assert.Len(history, 2) {
			t.FailNow()
		}
		_, _, flowID, _, err := keys.ParseFlowKey(flowKey)
		assert.NoError(err)
		_, err = db.ExecContext(ctx,
			"UPDATE dwarf_steps SET changes=? WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'",
			[]byte(`[1]`), flowID)
		assert.NoError(err)
		return flowKey, history[1].StepKey
	}

	flowKey, stepKey := interrupted()
	_, err = e.Snapshot(ctx, flowKey)
	assert.Error(err, "Snapshot must not report the interrupted state without the step's writes")
	_, err = e.Step(ctx, stepKey)
	assert.Error(err, "Step must not report the step without its writes")

	runsBefore := bRuns.Load()
	assert.NoError(e.Resume(ctx, flowKey, nil))
	outcome, err := enginetest.BoundedAwait(t, e, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status, "the resumed step cannot run without its earlier writes")
	}
	assert.Equal(runsBefore, bRuns.Load(), "the task is not run without its earlier writes")

	flowKey, _ = interrupted()
	assert.NoError(e.Terminate(ctx, flowKey, "stop"))
	outcome, err = e.Snapshot(ctx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusTerminated, outcome.Status)
		assert.Equal(0, outcome.State.Len(), "a final state that cannot be read is stored empty")
	}
}
