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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// unseeded makes an engine resolve every ref from the database. The minting replica seeds its resolve cache, so
// a single-engine test would otherwise never exercise the read path a successor on another replica takes.
func unseeded(e *Engine) *Engine {
	e.seams.InjectN(engineseams.FaultSkipRefSeed, 1<<20)
	return e
}

// blob returns a JSON string value of roughly n encoded bytes.
func blob(n int) string {
	return strings.Repeat("x", n)
}

// stepRefsOf reads a step's persisted state column and refs map, so a test can assert what was actually
// STORED - which is the whole point of the feature and is invisible from the task's view (a task always sees
// materialized state).
func stepRefsOf(t *testing.T, e *Engine, stepID int) (state workflow.State, refs staterefs.Refs, stateBytes int) {
	t.Helper()
	assert := testarossa.For(t)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	var stateJSON, refsJSON []byte
	err = db.QueryRowContext(context.Background(),
		"SELECT state, state_refs FROM dwarf_steps WHERE step_id=?", stepID,
	).Scan(&stateJSON, &refsJSON)
	assert.NoError(err)
	state, _ = workflow.NewState(stateJSON)
	refs, err = staterefs.Parse(refsJSON)
	assert.NoError(err)
	return state, refs, len(stateJSON)
}

// stepIDsByTask maps each task name to its step ids, in step order.
func stepIDsByTask(t *testing.T, e *Engine, taskName string) []int {
	t.Helper()
	assert := testarossa.For(t)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	rows, err := db.QueryContext(context.Background(),
		"SELECT step_id FROM dwarf_steps WHERE task_name=? ORDER BY step_id", taskName)
	assert.NoError(err)
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		assert.NoError(rows.Scan(&id))
		ids = append(ids, id)
	}
	return ids
}

// TestStateRefs_ResolveReadsBothColumns pins the correction that motivated the whole redesign, against a real
// database: an anchor's bytes are NOT always in its `changes`. Two of the three places they sit are the `state`
// column - the flow's INITIAL INPUT at the entry step (no task produced it, so it appears in no changes
// anywhere), and a fan-in's reducer output. A changes-only resolver silently misses both, and the initial-input
// case is the headline one.
//
// internal/staterefs covers the same rule as a unit test over a fake loader; this copy is the layer that would
// catch a driver's column scanning behaving differently from the in-memory stand-in.
func TestStateRefs_ResolveReadsBothColumns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"ftok", "u", "W", []byte("{}"), "", workflow.StatusRunning, 1, 1, 60000,
	)
	assert.NoError(err)
	// An anchor holding one field in `state` (an initial input nobody produced) and another in `changes` (a
	// task's own output), plus a field present in BOTH - where changes, being the newer value, must win.
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, state, changes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		1, 1, "s1", "Anchor", "u", workflow.StatusCompleted, 60000,
		[]byte(`{"fromState":"S","shadowed":"old"}`), []byte(`{"fromChanges":"C","shadowed":"new"}`),
	)
	assert.NoError(err)

	state := newState()
	refs := staterefs.Refs{"fromState": {Anchor: 1}, "fromChanges": {Anchor: 1}, "shadowed": {Anchor: 1}}
	_, err = e.resolveStateRefs(ctx, db, 1, state, refs, nil, "u")
	assert.NoError(err)

	// A resolved field is DECODED, exactly like one that was never ref'd: the storage encoding must never leak
	// into a state map, or an API caller's state["pdf"].(string) would silently stop matching.
	var fromState, fromChanges, shadowed string
	_, _ = state.Get("fromState", &fromState)
	_, _ = state.Get("fromChanges", &fromChanges)
	_, _ = state.Get("shadowed", &shadowed)
	assert.Equal("S", fromState, "the entry step's state column is a legitimate anchor")
	assert.Equal("C", fromChanges)
	assert.Equal("new", shadowed, "changes shadows state - it is the newer value")

	// A dangling ref is an invariant violation and must be loud, never a silently-absent field.
	_, err = e.resolveStateRefs(ctx, db, 1, newState(), staterefs.Refs{"nope": {Anchor: 1}}, nil, "u")
	assert.Error(err)
	_, err = e.resolveStateRefs(ctx, db, 1, newState(), staterefs.Refs{"x": {Anchor: 999}}, nil, "u")
	assert.Error(err)

	// An anchor whose column does not decode says so, rather than reading as a field it does not hold.
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, state, changes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		1, 2, "s2", "Broken", "u", workflow.StatusCompleted, 60000, []byte(`[1]`), []byte(`{}`),
	)
	assert.NoError(err)
	_, err = e.resolveStateRefs(ctx, db, 1, newState(), staterefs.Refs{"x": {Anchor: 2}}, nil, "u")
	if assert.Error(err) {
		assert.Contains(err.Error(), "unreadable state")
	}
}

// TestStateRefs_UnreadableStateFailsTheStep pins that a step whose state column does not decode fails, rather
// than running its task on empty input and losing every field for good. B parks, its state is rewritten to a
// JSON value that is not an object, and the Resume's dispatch fails the flow without running the task again.
func TestStateRefs_UnreadableStateFailsTheStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var bRuns atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("Unreadable")
	g.SetURL("B", "unreadable/b")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("unreadable/g", g)
	mux.HandleTask("unreadable/b", func(ctx context.Context, f *workflow.Flow) error {
		bRuns.Add(1)
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "unreadable/g", map[string]any{"doc": "keep"}, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, flowKey, workflow.StatusInterrupted, 10*time.Second)
	bIDs := stepIDsByTask(t, e, "B")
	assert.Len(bIDs, 1)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET state=? WHERE step_id=?", []byte(`[1]`), bIDs[0])
	assert.NoError(err)

	assert.NoError(e.Resume(ctx, flowKey, nil))
	enginetest.AwaitFlowStatus(t, e, flowKey, workflow.StatusFailed, 10*time.Second)
	assert.Equal(int32(1), bRuns.Load(), "the task must not run on input it could not read")
	outcome, err := e.Snapshot(ctx, flowKey)
	if assert.NoError(err) {
		assert.Contains(outcome.Error, "JSON object", "the step failed on its unreadable state")
	}
}

// TestStateRefs_UnreadableInterruptedStateFailsSnapshot pins that Snapshot of an interrupted flow returns an
// error for a step state it cannot read. Discarded, the decode left a nil map that folding the step's changes
// onto panicked in the caller's goroutine, and with no changes it reported an empty State as the flow's.
func TestStateRefs_UnreadableInterruptedStateFailsSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("UnreadableSnapshot")
	g.SetURL("B", "unreadablesnap/b")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("unreadablesnap/g", g)
	mux.HandleTask("unreadablesnap/b", func(ctx context.Context, f *workflow.Flow) error {
		// A change before the interrupt, so Snapshot has something to fold onto the state.
		if err := f.Set("seen", true); err != nil {
			return err
		}
		_, err := f.Interrupt(nil, nil)
		return err
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "unreadablesnap/g", map[string]any{"doc": "keep"}, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, flowKey, workflow.StatusInterrupted, 10*time.Second)
	bIDs := stepIDsByTask(t, e, "B")
	assert.Len(bIDs, 1)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET state=? WHERE step_id=?", []byte(`[1]`), bIDs[0])
	assert.NoError(err)

	outcome, err := e.Snapshot(ctx, flowKey)
	if assert.Error(err) {
		assert.Contains(err.Error(), "JSON object")
	}
	assert.Nil(outcome)
}

// TestStateRefs_UnreadableSideColumnFailsTheStep pins that a resume payload or baggage this replica cannot read
// fails the step rather than reaching the task as empty: discarded, the decode handed the task an empty State and
// nothing said so.
func TestStateRefs_UnreadableSideColumnFailsTheStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("UnreadableSide")
	g.SetURL("B", "unreadableside/b")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("unreadableside/g", g)
	mux.HandleTask("unreadableside/b", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		_, err := f.Interrupt(nil, nil)
		return err
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)

	for _, column := range []string{"resume data", "baggage"} {
		runs.Store(0)
		flowKey, err := e.Create(ctx, "unreadableside/g", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, flowKey, workflow.StatusInterrupted, 10*time.Second)
		_, flowID, _, err := keys.ParseFlowKey(flowKey)
		assert.NoError(err)
		switch column {
		case "resume data":
			// What Resume does, with a payload no reader can decode: the leaf pending with its answer, the flow running.
			_, err = db.ExecContext(ctx,
				"UPDATE dwarf_steps SET resume_data=?, status='"+workflow.StatusPending+"' WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'",
				[]byte(`[1]`), flowID)
			assert.NoError(err)
			_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status='"+workflow.StatusRunning+"' WHERE flow_id=?", flowID)
			assert.NoError(err)
		case "baggage":
			_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET baggage=? WHERE flow_id=?", []byte(`[1]`), flowID)
			assert.NoError(err)
			assert.NoError(e.Resume(ctx, flowKey, nil))
		}
		enginetest.AwaitFlowStatus(t, e, flowKey, workflow.StatusFailed, 10*time.Second)
		assert.Equal(int32(1), runs.Load(), "%s: the task must not run on a payload it could not read", column)
		outcome, err := e.Snapshot(ctx, flowKey)
		if assert.NoError(err) {
			assert.Contains(outcome.Error, "unreadable "+column)
		}
	}
}

// TestStateRefs_CarryAcrossFanOut is the end-to-end shape the design exists for, asserted on what is actually
// STORED. A large document is fanned out over N branches: the payload must be written ONCE (at the anchor) and
// every branch's step row must carry a ref instead of a copy - while each branch's task still sees the whole
// document. It also pins the two rules that make the fan-out case work: the injected element is never ref'd,
// and a carried ref crosses the fan-in still pointing at its ORIGINAL anchor rather than being re-anchored.
func TestStateRefs_CarryAcrossFanOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const doc = 20000
	g := workflow.NewGraph("Refs")
	g.SetURL("Seed", "refs/seed")
	g.SetURL("Work", "refs/work")
	g.SetURL("Join", "refs/join")
	g.SetFanIn("Join")
	g.SetReducer("processed", workflow.ReducerAdd)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("refs/wf", g)

	mux.HandleTask("refs/seed", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// Every branch must SEE the whole document, even though its own row does not store it.
	mux.HandleTask("refs/work", func(ctx context.Context, f *workflow.Flow) error {
		if len(f.GetString("pdf")) != doc {
			return errors.New("branch cannot see the carried document")
		}
		f.SetInt("processed", 1)
		return nil
	})
	mux.HandleTask("refs/join", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("total", f.GetInt("processed"))
		f.SetInt("pdfLen", len(f.GetString("pdf")))
		return nil
	})

	pages := make([]string, 8)
	for i := range pages {
		pages[i] = "p"
	}
	initial := map[string]any{"pdf": blob(doc), "pages": pages}
	_, outcome, err := e.Run(ctx, "refs/wf", initial, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(8.0, stateVal(outcome.State, "total"))
	// FLATTEN at the flow boundary: final_state is materialized, so a reader never sees a ref.
	assert.Equal(float64(doc), stateVal(outcome.State, "pdfLen"))
	assert.Equal(doc, len(stateVal(outcome.State, "pdf").(string)))

	// The entry step is the anchor: it holds the initial input in its `state` column, inline.
	entry := stepIDsByTask(t, e, "Seed")[0]
	entryState, entryRefs, _ := stepRefsOf(t, e, entry)
	assert.Equal(0, len(entryRefs), "the entry step holds the initial input itself - it anchors, it does not ref")
	assert.Equal(doc, len(entryState.GetString("pdf")))

	// Every branch refs that one anchor rather than copying the document.
	branches := stepIDsByTask(t, e, "Work")
	assert.Equal(8, len(branches))
	for _, id := range branches {
		st, refs, size := stepRefsOf(t, e, id)
		assert.Equal(entry, refs["pdf"].Anchor, "a branch carries the document BY REFERENCE to the entry step")
		assert.False(st.Has("pdf"), "the payload is not copied into the branch's row")
		assert.True(st.Has("page"), "a one-byte element is cheaper inline than as an item ref")
		assert.True(size < doc/2, "a branch's stored state must be a small fraction of the payload it carries")
	}

	// The fan-in CARRIES the ref through rather than materializing and re-anchoring it: resolving a merely
	// carried field at every fan-in would hand back the win in exactly these graphs.
	join := stepIDsByTask(t, e, "Join")[0]
	joinState, joinRefs, _ := stepRefsOf(t, e, join)
	assert.Equal(entry, joinRefs["pdf"].Anchor, "a carried ref crosses the fan-in still pointing at its original anchor")
	assert.False(joinState.Has("pdf"))
	assert.True(joinState.Has("processed"), "the REDUCED field is materialized into the fan-in step's own state")
}

// TestStateRefs_ReducedFieldIsResolvedAndReanchored pins the fan-in's selective resolve from the other side.
// A COMBINING reducer needs its accumulated base, so a ref'd field it folds must be materialized first -
// folding a delta onto an absent base would silently lose everything accumulated so far. The reduced result
// then exists nowhere else, so it is inlined into the fan-in step's own state, which becomes its anchor.
func TestStateRefs_ReducedFieldIsResolvedAndReanchored(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("Reduced")
	g.SetURL("Seed", "red/seed")
	g.SetURL("Work", "red/work")
	g.SetURL("Join", "red/join")
	g.SetFanIn("Join")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("red/wf", g)

	mux.HandleTask("red/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("red/work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("log", []string{"entry"})
		return nil
	})
	mux.HandleTask("red/join", func(ctx context.Context, f *workflow.Flow) error {
		var log []string
		_, err := f.Get("log", &log)
		assert.NoError(err)
		f.SetInt("logLen", len(log))
		return nil
	})

	// A big pre-existing "log" that is ref'd out of the branches' rows, and which the append reducer must
	// nonetheless fold the branches' deltas ONTO - so the fan-in has to materialize it.
	base := make([]string, 400)
	for i := range base {
		base[i] = blob(64)
	}
	pages := []string{"a", "b", "c"}
	_, outcome, err := e.Run(ctx, "red/wf", map[string]any{"log": base, "pages": pages}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	// 400 accumulated + one entry per branch. Had the fan-in folded onto an absent base, this would be 3.
	assert.Equal(403.0, stateVal(outcome.State, "logLen"))

	// The reduced value exists in no other row, so the fan-in step INLINES it and becomes its anchor - the
	// third of the three places an anchor's bytes can live.
	join := stepIDsByTask(t, e, "Join")[0]
	joinState, joinRefs, _ := stepRefsOf(t, e, join)
	assert.NotContains(joinRefs, "log", "a reduced field cannot be a ref - the merged value exists nowhere else")
	assert.True(joinState.Has("log"), "the fan-in step inlines it, anchoring it for everything downstream")
}

// TestStateRefs_SpawnWrittenReducedFieldIsItsOwnValue pins where a fan-out source's own write to a large reduced
// field is stored at the fan-in. A Set folds into the input, and the folded value exists in no row - not the
// spawn's state (before the fold) nor its changes (the delta alone) - so the fan-in must store it inline; a ref
// to the spawn would resolve to the delta. An Overwrite is the value the spawn wrote, which is in its changes,
// so the fan-in may anchor it there.
func TestStateRefs_SpawnWrittenReducedFieldIsItsOwnValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("SpawnWritten")
	g.SetURL("Seed", "sc/seed")
	g.SetURL("Work", "sc/work")
	g.SetURL("Join", "sc/join")
	g.SetFanIn("Join")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("sc/wf", g)

	// The spawn writes enough entries to clear the ref threshold.
	written := make([]string, 400)
	for i := range written {
		written[i] = blob(64)
	}
	mux.HandleTask("sc/seed", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetBool("overwrite") {
			return f.Overwrite("log", written)
		}
		return f.Set("log", written)
	})
	var workLens []int
	var mu sync.Mutex
	mux.HandleTask("sc/work", func(ctx context.Context, f *workflow.Flow) error {
		var log []string
		_, err := f.Get("log", &log)
		mu.Lock()
		workLens = append(workLens, len(log))
		mu.Unlock()
		return err
	})
	mux.HandleTask("sc/join", func(ctx context.Context, f *workflow.Flow) error {
		var log []string
		_, err := f.Get("log", &log)
		assert.NoError(err)
		f.SetInt("logLen", len(log))
		return nil
	})

	for _, tc := range []struct {
		overwrite bool
		want      int
		anchored  bool
	}{
		{overwrite: false, want: 401, anchored: false},
		{overwrite: true, want: 400, anchored: true},
	} {
		workLens = nil
		flowKey, outcome, err := e.Run(ctx, "sc/wf", map[string]any{"log": []string{"initial"}, "pages": []string{"a", "b", "c"}, "overwrite": tc.overwrite}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
		assert.Equal([]int{tc.want, tc.want, tc.want}, workLens, "each branch is handed the spawn's result")
		assert.Equal(float64(tc.want), stateVal(outcome.State, "logLen"), "the fan-in sees the same value")

		hist, err := e.History(ctx, flowKey)
		assert.NoError(err)
		for _, step := range hist {
			if step.TaskName != "Join" {
				continue
			}
			_, joinID, _, err := keys.ParseStepKey(step.StepKey)
			assert.NoError(err)
			_, joinRefs, _ := stepRefsOf(t, e, joinID)
			_, isRef := joinRefs["log"]
			assert.Equal(tc.anchored, isRef, "overwrite=%v", tc.overwrite)
		}
	}
}

// newState is an initialized empty State - the destination a Resolve writes into. The ZERO State is
// deliberately not that: it allocates nothing, so Resolve declines it rather than silently resolving into
// a value the caller cannot see.
func newState() workflow.State {
	s, _ := workflow.NewState()
	return s
}

// TestStateRefs_DispatchResolveTakesATurnOnlyToRead pins where the dispatch resolve takes its turn: around the
// anchor read and nowhere else. With every turn on the shard taken, a cold resolve waits for one before it reads,
// and a resolve the cache can answer returns at once - it touches no connection, so it must not queue.
func TestStateRefs_DispatchResolveTakesATurnOnlyToRead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		"ftok", "u", "W", []byte("{}"), "", workflow.StatusRunning, 1, 1, 60000,
	)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, state) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		1, 1, "s1", "Anchor", "u", workflow.StatusCompleted, 60000, []byte(`{"doc":"D"}`),
	)
	assert.NoError(err)
	refs := staterefs.Refs{"doc": {Anchor: 1}}

	// A dispatch's context carries its claim, so a turn it asks for is really waited on.
	ctx = e.turnstiles.ContextWithPriority(ctx, 1, priorityCommon)
	size := e.turnstiles.Turnstile(1).Size()
	e.turnstiles.Resize(1, 0)
	defer e.turnstiles.Resize(1, size)

	done := make(chan error, 1)
	go func() {
		_, err := e.resolveDispatchRefs(ctx, db, 1, newState(), refs, "u")
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("a cold resolve read the anchor without a turn (error: %v)", err)
	case <-time.After(200 * time.Millisecond):
	}
	e.turnstiles.Resize(1, size)
	select {
	case err := <-done:
		assert.NoError(err)
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the cold resolve never finished once a turn was free")
	}

	// The field is cached now: with every turn taken again, the resolve answers without queueing.
	e.turnstiles.Resize(1, 0)
	warmCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	state := newState()
	start := time.Now()
	_, err = e.resolveDispatchRefs(warmCtx, db, 1, state, refs, "u")
	assert.NoError(err)
	assert.True(time.Since(start) < time.Second, "a cache hit must not queue for a turn, took %v", time.Since(start))
	var doc string
	_, _ = state.Get("doc", &doc)
	assert.Equal("D", doc)
}

// TestStateRefs_UnreadableRefsFailTheStep pins the reader's posture toward a refs column it cannot decode - a
// bug-state row, or one a newer version wrote in a format this one does not know. The step fails with an
// error naming the cause; running it without the fields the column points at would lose them silently.
func TestStateRefs_UnreadableRefsFailTheStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	registerGate(mux, "badrefs")
	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk := createInterruptedFlow(t, e, "badrefs/g")
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET state_refs=? WHERE flow_id=?", []byte(`{"doc":{"v":9}}`), flowID)
	assert.NoError(err)

	assert.NoError(e.Resume(ctx, fk, nil))
	out, err := e.Await(ctx, fk)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, out.Status)
	assert.Contains(out.Error, "unreadable state refs")
}

// TestStateRefs_FoldedFieldIsStoredInline pins the linear case of the same trap: each step of a chain adds a
// large message to an append field, so the folded history outgrows the ref threshold at every hand-off. It
// exists in no row until the successor's state holds it, so it must be stored inline there; a ref to the step
// would resolve to the step's own message alone.
func TestStateRefs_FoldedFieldIsStoredInline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("Folded")
	for _, n := range []string{"A", "B", "C"} {
		g.SetURL(n, "folded/"+n)
		mux.HandleTask("folded/"+n, func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("saw"+n, len(f.GetStrings("log")))
			return f.Set("log", []string{n + blob(30_000)})
		})
	}
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransitionChain("A", "B", "C", workflow.End)
	mux.HandleGraph("folded/wf", g)

	_, outcome, err := e.Run(ctx, "folded/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal(0, outcome.State.GetInt("sawA"))
	assert.Equal(1, outcome.State.GetInt("sawB"))
	assert.Equal(2, outcome.State.GetInt("sawC"))
	assert.Len(outcome.State.GetStrings("log"), 3)
}

// TestStateRefs_EmptyForEachFoldsAReffedBase pins the empty-forEach fan-in path's two halves of a fold over a field
// carried by ref: the base is resolved before the source's contribution folds onto it, and the folded value is
// stored inline in the fan-in step's state. Without the first the contribution folds onto nothing; without the
// second the fan-in's ref points at the source, whose changes hold the contribution alone. Either way the field
// comes out holding the one contribution.
func TestStateRefs_EmptyForEachFoldsAReffedBase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("EmptyForEachRef")
	for _, n := range []string{"Seed", "Src", "Work", "Join"} {
		g.SetURL(n, "emptyref/"+n)
	}
	g.SetFanIn("Join")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransition("Seed", "Src")
	g.AddTransitionForEach("Src", "Work", "items", "item")
	g.AddTransitionChain("Work", "Join", workflow.End)
	mux.HandleGraph("emptyref/wf", g)
	mux.HandleTask("emptyref/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("emptyref/Src", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("log", []string{"src"})
	})
	mux.HandleTask("emptyref/Work", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("emptyref/Join", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("joinSaw", len(f.GetStrings("log")))
		return nil
	})

	// Large enough that Src carries the input by a ref to the entry step.
	initial := make([]string, 3_000)
	for i := range initial {
		initial[i] = blob(64)
	}
	_, outcome, err := e.Run(ctx, "emptyref/wf", map[string]any{"log": initial, "items": []string{}}, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
		assert.Equal(3_001, outcome.State.GetInt("joinSaw"))
		assert.Len(outcome.State.GetStrings("log"), 3_001)
	}
	refs := stepColumn(t, e, "Src", "state_refs")
	if assert.Len(refs, 1) {
		assert.Contains(refs[0], "log", "the input was carried by a ref, or the test covers nothing")
	}
}

// TestStateRefs_DeletesAndNullsOfCarriedFields pins how a write that removes or adds nothing meets a field carried
// by ref. A branch's Del of a plain field the spawn carried must reach the fan-in as a delete - the delete is a
// mark with no value, and the fan-in must count it as a member write or the spawn's ref is carried forward and
// the field comes back. And a null on a reduced field contributes nothing, so a field carried by ref must survive
// it - read as a rewrite, it would be re-anchored at a step whose changes hold the null.
func TestStateRefs_DeletesAndNullsOfCarriedFields(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := unseeded(NewEngineUnderTest(t.Name()))
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	t.Run("branch_delete_of_a_carried_plain_field", func(t *testing.T) {
		assert := testarossa.For(t)
		// Seed holds the document, so Split carries it by a ref, as does each branch.
		g := workflow.NewGraph("DeleteCarried")
		g.SetURL("Seed", "delcarried/seed")
		g.SetURL("Split", "delcarried/split")
		g.SetURL("Work", "delcarried/work")
		g.SetURL("Join", "delcarried/join")
		g.SetFanIn("Join")
		g.AddTransition("Seed", "Split")
		g.AddTransitionForEach("Split", "Work", "pages", "page")
		g.AddTransitionChain("Work", "Join", workflow.End)
		mux.HandleGraph("delcarried/wf", g)
		mux.HandleTask("delcarried/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
		mux.HandleTask("delcarried/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
		mux.HandleTask("delcarried/work", func(ctx context.Context, f *workflow.Flow) error {
			f.Del("pdf")
			return nil
		})
		var joinSaw atomic.Bool
		mux.HandleTask("delcarried/join", func(ctx context.Context, f *workflow.Flow) error {
			joinSaw.Store(f.Has("pdf"))
			return nil
		})

		_, outcome, err := e.Run(ctx, "delcarried/wf", map[string]any{"pdf": blob(20_000), "pages": []string{"a", "b"}}, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
			assert.False(joinSaw.Load(), "the fan-in still sees a field every branch deleted")
			assert.False(outcome.State.Has("pdf"))
		}
	})

	t.Run("null_on_a_carried_reduced_field", func(t *testing.T) {
		assert := testarossa.For(t)
		g := workflow.NewGraph("NullCarried")
		g.SetURL("First", "nullcarried/first")
		g.SetURL("Last", "nullcarried/last")
		g.SetReducer("log", workflow.ReducerConcat)
		g.AddTransitionChain("First", "Last", workflow.End)
		mux.HandleGraph("nullcarried/wf", g)
		mux.HandleTask("nullcarried/first", func(ctx context.Context, f *workflow.Flow) error {
			// What a remote host's decoded result can carry; the carrier itself records nothing for a nil.
			data, _ := json.Marshal(map[string]any{"state": map[string]any{}, "changes": map[string]any{"log": nil}})
			return f.UnmarshalJSON(data)
		})
		var lastSaw atomic.Int64
		mux.HandleTask("nullcarried/last", func(ctx context.Context, f *workflow.Flow) error {
			lastSaw.Store(int64(len(f.GetString("log"))))
			return nil
		})

		_, outcome, err := e.Run(ctx, "nullcarried/wf", map[string]any{"log": blob(20_000)}, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
			assert.Equal(int64(20_000), lastSaw.Load())
			assert.Len(outcome.State.GetString("log"), 20_000)
		}
	})
}
