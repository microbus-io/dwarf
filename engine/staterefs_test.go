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
	"strings"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

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

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		"ftok", "u", "W", []byte("{}"), workflow.StatusRunning, 1, 1, 60000,
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

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
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

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
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

// TestStateRefs_SpawnWrittenReducedFieldIsItsOwnValue pins what a fan-out source's own write to a reduced field
// means at the fan-in: the value it wrote, replacing its input, exactly as each branch was handed it - a reducer
// folds only what the branches contribute. That value is the spawn's `changes`, so the fan-in may anchor it there.
// The branches never touch the field, so the spawn's write is the only thing deciding it.
func TestStateRefs_SpawnWrittenReducedFieldIsItsOwnValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
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

	// The spawn replaces a one-entry log with one large enough to clear the ref threshold.
	written := make([]string, 400)
	for i := range written {
		written[i] = blob(64)
	}
	mux.HandleTask("sc/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("log", written)
		return nil
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

	_, outcome, err := e.Run(ctx, "sc/wf", map[string]any{"log": []string{"initial"}, "pages": []string{"a", "b", "c"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal([]int{400, 400, 400}, workLens, "each branch is handed the spawn's value")
	assert.Equal(400.0, stateVal(outcome.State, "logLen"), "the fan-in sees the same value, not the input with it appended")

	join := stepIDsByTask(t, e, "Join")[0]
	_, joinRefs, _ := stepRefsOf(t, e, join)
	assert.Contains(joinRefs, "log", "the spawn's own value is in its changes, so the fan-in anchors it there")
}

// newState is an initialized empty State - the destination a Resolve writes into. The ZERO State is
// deliberately not that: it allocates nothing, so Resolve declines it rather than silently resolving into
// a value the caller cannot see.
func newState() workflow.State {
	s, _ := workflow.NewState()
	return s
}

// TestStateRefs_UnreadableRefsFailTheStep pins the reader's posture toward a refs column it cannot decode - a
// bug-state row, or one a newer version wrote in a format this one does not know. The step fails with an
// error naming the cause; running it without the fields the column points at would lose them silently.
func TestStateRefs_UnreadableRefsFailTheStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := NewHostMux()
	registerGate(mux, "badrefs")
	e := NewEngineUnderTest(t.Name())
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
