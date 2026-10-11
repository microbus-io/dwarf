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
	"net/http"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// stepColumnsByTask reads the stored state and changes of every step of a task, in step_id order.
func stepColumnsByTask(t *testing.T, e *Engine, taskName string) (states, changes []workflow.State) {
	t.Helper()
	assert := testarossa.For(t)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	for _, id := range stepIDsByTask(t, e, taskName) {
		var stateJSON, changesJSON []byte
		err := db.QueryRowContext(context.Background(),
			"SELECT state, changes FROM dwarf_steps WHERE step_id=?", id,
		).Scan(&stateJSON, &changesJSON)
		assert.NoError(err)
		s, _ := workflow.NewState(stateJSON)
		c, _ := workflow.NewState(changesJSON)
		states = append(states, s)
		changes = append(changes, c)
	}
	return states, changes
}

// TestMeta_StampedRedactedAndRebuilt pins the three rules of a state's metadata: the engine stamps every
// declared reducer into the state it stores and hands a task, a field with no value included; it strips the
// metadata from everything it hands a caller, while the stored columns keep it; and it rebuilds it rather
// than trusting what a task returns.
func TestMeta_StampedRedactedAndRebuilt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("Meta")
	g.SetURL("Seed", "meta/seed")
	g.SetURL("Work", "meta/work")
	g.SetURL("Join", "meta/join")
	g.SetURL("Tail", "meta/tail")
	g.SetFanIn("Join")
	// Join converges a cohort on the trunk, so it describes nothing about its cohort.
	g.SetReducer("log", workflow.ReducerAppend)
	g.SetReducer("total", workflow.ReducerAdd)
	g.AddTransitionForEach("Seed", "Work", "items", "item")
	g.AddTransitionChain("Work", "Join", "Tail", workflow.End)
	mux.HandleGraph("meta/wf", g)
	tags := map[string]workflow.FieldMeta{
		"log":   {Reducer: workflow.ReducerAppend},
		"total": {Reducer: workflow.ReducerAdd},
	}

	var mu sync.Mutex
	var remoteViews []map[string]workflow.FieldMeta
	mux.HandleTask("meta/seed", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("meta/work", func(ctx context.Context, f *workflow.Flow) error {
		if f.Has(workflow.MetaKey) {
			return errors.New("the task sees the metadata as a field")
		}
		f.Set("log", []string{f.GetString("item")})
		f.SetInt("total", 1)
		// Act as a remote host would: the carrier crosses the wire as JSON, and what comes back is decoded
		// over it. The returned JSON forges the input's metadata, which the engine rebuilds rather than reads.
		// (Forged metadata in the changes fails the step: TestMeta_ForgedChangesMetaFailsTheStep.)
		data, err := json.Marshal(f)
		if err != nil {
			return err
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(data, &wire); err != nil {
			return err
		}
		var state, changes map[string]json.RawMessage
		if err := json.Unmarshal(wire["state"], &state); err != nil {
			return err
		}
		if err := json.Unmarshal(wire["changes"], &changes); err != nil {
			return err
		}
		var view map[string]workflow.FieldMeta
		_ = json.Unmarshal(state[workflow.MetaKey], &view)
		mu.Lock()
		remoteViews = append(remoteViews, view)
		mu.Unlock()
		forged := json.RawMessage(`{"log":{"reducer":"union"},"item":{"reducer":"add"}}`)
		state[workflow.MetaKey] = forged
		wire["state"], _ = json.Marshal(state)
		wire["changes"], _ = json.Marshal(changes)
		data, err = json.Marshal(wire)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, f)
	})
	mux.HandleTask("meta/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("meta/tail", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	flowKey, outcome, err := e.Run(ctx, "meta/wf", map[string]any{"items": []string{"a", "b"}}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal([]string{"a", "b"}, outcome.State.GetStrings("log"))

	t.Run("stamped", func(t *testing.T) {
		assert := testarossa.For(t)
		// The entry step's state is tagged before any field has a value.
		seedStates, _ := stepColumnsByTask(t, e, "Seed")
		if assert.Len(seedStates, 1) {
			assert.Equal(tags, seedStates[0].Meta())
			assert.False(seedStates[0].Has("log"))
		}
		// A remote task receives the tags with its state.
		mu.Lock()
		defer mu.Unlock()
		assert.Len(remoteViews, 2)
		for _, view := range remoteViews {
			assert.Equal(tags, view)
		}
	})

	t.Run("rebuilt_not_trusted", func(t *testing.T) {
		assert := testarossa.For(t)
		_, workChanges := stepColumnsByTask(t, e, "Work")
		for _, c := range workChanges {
			assert.Nil(c.MetaJSON(), "a task's forged metadata was stored with its changes")
		}
		for _, task := range []string{"Join", "Tail"} {
			states, _ := stepColumnsByTask(t, e, task)
			if assert.Len(states, 1) {
				assert.Equal(tags, states[0].Meta(), "task %s", task)
			}
		}
	})

	t.Run("redacted_outside_the_engine", func(t *testing.T) {
		assert := testarossa.For(t)
		assert.Nil(outcome.State.MetaJSON())
		snap, err := e.Snapshot(ctx, flowKey)
		if assert.NoError(err) {
			assert.Nil(snap.State.MetaJSON())
		}
		hist, err := e.History(ctx, flowKey)
		assert.NoError(err)
		for _, s := range hist {
			step, err := e.Step(ctx, s.StepKey)
			if assert.NoError(err) {
				assert.Nil(step.State.MetaJSON(), "step %s state", s.TaskName)
				assert.Nil(step.Changes.MetaJSON(), "step %s changes", s.TaskName)
			}
		}
		// The stored final state keeps it, for Continue and Fork to build on.
		_, flowID, _, err := keys.ParseFlowKey(flowKey)
		assert.NoError(err)
		db, err := e.db.Shard(1)
		assert.NoError(err)
		var finalJSON []byte
		assert.NoError(db.QueryRowContext(ctx, "SELECT final_state FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&finalJSON))
		final, _ := workflow.NewState(finalJSON)
		assert.Equal(tags, final.Meta())
	})
}

// TestMeta_SubgraphBoundary pins that a child flow's state is tagged by the child's own graph whatever the
// caller passes in, and that the child's result reaches the caller without metadata.
func TestMeta_SubgraphBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	child := workflow.NewGraph("Child")
	child.SetURL("Leaf", "metasub/leaf")
	child.SetReducer("found", workflow.ReducerUnion)
	child.AddTransition("Leaf", workflow.End)
	mux.HandleGraph("metasub/child", child)
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "metasub/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("metasub/parent", parent)

	mux.HandleTask("metasub/leaf", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("found", []string{"x"})
		return nil
	})
	mux.HandleTask("metasub/call", func(ctx context.Context, f *workflow.Flow) error {
		in := json.RawMessage(`{"q":1,"__meta__":{"q":{"reducer":"add"}}}`)
		var out map[string]any
		yield, err := f.Subgraph("metasub/child", in, &out)
		if yield || err != nil {
			return err
		}
		if _, leaked := out[workflow.MetaKey]; leaked {
			return errors.New("the subgraph's result carries metadata")
		}
		f.Set("childFound", out["found"])
		return nil
	})

	_, outcome, err := e.Run(ctx, "metasub/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal([]string{"x"}, outcome.State.GetStrings("childFound"))

	leafStates, _ := stepColumnsByTask(t, e, "Leaf")
	if assert.Len(leafStates, 1) {
		assert.Equal(map[string]workflow.FieldMeta{"found": {Reducer: workflow.ReducerUnion}}, leafStates[0].Meta())
	}
}

// TestMeta_ReservedInCallerInput pins that every operation taking a caller's state rejects one that carries
// metadata, since only the engine writes it.
func TestMeta_ReservedInCallerInput(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("Reserved")
	g.SetURL("Only", "metares/only")
	g.AddTransition("Only", workflow.End)
	mux.HandleGraph("metares/wf", g)
	mux.HandleTask("metares/only", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	forged := map[string]any{workflow.MetaKey: map[string]any{"a": map[string]any{"reducer": "add"}}}

	_, err := e.Create(ctx, "metares/wf", forged, nil)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	flowKey, outcome, err := e.Run(ctx, "metares/wf", nil, nil)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
		return
	}
	_, err = e.Continue(ctx, flowKey, forged)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	hist, err := e.History(ctx, flowKey)
	if assert.NoError(err) && assert.Len(hist, 1) {
		_, err = e.Fork(ctx, hist[0].StepKey, forged)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	}
}

// TestMeta_DispatchRestampsAnUntaggedRow pins that a task receives the tags even when its step's stored state
// has none - a flow already in flight when the engine was upgraded - because dispatch stamps them from the
// graph rather than trusting the row.
func TestMeta_DispatchRestampsAnUntaggedRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("Restamp")
	g.SetURL("Ask", "metarestamp/ask")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransition("Ask", workflow.End)
	mux.HandleGraph("metarestamp/wf", g)

	var resumedView map[string]workflow.FieldMeta
	mux.HandleTask("metarestamp/ask", func(ctx context.Context, f *workflow.Flow) error {
		var answer map[string]any
		yield, err := f.Interrupt(map[string]any{"q": 1}, &answer)
		if yield || err != nil {
			return err
		}
		data, err := json.Marshal(f)
		if err != nil {
			return err
		}
		var wire struct {
			State workflow.State `json:"state"`
		}
		if err := json.Unmarshal(data, &wire); err != nil {
			return err
		}
		resumedView = wire.State.Meta()
		return nil
	})

	flowKey, err := e.Create(ctx, "metarestamp/wf", nil, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err := e.Await(ctx, flowKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status) {
		return
	}
	db, err := e.db.Shard(1)
	assert.NoError(err)
	ids := stepIDsByTask(t, e, "Ask")
	if !assert.Len(ids, 1) {
		return
	}
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET state=? WHERE step_id=?", []byte("{}"), ids[0])
	assert.NoError(err)

	assert.NoError(e.Resume(ctx, flowKey, map[string]any{"ok": true}))
	outcome, err = e.Await(ctx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
	assert.Equal(map[string]workflow.FieldMeta{"log": {Reducer: workflow.ReducerAppend}}, resumedView)
}

// TestMeta_ReducedStaysWithTheInnerFanIn pins where a cohort's description lives: on an inner fan-in's row,
// which an outer fan-in reads, and nowhere else - not on the step after it, and not on a fan-in on the trunk,
// which no outer cohort reads.
func TestMeta_ReducedStaysWithTheInnerFanIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("Reduced")
	for _, n := range []string{"Seed", "Cell", "Chunk", "Inner", "After", "Outer"} {
		g.SetURL(n, "metareduced/"+n)
		mux.HandleTask("metareduced/"+n, func(ctx context.Context, f *workflow.Flow) error {
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
	mux.HandleGraph("metareduced/wf", g)

	_, outcome, err := e.Run(ctx, "metareduced/wf", map[string]any{"pages": []string{"a", "b"}}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal([]string{"x", "y", "x", "y"}, outcome.State.GetStrings("log"))
	assert.Equal("by"+blob(60_000), outcome.State.GetString("big"))

	tags := map[string]workflow.FieldMeta{"log": {Reducer: workflow.ReducerAppend}}
	inner, _ := stepColumnsByTask(t, e, "Inner")
	if assert.Len(inner, 2) {
		for _, s := range inner {
			from := 0
			// Only what the inner cohort wrote: lines is the inner spawn's own write.
			want := map[string]workflow.FieldMeta{
				"log": {Reducer: workflow.ReducerAppend, Reduced: &workflow.Reduced{From: &from}},
				"big": {Reduced: &workflow.Reduced{Keys: []string{}}},
			}
			assert.Equal(want, s.Meta())
			// The outer fold reads this row as stored, so what the cohort wrote is inline, never a ref.
			assert.True(s.Has("big"))
		}
	}
	for _, task := range []string{"After", "Outer"} {
		states, _ := stepColumnsByTask(t, e, task)
		for _, s := range states {
			assert.Equal(tags, s.Meta(), "task %s", task)
		}
	}
}

// TestMeta_ForgedChangesMetaFailsTheStep pins what a task's changes may carry in their metadata: an overwrite
// mark, and nothing else - and not on a reduced field inside a fan-out branch.
func TestMeta_ForgedChangesMetaFailsTheStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	// returning makes the task return its carrier as a remote host would, with changesMeta as the metadata of
	// its changes.
	returning := func(f *workflow.Flow, changesMeta string) error {
		data, err := json.Marshal(f)
		if err != nil {
			return err
		}
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(data, &wire); err != nil {
			return err
		}
		var changes map[string]json.RawMessage
		_ = json.Unmarshal(wire["changes"], &changes)
		if changes == nil {
			changes = map[string]json.RawMessage{}
		}
		changes[workflow.MetaKey] = json.RawMessage(changesMeta)
		wire["changes"], _ = json.Marshal(changes)
		data, _ = json.Marshal(wire)
		return json.Unmarshal(data, f)
	}

	g := workflow.NewGraph("Forged")
	g.SetURL("Only", "metaforged/only")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransition("Only", workflow.End)
	mux.HandleGraph("metaforged/wf", g)
	var changesMeta string
	mux.HandleTask("metaforged/only", func(ctx context.Context, f *workflow.Flow) error {
		return returning(f, changesMeta)
	})

	for _, forged := range []string{
		`{"log":{"reducer":"union"}}`,
		`{"log":{"overwrite":true,"reduced":{"from":0}}}`,
		`{"log":{"overwrite":false}}`,
		`{"log":"x"}`,
	} {
		changesMeta = forged
		_, outcome, err := e.Run(ctx, "metaforged/wf", map[string]any{"log": []string{"a"}}, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusFailed, outcome.Status, forged)
			assert.Contains(outcome.Error, `"log"`, forged)
		}
	}

	// An overwrite mark with no value is a delete, which a step on the trunk may make.
	changesMeta = `{"log":{"overwrite":true}}`
	_, outcome, err := e.Run(ctx, "metaforged/wf", map[string]any{"log": []string{"a"}}, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
		assert.False(outcome.State.Has("log"))
	}

	// Inside a fan-out branch it is not allowed on a reduced field, even when the carrier was bypassed.
	fan := workflow.NewGraph("ForgedBranch")
	fan.SetURL("Seed", "metaforged/seed")
	fan.SetURL("Branch", "metaforged/branch")
	fan.SetURL("Join", "metaforged/join")
	fan.SetFanIn("Join")
	fan.SetReducer("log", workflow.ReducerAppend)
	fan.AddTransitionForEach("Seed", "Branch", "items", "item")
	fan.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph("metaforged/fan", fan)
	mux.HandleTask("metaforged/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("metaforged/join", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("metaforged/branch", func(ctx context.Context, f *workflow.Flow) error {
		return returning(f, `{"log":{"overwrite":true}}`)
	})
	_, outcome, err = e.Run(ctx, "metaforged/fan", map[string]any{"items": []string{"x"}, "log": []string{"a"}}, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "inside a fan-out branch")
	}
}
