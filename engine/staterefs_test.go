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
	"testing"

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
func stepRefsOf(t *testing.T, e *Engine, stepID int) (state map[string]any, refs stateRefs, stateBytes int) {
	t.Helper()
	db, err := e.db.Shard(1)
	testarossa.For(t).NoError(err)
	var stateJSON, refsJSON string
	err = db.QueryRowContext(context.Background(),
		"SELECT state, state_refs FROM dwarf_steps WHERE step_id=?", stepID,
	).Scan(&stateJSON, &refsJSON)
	testarossa.For(t).NoError(err)
	unmarshalJSONMap(stateJSON, &state)
	return state, parseStateRefs(refsJSON), len(stateJSON)
}

// stepIDsByTask maps each task name to its step ids, in step order.
func stepIDsByTask(t *testing.T, e *Engine, taskName string) []int {
	t.Helper()
	db, err := e.db.Shard(1)
	testarossa.For(t).NoError(err)
	rows, err := db.QueryContext(context.Background(),
		"SELECT step_id FROM dwarf_steps WHERE task_name=? ORDER BY step_id", taskName)
	testarossa.For(t).NoError(err)
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		testarossa.For(t).NoError(rows.Scan(&id))
		ids = append(ids, id)
	}
	return ids
}

// TestStateRefs_OpenThreshold pins the anchor-cost model's central claim: the bar for opening a new anchor is
// set by the FAN-OUT WIDTH, not by the field alone. A linear successor must clear the full threshold (refs
// barely pay for a pure linear carry - cost is ~D resolves and savings are S*D, so the depth cancels), while a
// wide fan-out amortizes one cache-missed resolve over N branches and so refs far smaller fields.
func TestStateRefs_OpenThreshold(t *testing.T) {
	assert := testarossa.For(t)

	// Linear: the full threshold, never lower.
	assert.Equal(stateRefThreshold, stateRefOpenThreshold(1))
	assert.Equal(stateRefThreshold, stateRefOpenThreshold(0)) // degenerate n is clamped to 1

	// Fan-out: the bar falls as budget/N ...
	assert.Equal(stateRefBudget/8, stateRefOpenThreshold(8))
	assert.True(stateRefOpenThreshold(8) < stateRefThreshold)

	// ... but never below the floor, so a wide fan-out refs everything meaningful and nothing trivial.
	assert.Equal(stateRefFloor, stateRefOpenThreshold(100))
	assert.Equal(stateRefFloor, stateRefOpenThreshold(10000))

	// Monotone in N: more branches never raise the bar.
	for n := 1; n < 64; n++ {
		assert.True(stateRefOpenThreshold(n+1) <= stateRefOpenThreshold(n))
	}
}

// TestStateRefs_MintTiers pins the three tiers of the anchor-cost policy directly on mintStateRefs, where the
// decision is made. The read cost of a ref is paid per ANCHOR ROW, never per field - so every case here is
// really asking "is this row worth opening?", and once it is open everything else in it is free.
func TestStateRefs_MintTiers(t *testing.T) {
	const anchor = 42

	mint := func(t *testing.T, state map[string]any, changes map[string]any, inherited stateRefs, successors int) (map[string]any, stateRefs) {
		t.Helper()
		stateJSON, refsJSON, err := mintStateRefs(state, changes, inherited, anchor, successors, nil)
		testarossa.For(t).NoError(err)
		var stored map[string]any
		unmarshalJSONMap(stateJSON, &stored)
		return stored, parseStateRefs(refsJSON)
	}

	t.Run("linear_below_threshold_is_inlined", func(t *testing.T) {
		assert := testarossa.For(t)
		// 2KB clears the floor but not the linear bar (4KB): one round-trip is worth ~23KB of avoided writes,
		// and a linear chain's depth cancels out of the trade, so this does NOT earn an anchor.
		state := map[string]any{"doc": blob(2048)}
		stored, refs := mint(t, state, state, nil, 1)
		assert.Equal(0, len(refs), "a 2KB field must not open an anchor for a single successor")
		assert.Contains(stored, "doc")
	})

	t.Run("linear_above_threshold_opens_the_anchor", func(t *testing.T) {
		assert := testarossa.For(t)
		state := map[string]any{"doc": blob(8192), "small": "s"}
		stored, refs := mint(t, state, state, nil, 1)
		assert.Equal(anchor, refs["doc"])
		assert.NotContains(stored, "doc", "a ref'd field is OMITTED from state - that is the entire saving")
		assert.Contains(stored, "small", "a field below the floor stays inline")
	})

	t.Run("fanout_lowers_the_bar", func(t *testing.T) {
		assert := testarossa.For(t)
		// The SAME 2KB field that stayed inline for one successor is ref'd across a 16-way fan-out: the bar is
		// budget/16 (~1.4KB), because one cache-missed resolve now serves 16 branches while the write saving is
		// paid 16 times. This is the case the whole design exists for.
		state := map[string]any{"doc": blob(2048)}
		stored, refs := mint(t, state, state, nil, 16)
		assert.Equal(anchor, refs["doc"], "a wide fan-out must ref a field a linear successor would inline")
		assert.NotContains(stored, "doc")
	})

	t.Run("free_tier_colocated_fields_ride_along", func(t *testing.T) {
		assert := testarossa.For(t)
		// One big field opens the anchor. Every OTHER field in that same row is then free to ref: resolving
		// fetches whole payload columns, so the row is already on the wire and a second ref costs no extra
		// read at all. The floor is the only bar it must clear.
		state := map[string]any{
			"doc":    blob(8192), // opens the anchor
			"medium": blob(1200), // >= floor: rides along for free
			"tiny":   blob(50),   // < floor: not worth its own bookkeeping
		}
		stored, refs := mint(t, state, state, nil, 1)
		assert.Equal(anchor, refs["doc"])
		assert.Equal(anchor, refs["medium"], "a co-located field above the floor is free to ref")
		assert.NotContains(refs, "tiny")
		assert.Contains(stored, "tiny")
	})

	t.Run("per_anchor_sum_opens_the_anchor", func(t *testing.T) {
		assert := testarossa.For(t)
		// No single field clears the 4KB linear bar, but they are all CO-LOCATED in this one step's row, so
		// they share one prospective anchor and one round-trip. Summing is legitimate precisely because of
		// that: four 1.5KB fields at four DIFFERENT anchors would mean four whole-row overfetches for the same
		// bytes, and the sum is never taken across anchors.
		state := map[string]any{
			"a": blob(1500), "b": blob(1500), "c": blob(1500),
		}
		stored, refs := mint(t, state, state, nil, 1)
		assert.Equal(3, len(refs), "co-located fields summing past the bar open the anchor together")
		assert.Equal(0, len(stored))
	})

	t.Run("sum_of_sub_floor_fields_does_not_open_it", func(t *testing.T) {
		assert := testarossa.For(t)
		// Only fields at or above the floor are candidates, so a swarm of small fields cannot conspire to open
		// an anchor none of them would benefit from.
		state := map[string]any{}
		for i := range 20 {
			state[string(rune('a'+i))] = blob(500)
		}
		_, refs := mint(t, state, state, nil, 1)
		assert.Equal(0, len(refs))
	})

	t.Run("inherited_ref_is_carried_never_reminted", func(t *testing.T) {
		assert := testarossa.For(t)
		// The one-hop invariant. The field arrived as a ref to step 7 and the task did not rewrite it, so the
		// successor keeps pointing at 7 - NOT at this step, whose row does not hold the bytes. Re-minting here
		// would build a chain, which is the rejected delta design wearing a hat.
		state := map[string]any{"doc": blob(8192)} // materialized at dispatch
		stored, refs := mint(t, state, map[string]any{}, stateRefs{"doc": 7}, 1)
		assert.Equal(7, refs["doc"], "a carried ref keeps its original anchor")
		assert.NotContains(stored, "doc")
	})

	t.Run("rewritten_ref_is_reanchored_here", func(t *testing.T) {
		assert := testarossa.For(t)
		// The task REWROTE the field, so its bytes are now in this step's changes and the inherited anchor is
		// stale. The ref must move to this step.
		state := map[string]any{"doc": blob(8192)}
		changes := map[string]any{"doc": blob(8192)}
		_, refs := mint(t, state, changes, stateRefs{"doc": 7}, 1)
		assert.Equal(anchor, refs["doc"], "a rewritten field re-anchors at the step that wrote it")
	})

	t.Run("excluded_fields_are_never_refd", func(t *testing.T) {
		assert := testarossa.For(t)
		// The forEach element is SYNTHESIZED per branch - its bytes are in no step row - so a ref to it would
		// dangle. Even a huge element stays inline. (The same exclusion carries a cohort member's contribution
		// past the fan-in's mint, whose bytes are likewise not in the anchor's row.)
		state := map[string]any{"item": blob(50000), "doc": blob(50000)}
		stateJSON, refsJSON, err := mintStateRefs(state, map[string]any{}, nil, anchor, 32, map[string]bool{"item": true})
		assert.NoError(err)
		var stored map[string]any
		unmarshalJSONMap(stateJSON, &stored)
		refs := parseStateRefs(refsJSON)
		assert.NotContains(refs, "item", "an excluded field is never ref'd, however large")
		assert.Contains(stored, "item")
		assert.Equal(anchor, refs["doc"], "its neighbours still ref normally")
		assert.NotContains(stored, "doc")
	})

	t.Run("max_anchors_inlines_the_cheapest", func(t *testing.T) {
		assert := testarossa.For(t)
		// More distinct anchors than maxStateAnchors: the pointer fan is capped by inlining the anchors whose
		// refs buy the least (fewest bytes), paying one copy to shorten the resolve IN-list. maxStateAnchors is
		// the same knob as the threshold - the cost model prices ROWS.
		state := map[string]any{}
		inherited := stateRefs{}
		for i := range maxStateAnchors + 2 {
			field := "f" + string(rune('0'+i))
			state[field] = blob(2000 + i*1000) // f0 smallest ... f5 largest
			inherited[field] = 100 + i
		}
		stored, refs := mint(t, state, map[string]any{}, inherited, 1)
		assert.Equal(maxStateAnchors, len(refs), "the pointer fan is capped")
		// The two cheapest anchors were inlined back as literals; the largest survive as refs.
		assert.Contains(stored, "f0")
		assert.Contains(stored, "f1")
		assert.NotContains(stored, "f5")
		assert.Equal(105, refs["f5"])
	})

	t.Run("carried_ref_absent_from_merged_is_re_emitted", func(t *testing.T) {
		assert := testarossa.For(t)
		// The fan-in mint case: a merely-CARRIED field is not materialized into `merged` (resolveReducedRefs
		// resolves only what its reducers fold), so its key is absent from `merged`. The inherited ref must
		// still be re-emitted, or the carried field is silently dropped from the fan-in step onward. This is the
		// shape a spawn that only carries the ref (an anchoring step precedes the fan-out source) produces.
		merged := map[string]any{"local": "s"} // `pdf` deliberately absent - carried, not materialized
		stored, refs := mint(t, merged, map[string]any{}, stateRefs{"pdf": 7}, 1)
		assert.Equal(7, refs["pdf"], "a carried ref whose payload is not in merged must still be re-emitted")
		assert.NotContains(stored, "pdf")
		assert.Contains(stored, "local")
	})

	t.Run("carried_ref_absent_from_merged_but_rewritten_is_dropped", func(t *testing.T) {
		assert := testarossa.For(t)
		// A tombstone (member/spawn deleted the carried field) leaves the key in `changes` and out of `merged`.
		// The stale carried ref must NOT be resurrected - the field is genuinely gone.
		_, refs := mint(t, map[string]any{}, map[string]any{"pdf": nil}, stateRefs{"pdf": 7}, 1)
		assert.NotContains(refs, "pdf", "a tombstoned carried field must not be re-emitted through its stale ref")
	})

	t.Run("un_inlineable_carried_anchor_survives_the_cap", func(t *testing.T) {
		assert := testarossa.For(t)
		// More distinct anchors than maxStateAnchors, where one anchor is a CARRIED ref whose payload is absent
		// from `merged` (un-inlineable). The cap must never drop it - there is no literal to inline it back to,
		// so dropping would delete the field. It survives even if that means keeping more than maxStateAnchors.
		merged := map[string]any{}
		inherited := stateRefs{}
		for i := range maxStateAnchors + 1 {
			field := "f" + string(rune('0'+i))
			merged[field] = blob(2000 + i*1000) // inlineable: present in merged
			inherited[field] = 100 + i
		}
		inherited["carried"] = 999 // absent from merged: un-inlineable, must be pinned
		stored, refs := mint(t, merged, map[string]any{}, inherited, 1)
		assert.Equal(999, refs["carried"], "an un-inlineable carried anchor must survive the cap")
		assert.NotContains(stored, "carried")
	})
}

// TestStateRefs_ResolveReadsBothColumns pins the correction that motivated the whole redesign: an anchor's
// bytes are NOT always in its `changes`. Two of the three places they sit are the `state` column - the flow's
// INITIAL INPUT at the entry step (no task produced it, so it appears in no changes anywhere), and a fan-in's
// reducer output. A changes-only resolver silently misses both, and the initial-input case is the headline one.
func TestStateRefs_ResolveReadsBothColumns(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngine()
	proxy := NewTestProxy()
	e.SetHost(proxy)
	e.RunInTest(t)

	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		"ftok", "u", "W", "{}", workflow.StatusRunning, 1, 1, 60000,
	)
	assert.NoError(err)
	// An anchor holding one field in `state` (an initial input nobody produced) and another in `changes` (a
	// task's own output), plus a field present in BOTH - where changes, being the newer value, must win.
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, state, changes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		1, 1, "s1", "Anchor", "u", workflow.StatusCompleted, 60000,
		`{"fromState":"S","shadowed":"old"}`, `{"fromChanges":"C","shadowed":"new"}`,
	)
	assert.NoError(err)

	state := map[string]any{}
	refs := stateRefs{"fromState": 1, "fromChanges": 1, "shadowed": 1}
	err = e.resolveStateRefs(ctx, db, 1, state, refs, nil, "u")
	assert.NoError(err)

	// A resolved field is DECODED, exactly like one that was never ref'd: the storage encoding must never leak
	// into a state map, or an API caller's state["pdf"].(string) would silently stop matching.
	assert.Equal("S", state["fromState"], "the entry step's state column is a legitimate anchor")
	assert.Equal("C", state["fromChanges"])
	assert.Equal("new", state["shadowed"], "changes shadows state - it is the newer value")

	// A dangling ref is an invariant violation and must be loud, never a silently-absent field.
	err = e.resolveStateRefs(ctx, db, 1, map[string]any{}, stateRefs{"nope": 1}, nil, "u")
	assert.Error(err)
	err = e.resolveStateRefs(ctx, db, 1, map[string]any{}, stateRefs{"x": 999}, nil, "u")
	assert.Error(err)
}

// TestStateRefs_CarryAcrossFanOut is the end-to-end shape the design exists for, asserted on what is actually
// STORED. A large document is fanned out over N branches: the payload must be written ONCE (at the anchor) and
// every branch's step row must carry a ref instead of a copy - while each branch's task still sees the whole
// document. It also pins the two rules that make the fan-out case work: the injected element is never ref'd,
// and a carried ref crosses the fan-in still pointing at its ORIGINAL anchor rather than being re-anchored.
func TestStateRefs_CarryAcrossFanOut(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngine()
	proxy := NewTestProxy()
	e.SetHost(proxy)
	e.RunInTest(t)

	const doc = 20000
	g := workflow.NewGraph("Refs")
	g.SetEndpoint("Seed", "refs/seed")
	g.SetEndpoint("Work", "refs/work")
	g.SetEndpoint("Join", "refs/join")
	g.SetFanIn("Join")
	g.SetReducer("processed", workflow.ReducerAdd)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Join", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("refs/wf", g)

	proxy.HandleTask("refs/seed", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// Every branch must SEE the whole document, even though its own row does not store it.
	proxy.HandleTask("refs/work", func(ctx context.Context, f *workflow.Flow) error {
		if len(f.GetString("pdf")) != doc {
			return errors.New("branch cannot see the carried document")
		}
		f.SetInt("processed", 1)
		return nil
	})
	proxy.HandleTask("refs/join", func(ctx context.Context, f *workflow.Flow) error {
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
	assert.Equal(8.0, outcome.State["total"])
	// FLATTEN at the flow boundary: final_state is materialized, so a reader never sees a ref.
	assert.Equal(float64(doc), outcome.State["pdfLen"])
	assert.Equal(doc, len(outcome.State["pdf"].(string)))

	// The entry step is the anchor: it holds the initial input in its `state` column, inline.
	entry := stepIDsByTask(t, e, "Seed")[0]
	entryState, entryRefs, _ := stepRefsOf(t, e, entry)
	assert.Equal(0, len(entryRefs), "the entry step holds the initial input itself - it anchors, it does not ref")
	assert.Equal(doc, len(entryState["pdf"].(string)))

	// Every branch refs that one anchor rather than copying the document.
	branches := stepIDsByTask(t, e, "Work")
	assert.Equal(8, len(branches))
	for _, id := range branches {
		st, refs, size := stepRefsOf(t, e, id)
		assert.Equal(entry, refs["pdf"], "a branch carries the document BY REFERENCE to the entry step")
		assert.NotContains(st, "pdf", "the payload is not copied into the branch's row")
		assert.Contains(st, "page", "the injected element is synthesized per branch and can never be ref'd")
		assert.True(size < doc/2, "a branch's stored state must be a small fraction of the payload it carries")
	}

	// The fan-in CARRIES the ref through rather than materializing and re-anchoring it: resolving a merely
	// carried field at every fan-in would hand back the win in exactly these graphs.
	join := stepIDsByTask(t, e, "Join")[0]
	joinState, joinRefs, _ := stepRefsOf(t, e, join)
	assert.Equal(entry, joinRefs["pdf"], "a carried ref crosses the fan-in still pointing at its original anchor")
	assert.NotContains(joinState, "pdf")
	assert.Contains(joinState, "processed", "the REDUCED field is materialized into the fan-in step's own state")
}

// TestStateRefs_ReducedFieldIsResolvedAndReanchored pins the fan-in's selective resolve from the other side.
// A COMBINING reducer needs its accumulated base, so a ref'd field it folds must be materialized first -
// folding a delta onto an absent base would silently lose everything accumulated so far. The reduced result
// then exists nowhere else, so it is inlined into the fan-in step's own state, which becomes its anchor.
func TestStateRefs_ReducedFieldIsResolvedAndReanchored(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngine()
	proxy := NewTestProxy()
	e.SetHost(proxy)
	e.RunInTest(t)

	g := workflow.NewGraph("Reduced")
	g.SetEndpoint("Seed", "red/seed")
	g.SetEndpoint("Work", "red/work")
	g.SetEndpoint("Join", "red/join")
	g.SetFanIn("Join")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Join", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("red/wf", g)

	proxy.HandleTask("red/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("red/work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("log", []string{"entry"})
		return nil
	})
	proxy.HandleTask("red/join", func(ctx context.Context, f *workflow.Flow) error {
		var log []string
		testarossa.For(t).NoError(f.Get("log", &log))
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
	assert.Equal(403.0, outcome.State["logLen"])

	// The reduced value exists in no other row, so the fan-in step INLINES it and becomes its anchor - the
	// third of the three places an anchor's bytes can live.
	join := stepIDsByTask(t, e, "Join")[0]
	joinState, joinRefs, _ := stepRefsOf(t, e, join)
	assert.NotContains(joinRefs, "log", "a reduced field cannot be a ref - the merged value exists nowhere else")
	assert.Contains(joinState, "log", "the fan-in step inlines it, anchoring it for everything downstream")
}

// TestStateRefs_SpawnCombinedFieldIsNotAnchored pins that a field the SPAWN task itself writes through a
// COMBINING reducer, over a base it also holds, has a merged fan-in value (reduce(base, delta)) that exists in no
// row - the spawn's `changes` holds only the delta. Anchoring it at the spawn (as the fan-in mint did) made
// resolution splice back the bare delta, silently dropping the accumulated base. It must be inlined instead.
// Distinct from the member-written case above: here no cohort member touches the field, so the only reason it
// must not be anchored is that the SPAWN combined it. Without the fix, logLen reads 1 (just the spawn delta).
func TestStateRefs_SpawnCombinedFieldIsNotAnchored(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngine()
	proxy := NewTestProxy()
	e.SetHost(proxy)
	e.RunInTest(t)

	g := workflow.NewGraph("SpawnCombined")
	g.SetEndpoint("Seed", "sc/seed")
	g.SetEndpoint("Work", "sc/work")
	g.SetEndpoint("Join", "sc/join")
	g.SetFanIn("Join")
	g.SetReducer("log", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Join", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("sc/wf", g)

	// The SPAWN (fan-out source) appends to log; its merged value (base + delta) clears the ref threshold. The
	// branches deliberately never touch log, so it is NOT a member write - the spawn's combine is the only reason
	// it must not be anchored.
	proxy.HandleTask("sc/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("log", []string{"spawn-delta"})
		return nil
	})
	proxy.HandleTask("sc/work", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("sc/join", func(ctx context.Context, f *workflow.Flow) error {
		var log []string
		testarossa.For(t).NoError(f.Get("log", &log))
		f.SetInt("logLen", len(log))
		return nil
	})

	base := make([]string, 400)
	for i := range base {
		base[i] = blob(64)
	}
	pages := []string{"a", "b", "c"}
	_, outcome, err := e.Run(ctx, "sc/wf", map[string]any{"log": base, "pages": pages}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	// 400 accumulated base + the spawn's one appended entry. Anchoring the spawn's combined output would have
	// dropped the base and read back 1.
	assert.Equal(401.0, outcome.State["logLen"])

	// The fan-in step inlines the combined value rather than ref'ing it at the spawn (whose row holds only the delta).
	join := stepIDsByTask(t, e, "Join")[0]
	joinState, joinRefs, _ := stepRefsOf(t, e, join)
	assert.NotContains(joinRefs, "log", "the spawn's combined output cannot be anchored at the spawn - its row holds only the delta")
	assert.Contains(joinState, "log", "the fan-in step inlines the combined value, anchoring it downstream")
}
