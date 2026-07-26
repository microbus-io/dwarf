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

package staterefs

import (
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// blob returns a JSON string value of roughly n encoded bytes.
func blob(n int) string {
	return strings.Repeat("x", n)
}

// TestOpenThreshold pins the anchor-cost model's central claim: the bar for opening a new anchor is
// set by the FAN-OUT WIDTH, not by the field alone. A linear successor must clear the full threshold (refs
// barely pay for a pure linear carry - cost is ~D resolves and savings are S*D, so the depth cancels), while a
// wide fan-out amortizes one cache-missed resolve over N branches and so refs far smaller fields.
func TestOpenThreshold(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Linear: the full threshold, never lower.
	assert.Equal(threshold, openThreshold(1, 1))
	assert.Equal(threshold, openThreshold(0, 1)) // degenerate n is clamped to 1

	// Fan-out: the bar falls as budget/N ...
	assert.Equal(budget/8, openThreshold(8, 1))
	assert.True(openThreshold(8, 1) < threshold)

	// ... and KEEPS falling past floor, down to minField. Clamping at the floor instead made
	// the 1/N term dead code for every fan-out wider than budget/floor = 23, which is what let
	// a forEach source array stay inline in all N branches - the quadratic case this policy exists to stop.
	assert.Equal(budget/100, openThreshold(100, 1))
	assert.True(openThreshold(100, 1) < floor)
	assert.Equal(minField, openThreshold(10000, 1)) // floors at minField, never 0

	// The dialect factor divides the bar, and applies only at a fan-out.
	assert.Equal(1, New("pgx").inflation(1))
	assert.Equal(postgresInflation, New("pgx").inflation(64))
	assert.Equal(1, New("sqlite").inflation(64))
	assert.Equal(1, New("mysql").inflation(64))
	assert.Equal(openThreshold(64, 1)/postgresInflation, openThreshold(64, postgresInflation))
	// Linear is untouched by the dialect on every driver - that is where estimate accuracy would decide cases.
	assert.Equal(threshold, openThreshold(1, New("pgx").inflation(1)))

	// Monotone in N: more branches never raise the bar.
	for n := 1; n < 64; n++ {
		assert.True(openThreshold(n+1, 1) <= openThreshold(n, 1))
	}
}

// TestCandidateFloor pins the per-field candidacy bar. Once an anchor is open the marginal read
// cost of one more field is zero (they share the row), so the only question is whether a field outweighs its
// own state_refs entry - a test in which N cancels, since saving and cost both scale with it.
func TestCandidateFloor(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Linear reproduces the old flat floor exactly.
	assert.Equal(floor, candidateFloor(1))
	assert.Equal(floor, candidateFloor(0))

	// Fan-out scales down, but never below the refs-entry break-even: a field smaller than its own entry
	// LOSES bytes on every branch, so the floor is what keeps "ref everything" from becoming a regression.
	assert.Equal(floor/8, candidateFloor(8))
	assert.Equal(minField, candidateFloor(64))
	assert.Equal(minField, candidateFloor(10000))

	for n := 1; n < 64; n++ {
		assert.True(candidateFloor(n+1) <= candidateFloor(n))
		assert.True(candidateFloor(n) >= minField)
	}
}

// TestLinker_MintTiers pins the three tiers of the anchor-cost policy directly on Mint, where the
// decision is made. The read cost of a ref is paid per ANCHOR ROW, never per field - so every case here is
// really asking "is this row worth opening?", and once it is open everything else in it is free.
func TestLinker_MintTiers(t *testing.T) {
	t.Parallel()
	const anchor = 42

	mint := func(t *testing.T, state map[string]any, changes map[string]any, inherited Refs, successors int) (workflow.State, Refs) {
		t.Helper()
		assert := testarossa.For(t)
		stateJSON, refsJSON, err := New("sqlite").Mint(state, changes, inherited, anchor, successors, nil)
		assert.NoError(err)
		var stored workflow.State
		_ = stored.UnmarshalJSON(stateJSON)
		return stored, Parse(refsJSON)
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
		stored, refs := mint(t, state, map[string]any{}, Refs{"doc": 7}, 1)
		assert.Equal(7, refs["doc"], "a carried ref keeps its original anchor")
		assert.NotContains(stored, "doc")
	})

	t.Run("rewritten_ref_is_reanchored_here", func(t *testing.T) {
		assert := testarossa.For(t)
		// The task REWROTE the field, so its bytes are now in this step's changes and the inherited anchor is
		// stale. The ref must move to this step.
		state := map[string]any{"doc": blob(8192)}
		changes := map[string]any{"doc": blob(8192)}
		_, refs := mint(t, state, changes, Refs{"doc": 7}, 1)
		assert.Equal(anchor, refs["doc"], "a rewritten field re-anchors at the step that wrote it")
	})

	t.Run("inline_only_fields_are_never_refd", func(t *testing.T) {
		assert := testarossa.For(t)
		// The forEach element is SYNTHESIZED per branch - its bytes are in no step row - so a ref to it would
		// dangle. Even a huge element stays inline. (The same inlineOnly set carries a cohort member's contribution
		// past the fan-in's mint, whose bytes are likewise not in the anchor's row.)
		state := map[string]any{"item": blob(50000), "doc": blob(50000)}
		stateJSON, refsJSON, err := New("sqlite").Mint(state, map[string]any{}, nil, anchor, 32, map[string]bool{"item": true})
		assert.NoError(err)
		var stored workflow.State
		_ = stored.UnmarshalJSON(stateJSON)
		refs := Parse(refsJSON)
		assert.NotContains(refs, "item", "an inline-only field is never ref'd, however large")
		assert.Contains(stored, "item")
		assert.Equal(anchor, refs["doc"], "its neighbours still ref normally")
		assert.NotContains(stored, "doc")
	})

	t.Run("foreach_source_array_is_refd_at_width", func(t *testing.T) {
		assert := testarossa.For(t)
		// THE motivating case, and the one that regressed silently for as long as the open threshold clamped at
		// floor. A forEach's branch count IS its source array's element count, so leaving the array
		// inline costs N copies of an N-element array - quadratic, and worse under nesting. A 64-element int
		// array marshals to ~183 bytes, which the old flat 1024 bar rejected outright.
		items := make([]any, 64)
		for i := range items {
			items[i] = i
		}
		state := map[string]any{"items": items, "item": 7, "itemIndex": 7, "itemCount": 64}
		noRef := map[string]bool{"item": true, "itemIndex": true, "itemCount": true}

		// PostgreSQL: jsonb stores this array ~4.7x larger than its text length, which the dialect factor
		// corrects for, so the array refs.
		stateJSON, refsJSON, err := New("pgx").Mint(state, map[string]any{}, nil, anchor, 64, noRef)
		assert.NoError(err)
		var stored workflow.State
		_ = stored.UnmarshalJSON(stateJSON)
		refs := Parse(refsJSON)
		assert.Equal(anchor, refs["items"], "the forEach source array must be ref'd at width 64")
		assert.NotContains(stored, "items", "one stored copy on the spawn row, not one per branch")
		// The synthesized per-branch bookkeeping is still never ref'd - its bytes are in no step row.
		for _, k := range []string{"item", "itemIndex", "itemCount"} {
			assert.NotContains(refs, k)
			assert.Contains(stored, k)
		}

		// A narrow fan-out keeps it inline: at width 4 the array is ~9 bytes and the quadratic term is nothing.
		few := make([]any, 4)
		for i := range few {
			few[i] = i
		}
		_, refsFew := mint(t, map[string]any{"items": few}, map[string]any{}, nil, 4)
		assert.NotContains(refsFew, "items", "a 4-element array is not worth a refs entry")
	})

	t.Run("subrefsentry_field_stays_inline_at_any_width", func(t *testing.T) {
		assert := testarossa.For(t)
		// The floor that keeps "ref everything on a fan-out" from becoming a regression: a field smaller than
		// its own state_refs entry loses bytes on every branch, so no width may drag it below minField.
		state := map[string]any{"tiny": "abc", "flag": true, "n": 42}
		_, refs := mint(t, state, map[string]any{}, nil, 4096)
		assert.Equal(0, len(refs), "trivial scalars are never ref'd, however wide the fan-out")
	})

	t.Run("max_anchors_inlines_the_cheapest", func(t *testing.T) {
		assert := testarossa.For(t)
		// More distinct anchors than maxAnchors: the pointer fan is capped by inlining the anchors whose
		// refs buy the least (fewest bytes), paying one copy to shorten the resolve IN-list. maxAnchors is
		// the same knob as the threshold - the cost model prices ROWS.
		state := map[string]any{}
		inherited := Refs{}
		for i := range maxAnchors + 2 {
			field := "f" + string(rune('0'+i))
			state[field] = blob(2000 + i*1000) // f0 smallest ... f5 largest
			inherited[field] = 100 + i
		}
		stored, refs := mint(t, state, map[string]any{}, inherited, 1)
		assert.Equal(maxAnchors, len(refs), "the pointer fan is capped")
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
		stored, refs := mint(t, merged, map[string]any{}, Refs{"pdf": 7}, 1)
		assert.Equal(7, refs["pdf"], "a carried ref whose payload is not in merged must still be re-emitted")
		assert.NotContains(stored, "pdf")
		assert.Contains(stored, "local")
	})

	t.Run("carried_ref_absent_from_merged_but_rewritten_is_dropped", func(t *testing.T) {
		assert := testarossa.For(t)
		// A tombstone (member/spawn deleted the carried field) leaves the key in `changes` and out of `merged`.
		// The stale carried ref must NOT be resurrected - the field is genuinely gone.
		_, refs := mint(t, map[string]any{}, map[string]any{"pdf": nil}, Refs{"pdf": 7}, 1)
		assert.NotContains(refs, "pdf", "a tombstoned carried field must not be re-emitted through its stale ref")
	})

	t.Run("un_inlineable_carried_anchor_survives_the_cap", func(t *testing.T) {
		assert := testarossa.For(t)
		// More distinct anchors than maxAnchors, where one anchor is a CARRIED ref whose payload is absent
		// from `merged` (un-inlineable). The cap must never drop it - there is no literal to inline it back to,
		// so dropping would delete the field. It survives even if that means keeping more than maxAnchors.
		merged := map[string]any{}
		inherited := Refs{}
		for i := range maxAnchors + 1 {
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
