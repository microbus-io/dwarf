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
	"sort"
	"strconv"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// State refs de-duplicate a large state field across the steps that merely CARRY it. A step's `state` is a
// full input snapshot, so a field that survives D steps of a chain is re-serialized D times, and N·D times
// across a fan-out of N branches - and bytes are the binding resource for large-state workflows (a shard
// sustaining ~4,600 steps/s on small state caps near ~700-900 steps/s rewriting 64KB per step, bound by the
// instance's disk/WAL path). So a field above a size bar is NOT written into the successor's `state` at all:
// it is omitted, and `state_refs` records `{"<field>": <anchor step_id>}` - the step that physically holds
// the bytes.
//
// THE BYTES CAN BE IN EITHER PAYLOAD COLUMN OF THE ANCHOR, and resolution reads both (`changes` shadowing
// `state`, being the newer value). Three places they sit, and only the first is the "obvious" one:
//
//   - a task produced the value -> the anchor's `changes`;
//   - the flow's INITIAL INPUT -> the ENTRY step's `state` (no task produced it, so it is in no `changes`
//     anywhere - and it is the headline case: a large initialState, or a Continue carry-forward);
//   - a fan-in reducer's OUTPUT -> the FAN-IN step's `state` (the merged value exists nowhere else: not in
//     the spawn's row, not in any one member's changes).
//
// A changes-only resolver silently misses the last two.
//
// Two invariants constrain every edit here:
//
//  1. Refs live only in `state`, never in `changes`. `changes` is always a task's literal output (or a
//     reducer's literal result). This is what makes ONE refs column sufficient, and what makes the
//     changes-shadows-state rule safe - there is no case where both sides are indirections.
//  2. Refs are ONE HOP: an anchor holds the bytes, never another ref. This mostly holds by construction (a
//     ref is a few bytes, so it never trips the size bar and is never re-minted) - but that is a side effect
//     of a SIZE comparison, and the fan-out policy below is deliberately size-INdependent. So mintStateRefs
//     carries an inherited ref forward unconditionally and never re-mints it. resolveStateRefs asserts the
//     invariant rather than walking a chain.
type stateRefs map[string]int

const (
	// stateRefFloor is the size below which a field is never ref'd: the bookkeeping (a refs entry, a place in
	// the resolve IN-list) is not worth it, and a small field costs little to copy.
	stateRefFloor = 1024
	// stateRefThreshold is the bar for OPENING a new anchor for a single (linear) successor.
	stateRefThreshold = 4096
	// stateRefBudget is one round-trip's worth of avoided writes, from the measured constants
	// (docs/benchmark-cloud.md): per-step DB time is k*L + s (k ~ 11-12 round-trips, s ~ 4.4ms) and the byte
	// ceiling is 46-60 MB/s, so same-zone (L ~ 0.5ms) one extra round-trip costs about what ~23KB of avoided
	// writes buys. Divided by the fan-out width, it is the bar for opening an anchor whose cost is amortized
	// over N branches - see stateRefOpenThreshold.
	stateRefBudget = 23 * 1024
	// maxStateAnchors bounds the resolve IN-list. It is the SAME knob as the threshold seen from the other
	// end (the cost model prices anchor ROWS), and the scheme is correct at any value.
	maxStateAnchors = 4
)

// stateRefOpenThreshold is the size bar for opening a NEW anchor, given how many successor steps are about to
// carry this state. Fan-out width is the primary axis, not a refinement:
//
//   - LINEAR (n=1): cost is ~D resolves down the chain and savings are S*D - the depth cancels, so break-even
//     sits near stateRefBudget. Refs barely pay for a pure linear carry, which is why the bar stays high.
//   - FAN-OUT (n>1): the resolve cache collapses N resolves into one miss plus N-1 hits (every branch resolves
//     the SAME anchor set), while savings are S*N. Break-even becomes S*N >= stateRefBudget, so the bar falls
//     as 1/N - at N=100 even a few hundred bytes pays for itself.
//
// The floor still applies, so a wide fan-out refs everything meaningful and nothing trivial.
func stateRefOpenThreshold(successors int) int {
	if successors < 1 {
		successors = 1
	}
	return max(stateRefFloor, min(stateRefThreshold, stateRefBudget/successors))
}

// rawEncode re-encodes a materialized state map as one raw message per field, which is both how field sizes
// are measured (exactly, on the ENCODED form) and how the state column is then marshalled - so nothing is
// marshalled twice. Values already carrying json.RawMessage (a task's own writes, via Flow.record) pass
// through untouched.
func rawEncode(state map[string]any) (map[string]json.RawMessage, error) {
	if len(state) == 0 {
		return map[string]json.RawMessage{}, nil
	}
	out := make(map[string]json.RawMessage, len(state))
	for k, v := range state {
		if raw, ok := v.(json.RawMessage); ok {
			out[k] = raw
			continue
		}
		data, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		out[k] = data
	}
	return out, nil
}

// mintStateRefs decides which of a successor step's state fields are stored by reference rather than by
// value. It returns the JSON to write into `state` (the ref'd fields omitted) and into `state_refs`.
//
// merged is the successor's fully MATERIALIZED state (refs resolved at dispatch, then the task's changes
// overlaid), changes is the accumulated delta that produced it, inherited is the DISPATCHED step's own refs,
// and anchorID is that step - the only step whose row can newly anchor anything here, because every candidate
// field's bytes are either in its `changes` (the task just wrote them) or in its `state` (carried, and not
// itself a ref). That single-candidate-anchor fact is what makes the free tier free: once any one field opens
// the anchor, every other field in that row rides along at zero extra read cost.
//
// exclude names fields that must never be ref'd because their bytes are in NO step row - the forEach element
// and its ordinal context, which the engine synthesizes per branch at spawn time. Ref'ing one would dangle.
//
// Minting needs no database read: state was resolved at dispatch, so every literal is already in hand and
// "inlining" is simply declining to omit a field.
func mintStateRefs(merged map[string]any, changes map[string]any, inherited stateRefs, anchorID int, successors int, exclude map[string]bool) (string, string, error) {
	raw, err := rawEncode(merged)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	refs := stateRefs{}
	type candidate struct {
		key  string
		size int
	}
	var candidates []candidate
	for k, v := range raw {
		if exclude[k] {
			continue
		}
		// A field the task did not rewrite, and which arrived as a ref, KEEPS that ref - it is never
		// re-minted against this step, whose row does not hold the bytes. This is the one-hop guard, and it
		// is unconditional precisely because the fan-out policy below is not size-based (invariant 2).
		if anchor, isRef := inherited[k]; isRef {
			if _, rewritten := changes[k]; !rewritten {
				refs[k] = anchor
				delete(raw, k)
				continue
			}
		}
		if len(v) >= stateRefFloor {
			candidates = append(candidates, candidate{k, len(v)})
		}
	}

	// Does this step's row earn an anchor? Every candidate shares the same prospective anchor (this step), so
	// the SUM is the right test: four 1KB fields justify one round-trip exactly as one 4KB field does. Summing
	// is legitimate only BECAUSE they are co-located - four 1KB fields at four different anchors would mean
	// four whole-row overfetches for the same 4KB, a far worse trade, and that is never what is summed here.
	sum := 0
	for _, c := range candidates {
		sum += c.size
	}
	if sum >= stateRefOpenThreshold(successors) {
		for _, c := range candidates {
			refs[c.key] = anchorID
			delete(raw, c.key)
		}
	}

	// Carry forward an inherited ref whose key never appeared in `merged`. The loop above only sees keys
	// present in `merged` (via `raw`), which is every inherited key on the paths that resolve state in FULL at
	// dispatch (linear, fan-out branch). But the fan-in paths resolve only the fields their reducers fold
	// (resolveReducedRefs): a merely-CARRIED ref is deliberately not materialized, so its key is absent from
	// `merged` and the loop never carried it. Without this, that ref is neither materialized nor re-emitted and
	// the carried field is silently, permanently dropped from the fan-in step onward and from final_state. A
	// key already handled above (rewritten -> re-anchored, or excluded) is skipped; a tombstoned or
	// member-overwritten field appears in `changes`/`exclude` and is correctly left dropped.
	for k, anchor := range inherited {
		if _, inMerged := merged[k]; inMerged {
			continue
		}
		if exclude[k] {
			continue
		}
		if _, rewritten := changes[k]; rewritten {
			continue
		}
		refs[k] = anchor
	}

	inlineExcessAnchors(raw, refs, merged)

	stateJSON, err := json.Marshal(raw)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	refsJSON, err := json.Marshal(refs)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	return string(stateJSON), string(refsJSON), nil
}

// combinedReducerFields names the keys whose merged fan-in value came from a COMBINING (non-replace) reducer
// folding a delta the anchor step wrote onto a base it also held - reduce(state[k], changes[k]). That value
// exists in NO step row: the anchor's `changes` column holds only the delta, its `state` only the base. So it
// must never be minted as a ref against that anchor (resolution reads `changes` first and would splice back the
// bare delta, silently dropping the accumulated base). The fan-in mint therefore excludes these, inlining them
// as literals into the fan-in step's own `state`. A key present only in `changes` (no base) is left off: there
// merged[k] == changes[k], so the anchor's `changes` is a sound anchor and keeping the ref preserves the byte win.
func combinedReducerFields(state, changes map[string]any, reducers map[string]workflow.Reducer) map[string]bool {
	out := map[string]bool{}
	for k := range changes {
		if r := reducers[k]; r == "" || r == workflow.ReducerReplace {
			continue
		}
		if _, hasBase := state[k]; hasBase {
			out[k] = true
		}
	}
	return out
}

// inlineExcessAnchors caps the pointer fan at maxStateAnchors by inlining the cheapest anchors' fields back
// into state as literals - one copy, paid once, to shorten the resolve IN-list. It costs no database read:
// the literals are all present in merged (state was resolved at dispatch). Anchors are dropped smallest-bytes
// first, since those are the ones whose refs buy the least.
func inlineExcessAnchors(raw map[string]json.RawMessage, refs stateRefs, merged map[string]any) {
	if len(refs) == 0 {
		return
	}
	byAnchor := map[int][]string{}
	bytesOf := map[int]int{}
	// An anchor is PINNED (un-droppable) if any of its fields' literals are absent from `merged` - a merely
	// carried ref whose payload this step never materialized. Inlining it would require a literal we do not
	// have, so dropping it would delete the field outright: the exact data loss the carry-forward above closes.
	// Such a ref must survive the cap even if that means a wider IN-list (correctness over the perf bound).
	pinned := map[int]bool{}
	for k, anchor := range refs {
		byAnchor[anchor] = append(byAnchor[anchor], k)
		if v, ok := merged[k]; ok {
			if data, err := json.Marshal(v); err == nil {
				bytesOf[anchor] += len(data)
			}
		} else {
			pinned[anchor] = true
		}
	}
	if len(byAnchor) <= maxStateAnchors {
		return
	}
	// Only inlineable (non-pinned) anchors are droppable, largest-bytes first so the survivors are the ones the
	// refs buy the most on. Ties break on the anchor id, so the choice is deterministic.
	droppable := make([]int, 0, len(byAnchor))
	pinnedCount := 0
	for a := range byAnchor {
		if pinned[a] {
			pinnedCount++
			continue
		}
		droppable = append(droppable, a)
	}
	sort.Slice(droppable, func(i, j int) bool {
		if bytesOf[droppable[i]] != bytesOf[droppable[j]] {
			return bytesOf[droppable[i]] > bytesOf[droppable[j]]
		}
		return droppable[i] < droppable[j]
	})
	// Keep the pinned anchors plus the largest droppable ones up to the cap; inline the rest.
	keepDroppable := max(0, maxStateAnchors-pinnedCount)
	for _, a := range droppable[min(keepDroppable, len(droppable)):] {
		for _, k := range byAnchor[a] {
			if v, ok := merged[k]; ok {
				if data, err := json.Marshal(v); err == nil {
					raw[k] = data
				}
			}
			delete(refs, k)
		}
	}
}

// parseStateRefs decodes the state_refs column. An empty/absent map is the overwhelmingly common case and
// allocates nothing.
func parseStateRefs(refsJSON string) stateRefs {
	refsJSON = strings.TrimSpace(refsJSON)
	if refsJSON == "" || refsJSON == "{}" {
		return nil
	}
	var refs stateRefs
	if err := json.Unmarshal([]byte(refsJSON), &refs); err != nil {
		return nil
	}
	if len(refs) == 0 {
		return nil
	}
	return refs
}

// stateRefCacheKey identifies one ref'd field's bytes. Immutable once the anchor step settles (a terminal
// step is immutable, and flow.Retry rewrites a step's changes only BEFORE any successor exists - invariant
// 8), so it is as good a cache key as a content hash. Shard-scoped: step_id is only unique within a shard.
func stateRefCacheKey(shardNum, anchorID int, field string) string {
	return strconv.Itoa(shardNum) + ":" + strconv.Itoa(anchorID) + ":" + field
}

// resolveStateRefs materializes ref'd fields into state, in place. It is the read side of the whole design,
// and the reason the anchor - not the field - is what policy prices: this fetches whole payload columns, so
// the cost is one row per DISTINCT anchor (and, on a cache miss, one round-trip for all of them together),
// while any number of fields living in an already-fetched row are free.
//
// want, when non-nil, selects which refs to resolve; the rest are left for the caller to carry forward. That
// is what lets a fan-in reduce the fields it must while a large CARRIED field crosses the cohort as a ref
// (see resolveReducedRefs).
//
// db may be a transaction - anchors never cross a flow, and a flow never crosses a shard (invariant 7), so
// resolution is always a same-connection read and the fan-in and final-state paths can resolve inside the
// transactions they already run in.
func (e *Engine) resolveStateRefs(ctx context.Context, db sequel.Executor, shardNum int, state map[string]any, refs stateRefs, want map[string]bool, workflowURL string) error {
	if len(refs) == 0 || state == nil {
		return nil
	}
	// Serve what the cache can, and collect the anchors still needed.
	pending := map[string]int{}
	anchorSet := map[int]bool{}
	for field, anchor := range refs {
		if want != nil && !want[field] {
			continue
		}
		if cached, ok := e.stateRefCache.Load(stateRefCacheKey(shardNum, anchor, field)); ok {
			if err := spliceResolved(state, field, cached); err != nil {
				return errors.Trace(err)
			}
			continue
		}
		pending[field] = anchor
		anchorSet[anchor] = true
	}
	if len(pending) == 0 {
		return nil
	}

	anchors := make([]any, 0, len(anchorSet))
	for a := range anchorSet {
		anchors = append(anchors, a)
	}
	// One query, not k round-trips: the anchors are primary keys, so scattered anchors cost a wider IN-list
	// rather than more round-trips. state_refs comes along so the one-hop invariant can be ASSERTED rather
	// than trusted - a violated one would otherwise degrade silently into a chain walk.
	ph := strings.Repeat("?,", len(anchors)-1) + "?"
	rows, err := db.QueryContext(ctx,
		"SELECT step_id, state, changes, state_refs FROM dwarf_steps WHERE step_id IN ("+ph+")",
		anchors...,
	)
	if err != nil {
		return errors.Trace(err)
	}
	type anchorRow struct {
		state   map[string]json.RawMessage
		changes map[string]json.RawMessage
		refs    stateRefs
	}
	fetched := map[int]anchorRow{}
	readBytes := 0
	for rows.Next() {
		var anchorID int
		var stateJSON, changesJSON, refsJSON string
		if err := rows.Scan(&anchorID, &stateJSON, &changesJSON, &refsJSON); err != nil {
			rows.Close()
			return errors.Trace(err)
		}
		readBytes += len(stateJSON) + len(changesJSON)
		row := anchorRow{refs: parseStateRefs(refsJSON)}
		json.Unmarshal([]byte(stateJSON), &row.state)
		json.Unmarshal([]byte(changesJSON), &row.changes)
		fetched[anchorID] = row
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return errors.Trace(err)
	}
	e.metricStateReadBytes(ctx, workflowURL, "state_ref", readBytes)

	for field, anchor := range pending {
		row, ok := fetched[anchor]
		if !ok {
			return errors.New("state ref %q points at missing step %d", field, anchor)
		}
		// changes shadows state: the anchor's changes hold the value its task PRODUCED, while its state holds
		// a value it merely received (the flow's initial input at the entry step, or a fan-in's reduced
		// output). Both are legitimate anchor locations; changes is the newer of the two.
		value, found := row.changes[field]
		if !found {
			value, found = row.state[field]
		}
		if !found {
			if _, isRef := row.refs[field]; isRef {
				return errors.New("state ref %q resolves to step %d, which itself refs it - one-hop violated", field, anchor)
			}
			return errors.New("state ref %q not found in step %d", field, anchor)
		}
		e.stateRefCache.Store(stateRefCacheKey(shardNum, anchor, field), value)
		if err := spliceResolved(state, field, value); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// spliceResolved decodes an anchor's raw bytes into the state map, so a resolved field is INDISTINGUISHABLE
// from one that was never ref'd. Two reasons this must decode rather than drop the json.RawMessage in:
//
//   - Type. Every other path builds state via unmarshalJSONMap, so a value is a decoded string/float64/map.
//     A RawMessage would leak the storage encoding into API surface (FlowStep.State, FlowOutcome.State), where
//     a caller's `state["pdf"].(string)` would simply stop matching - the ref encoding is internal, and this is
//     what keeps it that way.
//   - Isolation. The cache holds the immutable BYTES and every resolve decodes its own copy, so two fan-out
//     branches resolving the same anchor never share a decoded map or slice. Caching the decoded value would
//     alias it across the branch goroutines that are about to hand it to concurrent tasks.
func spliceResolved(state map[string]any, field string, raw json.RawMessage) error {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return errors.Trace(err)
	}
	state[field] = decoded
	return nil
}

// resolveReducedRefs materializes only the ref'd fields a fan-in's reducers actually need, into state in place.
//
// A reducer that COMBINES (append/add/union/merge/min/max/and/or/concat) needs its accumulated base value, so
// a ref'd field it touches must be materialized or the fold would apply the delta to an absent base and lose
// everything accumulated so far. The default REPLACE reducer needs nothing: if a branch wrote the field, its
// literal wins outright (and the stale ref is dropped by the caller's mint); if no branch wrote it, the field
// is carried and the ref rides forward untouched.
//
// So a field is materialized iff the graph registers a reducer for it. That is a safe superset - a registered
// ReducerReplace merely costs a wasted fetch - and it is what keeps a large CARRIED field (the motivating
// case: a document fanned out over its pages) from being materialized and re-anchored at every fan-in, which
// would hand back the win in precisely the fan-out graphs the design exists for.
//
// A merely-carried (non-reduced) ref is left un-materialized here and re-emitted onto the fan-in step by
// mintStateRefs' inherited-carry-forward (which handles a ref whose key is absent from the merged state,
// exactly the case a non-materialized carry produces). That is why this returns nothing: the carried set is
// not the caller's to thread - mintStateRefs recovers it from the same `inherited` refs it is already passed.
func (e *Engine) resolveReducedRefs(ctx context.Context, tx sequel.Executor, shardNum int, state map[string]any, refs stateRefs, reducers map[string]workflow.Reducer, workflowURL string) error {
	if len(refs) == 0 {
		return nil
	}
	want := map[string]bool{}
	for field := range refs {
		if _, reduced := reducers[field]; reduced {
			want[field] = true
		}
	}
	if len(want) > 0 {
		if err := e.resolveStateRefs(ctx, tx, shardNum, state, refs, want, workflowURL); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// resolvedStepState materializes a step's state column into JSON with every ref'd field spliced back in - the
// flatten used where a state snapshot must stand on its own, detached from the anchors that backed it (Fork's
// leaf step, whose anchor ids are about to be remapped into a different flow).
func (e *Engine) resolvedStepState(ctx context.Context, db sequel.Executor, shardNum int, stateJSON, refsJSON, workflowURL string) (string, error) {
	refs := parseStateRefs(refsJSON)
	if len(refs) == 0 {
		return stateJSON, nil
	}
	var state map[string]any
	unmarshalJSONMap(stateJSON, &state)
	if state == nil {
		state = map[string]any{}
	}
	if err := e.resolveStateRefs(ctx, db, shardNum, state, refs, nil, workflowURL); err != nil {
		return "", errors.Trace(err)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(data), nil
}
