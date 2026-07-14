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
	"encoding/json"
	"net/http"

	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/internal/jsonx"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// graphCacheKey scopes the per-flow graph cache by shard, since flow_id is only unique within a shard.
type graphCacheKey struct {
	shard  int
	flowID int
}

// cachedGraph is the per-flow parsed graph paired with its derived fan-in map. The fan-in map is a pure
// function of the graph, not persisted with it, so it is computed once when a flow's graph is first parsed
// and cached alongside - amortizing the O(V+E) analysis across every step of the flow.
type cachedGraph struct {
	graph *workflow.Graph
	fanIn *faninmap.Map
}

// unmarshalJSONMap parses a JSON string into a map. An empty string (an absent/NULL column) yields a nil map;
// "{}" yields a non-nil EMPTY map. The distinction is load-bearing for state refs: a step whose entire state is
// carried by reference persists state="{}" alongside a non-empty state_refs, and resolveStateRefs skips a nil
// state map - so a nil map here would drop every ref and dispatch the task with empty state, losing the field
// from every downstream step and from final_state (silent, permanent). rawEncode always marshals to "{}" (never
// "null"), so this case is the exact write-side counterpart.
func unmarshalJSONMap(jsonStr string, out *map[string]any) {
	if jsonStr == "{}" {
		*out = map[string]any{}
		return
	}
	if jsonStr == "" {
		return
	}
	json.Unmarshal([]byte(jsonStr), out)
}

// canonicalStateMap round-trips a caller-supplied value into the shape every value read back from a state
// column already has: nested objects as map[string]any, numbers as float64. Reducers compare and dedupe on
// the MARSHALLED bytes of their operands, and those bytes are canonical only for a decoded value - Go sorts
// a map's keys but marshals a STRUCT's fields in declaration order, and a json.RawMessage passes through
// verbatim (keeping the author's key order, and a literal 1.0 that a float64 would have written as 1). So a
// raw Go value folded against the flow's accumulated state compares unequal to its own decoded twin: a union
// reducer keeps both spellings of one element, a merge reducer double-writes a key.
//
// Every other reducer input is decoded from the database on its way in (the fan-in merge, computeFinalState),
// which is what makes their byte comparison sound. Continue's additionalState is the one input that skips the
// database, so it is canonicalized here - keeping "reducers only ever see decoded values" an invariant rather
// than a coincidence. A nil value yields a nil map ("no additional state"); a value that is not a JSON object
// is a caller error.
//
// This is also Continue's storability ingress: additionalState is host input that never passes through
// createWithGraph's CheckStorable, so the check runs here on the MARSHALLED bytes - before the json.Unmarshal
// below rounds a >2^53 integer to float64, where it would slip through undetected. Checking the caller's raw
// input (not the merged carry-forward) is deliberate: a legitimate ReducerAdd sum past 2^53 is integer-shaped
// too, so checking the merge result would 400 a valid continuation.
func canonicalStateMap(v any) (map[string]any, error) {
	if v == nil {
		return nil, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, errors.New("additional state is not JSON-marshalable: %v", err, http.StatusBadRequest)
	}
	err = jsonx.CheckStorable(data)
	if err != nil {
		return nil, errors.New("invalid additional state: %v", err, http.StatusBadRequest)
	}
	var m map[string]any
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, errors.New("additional state must be a JSON object: %v", err, http.StatusBadRequest)
	}
	return m, nil
}
