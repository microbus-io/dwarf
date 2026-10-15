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
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"slices"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// reduced describes which part of a fan-in step's value for a field its cohort added, as the step's
// state_reduced column records it per field. Exactly one form is set, chosen by the field's reducer:
//   - From, for append, union and concat: the cohort's part starts at this offset into the value (array
//     elements, or bytes of a concat string);
//   - Keys, for merge: the keys the cohort wrote; and for a field with no reducer, an empty list meaning only
//     that the cohort wrote it;
//   - Value, for add, min, max, and and or: the cohort's combined contribution itself; and for every field of a
//     Fork's re-run leaf (frozenReduced).
type reduced struct {
	From  *int            `json:"from,omitempty"`
	Keys  []string        `json:"keys,omitzero"`
	Value json.RawMessage `json:"value,omitzero"`
}

// reducedColumn is what a state_reduced column stores for rec: its JSON, or an empty object.
func reducedColumn(rec map[string]*reduced) []byte {
	if len(rec) == 0 {
		return []byte("{}")
	}
	b, err := json.Marshal(rec)
	if err != nil {
		// Every property is an int, a string list or a Value that was itself produced as JSON.
		panic(errors.Trace(err))
	}
	return b
}

// foldCohort folds a cohort's contributions onto the fan-in's base, in order, and, if describe is set, returns
// for every field a contribution mentions which part of the folded value the cohort added. It mutates base; on
// an error base is partly folded and must be discarded.
//
// The description is what lets the next cohort out take this fan-in's contribution from its one row, rather
// than re-reading every member below it (see projectReduced).
func foldCohort(base workflow.State, contributions []workflow.Changes, reducers map[string]workflow.Reducer, describe bool) (map[string]*reduced, error) {
	if !describe {
		return nil, errors.Trace(base.ApplyAll(contributions, reducers))
	}
	// A field is described if the cohort wrote it. On a reduced field a null is the reducer's identity, so only
	// a write with a value counts; on a plain field a delete is a write like any other.
	written := map[string]bool{}
	for _, c := range contributions {
		for k := range c.Names() {
			if r := reducers[k]; r == "" || r == workflow.ReducerReplace || c.Contributions().Has(k) || c.Overwrites().Has(k) {
				written[k] = true
			}
		}
	}
	rec := make(map[string]*reduced, len(written))
	for k := range written {
		r := reducers[k]
		switch r {
		case workflow.ReducerAppend, workflow.ReducerUnion, workflow.ReducerConcat:
			// These only ever add to the END of the base, so the base is a prefix of the folded value and the
			// cohort's part is everything past it.
			from, err := lengthOf(base.GetJSON(k), r)
			if err != nil {
				return nil, errors.New("reducer '%s' for field %q: %w", string(r), k, err)
			}
			rec[k] = &reduced{From: &from}
		case workflow.ReducerMerge:
			// New wins, so for a key the cohort wrote the folded value IS the cohort's value.
			keys := map[string]bool{}
			for _, c := range contributions {
				if !c.Contributions().Has(k) {
					continue
				}
				var obj map[string]json.RawMessage
				if json.Unmarshal(c.Contributions().GetJSON(k), &obj) == nil {
					for key := range obj {
						keys[key] = true
					}
				}
			}
			rec[k] = &reduced{Keys: slices.Sorted(maps.Keys(keys))}
		case "", workflow.ReducerReplace:
			rec[k] = &reduced{Keys: []string{}}
		default:
			// A sum loses its split and a minimum loses its base, so the cohort's part cannot be read back out of
			// the folded value. It is kept instead: one number or bool. Numbers decode as json.Number, so the
			// record holds them exactly, as the state's own fold does.
			var acc any
			for _, c := range contributions {
				if !c.Contributions().Has(k) {
					continue
				}
				var inc any
				dec := json.NewDecoder(bytes.NewReader(c.Contributions().GetJSON(k)))
				dec.UseNumber()
				if err := dec.Decode(&inc); err != nil {
					return nil, errors.New("reducer '%s' for field %q: %w", string(r), k, err)
				}
				var err error
				if acc, err = r.Reduce(acc, inc); err != nil {
					return nil, errors.New("reducer '%s' for field %q: %w", string(r), k, err)
				}
			}
			if acc == nil {
				continue // only identity writes
			}
			value, err := json.Marshal(acc)
			if err != nil {
				return nil, errors.New("reducer '%s' for field %q: %w", string(r), k, err)
			}
			rec[k] = &reduced{Value: value}
		}
	}
	if err := base.ApplyAll(contributions, reducers); err != nil {
		return nil, errors.Trace(err)
	}
	return rec, nil
}

// lengthOf is the offset a cohort's part starts at: the element count of an array, or the byte length of a
// string for concat. A field with no value has length zero.
func lengthOf(raw json.RawMessage, r workflow.Reducer) (int, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return 0, nil
	}
	if r == workflow.ReducerConcat {
		var s string
		err := json.Unmarshal(raw, &s)
		return len(s), errors.Trace(err)
	}
	var arr []json.RawMessage
	err := json.Unmarshal(raw, &arr)
	return len(arr), errors.Trace(err)
}

// projectReduced returns what a fan-in step's cohort added, read from the step's state and the record foldCohort
// stored in its state_reduced column: Changes that fold onto an outer cohort's accumulator exactly as the inner
// cohort's contributions would have, one by one. A field with no reducer projects as its value, or as a delete if
// the cohort left it absent. The reducer is taken from the graph, never from the stored record.
func projectReduced(state workflow.State, rec map[string]*reduced, reducers map[string]workflow.Reducer) (workflow.Changes, error) {
	part := workflow.NewChanges()
	contributed := part.Contributions()
	for k, rd := range rec {
		r := reducers[k]
		raw := state.GetJSON(k)
		switch {
		case rd.From != nil:
			from := *rd.From
			if r == workflow.ReducerConcat {
				var s string
				if len(raw) > 0 {
					if err := json.Unmarshal(raw, &s); err != nil {
						return workflow.Changes{}, errors.New("field %q: %w", k, err)
					}
				}
				if err := contributed.Set(k, s[min(from, len(s)):]); err != nil {
					return workflow.Changes{}, errors.Trace(err)
				}
				continue
			}
			var arr []json.RawMessage
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &arr); err != nil {
					return workflow.Changes{}, errors.New("field %q: %w", k, err)
				}
			}
			tail := arr[min(from, len(arr)):]
			if tail == nil {
				tail = []json.RawMessage{}
			}
			if err := contributed.Set(k, tail); err != nil {
				return workflow.Changes{}, errors.Trace(err)
			}
		case rd.Value != nil:
			contributed.SetJSON(k, rd.Value)
		case r == workflow.ReducerMerge:
			var obj map[string]json.RawMessage
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &obj); err != nil {
					return workflow.Changes{}, errors.New("field %q: %w", k, err)
				}
			}
			kept := make(map[string]json.RawMessage, len(rd.Keys))
			for _, key := range rd.Keys {
				if v, ok := obj[key]; ok {
					kept[key] = v
				}
			}
			if err := contributed.Set(k, kept); err != nil {
				return workflow.Changes{}, errors.Trace(err)
			}
		default:
			if state.Has(k) {
				contributed.SetJSON(k, raw)
			} else if err := part.Overwrite(k, nil); err != nil {
				return workflow.Changes{}, errors.Trace(err)
			}
		}
	}
	return part, nil
}

// frozenReduced is rec with every field's part projected from state and kept as a Value. A Fork's leaf replaces
// the step's state with the overridden one, which no longer holds what the cohort added at the recorded offsets
// and keys; an override edits the step's input and leaves what its cohort wrote alone, so the part is taken
// from the original state first. A field the cohort deleted is kept as a null Value: a null contribution deletes
// a field with no reducer.
func frozenReduced(state workflow.State, rec map[string]*reduced, reducers map[string]workflow.Reducer) (map[string]*reduced, error) {
	part, err := projectReduced(state, rec, reducers)
	if err != nil {
		return nil, errors.Trace(err)
	}
	frozen := make(map[string]*reduced, part.Len())
	for k := range part.Names() {
		value := part.Contributions().GetJSON(k)
		if !part.Contributions().Has(k) {
			value = json.RawMessage("null")
		}
		frozen[k] = &reduced{Value: value}
	}
	return frozen, nil
}

// memberContribution is what one cohort member adds to its cohort's fold. part is what the member's own cohort
// added if the member is an inner fan-in (cohortParts), and the zero Changes otherwise; changes are the member's
// own changes if it completed, and the zero Changes otherwise. ok is false for a member that adds nothing.
//
// An inner fan-in's part counts whatever the fan-in step's own status: every step of its cohort completed, and
// the fan-in step only exists because the cohort converged. Its own writes count only if it completed, like any
// member's.
func memberContribution(part, changes workflow.Changes, reducers map[string]workflow.Reducer) (contribution workflow.Changes, ok bool, err error) {
	if part.IsZero() {
		return changes, !changes.IsZero(), nil
	}
	if changes.IsZero() {
		return part, true, nil
	}
	if err := part.Compose(changes, reducers); err != nil {
		return workflow.Changes{}, false, errors.Trace(err)
	}
	return part, true, nil
}

// innerFanInIDs returns the members of a cohort that are themselves fan-in steps, whatever their status.
func innerFanInIDs(graph *workflow.Graph, members []cohortMember) []int {
	var ids []int
	for _, m := range members {
		if graph.IsFanIn(strings.TrimSpace(m.taskName)) {
			ids = append(ids, m.stepID)
		}
	}
	return ids
}

// cohortParts reads the state of the given inner fan-in steps and returns, keyed by step_id, what each one's
// cohort added (projectReduced), along with the bytes it read. Each row is projected as it is read and then
// dropped, so a wide cohort holds the parts rather than every inner fan-in's full state.
func cohortParts(ctx context.Context, db sequel.Executor, stepIDs []int, reducers map[string]workflow.Reducer) (map[int]workflow.Changes, int, error) {
	parts := make(map[int]workflow.Changes, len(stepIDs))
	read := 0
	for chunk := range slices.Chunk(stepIDs, inListChunk) {
		ph, args := inList(chunk)
		rows, err := db.QueryContext(ctx,
			"SELECT step_id, state, state_reduced FROM dwarf_steps WHERE step_id IN ("+ph+")",
			args...)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		for rows.Next() {
			var id int
			var stateJSON, reducedJSON []byte
			if err := rows.Scan(&id, &stateJSON, &reducedJSON); err != nil {
				rows.Close()
				return nil, 0, errors.Trace(err)
			}
			read += len(stateJSON) + len(reducedJSON)
			state, _ := workflow.NewState(stateJSON)
			var rec map[string]*reduced
			if err := json.Unmarshal(reducedJSON, &rec); err != nil {
				rows.Close()
				return nil, 0, errors.Trace(&permanentError{err})
			}
			part, err := projectReduced(state, rec, reducers)
			if err != nil {
				rows.Close()
				return nil, 0, errors.Trace(&permanentError{err})
			}
			parts[id] = part
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
	}
	return parts, read, nil
}
