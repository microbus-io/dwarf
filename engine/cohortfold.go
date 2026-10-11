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
	"maps"
	"slices"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// foldCohort folds a cohort's contributions onto the fan-in's base, in order, and, if describe is set, returns
// for every field a contribution mentions which part of the folded value the cohort added. It mutates base; on
// an error base is partly folded and must be discarded.
//
// The description is what lets the next cohort out take this fan-in's contribution from its one row, rather
// than re-reading every member below it (see projectReduced).
func foldCohort(base workflow.State, contributions []workflow.State, reducers map[string]workflow.Reducer, describe bool) (map[string]*workflow.Reduced, error) {
	if !describe {
		return nil, errors.Trace(base.MergeReduceAll(contributions, reducers))
	}
	// A field is described if the cohort wrote it. On a reduced field a cleared write is the reducer's identity,
	// so only a write with a value counts; on a plain field a delete is a write like any other.
	written := map[string]bool{}
	for _, c := range contributions {
		for k := range c.Names() {
			if c.Has(k) {
				written[k] = true
			} else if r := reducers[k]; r == "" || r == workflow.ReducerReplace {
				written[k] = true
			}
		}
	}
	reduced := make(map[string]*workflow.Reduced, len(written))
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
			reduced[k] = &workflow.Reduced{From: &from}
		case workflow.ReducerMerge:
			// New wins, so for a key the cohort wrote the folded value IS the cohort's value.
			keys := map[string]bool{}
			for _, c := range contributions {
				if !c.Has(k) {
					continue
				}
				var obj map[string]json.RawMessage
				if json.Unmarshal(c.GetJSON(k), &obj) == nil {
					for key := range obj {
						keys[key] = true
					}
				}
			}
			reduced[k] = &workflow.Reduced{Keys: slices.Sorted(maps.Keys(keys))}
		case "", workflow.ReducerReplace:
			reduced[k] = &workflow.Reduced{Keys: []string{}}
		default:
			// A sum loses its split and a minimum loses its base, so the cohort's part cannot be read back out of
			// the folded value. It is kept instead: one number or bool.
			var acc any
			for _, c := range contributions {
				if !c.Has(k) {
					continue
				}
				var inc any
				if err := json.Unmarshal(c.GetJSON(k), &inc); err != nil {
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
			reduced[k] = &workflow.Reduced{Value: value}
		}
	}
	if err := base.MergeReduceAll(contributions, reducers); err != nil {
		return nil, errors.Trace(err)
	}
	return reduced, nil
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

// projectReduced returns what a fan-in step's cohort added, read from the step's state and the reduced
// metadata foldCohort recorded there: a delta that folds onto an outer cohort's accumulator exactly as the
// inner cohort's contributions would have, one by one. A field with no reducer projects as its value, or as a
// delete if the cohort left it absent. The reducer is taken from the graph, never from the stored metadata.
func projectReduced(state workflow.State, reducers map[string]workflow.Reducer) (workflow.State, error) {
	delta, _ := workflow.NewState()
	for k, fm := range state.Meta() {
		rd := fm.Reduced
		if rd == nil {
			continue
		}
		r := reducers[k]
		raw := state.GetJSON(k)
		switch {
		case rd.From != nil:
			from := *rd.From
			if r == workflow.ReducerConcat {
				var s string
				if len(raw) > 0 {
					if err := json.Unmarshal(raw, &s); err != nil {
						return workflow.State{}, errors.New("field %q: %w", k, err)
					}
				}
				if err := delta.Set(k, s[min(from, len(s)):]); err != nil {
					return workflow.State{}, errors.Trace(err)
				}
				continue
			}
			var arr []json.RawMessage
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &arr); err != nil {
					return workflow.State{}, errors.New("field %q: %w", k, err)
				}
			}
			tail := arr[min(from, len(arr)):]
			if tail == nil {
				tail = []json.RawMessage{}
			}
			if err := delta.Set(k, tail); err != nil {
				return workflow.State{}, errors.Trace(err)
			}
		case rd.Value != nil:
			delta.SetJSON(k, rd.Value)
		case r == workflow.ReducerMerge:
			var obj map[string]json.RawMessage
			if len(raw) > 0 {
				if err := json.Unmarshal(raw, &obj); err != nil {
					return workflow.State{}, errors.New("field %q: %w", k, err)
				}
			}
			part := make(map[string]json.RawMessage, len(rd.Keys))
			for _, key := range rd.Keys {
				if v, ok := obj[key]; ok {
					part[key] = v
				}
			}
			if err := delta.Set(k, part); err != nil {
				return workflow.State{}, errors.Trace(err)
			}
		default:
			if state.Has(k) {
				delta.SetJSON(k, raw)
			} else {
				_ = delta.Set(k, nil)
			}
		}
	}
	return delta, nil
}

// memberContribution is what one cohort member adds to its cohort's fold. part is what the member's own cohort
// added if the member is an inner fan-in (cohortParts), and the zero State otherwise; changes is the member's
// own changes if it completed, and the zero State otherwise. ok is false for a member that adds nothing.
//
// An inner fan-in's part counts whatever the fan-in step's own status: every step of its cohort completed, and
// the fan-in step only exists because the cohort converged. Its own writes count only if it completed, like any
// member's.
func memberContribution(part, changes workflow.State, reducers map[string]workflow.Reducer) (contribution workflow.State, ok bool, err error) {
	if part.IsZero() {
		return changes, !changes.IsZero(), nil
	}
	if changes.IsZero() {
		return part, true, nil
	}
	if err := part.MergeReduceAll([]workflow.State{changes}, reducers); err != nil {
		return workflow.State{}, false, errors.Trace(err)
	}
	return part, true, nil
}

// stampFanIn stamps a fan-in step's state: the graph's reducers, plus the description of what its cohort added.
func stampFanIn(state workflow.State, reducers map[string]workflow.Reducer, reduced map[string]*workflow.Reduced) {
	stampReducers(state, reducers)
	if len(reduced) == 0 {
		return
	}
	meta := state.Meta()
	if meta == nil {
		meta = make(map[string]workflow.FieldMeta, len(reduced))
	}
	for k, rd := range reduced {
		fm := meta[k]
		fm.Reduced = rd
		meta[k] = fm
	}
	state.SetMeta(meta)
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
// dropped, so a wide cohort holds the parts rather than every inner fan-in's full state. The id list goes out in
// chunks padded to a power of two, so the statement text takes one of a few arities rather than one per width.
func cohortParts(ctx context.Context, db sequel.Executor, stepIDs []int, reducers map[string]workflow.Reducer) (map[int]workflow.State, int, error) {
	parts := make(map[int]workflow.State, len(stepIDs))
	read := 0
	for chunk := range slices.Chunk(stepIDs, 512) {
		arity := 1
		for arity < len(chunk) {
			arity *= 2
		}
		args := make([]any, arity)
		for i := range args {
			args[i] = chunk[min(i, len(chunk)-1)]
		}
		rows, err := db.QueryContext(ctx,
			"SELECT step_id, state FROM dwarf_steps WHERE step_id IN ("+strings.Repeat("?,", arity-1)+"?)",
			args...)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		for rows.Next() {
			var id int
			var stateJSON []byte
			if err := rows.Scan(&id, &stateJSON); err != nil {
				rows.Close()
				return nil, 0, errors.Trace(err)
			}
			read += len(stateJSON)
			state, _ := workflow.NewState(stateJSON)
			part, err := projectReduced(state, reducers)
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
