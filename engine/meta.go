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
	"slices"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// stampReducers records every field the graph declares a reducer for in the state's metadata, including a
// field with no value yet, and drops whatever metadata the state held before. The engine is the only author
// of a state's metadata, so a stamp is also what discards an entry a task or a caller forged. A zero State is left
// as it is.
func stampReducers(s workflow.State, reducers map[string]workflow.Reducer) {
	if s.IsZero() {
		return
	}
	var meta map[string]workflow.FieldMeta
	for k, r := range reducers {
		if r == "" || r == workflow.ReducerReplace {
			continue
		}
		if meta == nil {
			meta = make(map[string]workflow.FieldMeta, len(reducers))
		}
		meta[k] = workflow.FieldMeta{Reducer: r}
	}
	s.SetMeta(meta)
}

// rejectMeta fails a caller-supplied state that carries metadata. Only the engine writes it.
func rejectMeta(s workflow.State, what string) error {
	if s.MetaJSON() != nil {
		return errors.New("%s must not contain the reserved field %q", what, workflow.MetaKey, http.StatusBadRequest)
	}
	return nil
}

// redactMeta strips the metadata from a state the engine hands outside itself. The stored columns keep it.
func redactMeta(s workflow.State) workflow.State {
	if !s.IsZero() {
		s.SetMeta(nil)
	}
	return s
}

// checkDeltaMeta checks the metadata of a task's changes. The only thing a task may record there is an
// overwrite mark (`"<field>": {"overwrite": true}`), and not on a field the graph reduces when the step runs
// inside a fan-out branch: the branches are parallel, so a reset in one of them has no defined meaning for the
// others. Anything else - another property, a reducer, a fan-in's description - names a host that is buggy or
// out of date, and fails the step naming the field rather than being dropped.
func checkDeltaMeta(changes workflow.State, reducers map[string]workflow.Reducer, inBranch bool) error {
	raw := changes.MetaJSON()
	if raw == nil {
		return nil
	}
	var entries map[string]json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return errors.New("task changes carry unreadable %s: %v", workflow.MetaKey, err)
	}
	for k, entry := range entries {
		var props map[string]json.RawMessage
		if json.Unmarshal(entry, &props) != nil || len(props) != 1 || string(props["overwrite"]) != "true" {
			return errors.New("task changes carry unexpected %s for field %q", workflow.MetaKey, k)
		}
		if r := reducers[k]; inBranch && r != "" && r != workflow.ReducerReplace {
			return errors.New("cannot overwrite or delete reduced field %q inside a fan-out branch", k)
		}
	}
	return nil
}

// foldedFields returns the fields a delta folds through a reducer: written with a value, on a field the graph
// reduces, and not overwritten. The folded value exists in no row until a successor's state holds it, so these
// are stored inline there (Mint's inlineOnly).
func foldedFields(delta workflow.State, reducers map[string]workflow.Reducer) map[string]bool {
	var folded map[string]bool
	for k := range delta.Names() {
		r := reducers[k]
		if r == "" || r == workflow.ReducerReplace || !delta.Has(k) || delta.IsOverwritten(k) {
			continue
		}
		if folded == nil {
			folded = map[string]bool{}
		}
		folded[k] = true
	}
	return folded
}

// onlyFields returns the reducers of the given fields.
func onlyFields(reducers map[string]workflow.Reducer, fields map[string]bool) map[string]workflow.Reducer {
	out := make(map[string]workflow.Reducer, len(fields))
	for k := range fields {
		out[k] = reducers[k]
	}
	return out
}

// reducersOf returns the reducers a stored state is tagged with.
func reducersOf(s workflow.State) map[string]workflow.Reducer {
	reducers := map[string]workflow.Reducer{}
	for k, fm := range s.Meta() {
		if fm.Reducer != "" {
			reducers[k] = fm.Reducer
		}
	}
	return reducers
}

// dropIdentityNulls removes from a delta every null on a field the graph reduces. Such a null contributes
// nothing - it is the reducer's identity - but read as a field the delta writes, it would tell the state-refs
// mint that the step rewrote the field: the carried ref would be dropped and the field re-anchored at a step
// whose changes hold the null. The carrier never writes one; a remote host, or a row written before deletes
// were marks, can.
func dropIdentityNulls(delta workflow.State, reducers map[string]workflow.Reducer) {
	for _, k := range slices.Collect(delta.Names()) {
		if r := reducers[k]; r != "" && r != workflow.ReducerReplace && !delta.Has(k) && !delta.IsOverwritten(k) {
			delta.Del(k)
		}
	}
}
