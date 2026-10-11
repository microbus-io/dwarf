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
	"slices"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// changesOf decodes a changes column. An unreadable column reads as no changes, as a state column does.
func changesOf(data []byte) workflow.Changes {
	var c workflow.Changes
	if err := c.UnmarshalJSON(data); err != nil {
		return workflow.NewChanges()
	}
	return c
}

// checkChanges checks what a task wrote, which is untrusted. A reduced field may not be overwritten or deleted
// inside a fan-out branch: the branches are parallel, so a reset in one of them has no defined meaning for the
// others. And no field written may end with the overwrite suffix, which a remote host can send although the
// carrier refuses it.
func checkChanges(changes workflow.Changes, reducers map[string]workflow.Reducer, inBranch bool) error {
	for k := range changes.Names() {
		if strings.HasSuffix(k, workflow.OverwriteSuffix) {
			return errors.New("state field %q cannot be written: the name ends with %q", k, workflow.OverwriteSuffix)
		}
		if r := reducers[k]; inBranch && r != "" && r != workflow.ReducerReplace && changes.IsOverwritten(k) {
			return errors.New("cannot overwrite or delete reduced field %q inside a fan-out branch", k)
		}
	}
	return nil
}

// foldedFields returns the fields Changes fold through a reducer: contributed to with a value, on a field the
// graph reduces. The folded value exists in no row until a successor's state holds it, so these are stored
// inline there (Mint's inlineOnly).
func foldedFields(c workflow.Changes, reducers map[string]workflow.Reducer) map[string]bool {
	var folded map[string]bool
	for k := range c.Contributions().Names() {
		if r := reducers[k]; r == "" || r == workflow.ReducerReplace || !c.Contributions().Has(k) {
			continue
		}
		if folded == nil {
			folded = map[string]bool{}
		}
		folded[k] = true
	}
	return folded
}

// dropIdentityNulls removes every null contribution to a field the graph reduces. Such a null contributes
// nothing - it is the reducer's identity - but read as a field the Changes write, it would tell the state-refs
// mint that the step rewrote the field: the carried ref would be dropped and the field re-anchored at a step
// whose changes hold the null. The carrier never writes one; a remote host, or a row written before deletes
// were overwrites, can.
func dropIdentityNulls(c workflow.Changes, reducers map[string]workflow.Reducer) {
	contributed := c.Contributions()
	for _, k := range slices.Collect(contributed.Names()) {
		if r := reducers[k]; r != "" && r != workflow.ReducerReplace && !contributed.Has(k) {
			contributed.Del(k)
		}
	}
}

// onlyFields returns the reducers of the given fields.
func onlyFields(reducers map[string]workflow.Reducer, fields map[string]bool) map[string]workflow.Reducer {
	out := make(map[string]workflow.Reducer, len(fields))
	for k := range fields {
		out[k] = reducers[k]
	}
	return out
}

// changesView is how Step shows a step's changes: every field written, an overwrite as its new value and a
// delete as a null.
func changesView(c workflow.Changes) workflow.State {
	view := c.Contributions().Clone()
	for k := range c.Overwrites().Names() {
		view.SetJSON(k, c.Overwrites().GetJSON(k))
	}
	return view
}
