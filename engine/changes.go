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
	"encoding/json"
	"net/http"
	"slices"

	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// changesOf decodes a changes column. An empty or null column is no changes; an unreadable one is an error, never
// read as no changes, which would drop the step's writes silently from everything folded from it. The error is a
// permanentError: the stored bytes will not decode on a retry either.
func changesOf(data []byte) (workflow.Changes, error) {
	var c workflow.Changes
	if err := c.UnmarshalJSON(data); err != nil {
		return workflow.NewChanges(), errors.Trace(&permanentError{errors.New("unreadable changes column: %v", err)})
	}
	return c, nil
}

// checkChanges checks what a task wrote, which is untrusted. Every field written must have a valid name
// (workflow.IsValidFieldName), which a remote host can break although the carrier panics on it - a name ending
// with the overwrite or addition suffix among them. A reduced field may not be overwritten or deleted inside a
// fan-out branch: the branches are parallel, so a reset in one of them has no defined meaning for the others. And
// an addition - which only the engine records, and which a re-run hands back to the task with its earlier runs'
// changes - must be one the engine could have recorded: on a field with no reducer, outside a fan-out cohort.
func checkChanges(changes workflow.Changes, reducers map[string]workflow.Reducer, inBranch bool) error {
	for k := range changes.Names() {
		if !workflow.IsValidFieldName(k) {
			return errors.New("invalid state field name %q", k)
		}
		r := reducers[k]
		reduced := r != "" && r != workflow.ReducerReplace
		if inBranch && reduced && changes.IsOverwritten(k) {
			return errors.New("cannot overwrite or delete reduced field %q inside a fan-out branch", k)
		}
		if changes.Additions().Has(k) {
			if reduced || inBranch {
				return errors.New("state field %q holds an addition the engine never records there", k)
			}
			if v := bytes.TrimSpace(changes.Additions().GetJSON(k)); len(v) == 0 || v[0] != '[' {
				return errors.New("state field %q holds an addition that is not a list of elements", k)
			}
		}
	}
	return nil
}

// foldedFields returns the fields whose value the Changes fold: contributed to with a value on a field the graph
// reduces, or extended by an addition. The folded value exists in no row until a successor's state holds it, so
// these are never anchored at the step: stored as a list (appendFolds) or inline there (Mint's inlineOnly).
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
	for k := range c.Additions().Names() {
		if folded == nil {
			folded = map[string]bool{}
		}
		folded[k] = true
	}
	return folded
}

// appendFolds describes, for each field the changes append to - a contribution under the append reducer, or an
// addition - how its value folds onto input - the step's own state, the base those changes fold onto - so the mint
// can store the folded value as a list pointing at the rows that hold its elements (staterefs.Fold). step is the
// step whose changes these are. A field whose base is not an array (an append onto a bare value) is left out, and
// stays inline.
func appendFolds(input workflow.State, changes workflow.Changes, reducers map[string]workflow.Reducer, step int) map[string]staterefs.Fold {
	var folds map[string]staterefs.Fold
	for k := range foldedFields(changes, reducers) {
		var part staterefs.Part
		switch {
		case changes.Additions().Has(k):
			n, _ := arrayLen(changes.Additions().GetJSON(k))
			part = staterefs.Part{Step: step, Added: n}
		case reducers[k] == workflow.ReducerAppend:
			part = appendPart(step, changes.Contributions().GetJSON(k))
		default:
			continue
		}
		base, ok := arrayLen(input.GetJSON(k))
		if !ok {
			continue
		}
		if folds == nil {
			folds = map[string]staterefs.Fold{}
		}
		folds[k] = staterefs.Fold{Base: base, Parts: []staterefs.Part{part}}
	}
	return folds
}

// appendPart is how a contribution folds under append: a JSON array is spread, anything else is one element -
// the rule State.Apply's splice follows.
func appendPart(step int, contribution json.RawMessage) staterefs.Part {
	if v := bytes.TrimSpace(contribution); len(v) > 0 && v[0] == '[' {
		if n, ok := arrayLen(v); ok {
			return staterefs.Part{Step: step, Added: n}
		}
	}
	return staterefs.Part{Step: step, Single: true}
}

// arrayLen is the element count of a JSON array, with absent and null counting as empty. It is false for any
// other value, or for an array it cannot read.
func arrayLen(value json.RawMessage) (int, bool) {
	v := bytes.TrimSpace(value)
	if len(v) == 0 || bytes.Equal(v, []byte("null")) {
		return 0, true
	}
	if v[0] != '[' {
		return 0, false
	}
	var elems []json.RawMessage
	if err := json.Unmarshal(v, &elems); err != nil {
		return 0, false
	}
	return len(elems), true
}

// cohortAppendFolds describes, for each field under the append reducer a cohort folded, how its value came to
// be: the spawn's base (spawnState, its reduced refs resolved), the spawn's own contribution or overwrite, then
// each completed member's contribution in fold order. A field an inner fan-in's part touches is left out, and
// stays inline: that part is a projection of the inner fan-in's state, not a contribution in its changes. So is
// a field only the spawn overwrote: its value is the spawn's overwrite, which the mint anchors as a plain ref.
//
// A spawn's addition (never a member's: the engine records none in a cohort) is the spawn's base and its part,
// unless a member writes the field, whose value then replaces it.
func cohortAppendFolds(spawnState workflow.State, spawnChanges workflow.Changes, spawnID int, members []cohortMember, parts map[int]workflow.Changes, reducers map[string]workflow.Reducer) map[string]staterefs.Fold {
	var folds map[string]staterefs.Fold
	for k, r := range reducers {
		if r != workflow.ReducerAppend {
			continue
		}
		var f staterefs.Fold
		base, ok := arrayLen(spawnState.GetJSON(k))
		f.Base = base
		contributed := false
		switch {
		case spawnChanges.IsOverwritten(k):
			value := bytes.TrimSpace(spawnChanges.Overwrites().GetJSON(k))
			n, isArray := arrayLen(value)
			if !isArray || len(value) == 0 || value[0] != '[' {
				continue
			}
			f.Parts = append(f.Parts, staterefs.Part{Step: spawnID, Overwrite: true, Added: n})
			ok = true
		case spawnChanges.Contributions().Has(k):
			f.Parts = append(f.Parts, appendPart(spawnID, spawnChanges.Contributions().GetJSON(k)))
			contributed = true
		}
		if !ok {
			continue
		}
		usable := true
		for _, m := range members {
			if parts[m.stepID].Mentions(k) || m.changes.IsOverwritten(k) {
				usable = false
				break
			}
			if m.changes.Contributions().Has(k) {
				f.Parts = append(f.Parts, appendPart(m.stepID, m.changes.Contributions().GetJSON(k)))
				contributed = true
			}
		}
		if !usable || !contributed {
			continue
		}
		if folds == nil {
			folds = map[string]staterefs.Fold{}
		}
		folds[k] = f
	}
	for k := range spawnChanges.Additions().Names() {
		base, ok := arrayLen(spawnState.GetJSON(k))
		if !ok {
			continue
		}
		written := false
		for _, m := range members {
			if m.changes.Mentions(k) || parts[m.stepID].Mentions(k) {
				written = true
				break
			}
		}
		if written {
			continue
		}
		n, _ := arrayLen(spawnChanges.Additions().GetJSON(k))
		if folds == nil {
			folds = map[string]staterefs.Fold{}
		}
		folds[k] = staterefs.Fold{Base: base, Parts: []staterefs.Part{{Step: spawnID, Added: n}}}
	}
	return folds
}

// recordAdditions turns each write to a field with no reducer whose new value is an array that begins with the
// whole array the field held in input - the state the changes apply onto - into an addition of the elements after
// it. Applying the changes onto input gives the same state; they store only what the field gained.
//
// The common case is a byte prefix - the input array without its closing bracket, then a comma - which costs a
// memory compare: a task that decodes and re-encodes its history spells it as the engine handed it over, from the
// minting replica's cache. Only when that misses (the input was read back from a database that respells JSON) are
// both arrays decoded and compared element by element, by bytes, else decoded and re-encoded with numbers kept as
// written. That misses only a number spelled two ways, and a miss costs the addition, never the value. The
// decoding pass runs inside slow, which the caller uses to hand back its database turn while it runs.
func recordAdditions(c workflow.Changes, input workflow.State, reducers map[string]workflow.Reducer, slow func(func())) {
	contributed := c.Contributions()
	var undecided []string
	for _, k := range slices.Collect(contributed.Names()) {
		if r := reducers[k]; r != "" && r != workflow.ReducerReplace {
			continue
		}
		base := bytes.TrimSpace(input.GetJSON(k))
		value := bytes.TrimSpace(contributed.GetJSON(k))
		if len(base) < 3 || base[0] != '[' || base[len(base)-1] != ']' || len(value) <= len(base) || value[0] != '[' {
			continue
		}
		prefix := bytes.TrimRight(base[:len(base)-1], " \t\r\n")
		if rest := bytes.TrimLeft(value[min(len(prefix), len(value)):], " \t\r\n"); len(prefix) > 1 && bytes.HasPrefix(value, prefix) && len(rest) > 0 && rest[0] == ',' {
			added := append([]byte{'['}, bytes.TrimLeft(rest[1:], " \t\r\n")...)
			contributed.Del(k)
			c.Additions().SetJSON(k, added)
			continue
		}
		undecided = append(undecided, k)
	}
	if len(undecided) == 0 {
		return
	}
	slow(func() {
		for _, k := range undecided {
			if added, ok := addedElements(input.GetJSON(k), contributed.GetJSON(k)); ok {
				contributed.Del(k)
				c.Additions().SetJSON(k, added)
			}
		}
	})
}

// addedElements returns the elements value has after the whole of base, when value begins with base's elements.
func addedElements(base, value json.RawMessage) (json.RawMessage, bool) {
	var baseElems, valueElems []json.RawMessage
	if json.Unmarshal(base, &baseElems) != nil || json.Unmarshal(value, &valueElems) != nil {
		return nil, false
	}
	if len(baseElems) == 0 || len(valueElems) <= len(baseElems) {
		return nil, false
	}
	for i, e := range baseElems {
		if !sameElement(e, valueElems[i]) {
			return nil, false
		}
	}
	added, err := json.Marshal(valueElems[len(baseElems):])
	return added, err == nil
}

// sameElement reports whether two JSON values are equal: by bytes, else decoded with numbers kept as written and
// re-encoded, which makes key order and spacing irrelevant.
func sameElement(a, b json.RawMessage) bool {
	if bytes.Equal(a, b) {
		return true
	}
	canonical := func(raw json.RawMessage) ([]byte, bool) {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if dec.Decode(&v) != nil {
			return nil, false
		}
		out, err := json.Marshal(v)
		return out, err == nil
	}
	ca, aok := canonical(a)
	cb, bok := canonical(b)
	return aok && bok && bytes.Equal(ca, cb)
}

// withoutKeys is set minus the keys of drop, sharing set when there is nothing to drop.
func withoutKeys[V any](set map[string]bool, drop map[string]V) map[string]bool {
	if len(drop) == 0 {
		return set
	}
	out := make(map[string]bool, len(set))
	for k := range set {
		if _, gone := drop[k]; !gone {
			out[k] = true
		}
	}
	return out
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

// changesView is how Step shows a step's changes: every field written, an overwrite as its new value, a delete
// as a null, and an addition as the whole value the task wrote - the field's value in state, the step's input,
// followed by what it gained.
func changesView(c workflow.Changes, state workflow.State) workflow.State {
	view := c.Contributions().Clone()
	for k := range c.Overwrites().Names() {
		view.SetJSON(k, c.Overwrites().GetJSON(k))
	}
	for k := range c.Additions().Names() {
		one := workflow.NewChanges()
		one.Additions().SetJSON(k, c.Additions().GetJSON(k))
		whole := state.Clone()
		if whole.Apply(one, nil) == nil {
			view.SetJSON(k, whole.GetJSON(k))
		}
	}
	return view
}

// checkFieldNames returns a 400 naming the first field of the caller's state whose name is not a valid state
// field name (workflow.IsValidFieldName). Every operation that takes state from a caller checks it at the door.
func checkFieldNames(state workflow.State) error {
	var bad []string
	for k := range state.Names() {
		if !workflow.IsValidFieldName(k) {
			bad = append(bad, k)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	return errors.New("invalid state field name %q", slices.Min(bad), http.StatusBadRequest)
}
