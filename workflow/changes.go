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

package workflow

import (
	"bytes"
	"encoding/json"
	"iter"
	"strings"

	"github.com/microbus-io/errors"
)

// OverwriteSuffix marks a field in the JSON of Changes as overwritten rather than contributed to. A field name
// that ends with it cannot be written by a task.
const OverwriteSuffix = "!"

// AdditionSuffix marks a field in the JSON of Changes as an addition: elements appended to the array the field
// held in the step's input. A field name that ends with it cannot be written by a task.
const AdditionSuffix = "+"

/*
Changes is what a step writes to its flow's state, as the orchestrator records it. Each field it writes is
either a contribution - combined through the field's reducer with what the field already holds - or an
overwrite, which replaces the field, or deletes it when its value is nil, or an addition (below). A field is
exactly one of these. State.Apply folds Changes onto a state.

A task does not use Changes directly: Flow.Set, Flow.Overwrite and Flow.Del build them.

Its JSON is one object holding both kinds, an overwritten field's name carrying OverwriteSuffix:

	{"messages": ["m"], "summary!": "trimmed", "scratch!": null}

adds "m" to messages, replaces summary and deletes scratch.

A third kind, an addition, is recorded only by the orchestrator: a field with no reducer whose new value only
extends the array it held, stored as the elements it gained under the name with AdditionSuffix.

A zero Changes is empty and safe to read (Names, Len, Mentions, MarshalJSON all work on it), but the mutating
methods do not allocate, so Set, Overwrite and Compose panic on it: build one with NewChanges or UnmarshalJSON.
*/
type Changes struct {
	contributions State
	overwrites    State
	additions     State
}

// NewChanges returns empty Changes.
func NewChanges() Changes {
	contributions, _ := NewState()
	overwrites, _ := NewState()
	additions, _ := NewState()
	return Changes{contributions: contributions, overwrites: overwrites, additions: additions}
}

// ChangesOf returns Changes that contribute every field of s with its value as given. Applied, each combines
// through its field's reducer, and a null deletes a field with no reducer and contributes nothing to one with a
// reducer.
func ChangesOf(s State) Changes {
	c := NewChanges()
	for k := range s.Names() {
		c.contributions.d[k] = s.d[k]
	}
	return c
}

// IsZero reports whether the Changes were never initialized.
func (c Changes) IsZero() bool {
	return c.contributions.IsZero()
}

// Clone returns a copy of the Changes that shares no map with them.
func (c Changes) Clone() Changes {
	return Changes{contributions: c.contributions.Clone(), overwrites: c.overwrites.Clone(), additions: c.additions.Clone()}
}

// Contributions returns the fields contributed to, and their contributions. It is the Changes' own State: a
// mutation of it mutates the Changes.
func (c Changes) Contributions() State {
	return c.contributions
}

// Overwrites returns the fields overwritten, and their new values; a deleted field holds a null. It is the
// Changes' own State: a mutation of it mutates the Changes.
func (c Changes) Overwrites() State {
	return c.overwrites
}

// Additions returns the fields extended, and the elements each gained. It is the Changes' own State: a mutation
// of it mutates the Changes.
func (c Changes) Additions() State {
	return c.additions
}

// Names iterates the fields the Changes write, contributed to, overwritten or extended, in unspecified order.
func (c Changes) Names() iter.Seq[string] {
	return func(yield func(string) bool) {
		for _, m := range []State{c.contributions, c.overwrites, c.additions} {
			for k := range m.d {
				if !yield(k) {
					return
				}
			}
		}
	}
}

// Len returns the number of fields the Changes write.
func (c Changes) Len() int {
	return c.contributions.Len() + c.overwrites.Len() + c.additions.Len()
}

// Mentions reports whether the Changes write the field, contributed to, overwritten or extended.
func (c Changes) Mentions(name string) bool {
	_, contributed := c.contributions.d[name]
	_, overwritten := c.overwrites.d[name]
	_, added := c.additions.d[name]
	return contributed || overwritten || added
}

// IsOverwritten reports whether the Changes overwrite or delete the field.
func (c Changes) IsOverwritten(name string) bool {
	_, ok := c.overwrites.d[name]
	return ok
}

// IsDeleted reports whether the Changes overwrite the field with nothing, which deletes it whatever its reducer.
func (c Changes) IsDeleted(name string) bool {
	return c.overwrites.IsDeleted(name)
}

// Set records a contribution of value to the field, combined with any earlier write to it in these Changes: a
// contribution folds through the field's reducer onto the earlier one, the first onto nothing, so a single value
// under ReducerAppend becomes a list; onto an overwrite, it folds onto the overwritten value and the field stays
// overwritten. On a field with no reducer the value replaces, and a nil deletes, which is an overwrite with
// nothing. On a reduced field a nil contributes nothing. It returns an error if the value cannot be marshalled,
// if the reducer cannot combine it, or if name ends with OverwriteSuffix or AdditionSuffix; on an error the
// Changes are unchanged.
func (c Changes) Set(name string, value any, reducers map[string]Reducer) error {
	raw, err := marshalWrite(name, value)
	if err != nil {
		return errors.Trace(err)
	}
	return c.contribute(name, raw, reducers)
}

// marshalWrite encodes a value written to a field of these Changes, rejecting a name that ends with
// OverwriteSuffix or AdditionSuffix.
func marshalWrite(name string, value any) (json.RawMessage, error) {
	for _, suffix := range []string{OverwriteSuffix, AdditionSuffix} {
		if strings.HasSuffix(name, suffix) {
			return nil, errors.New("workflow: state field %q cannot be written: the name ends with %q", name, suffix)
		}
	}
	return marshalField(name, value)
}

// Overwrite records that the field is replaced by value, whatever its reducer; a nil value deletes it. It
// returns an error if the value cannot be marshalled or name ends with OverwriteSuffix or AdditionSuffix.
func (c Changes) Overwrite(name string, value any) error {
	raw, err := marshalWrite(name, value)
	if err != nil {
		return errors.Trace(err)
	}
	delete(c.contributions.d, name)
	delete(c.additions.d, name)
	c.overwrites.d[name] = raw
	return nil
}

// Compose folds later Changes onto these, mutating the receiver, so that applying the result equals applying the
// two in turn - up to floating-point rounding under ReducerAdd, which sums the contributions before adding them
// to the field. Like State.ApplyAll, it does not validate the values it folds. On a reducer error the receiver is
// left partly composed; discard it.
func (c Changes) Compose(later Changes, reducers map[string]Reducer) error {
	for k, v := range later.contributions.d {
		if err := c.contribute(k, v, reducers); err != nil {
			return errors.Trace(err)
		}
	}
	for k, v := range later.overwrites.d {
		delete(c.contributions.d, k)
		delete(c.additions.d, k)
		c.overwrites.d[k] = v
	}
	for k, v := range later.additions.d {
		if err := c.add(k, v); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// add folds an addition, given as the JSON array of the elements gained, onto the Changes: onto an earlier write
// to the field it extends that value, and onto nothing it is an addition of its own.
func (c Changes) add(name string, elems json.RawMessage) error {
	for _, m := range []State{c.contributions, c.overwrites, c.additions} {
		if prev, ok := m.d[name]; ok {
			extended, err := extend(name, prev, elems)
			if err != nil {
				return errors.Trace(err)
			}
			m.d[name] = extended
			return nil
		}
	}
	c.additions.d[name] = elems
	return nil
}

// extend appends an addition's elements to a field's value. The value must be a list, or absent.
func extend(name string, value, elems json.RawMessage) (json.RawMessage, error) {
	if v := bytes.TrimSpace(value); len(v) > 0 && !bytes.Equal(v, []byte("null")) && v[0] != '[' {
		return nil, errors.New("addition to field %q: the field does not hold a list", name)
	}
	if e := bytes.TrimSpace(elems); len(e) == 0 || e[0] != '[' {
		return nil, errors.New("addition to field %q: the addition is not a list of elements", name)
	}
	return appendRaw(name, value, []json.RawMessage{elems}, false)
}

// contribute folds one contribution, given as JSON, onto the Changes.
func (c Changes) contribute(name string, v json.RawMessage, reducers map[string]Reducer) error {
	r := reducers[name]
	if r == "" || r == ReducerReplace {
		c.setPlain(name, v)
		return nil
	}
	if prev, ok := c.overwrites.d[name]; ok {
		// An overwrite is sticky: a later write folds onto the overwritten value, which stays an overwrite.
		switch {
		case isCleared(v):
			// The reducer's identity.
		default:
			folded, err := fold(r, name, prev, v, false)
			if err != nil {
				return errors.Trace(err)
			}
			c.overwrites.d[name] = folded
		}
		return nil
	}
	switch {
	case isCleared(v):
		// The reducer's identity.
	default:
		folded, err := fold(r, name, c.contributions.d[name], v, true)
		if err != nil {
			return errors.Trace(err)
		}
		c.contributions.d[name] = folded
	}
	return nil
}

// setPlain records a write to a field with no reducer: v replaces the field, and a null deletes it, which is an
// overwrite with nothing. A field already overwritten stays overwritten.
func (c Changes) setPlain(name string, v json.RawMessage) {
	delete(c.additions.d, name)
	switch _, overwritten := c.overwrites.d[name]; {
	case overwritten:
		c.overwrites.d[name] = orNull(v)
	case isCleared(v):
		delete(c.contributions.d, name)
		c.overwrites.d[name] = json.RawMessage("null")
	default:
		c.contributions.d[name] = v
	}
}

// fold combines inc onto base through r. When base is a contribution (contributed), a bare value stored by an
// older writer under append or union stands for the one-element list it is, so it is wrapped first.
func fold(r Reducer, name string, base, inc json.RawMessage, contributed bool) (json.RawMessage, error) {
	switch r {
	case ReducerAppend:
		return appendRaw(name, base, []json.RawMessage{inc}, contributed)
	case ReducerConcat:
		return concatRaw(name, base, []json.RawMessage{inc})
	}
	baseVal, err := decodeValue(r, base)
	if err != nil {
		return nil, errors.New("reducer '%s' for field %q: %w", string(r), name, err)
	}
	if _, isList := baseVal.([]any); contributed && baseVal != nil && !isList && r == ReducerUnion {
		baseVal = []any{baseVal}
	}
	incVal, err := decodeValue(r, inc)
	if err != nil {
		return nil, errors.New("reducer '%s' for field %q: %w", string(r), name, err)
	}
	result, err := r.Reduce(baseVal, incVal)
	if err != nil {
		return nil, errors.New("reducer '%s' for field %q: %w", string(r), name, err)
	}
	b, err := json.Marshal(result)
	if err != nil {
		return nil, errors.New("reducer for field %q: %w", name, err)
	}
	return b, nil
}

// orNull is v, or the JSON null if v is empty.
func orNull(v json.RawMessage) json.RawMessage {
	if len(v) == 0 {
		return json.RawMessage("null")
	}
	return v
}

// MarshalJSON writes the Changes as one JSON object: each contribution under its field's name, each overwrite
// under its name with OverwriteSuffix, a delete as a null, and each addition under its name with AdditionSuffix.
func (c Changes) MarshalJSON() ([]byte, error) {
	all := make(map[string]json.RawMessage, c.Len())
	for k, v := range c.contributions.d {
		all[k] = orNull(v)
	}
	for k, v := range c.overwrites.d {
		all[k+OverwriteSuffix] = orNull(v)
	}
	for k, v := range c.additions.d {
		all[k+AdditionSuffix] = v
	}
	b, err := json.Marshal(all)
	return b, errors.Trace(err)
}

// UnmarshalJSON reads Changes as MarshalJSON writes them. A name ending with OverwriteSuffix is an overwrite of
// the field without it, one ending with AdditionSuffix an addition to it; any other name is a contribution. A
// field written more than one way is an overwrite, else a contribution. An empty payload and a JSON null are
// empty Changes; anything but an object is an error.
func (c *Changes) UnmarshalJSON(data []byte) error {
	*c = NewChanges()
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	var all State
	if err := all.UnmarshalJSON(data); err != nil {
		return errors.Trace(err)
	}
	for k, v := range all.d {
		if name, ok := strings.CutSuffix(k, OverwriteSuffix); ok {
			c.overwrites.d[name] = v
		}
	}
	for k, v := range all.d {
		if _, overwritten := c.overwrites.d[k]; !strings.HasSuffix(k, OverwriteSuffix) && !strings.HasSuffix(k, AdditionSuffix) && !overwritten {
			c.contributions.d[k] = v
		}
	}
	for k, v := range all.d {
		if name, ok := strings.CutSuffix(k, AdditionSuffix); ok && !c.Mentions(name) {
			c.additions.d[name] = v
		}
	}
	return nil
}
