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
	"maps"
	"time"

	"github.com/microbus-io/errors"
)

// State is a JSON-serializable key/value carrier for workflow data. Values are read and written through
// its methods - Get for a typed read into a caller pointer, Set to store, Has/Len/Del/Clear to inspect and
// remove, All to iterate, Map to take a plain copy. It wraps a map and is a reference type: methods take a
// value receiver yet mutate through to every copy of the State sharing that map.
//
// A zero State is empty and safe to read (Get, Has, Len, All, MarshalJSON all work on it), but the mutating
// methods do not allocate, so one must be initialized via NewState or UnmarshalJSON before Set/Merge.
//
// Numbers are float64-domain, as JSON's are: a value stored as an int reads back through GetInt fine, but
// a plain Get into an any yields a float64. Carry an integer that exceeds 2^53 as a string.
type State struct {
	d map[string]any
}

// NewState builds a State from its arguments, in one of two mutually exclusive modes.
//
// A single argument is normalized into a state map. A []byte or json.RawMessage is treated as raw JSON and
// must be an object (an empty/nil slice or a JSON "null" yields an empty State; any other non-object is an
// error - see UnmarshalJSON) - this is the one-liner for reading a state column back from the database:
// state, _ := NewState(stateJSON). A nil argument yields an empty State. A State is already normalized, so it
// is shallow-copied. Any other value - a map, a struct, or any JSON-marshalable value - is round-tripped
// through JSON into the map, so nested structs become canonical (sorted-key, float64-number) maps exactly as
// if read from a column. That canonicalization is deliberate: it is what lets reducers compare marshalled
// bytes, so a caller passing a map with a nested struct sees the same spelling as its decoded twin. This is
// the normalizer for a caller-supplied value at an API boundary: wrap it in NewState. It does NOT validate
// value ranges (a >2^53 integer, a NUL) - see the storability note in the package docs.
//
// Two or more arguments are variadic name/value pairs, e.g. NewState("count", 3, "name", "abc"); the value
// is stored as passed. An odd count, or a non-string in a name position, is an error.
func NewState(data ...any) (State, error) {
	s := State{d: map[string]any{}}
	// Single-argument normalize mode: a lone value is unmarshaled ([]byte), shallow-copied (an already-
	// normalized State), or JSON-normalized (a map, a struct, or anything else - so nested values decode
	// exactly as a column read would, which is why a raw map[string]any is NOT short-circuited).
	if len(data) == 1 {
		switch v := data[0].(type) {
		case nil:
			return s, nil
		case []byte:
			if err := s.UnmarshalJSON(v); err != nil {
				return State{}, errors.Trace(err)
			}
			return s, nil
		case json.RawMessage:
			if err := s.UnmarshalJSON(v); err != nil {
				return State{}, errors.Trace(err)
			}
			return s, nil
		case State:
			maps.Copy(s.d, v.d)
			return s, nil
		default:
			b, err := json.Marshal(v)
			if err != nil {
				return State{}, errors.Trace(err)
			}
			if err := json.Unmarshal(b, &s.d); err != nil {
				return State{}, errors.Trace(err)
			}
			return s, nil
		}
	}
	if len(data)%2 != 0 {
		return State{}, errors.New("workflow: NewState requires an even number of name/value arguments")
	}
	for i := 0; i < len(data); i += 2 {
		name, ok := data[i].(string)
		if !ok {
			return State{}, errors.New("workflow: NewState name argument must be a string")
		}
		s.d[name] = data[i+1]
	}
	return s, nil
}

// Set stores value under name, normalized through JSON: the value is marshalled and decoded back into its
// canonical JSON shape (numbers as float64, structs as sorted-key maps), exactly as if it had been read from
// the database. So a task that stores an int reads it back as an int via GetInt, but a plain Get into an any
// sees a float64 - state is float64-domain JSON, and the typed getters coerce. Storing a decoded copy also
// means a later mutation of the caller's value cannot corrupt what was stored. Returns an error only if
// value cannot be marshalled (a NaN, an +Inf, a channel).
func (s State) Set(name string, value any) error {
	raw, ok := value.(json.RawMessage)
	if !ok {
		b, err := json.Marshal(value)
		if err != nil {
			return errors.Trace(err)
		}
		raw = b
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return errors.Trace(err)
	}
	s.d[name] = v
	return nil
}

// SetString stores a string field (normalized through JSON, like Set).
func (s State) SetString(name string, value string) { s.mustSet(name, value) }

// SetStrings stores a string-slice field (normalized through JSON, like Set).
func (s State) SetStrings(name string, value []string) { s.mustSet(name, value) }

// SetInt stores an int field (normalized through JSON, like Set).
func (s State) SetInt(name string, value int) { s.mustSet(name, value) }

// SetFloat stores a float64 field (normalized through JSON, like Set).
func (s State) SetFloat(name string, value float64) { s.mustSet(name, value) }

// SetBool stores a bool field (normalized through JSON, like Set).
func (s State) SetBool(name string, value bool) { s.mustSet(name, value) }

// SetDuration stores a time.Duration field, in nanoseconds (normalized through JSON, like Set).
func (s State) SetDuration(name string, value time.Duration) { s.mustSet(name, value) }

// mustSet is Set for the typed setters, which have no error to return. The panic is a defensive assertion:
// these primitive types always marshal, so it is never a live path.
func (s State) mustSet(name string, value any) {
	if err := s.Set(name, value); err != nil {
		panic(errors.New("state field %q: %v", name, err))
	}
}

// Len returns the number of fields in the state.
func (s State) Len() int {
	return len(s.d)
}

// Del removes the named fields. Names that are not present are ignored.
func (s State) Del(names ...string) {
	for _, name := range names {
		delete(s.d, name)
	}
}

// Clear removes every field, leaving an empty State.
func (s State) Clear() {
	clear(s.d)
}

// All iterates the state's fields in unspecified order, for use with range-over-func:
//
//	for name, value := range state.All() { ... }
//
// Values are the stored (JSON-canonical) forms, so a number is a float64 and a nested object is a
// map[string]any. Do not mutate the State during iteration.
func (s State) All() iter.Seq2[string, any] {
	return func(yield func(string, any) bool) {
		for k, v := range s.d {
			if !yield(k, v) {
				return
			}
		}
	}
}

// Names returns the field names, in unspecified order.
func (s State) Names() []string {
	if len(s.d) == 0 {
		return nil
	}
	out := make([]string, 0, len(s.d))
	for k := range s.d {
		out = append(out, k)
	}
	return out
}

// Map returns the state's fields as a plain map, freshly allocated, so the caller may hold or mutate it
// without affecting the State. Use it at a boundary that must hand out a map; prefer Get/All in ordinary
// code, which need no copy.
func (s State) Map() map[string]any {
	out := make(map[string]any, len(s.d))
	maps.Copy(out, s.d)
	return out
}

// IsZero reports whether this is the zero State (never initialized). An initialized but empty State is not
// zero. It is what lets a State field be omitted from an encoded struct via the omitzero tag.
func (s State) IsZero() bool {
	return s.d == nil
}

// Get unmarshals the field named name into target, a non-nil pointer, coercing through JSON (so a stored
// float64 reads into an int, a stored object into a struct, etc.). It returns ok=true only when the field is
// present and was assigned. An absent or cleared field (Go nil or JSON null) returns (false, nil), leaving
// target untouched. A value that cannot be unmarshalled into target's type returns (false, err). Use this to
// HANDLE a type mismatch; the typed getters (GetInt, ...) panic on one instead.
func (s State) Get(name string, target any) (ok bool, err error) {
	v, exists := s.d[name]
	if !exists || isCleared(v) {
		return false, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return false, errors.Trace(err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

// The typed getters below coerce through Get and PANIC on a type mismatch - a GetInt off a 1.5 must not
// silently proceed with 0. The panic is not a crash: the orchestrator catches it at the task-call boundary,
// so it surfaces as a normal step failure (routed to onError if the graph has one). Absent and cleared
// fields are not a mismatch - they yield the zero value.

// GetString returns the field as a string. It returns "" if the field is absent or cleared, and panics if
// the field holds a non-string.
func (s State) GetString(name string) string {
	var v string
	if _, err := s.Get(name, &v); err != nil {
		panic(errors.Trace(err))
	}
	return v
}

// GetStrings returns the field as a string slice. It returns nil if the field is absent or cleared, and
// panics if the field holds anything but an array of strings.
func (s State) GetStrings(name string) []string {
	var v []string
	if _, err := s.Get(name, &v); err != nil {
		panic(errors.Trace(err))
	}
	return v
}

// GetInt returns the field as an int. It returns 0 if the field is absent or cleared, and panics if the
// field holds a non-integer (a fractional number included).
func (s State) GetInt(name string) int {
	var v int
	if _, err := s.Get(name, &v); err != nil {
		panic(errors.Trace(err))
	}
	return v
}

// GetFloat returns the field as a float64. It returns 0 if the field is absent or cleared, and panics if the
// field holds a non-number.
func (s State) GetFloat(name string) float64 {
	var v float64
	if _, err := s.Get(name, &v); err != nil {
		panic(errors.Trace(err))
	}
	return v
}

// GetBool returns the field as a bool. It returns false if the field is absent or cleared, and panics if the
// field holds a non-boolean.
func (s State) GetBool(name string) bool {
	var v bool
	if _, err := s.Get(name, &v); err != nil {
		panic(errors.Trace(err))
	}
	return v
}

// GetDuration returns the field as a time.Duration. It returns 0 if the field is absent or cleared, and
// panics if the field holds anything but a duration in nanoseconds.
func (s State) GetDuration(name string) time.Duration {
	var v time.Duration
	if _, err := s.Get(name, &v); err != nil {
		panic(errors.Trace(err))
	}
	return v
}

// Has reports whether a field is present and holds a value. A cleared slot (Go nil or JSON null) reads as
// absent, which is what a caller asking "is there something here to read" wants.
func (s State) Has(name string) bool {
	v, ok := s.d[name]
	return ok && !isCleared(v)
}

// Contains reports whether the field name is present, INCLUDING a cleared slot. It differs from Has on
// exactly one case and that case is a delete in transit: a changes delta records a delete as a cleared
// value, so Has says "nothing to read" while Contains says "this delta speaks about this field". Use Has to
// decide whether to read a value, Contains to decide whether a delta touched a field at all.
func (s State) Contains(name string) bool {
	_, ok := s.d[name]
	return ok
}

// Lookup returns the stored value for name and whether it was present, in the manner of a map index. A
// cleared slot returns (nil, true) - present, holding nothing - which Contains agrees with and Has does not.
func (s State) Lookup(name string) (any, bool) {
	v, ok := s.d[name]
	return v, ok
}

// Value returns the stored value for name, or nil if absent - Lookup without the presence flag. The value
// is in its stored JSON-canonical form, so a number is a float64 and an object a map[string]any; prefer Get
// or a typed getter when the target type is known, which coerce instead of asserting.
func (s State) Value(name string) any {
	return s.d[name]
}

// Parse unmarshals the state's fields into target (a struct pointer, matched by JSON tag, or any other
// JSON-unmarshalable target). Fields present in the state but absent from a struct target are ignored;
// cleared fields are skipped. A zero State is a no-op.
func (s State) Parse(target any) error {
	if s.d == nil {
		return nil
	}
	data, err := json.Marshal(s)
	if err != nil {
		return errors.Trace(err)
	}
	return json.Unmarshal(data, target)
}

// Clone returns a copy of the State backed by a freshly allocated map, so mutations to either side
// (Set/Del/Merge) do not affect the other. Field values are copied by reference (as stored), which is
// safe for restoring after a failed Merge: Merge reassigns map keys rather than mutating the values in
// place, so a pre-Merge Clone is unaffected by a Merge that later errors.
//
//	safe := state.Clone()
//	if err := state.MergeReduce(incoming, reducers); err != nil {
//	    state = safe // roll back to the pre-Merge state
//	}
func (s State) Clone() State {
	clone := State{d: make(map[string]any, len(s.d))}
	maps.Copy(clone.d, s.d)
	return clone
}

// Merge overlays the incoming State's fields onto this State with replace semantics (last write wins),
// mutating the receiver. It is MergeReduce with no reducers.
//
// Merge is an accumulation primitive: a cleared incoming value (Go nil or a JSON null, i.e. a delete
// tombstone) is preserved as-is, not dropped, so a delta can be built up across successive merges. To
// materialize a snapshot - folding pending deletes into actual absences - call DelNils afterward.
func (s State) Merge(incoming State) error {
	return s.MergeReduce(incoming, nil)
}

// MergeReduce overlays the incoming State's fields onto this State, mutating the receiver, using the
// per-field strategy in reducers (as produced by Graph.Reducers). Pass nil for plain last-write-wins on
// every field. Fan-in folds one member at a time, so a single incoming is all that is needed.
//
// A field mapped to ReducerReplace (or absent from reducers, or the empty Reducer) is overwritten,
// tombstones included (see Merge - call DelNils to enact them). A field mapped to any other reducer is
// combined via that reducer, whose base is this State's current value or nil if it holds none yet; the
// reducer is handed the incoming as-is (possibly cleared) and decides how to fold it.
//
// The incoming State is left unmodified. MergeReduce returns the first reducer error, having applied every
// field up to that point.
func (s State) MergeReduce(incoming State, reducers map[string]Reducer) error {
	for k, v := range incoming.d {
		r := reducers[k]
		if r == "" || r == ReducerReplace {
			s.d[k] = v
			continue
		}
		// A cleared incoming is the reducer's identity: ignored, leaving the accumulator untouched. So a
		// branch that deletes a reduced field does not wipe the cohort's contributions to it. (Deleting a
		// field is a REPLACE concern, handled above via the tombstone; reducers only ever combine values.)
		if isCleared(v) {
			continue
		}
		result, err := r.Reduce(s.d[k], v)
		if err != nil {
			return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
		}
		s.d[k] = result
	}
	return nil
}

// DelNils removes every field whose value is cleared - a Go nil or a JSON null. Merge and MergeReduce
// preserve such tombstones (a delete in transit while a changes-delta is accumulated); DelNils enacts
// them, so Merge followed by DelNils materializes a snapshot (the pending deletes become absences).
func (s State) DelNils() {
	for k, v := range s.d {
		if isCleared(v) {
			delete(s.d, k)
		}
	}
}

// MarshalJSON serializes the State's fields as a JSON object.
func (s State) MarshalJSON() ([]byte, error) {
	if s.d == nil {
		return []byte("{}"), nil
	}
	b, err := json.Marshal(s.d)
	return b, errors.Trace(err)
}

// UnmarshalJSON populates the State from a JSON object. Values decode to their JSON-native Go types
// (float64, string, bool, []any, map[string]any).
//
// The input must be a JSON object: it must begin with '{' and end with '}' (surrounding whitespace is
// ignored). Two inputs are tolerated as an empty State instead: an empty/nil payload, and a JSON "null" -
// matching how the engine reads an absent/NULL state or baggage column (and a nil value marshaled to "null").
// Anything else - a JSON array, number, string, or malformed bytes - is an error. The result is always
// initialized, so the mutating methods (Set/Merge), which do not allocate, are safe to call afterward.
func (s *State) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		s.d = map[string]any{}
		return nil
	}
	if data[0] != '{' || data[len(data)-1] != '}' {
		return errors.New("workflow: state must be a JSON object")
	}
	var m map[string]any
	err := json.Unmarshal(data, &m)
	if err != nil {
		return errors.Trace(err)
	}
	// A well-formed object never decodes to a nil map ("null" is handled above).
	s.d = m
	return nil
}
