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

// State is a JSON-serializable key/value carrier for workflow data. Values are read and written through its
// methods - Get for a typed read into a caller pointer, Set to store, Has/IsDeleted/Len/Del/Clear to inspect
// and remove, Names to iterate. It wraps a map and is a reference type: methods take a value receiver yet
// mutate through to every copy of the State sharing that map.
//
// The surface is a 2x2 over one question, what the CALLER holds:
//
//	                 in                      out
//	Go value    Set, NewState(v)        Get, GetInt/GetString/..., Parse
//	JSON        SetJSON, NewState(b)    GetJSON, MarshalJSON
//
// # Values are stored as JSON
//
// A field is held as its JSON encoding and decoded only when read. That is what lets a large field be
// carried through a step without being expanded into a Go value for the duration - a step's state is a full
// input snapshot, so most fields on most steps are carried rather than read.
//
// The price is that the Go TYPE is not preserved across a store: JSON has one number type, so a value stored
// as an int and read into an any comes back a float64. Read through a typed getter (GetInt, GetString, ...)
// or through Get with a typed target and you get the type you asked for; only an untyped read is
// float64-domain. Carry an integer that exceeds 2^53 as a string.
//
// A zero State is empty and safe to read (Get, Has, Len, Names, MarshalJSON all work on it), but the
// mutating methods do not allocate, so one must be initialized via NewState or UnmarshalJSON before
// Set/Merge.
type State struct {
	d map[string]json.RawMessage
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
	s := State{d: map[string]json.RawMessage{}}
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
			// One marshal plus a SHALLOW split into fields - the field values are kept as the encoder wrote
			// them rather than decoded and held as Go values.
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
		// Normalized through Set, not stored verbatim. Storing a caller's Go value as-is would put a struct
		// in the map, and a struct does not compare equal to its own decoded twin under a reducer's
		// reflect.DeepEqual - the same mismatch Set's round trip exists to prevent everywhere else.
		if err := s.Set(name, data[i+1]); err != nil {
			return State{}, errors.Trace(err)
		}
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
	// A json.RawMessage is already JSON and is stored as-is, validated but not decoded. A []byte is NOT -
	// it is binary data and marshals to a base64 string, which is what a caller storing a blob wants. The
	// two are distinguished by TYPE, so a call site cannot pick the wrong one by accident.
	if raw, ok := value.(json.RawMessage); ok {
		if !json.Valid(raw) {
			return errors.New("workflow: state field %q: value is not valid JSON", name)
		}
		s.d[name] = raw
		return nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return errors.Trace(err)
	}
	s.d[name] = b
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

// decodeValue turns a stored field into a Go value. An absent or cleared field decodes to nil, which is
// what a reducer expects for "no accumulated value yet".
func decodeValue(raw json.RawMessage) (any, error) {
	if isCleared(raw) {
		return nil, nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, errors.Trace(err)
	}
	return v, nil
}

// Names iterates the field names in unspecified order, for use with range-over-func:
//
//	for name := range state.Names() { ... }
//
// Pair it with Get (or a typed getter) to read only the fields you want:
//
//	for name := range state.Names() {
//	    var v MyType
//	    if ok, _ := state.Get(name, &v); ok { ... }
//	}
//
// There is deliberately NO name/value iterator. Yielding values means materializing every one of them, and
// a field held as raw JSON is decoded to be yielded - so a loop that reads two fields out of forty would pay
// the decode for all forty, which is the exact cost this type is built to avoid. Iterating names is free;
// make the reads explicit. Parse into a map[string]any is the escape hatch when a whole materialized copy
// really is wanted, and it says so at the call site because the caller writes the target.
//
// Collect a slice with slices.Collect(state.Names()) if one is needed. Do not mutate the State during
// iteration.
func (s State) Names() iter.Seq[string] {
	return func(yield func(string) bool) {
		for k := range s.d {
			if !yield(k) {
				return
			}
		}
	}
}

// IsZero reports whether this is the zero State (never initialized). An initialized but empty State is not
// zero. It is what lets a State field be omitted from an encoded struct via the omitzero tag.
func (s State) IsZero() bool {
	return s.d == nil
}

// GetJSON returns the field's value as JSON, or nil if the field is absent. Use it when JSON is what is
// wanted - sizing a field, splicing it into a document, forwarding it somewhere that takes JSON - rather
// than decoding to a Go value only to encode it again. Since values are stored as JSON, this is the read
// that costs nothing.
//
// The returned slice is the state's own. Do not modify it.
func (s State) GetJSON(name string) json.RawMessage {
	return s.d[name]
}

// SetJSON stores a field's value as pre-encoded JSON. It is the unchecked twin of Set: the bytes must be a
// single well-formed JSON value and are NOT validated, which is why it returns nothing - a method with no
// error return cannot be reporting one. Use it for bytes you produced or read back from storage; pass
// anything else through Set, which validates and can tell you.
//
// json.RawMessage rather than []byte is deliberate and is the same distinction Set draws: a []byte is binary
// data and belongs in a base64 string, a json.RawMessage is JSON. The type says which, so the compiler
// keeps a call site honest.
func (s State) SetJSON(name string, data json.RawMessage) {
	s.d[name] = data
}

// Get unmarshals the field named name into target, a non-nil pointer, coercing through JSON (so a stored
// float64 reads into an int, a stored object into a struct, etc.). It returns ok=true only when the field is
// present and was assigned. An absent or cleared field (Go nil or JSON null) returns (false, nil), leaving
// target untouched. A value that cannot be unmarshalled into target's type returns (false, err). Use this to
// HANDLE a type mismatch; the typed getters (GetInt, ...) panic on one instead.
func (s State) Get(name string, target any) (ok bool, err error) {
	raw, exists := s.d[name]
	if !exists || isCleared(raw) {
		return false, nil
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

// Has reports whether a field is present and holds a value. A DELETED field reads as absent, which is what
// a caller asking "is there something here to read" wants - see IsDeleted for the other question.
func (s State) Has(name string) bool {
	v, ok := s.d[name]
	return ok && !isCleared(v)
}

// IsDeleted reports whether the field is present as a DELETE - the marker Del writes, and the form a delete
// takes while it is in transit in a changes delta. It is false both for a field holding a value and for one
// that was never mentioned at all; Has answers the first, and neither being true means the second.
//
// A delta is the only place this is normally observable: materialized state has its deletes enacted, so
// nothing there is deleted-and-present.
func (s State) IsDeleted(name string) bool {
	v, ok := s.d[name]
	return ok && isCleared(v)
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
// (Set/Del/Merge) do not affect the other. Field values are shared, which is safe because a stored value is
// an immutable byte slice - nothing can rewrite one in place - and it is what makes cloning a state with a
// large carried field cost a map entry rather than the field.
//
//	safe := state.Clone()
//	if err := state.MergeReduce(incoming, reducers); err != nil {
//	    state = safe // roll back to the pre-Merge state
//	}
func (s State) Clone() State {
	clone := State{d: make(map[string]json.RawMessage, len(s.d))}
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
		// A combining reducer works on Go values, so both operands are decoded here and the result is
		// re-encoded. The REPLACE path above moves bytes and pays none of it, which is the overwhelming
		// majority of fields. See the note on Fold for why a wide fan-in should not drive this per member.
		base, err := decodeValue(s.d[k])
		if err != nil {
			return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
		}
		inc, err := decodeValue(v)
		if err != nil {
			return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
		}
		result, err := r.Reduce(base, inc)
		if err != nil {
			return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
		}
		s.d[k] = encoded
	}
	return nil
}

// MergeReduceAll folds a whole cohort of incoming deltas in one pass, in order, and is what a fan-in should
// use instead of calling MergeReduce once per member.
//
// The difference is not stylistic. Values are stored as JSON, so a COMBINING reducer (append, union, add,
// merge, ...) decodes its accumulator, folds, and re-encodes; doing that per member re-encodes an
// accumulator that grows as it goes, which is quadratic in the accumulated bytes across a wide cohort - at
// width 1,000 roughly 500x the element encodes of a single pass. Here each combined field is decoded once,
// folded against every member, and encoded once, whatever the width.
//
// Replace-reducer fields (the default, and the overwhelming majority) never decode at all on either path -
// they move bytes - so this only changes the cost of fields the graph actually registered a reducer for.
// Semantics are identical to folding one at a time: same order, same tombstone accumulation, and the first
// error stops the fold with everything before it applied.
func (s State) MergeReduceAll(incoming []State, reducers map[string]Reducer) error {
	if len(incoming) == 0 {
		return nil
	}
	if len(reducers) == 0 {
		for _, in := range incoming {
			if err := s.Merge(in); err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	}
	// Accumulators for the combined fields only, held decoded across the whole cohort.
	acc := map[string]any{}
	decoded := map[string]bool{}
	for _, in := range incoming {
		for k, v := range in.d {
			r := reducers[k]
			if r == "" || r == ReducerReplace {
				s.d[k] = v
				continue
			}
			if isCleared(v) {
				continue // the reducer's identity - see MergeReduce
			}
			if !decoded[k] {
				base, err := decodeValue(s.d[k])
				if err != nil {
					return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
				}
				acc[k], decoded[k] = base, true
			}
			inc, err := decodeValue(v)
			if err != nil {
				return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
			}
			result, err := r.Reduce(acc[k], inc)
			if err != nil {
				return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
			}
			acc[k] = result
		}
	}
	for k := range decoded {
		encoded, err := json.Marshal(acc[k])
		if err != nil {
			return errors.New("reducer for field %q: %w", k, err)
		}
		s.d[k] = encoded
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

// UnmarshalJSON populates the State from a JSON object. Reading is unaffected by how it got here: Get and
// the typed getters decode into whatever target you name, exactly as for a State built field by field.
//
// The whole document is validated here, so malformed input is an error now rather than a surprise later;
// individual field values are decoded when they are read, which is what lets a field be carried through a
// step without being expanded.
//
// The input must be a JSON object: it must begin with '{' and end with '}' (surrounding whitespace is
// ignored). Two inputs are tolerated as an empty State instead: an empty/nil payload, and a JSON "null" -
// matching how an absent or NULL column reads (and a nil value marshaled to "null"). Anything else - a JSON
// array, number, string, or malformed bytes - is an error. The result is always initialized, so the
// mutating methods (Set/Merge), which do not allocate, are safe to call afterward.
func (s *State) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		s.d = map[string]json.RawMessage{}
		return nil
	}
	if data[0] != '{' || data[len(data)-1] != '}' {
		return errors.New("workflow: state must be a JSON object")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return errors.Trace(err)
	}
	// A well-formed object never decodes to a nil map ("null" is handled above).
	s.d = fields
	return nil
}
