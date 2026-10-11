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
// # Metadata
//
// A State may also carry metadata about its fields, such as the reducer a field is declared with (see Meta).
// The orchestrator maintains it. It travels with the state's JSON under the reserved name MetaKey, but it is
// not a field: Names, Len, Get, GetJSON, Has and Parse do not see it, Set rejects the name, and Apply never
// takes it from the incoming State.
//
// A zero State is empty and safe to read (Get, Has, Len, Names, MarshalJSON all work on it), but the
// mutating methods do not allocate, so one must be initialized via NewState or UnmarshalJSON before
// Set/Apply.
type State struct {
	d map[string]json.RawMessage
}

// MetaKey is the reserved name under which a State's JSON carries its per-field metadata. No field may have
// this name.
const MetaKey = "__meta__"

// FieldMeta is what a State records about one of its fields beyond its value. A field may have metadata
// without having a value, such as a reduced field nothing has written yet.
type FieldMeta struct {
	// Reducer is the reducer the field is declared with. Empty means ReducerReplace.
	Reducer Reducer `json:"reducer,omitempty"`
	// Reduced is set only on the state of a fan-in step, for a field its cohort wrote, and describes which
	// part of the field's value the cohort added.
	Reduced *Reduced `json:"reduced,omitempty"`
	// Overwrite is set only in a delta, on a field the delta replaces rather than combines (see Overwrite).
	Overwrite bool `json:"overwrite,omitempty"`
}

// Reduced describes which part of a fan-in step's value for a field its cohort added. Exactly one form is
// set, chosen by the field's reducer:
//   - From, for append, union and concat: the cohort's part starts at this offset into the value (array
//     elements, or bytes of a concat string);
//   - Keys, for merge: the keys the cohort wrote; and for a field with no reducer, an empty list meaning only
//     that the cohort wrote it;
//   - Value, for add, min, max, and and or: the cohort's combined contribution itself.
type Reduced struct {
	From  *int            `json:"from,omitempty"`
	Keys  []string        `json:"keys,omitzero"`
	Value json.RawMessage `json:"value,omitzero"`
}

// NewState builds a State from its arguments, in one of two mutually exclusive modes.
//
// A single argument is normalized into a state map. A []byte or json.RawMessage is treated as raw JSON and
// must be an object (an empty/nil slice or a JSON "null" yields an empty State; any other non-object is an
// error - see UnmarshalJSON) - this is the one-liner for reading a state column back from the database:
// state, _ := NewState(stateJSON). A nil argument, including a nil map, pointer or slice, yields an empty State. A
// State is already normalized, so it is shallow-copied. Any other value - a map, a struct, or any
// JSON-marshalable value - is marshalled once and split into its top-level fields, each held as JSON just as
// Set would hold it; it must marshal to a JSON object. This is the normalizer for a caller-supplied value at
// an API boundary: wrap it in NewState. It does NOT validate value ranges: an integer beyond ±2^53 reads back
// inexact through an untyped reader (carry it as a string instead), and a NUL byte in a string is rejected by
// some SQL dialects' JSON storage (base64-encode binary data instead).
//
// Two or more arguments are variadic name/value pairs, e.g. NewState("count", 3, "name", "abc"); each value
// is stored through Set. An odd count, or a non-string in a name position, is an error.
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
			if s.d == nil { // a nil map or pointer marshals to null
				s.d = map[string]json.RawMessage{}
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
		if err := s.Set(name, data[i+1]); err != nil {
			return State{}, errors.Trace(err)
		}
	}
	return s, nil
}

// Set stores value under name as its JSON encoding, exactly as it would be held after a read from the
// database. So a task that stores an int reads it back as an int via GetInt, but a plain Get into an any
// sees a float64 - JSON has one number type, and the typed getters coerce. The stored bytes are the State's
// own, so a later mutation of the caller's value cannot change what was stored. Returns an error only if
// value cannot be marshalled (a NaN, an +Inf, a channel), is a json.RawMessage that is not valid JSON, or
// name is MetaKey.
func (s State) Set(name string, value any) error {
	if name == MetaKey {
		return errors.New("workflow: state field name %q is reserved", name)
	}
	// A json.RawMessage is already JSON and is stored as-is, validated but not decoded. A []byte is NOT -
	// it is binary data and marshals to a base64 string, which is what a caller storing a blob wants. The
	// two are distinguished by TYPE, so a call site cannot pick the wrong one by accident.
	if raw, ok := value.(json.RawMessage); ok {
		if !json.Valid(raw) {
			return errors.New("workflow: state field %q: value is not valid JSON", name)
		}
		s.d[name] = bytes.Clone(raw)
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
	if _, ok := s.d[MetaKey]; ok {
		return len(s.d) - 1
	}
	return len(s.d)
}

// Del removes the named fields. Names that are not present are ignored.
func (s State) Del(names ...string) {
	for _, name := range names {
		if name != MetaKey {
			delete(s.d, name)
		}
	}
}

// Clear removes every field, leaving a State with no fields. Its metadata is kept.
func (s State) Clear() {
	meta, ok := s.d[MetaKey]
	clear(s.d)
	if ok {
		s.d[MetaKey] = meta
	}
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
			if k == MetaKey {
				continue
			}
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
	if name == MetaKey {
		return nil
	}
	return s.d[name]
}

// SetJSON stores a field's value as pre-encoded JSON. It is the unchecked twin of Set: the bytes must be a
// single well-formed JSON value and are NOT validated, which is why it returns nothing - a method with no
// error return cannot be reporting one. Use it for bytes you produced or read back from storage; pass
// anything else through Set, which validates and can tell you. The State keeps data itself rather than a
// copy, so the caller must not modify it afterwards.
//
// json.RawMessage rather than []byte is deliberate and is the same distinction Set draws: a []byte is binary
// data and belongs in a base64 string, a json.RawMessage is JSON. The type says which, so the compiler
// keeps a call site honest.
//
// name must not be MetaKey.
func (s State) SetJSON(name string, data json.RawMessage) {
	s.d[name] = data
}

// Get unmarshals the field named name into target, a non-nil pointer, and reports whether the field was
// present. It coerces through JSON, so a stored number reads into an int or a float64 and a stored object
// into a struct or a map.
//
// An absent or cleared field (removed with Del, or set to nil) returns (false, nil) and leaves target
// untouched. A present value that cannot be unmarshalled into target's type returns (false, err); the typed
// getters (GetInt, ...) panic on the same mismatch instead.
func (s State) Get(name string, target any) (ok bool, err error) {
	raw, exists := s.d[name]
	if !exists || isCleared(raw) || name == MetaKey {
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
	return ok && !isCleared(v) && name != MetaKey
}

// IsDeleted reports whether this State, used as a delta, deletes the field: it records the field as
// overwritten with no value (see Overwrite), or holds a JSON null for it. It is false both for a field holding
// a value and for one the delta does not write at all; Has answers the first, and neither being true means
// the second. Del removes the field outright rather than recording a delete, so a field removed with Del is
// indistinguishable from one never mentioned.
//
// A delta is the only place this is observable: a materialized state has its deletes enacted.
func (s State) IsDeleted(name string) bool {
	if name == MetaKey {
		return false
	}
	v, ok := s.d[name]
	if ok {
		return isCleared(v)
	}
	return s.IsOverwritten(name)
}

// Parse unmarshals the state's fields into target, typically a pointer to a struct whose fields are matched
// by their JSON tags; any JSON-unmarshalable target works, a map included. State fields with no matching
// struct field are ignored, cleared fields are skipped, and struct fields with no matching state field keep
// their values. Parsing a zero State does nothing and returns nil.
func (s State) Parse(target any) error {
	if s.d == nil {
		return nil
	}
	// A cleared field is skipped rather than handed to the decoder as a null, which would reset a pointer,
	// slice, map or interface field the caller had set. The metadata is not a field.
	live := s.d
	for k, v := range s.d {
		if isCleared(v) || k == MetaKey {
			if len(live) == len(s.d) {
				live = maps.Clone(s.d)
			}
			delete(live, k)
		}
	}
	data, err := json.Marshal(live)
	if err != nil {
		return errors.Trace(err)
	}
	return json.Unmarshal(data, target)
}

// Clone returns a copy of the State backed by a freshly allocated map, so mutations to either side
// (Set/Del/Apply) do not affect the other. Field values are shared, which is safe because a stored value is
// an immutable byte slice - nothing can rewrite one in place - and it is what makes cloning a state with a
// large carried field cost a map entry rather than the field.
//
//	safe := state.Clone()
//	if err := state.Apply(delta, reducers); err != nil {
//	    state = safe // roll back to the state before the failed fold
//	}
func (s State) Clone() State {
	clone := State{d: make(map[string]json.RawMessage, len(s.d))}
	maps.Copy(clone.d, s.d)
	return clone
}

// Apply folds a delta onto this State, mutating the receiver. It is ApplyAll of one delta.
func (s State) Apply(delta State, reducers map[string]Reducer) error {
	return s.ApplyAll([]State{delta}, reducers)
}

// ApplyAll folds deltas onto this State in order, mutating the receiver. A delta is what a step writes: a value
// per field it wrote, and a mark (see Overwrite) on each field it overwrote or deleted. For each field:
//
//   - a field marked as overwritten takes the delta's value, or is removed if the delta holds none;
//   - a field with no reducer, or ReducerReplace, takes the delta's value, or is removed if it is null;
//   - any other field is combined through its reducer, whose base is this State's current value or nil if it
//     holds none. A null contributes nothing: it is the reducer's identity.
//
// The receiver's metadata is kept, and the deltas' is consumed rather than copied. Pass nil reducers to replace
// every field. Each combined field is decoded once and encoded once whatever the number of deltas, because a
// combining reducer re-encodes its accumulator, and folding per delta would be quadratic in the accumulated
// bytes across a wide fan-in. On a reducer error the receiver is left partly folded; discard it.
func (s State) ApplyAll(deltas []State, reducers map[string]Reducer) error {
	acc := map[string]any{}
	decoded := map[string]bool{}
	set := func(k string, v json.RawMessage) {
		delete(decoded, k)
		delete(acc, k)
		if isCleared(v) {
			delete(s.d, k)
		} else {
			s.d[k] = v
		}
	}
	for _, d := range deltas {
		overwritten := d.overwritten()
		for k, v := range d.d {
			if k == MetaKey {
				continue
			}
			r := reducers[k]
			if overwritten[k] || r == "" || r == ReducerReplace {
				set(k, v)
				continue
			}
			if isCleared(v) {
				continue
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
		for k := range overwritten {
			if _, written := d.d[k]; !written {
				set(k, nil)
			}
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

// Compose folds a later delta onto this one, mutating the receiver, so that applying the result equals applying
// the two in turn. A field the later delta overwrites is overwritten in the result. A later write to a field the
// receiver overwrote folds onto the overwritten value, and the field stays overwritten. Two writes to a reduced
// field combine through its reducer, the first onto nil, so a single value under ReducerAppend becomes a list. On
// a reducer error the receiver is left partly composed; discard it.
func (s State) Compose(later State, reducers map[string]Reducer) error {
	marks := s.overwritten()
	laterMarks := later.overwritten()
	for k, v := range later.d {
		if k == MetaKey {
			continue
		}
		r := reducers[k]
		switch {
		case laterMarks[k], (r == "" || r == ReducerReplace) && isCleared(v):
			// An overwrite, or a delete of a plain field, which is an overwrite with nothing.
			marks[k] = true
			if isCleared(v) {
				delete(s.d, k)
			} else {
				s.d[k] = v
			}
		case r == "" || r == ReducerReplace:
			s.d[k] = v
		case isCleared(v):
			// The reducer's identity.
		default:
			base, err := decodeValue(s.d[k])
			if err != nil {
				return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
			}
			if _, isList := base.([]any); base != nil && !isList && !marks[k] && (r == ReducerAppend || r == ReducerUnion) {
				// A delta's value is the fold of its writes onto nothing, which under append and union is a list;
				// an older writer may have stored a bare value, which is the one-element list it stands for.
				base = []any{base}
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
				return errors.New("reducer for field %q: %w", k, err)
			}
			s.d[k] = encoded
		}
	}
	for k := range laterMarks {
		if _, written := later.d[k]; !written {
			marks[k] = true
			delete(s.d, k)
		}
	}
	s.setOverwritten(marks)
	return nil
}

// Overwrite records, in this State used as a delta, that the field is replaced by value rather than combined
// through its reducer. A nil value records a delete. See Apply.
func (s State) Overwrite(name string, value any) error {
	if name == MetaKey {
		return errors.New("workflow: state field name %q is reserved", name)
	}
	if value == nil {
		delete(s.d, name)
	} else if err := s.Set(name, value); err != nil {
		return errors.Trace(err)
	}
	marks := s.overwritten()
	marks[name] = true
	s.setOverwritten(marks)
	return nil
}

// IsOverwritten reports whether this State, used as a delta, records the field as overwritten or deleted.
func (s State) IsOverwritten(name string) bool {
	return s.overwritten()[name]
}

// Writes iterates the fields this State, used as a delta, writes: every field it holds a value for, and every
// field it records as overwritten or deleted. Order is unspecified.
func (s State) Writes() iter.Seq[string] {
	return func(yield func(string) bool) {
		for k := range s.Names() {
			if !yield(k) {
				return
			}
		}
		for k := range s.overwritten() {
			if _, ok := s.d[k]; ok {
				continue
			}
			if !yield(k) {
				return
			}
		}
	}
}

// overwritten returns the set of fields the state's metadata marks as overwritten, never nil.
func (s State) overwritten() map[string]bool {
	marks := map[string]bool{}
	for k, fm := range s.Meta() {
		if fm.Overwrite {
			marks[k] = true
		}
	}
	return marks
}

// setOverwritten records exactly the given fields as overwritten, keeping the rest of the metadata.
func (s State) setOverwritten(marks map[string]bool) {
	meta := s.Meta()
	if meta == nil {
		meta = map[string]FieldMeta{}
	}
	for k, fm := range meta {
		fm.Overwrite = marks[k]
		if fm == (FieldMeta{}) {
			delete(meta, k)
		} else {
			meta[k] = fm
		}
	}
	for k := range marks {
		if _, ok := meta[k]; !ok {
			meta[k] = FieldMeta{Overwrite: true}
		}
	}
	s.SetMeta(meta)
}

// Meta returns a copy of the state's per-field metadata, keyed by field name, or nil if it has none.
func (s State) Meta() map[string]FieldMeta {
	raw, ok := s.d[MetaKey]
	if !ok || isCleared(raw) {
		return nil
	}
	var meta map[string]FieldMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return nil
	}
	return meta
}

// SetMeta replaces the state's per-field metadata. A nil or empty map removes it. Like Set, it needs an
// initialized State.
func (s State) SetMeta(meta map[string]FieldMeta) {
	if len(meta) == 0 {
		delete(s.d, MetaKey)
		return
	}
	b, err := json.Marshal(meta)
	if err != nil {
		// The metadata the engine builds always marshals; only a Reduced.Value that is not valid JSON fails.
		panic(errors.Trace(err))
	}
	s.d[MetaKey] = b
}

// MetaJSON returns the state's per-field metadata as JSON, or nil if it has none. The returned slice is the
// state's own. Do not modify it.
func (s State) MetaJSON() json.RawMessage {
	return s.d[MetaKey]
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
// mutating methods (Set/Apply), which do not allocate, are safe to call afterward.
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
