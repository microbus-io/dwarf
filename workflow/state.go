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
	"io"
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
// float64-domain. An integer that exceeds 2^53 is stored exactly; carry it as a string only if it must survive an
// untyped read.
//
// A zero State is empty and safe to read (Get, Has, Len, Names, MarshalJSON all work on it), but the
// mutating methods do not allocate, so one must be initialized via NewState or UnmarshalJSON before
// Set/Apply.
type State struct {
	d map[string]json.RawMessage
}

// NewState builds a State from its arguments, in one of two mutually exclusive modes.
//
// A single argument is normalized into a state map. A []byte or json.RawMessage is treated as raw JSON and
// must be an object (an empty/nil slice or a JSON "null" yields an empty State; any other non-object is an
// error - see UnmarshalJSON) - this is the one-liner for turning a stored JSON object back into a State:
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
// value cannot be marshalled (a NaN, an +Inf, a channel) or is a json.RawMessage that is not valid JSON.
func (s State) Set(name string, value any) error {
	raw, err := marshalField(name, value)
	if err != nil {
		return errors.Trace(err)
	}
	s.d[name] = raw
	return nil
}

// marshalField encodes a value as Set stores it. A json.RawMessage is already JSON and is kept as-is, validated
// but not decoded. A []byte is NOT - it is binary data and marshals to a base64 string, which is what a caller
// storing a blob wants. The two are distinguished by TYPE, so a call site cannot pick the wrong one by accident.
func marshalField(name string, value any) (json.RawMessage, error) {
	if raw, ok := value.(json.RawMessage); ok {
		if !json.Valid(raw) {
			return nil, errors.New("workflow: state field %q: value is not valid JSON", name)
		}
		return bytes.Clone(raw), nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b, nil
}

// SetString stores a string field (normalized through JSON, like Set).
func (s State) SetString(name string, value string) { s.mustSet(name, value) }

// SetStrings stores a string-slice field (normalized through JSON, like Set).
func (s State) SetStrings(name string, value []string) { s.mustSet(name, value) }

// SetInt stores an int field (normalized through JSON, like Set).
func (s State) SetInt(name string, value int) { s.mustSet(name, value) }

// SetFloat stores a float64 field (normalized through JSON, like Set). It panics on NaN or an infinity, which
// JSON cannot represent.
func (s State) SetFloat(name string, value float64) { s.mustSet(name, value) }

// SetBool stores a bool field (normalized through JSON, like Set).
func (s State) SetBool(name string, value bool) { s.mustSet(name, value) }

// SetDuration stores a time.Duration field, in nanoseconds (normalized through JSON, like Set).
func (s State) SetDuration(name string, value time.Duration) { s.mustSet(name, value) }

// mustSet is Set for the typed setters, which have no error to return. The panic is live only for SetFloat's
// NaN or infinity, the one primitive value that does not marshal.
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

// Clear removes every field, leaving a State with no fields.
func (s State) Clear() {
	clear(s.d)
}

// decodeValue turns a stored field into the Go value reducer r folds. An absent or cleared field decodes to nil,
// which is what a reducer expects for "no accumulated value yet". Numbers decode as json.Number, so a fold writes
// back every number it did not compute as the literal it read; merge looks only one level down and compares
// nothing, so it keeps each entry's bytes.
func decodeValue(r Reducer, raw json.RawMessage) (any, error) {
	if isCleared(raw) {
		return nil, nil
	}
	if r == ReducerMerge {
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, errors.Trace(err)
		}
		return m, nil
	}
	// A number or a bool is what add/min/max/and/or fold, on every write; read it without a Decoder, whose
	// setup is most of a Set's allocations.
	if v, ok := scalarValue(raw); ok {
		return v, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, errors.Trace(err)
	}
	// A Decoder stops after the first value, where Unmarshal rejects anything after it. SetJSON does not
	// validate, so without this a field holding `[1] [2]` would fold as `[1]` and hide the bad bytes.
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, errors.New("workflow: invalid JSON: data after the first value")
	}
	return v, nil
}

// scalarValue reads raw as a bool, or as a number in the json.Number form a UseNumber Decoder gives, when it is
// exactly one such literal (JSON whitespace around it allowed). ok is false for anything else, which the Decoder
// then reads.
func scalarValue(raw json.RawMessage) (v any, ok bool) {
	b := bytes.Trim(raw, " \t\r\n")
	switch string(b) {
	case "true":
		return true, true
	case "false":
		return false, true
	}
	if !isNumberLiteral(b) {
		return nil, false
	}
	return json.Number(b), true
}

// isNumberLiteral reports whether b is exactly a JSON number: -? (0 | [1-9][0-9]*) (.[0-9]+)? ([eE][+-]?[0-9]+)?
func isNumberLiteral(b []byte) bool {
	i := 0
	if i < len(b) && b[i] == '-' {
		i++
	}
	switch {
	case i < len(b) && b[i] == '0':
		i++
	case i < len(b) && b[i] >= '1' && b[i] <= '9':
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	default:
		return false
	}
	if i < len(b) && b[i] == '.' {
		i++
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	if i < len(b) && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < len(b) && (b[i] == '+' || b[i] == '-') {
			i++
		}
		start := i
		for i < len(b) && b[i] >= '0' && b[i] <= '9' {
			i++
		}
		if i == start {
			return false
		}
	}
	return i == len(b)
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
// A field's value is decoded only when it is read, so a loop pays for the fields it reads and no others.
// To decode every field at once, Parse into a map[string]any.
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
// single well-formed JSON value and are NOT validated. Use it for bytes you produced or read back from
// storage; pass anything else through Set, which validates them and returns an error for invalid JSON. The
// State keeps data itself rather than a copy, so the caller must not modify it afterwards.
//
// json.RawMessage rather than []byte is deliberate and is the same distinction Set draws: a []byte is binary
// data and belongs in a base64 string, a json.RawMessage is JSON. The type says which, so the compiler
// keeps a call site honest.
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

// IsDeleted reports whether the field is present and holds a JSON null - what Set(name, nil) stores. Applied as
// a contribution (see Changes), a null deletes a field with no reducer and is a reducer's identity otherwise.
func (s State) IsDeleted(name string) bool {
	v, ok := s.d[name]
	return ok && isCleared(v)
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
	// slice, map or interface field the caller had set.
	live := s.d
	for k, v := range s.d {
		if isCleared(v) {
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
//	if err := state.Apply(changes, reducers); err != nil {
//	    state = safe // roll back to the state before the failed fold
//	}
func (s State) Clone() State {
	clone := State{d: make(map[string]json.RawMessage, len(s.d))}
	maps.Copy(clone.d, s.d)
	return clone
}

// Apply folds a step's changes onto this State, mutating the receiver. It is ApplyAll of one Changes.
func (s State) Apply(c Changes, reducers map[string]Reducer) error {
	return s.ApplyAll([]Changes{c}, reducers)
}

// ApplyAll folds several steps' changes onto this State in order, mutating the receiver. For each field:
//
//   - an overwrite takes the new value, or removes the field if the value is null;
//   - a contribution to a field with no reducer, or ReducerReplace, takes the new value, or removes the field
//     if the value is null;
//   - a contribution to any other field is combined through its reducer, whose base is this State's current
//     value or nil if it holds none. A null contributes nothing: it is the reducer's identity;
//   - an addition appends its elements to the field's current value, which must be a list or absent.
//
// Pass nil reducers to replace every field. Each combined field is decoded once and encoded once whatever the
// number of changes, because a combining reducer re-encodes its accumulator, and folding per member would be
// quadratic in the accumulated bytes across a wide fan-in. On a reducer error the receiver is left partly
// folded; discard it.
func (s State) ApplyAll(cs []Changes, reducers map[string]Reducer) error {
	acc := map[string]any{}
	decoded := map[string]bool{}
	// An append or a concat is spliced as bytes rather than decoded: the base and every contribution are collected
	// and joined once at the end, so the cost is linear in the bytes and allocates no decoded values.
	spliced := map[string][]json.RawMessage{}
	set := func(k string, v json.RawMessage) {
		delete(decoded, k)
		delete(acc, k)
		delete(spliced, k)
		if isCleared(v) {
			delete(s.d, k)
		} else {
			s.d[k] = v
		}
	}
	for _, c := range cs {
		for k, v := range c.contributions.d {
			r := reducers[k]
			if r == "" || r == ReducerReplace {
				set(k, v)
				continue
			}
			if isCleared(v) {
				continue
			}
			if r == ReducerAppend || r == ReducerConcat {
				if _, ok := spliced[k]; !ok {
					spliced[k] = []json.RawMessage{s.d[k]}
				}
				spliced[k] = append(spliced[k], v)
				continue
			}
			if !decoded[k] {
				base, err := decodeValue(r, s.d[k])
				if err != nil {
					return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
				}
				if base == nil && r == ReducerUnion {
					// Onto nothing, a single value would start a typed list ([]string) that a later member's
					// decoded []any cannot extend; Apply per member never sees it, re-encoding in between.
					base = []any{}
				}
				acc[k], decoded[k] = base, true
			}
			inc, err := decodeValue(r, v)
			if err != nil {
				return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
			}
			result, err := r.Reduce(acc[k], inc)
			if err != nil {
				return errors.New("reducer '%s' for field %q: %w", string(r), k, err)
			}
			acc[k] = result
		}
		for k, v := range c.overwrites.d {
			set(k, v)
		}
		// An addition is only ever recorded on a field with no reducer, so nothing is pending for it here: it
		// extends the field's current value.
		for k, v := range c.additions.d {
			extended, err := extend(k, s.d[k], v)
			if err != nil {
				return errors.Trace(err)
			}
			set(k, extended)
		}
	}
	for k := range decoded {
		encoded, err := json.Marshal(acc[k])
		if err != nil {
			return errors.New("reducer for field %q: %w", k, err)
		}
		s.d[k] = encoded
	}
	for k, parts := range spliced {
		var joined json.RawMessage
		var err error
		if reducers[k] == ReducerConcat {
			joined, err = concatRaw(k, parts[0], parts[1:])
		} else {
			joined, err = appendRaw(k, parts[0], parts[1:], false)
		}
		if err != nil {
			return errors.Trace(err)
		}
		s.d[k] = joined
	}
	return nil
}

// concatRaw is ReducerConcat on JSON bytes: base's string followed by each incoming string, spliced between the
// quotes rather than decoded. JSON escapes are self-contained, so the joined bytes decode to the joined string.
// (A UTF-16 surrogate pair escaped across two writes, "\ud83d" then "\ude00", is the exception: decoded
// separately each half reads as U+FFFD, spliced the pair is whole. Go's encoder never escapes that way.) A base
// that is absent or null is the empty string; a base or an incoming value that is not a string is an error, as
// it is for the reducer.
func concatRaw(name string, base json.RawMessage, incoming []json.RawMessage) (json.RawMessage, error) {
	isString := func(v []byte) bool { return len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' }
	parts := make([][]byte, 0, len(incoming)+1)
	size := 2
	b := bytes.TrimSpace(base)
	switch {
	case len(b) == 0 || bytes.Equal(b, []byte("null")):
	case isString(b):
		parts = append(parts, b[1:len(b)-1])
		size += len(b) - 2
	default:
		return nil, errors.New("reducer '%s' for field %q: base must be a string", string(ReducerConcat), name)
	}
	for _, inc := range incoming {
		v := bytes.TrimSpace(inc)
		if !isString(v) {
			return nil, errors.New("reducer '%s' for field %q: incoming must be a string", string(ReducerConcat), name)
		}
		parts = append(parts, v[1:len(v)-1])
		size += len(v) - 2
	}
	out := make([]byte, 0, size)
	out = append(out, '"')
	for _, p := range parts {
		out = append(out, p...)
	}
	return append(out, '"'), nil
}

// appendRaw is ReducerAppend on JSON bytes: the elements of base followed by those of each incoming value, where
// an incoming list contributes its elements and anything else is one element. The bytes are spliced, never
// decoded, so a long list costs a copy rather than a decode and re-encode. A base that is absent or null is the
// empty list; a base that is not a list is an error, as it is for the reducer, unless bareBase says a single
// value stored by an older writer stands for the one-element list it is. Every input is already valid JSON: it
// was marshalled by Set or read from a column or a decoded payload.
func appendRaw(name string, base json.RawMessage, incoming []json.RawMessage, bareBase bool) (json.RawMessage, error) {
	parts := make([][]byte, 0, len(incoming)+1)
	size := 2
	add := func(elems []byte) {
		if len(elems) > 0 {
			parts = append(parts, elems)
			size += len(elems) + 1
		}
	}
	b := bytes.TrimSpace(base)
	switch {
	case len(b) == 0 || bytes.Equal(b, []byte("null")):
	case b[0] == '[':
		add(listElements(b))
	case bareBase:
		add(b)
	default:
		return nil, errors.New("reducer '%s' for field %q: base must be a slice or array", string(ReducerAppend), name)
	}
	for _, inc := range incoming {
		v := bytes.TrimSpace(inc)
		if len(v) > 0 && v[0] == '[' {
			add(listElements(v))
		} else {
			add(v)
		}
	}
	out := make([]byte, 0, size)
	out = append(out, '[')
	for i, p := range parts {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, p...)
	}
	return append(out, ']'), nil
}

// listElements is what lies between a JSON list's brackets, trimmed; empty for an empty list.
func listElements(list []byte) []byte {
	return bytes.TrimSpace(list[1 : len(list)-1])
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
// matching an absent or NULL stored value (and a nil value marshaled to "null"). Anything else - a JSON
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
