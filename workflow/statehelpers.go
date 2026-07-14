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
	"encoding/json"
	"reflect"
	"strings"

	"github.com/microbus-io/dwarf/internal/jsonx"
	"github.com/microbus-io/errors"
)

// getFromMap unmarshals a value from a map into the target. Treats a cleared
// slot (Go nil or JSON null) as absent so the target keeps its zero value.
func getFromMap(m map[string]any, key string, target any) error {
	val, ok := m[key]
	if !ok || isCleared(val) {
		return nil
	}
	raw, err := marshalAny(val)
	if err != nil {
		return errors.Trace(err)
	}
	return json.Unmarshal(raw, target)
}

// mustGetFromMap is getFromMap for the single-return typed getters, which have no channel to report a
// type mismatch through. It PANICS when the key holds a value of the wrong type - a task reading
// GetInt("retryAfter") off a 1.5 must not silently proceed with 0, which is how a zero-delay retry loop
// against a downstream gets built. The panic is not a crash: the orchestrator wraps its task call in a
// panic catcher, so this surfaces as a normal step failure (routed to onError if the graph has one, else
// the step fails) carrying the stack trace. A task that wants to HANDLE a mistyped field instead of
// failing on it uses Get, which returns the error.
//
// Absent and cleared keys are not a mismatch - they yield the zero value, so an optional field still
// reads as one.
func mustGetFromMap(m map[string]any, key string, target any) {
	if err := getFromMap(m, key, target); err != nil {
		panic(errors.New("state field %q is not a %s: %v", key, reflect.TypeOf(target).Elem(), err))
	}
}

// isCleared reports whether v represents a cleared state slot. Either a Go nil
// or a json.RawMessage equal to "null" (after trimming whitespace) qualifies.
// Both forms appear after Clear or Set(name, nil).
func isCleared(v any) bool {
	if v == nil {
		return true
	}
	if raw, ok := v.(json.RawMessage); ok {
		return strings.TrimSpace(string(raw)) == "null"
	}
	return false
}

// parseMapInto unmarshals fields from a map into a struct, matching by JSON tag name.
func parseMapInto(m map[string]any, target any) error {
	if m == nil {
		return nil
	}
	v := reflect.ValueOf(target)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()
	if t.Kind() != reflect.Struct {
		data, err := json.Marshal(m)
		if err != nil {
			return errors.Trace(err)
		}
		return json.Unmarshal(data, target)
	}
	for i := range t.NumField() {
		field := t.Field(i)
		tag := jsonTagName(field)
		if tag == "" || tag == "-" {
			continue
		}
		val, ok := m[tag]
		if !ok || isCleared(val) {
			continue
		}
		fieldVal := v.Field(i)
		if fieldVal.CanSet() {
			raw, err := marshalAny(val)
			if err != nil {
				return errors.Trace(err)
			}
			ptr := reflect.New(field.Type)
			err = json.Unmarshal(raw, ptr.Interface())
			if err != nil {
				return err
			}
			fieldVal.Set(ptr.Elem())
		}
	}
	return nil
}

// toStateMap converts an arbitrary JSON-marshalable value (a struct, or a map) into a state map. A nil
// value yields a nil map ("no arguments").
//
// EVERY input is round-tripped through JSON, including a map[string]any that could have been returned
// as-is. Three things follow from the round trip, and the map short-circuit forfeited all three:
//
//   - The payload is VALIDATED. A value that cannot be marshalled (a NaN, an +Inf, a channel) or cannot
//     be stored (jsonx) is reported to the caller here. Short-circuited, the same NaN was accepted from a
//     map and rejected from a struct, and only surfaced later as an opaque step failure from deep inside
//     the orchestrator.
//   - The Flow gets its OWN COPY. The caller's map is live: a task that keeps a reference and mutates it
//     after the call would otherwise be editing the payload the orchestrator is about to persist, from
//     under it.
//   - The shape is uniform - plain decoded values, as if they had come back from the database - rather
//     than whatever Go types the caller happened to hold.
func toStateMap(v any) (map[string]any, error) {
	if v == nil {
		return nil, nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = jsonx.CheckStorable(data)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var m map[string]any
	err = json.Unmarshal(data, &m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return m, nil
}

// jsonTagName extracts the JSON tag name from a struct field.
func jsonTagName(field reflect.StructField) string {
	tag := field.Tag.Get("json")
	if tag == "" {
		return ""
	}
	name, _, _ := strings.Cut(tag, ",")
	return name
}

// marshalAny marshals a value to JSON bytes. If the value is already json.RawMessage, it is returned as-is.
func marshalAny(v any) ([]byte, error) {
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(v)
}
