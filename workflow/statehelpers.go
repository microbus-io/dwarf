/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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
			if err := json.Unmarshal(raw, ptr.Interface()); err != nil {
				return err
			}
			fieldVal.Set(ptr.Elem())
		}
	}
	return nil
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
