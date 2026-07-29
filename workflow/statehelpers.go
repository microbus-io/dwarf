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
	"reflect"
	"strings"
)

// isCleared reports whether v represents a cleared state slot - a delete tombstone written by
// flow.Del/Set(name, nil), or a JSON null carried in from a column or off the wire.
//
// A tombstone has TWO spellings and both must be recognised, because a field held as raw JSON has not been
// through the decode that used to turn a JSON null into a Go nil. Missing the raw one is silent and nasty:
// a delete read back from a changes column simply stops taking effect (DelNils leaves it, Has reports the
// field present), so the delete is lost with nothing logged.
func isCleared(v any) bool {
	if v == nil {
		return true
	}
	raw, ok := v.(json.RawMessage)
	if !ok {
		return false
	}
	raw = bytes.TrimSpace(raw)
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
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
