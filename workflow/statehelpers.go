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
	"reflect"
	"strings"
)

// isCleared reports whether v represents a cleared state slot. State stores decoded values, so a delete
// tombstone - written by flow.Del/Set(name, nil), or decoded from a JSON null in the DB or off the wire -
// is a Go nil.
func isCleared(v any) bool {
	return v == nil
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
