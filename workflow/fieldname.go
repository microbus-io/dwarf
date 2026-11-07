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
	"github.com/microbus-io/errors"
)

// fieldNameRule describes a valid state field name, for error messages.
const fieldNameRule = "a state field name must start with an ASCII letter or underscore, followed by ASCII letters, digits, underscores or hyphens"

// IsValidFieldName reports whether name is a valid state field name: an ASCII letter or underscore, followed by
// ASCII letters, digits, underscores or hyphens. "orderID", "line_item" and "line-item-2" are valid; "2nd",
// "-x", "a.b" and "total!" are not.
//
// A name with a hyphen is valid but cannot be referenced from a transition's 'when' expression, which reads
// letters, digits and underscores.
func IsValidFieldName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_', 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z':
		case i > 0 && ('0' <= c && c <= '9' || c == '-'):
		default:
			return false
		}
	}
	return true
}

// mustBeFieldName panics if name is not a valid state field name. A Flow writer calls it first: a bad name is a
// typo in the task's code, and an error the task ignores would drop the write without a word. The orchestrator
// turns the panic into a failure of the step.
func mustBeFieldName(name string) {
	if !IsValidFieldName(name) {
		panic(errors.New("invalid state field name %q: %s", name, fieldNameRule))
	}
}

// checkFieldName returns an error if name is not a valid state field name.
func checkFieldName(name string) error {
	if !IsValidFieldName(name) {
		return errors.New("invalid state field name %q: %s", name, fieldNameRule)
	}
	return nil
}
