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

// Package cancelmarker builds the error the engine delivers when its Cancel operation covers a step. It is
// internal so that only the engine can construct one; workflow.IsCancelled detects it.
package cancelmarker

import "github.com/microbus-io/errors"

// Property and Value mark an error as a cancellation. The workflow package keeps its own copy of both for
// IsCancelled, and its tests pin the two together.
const (
	Property = "dwarf_type"
	Value    = "cancelled"
)

// New returns a cancellation error carrying the operator's reason, if one was given.
func New(reason string) error {
	msg := "flow cancelled"
	if reason != "" {
		msg += ": " + reason
	}
	return errors.New(msg, Property, Value)
}
