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

// Package cancelmarker builds the error the engine delivers when its Cancel operation covers a step;
// workflow.IsCancelled detects it. It is internal so the constructor is not part of the public API - detection
// is by a plain error property, so it does not stop a task from building one by hand.
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
	// Never pass the message as the pattern: errors.New treats every % in it as a format verb consuming a
	// trailing argument, so an operator's "50% off" would swallow the marker property.
	return errors.New("%s", msg, Property, Value)
}
