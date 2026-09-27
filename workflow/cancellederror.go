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

import "github.com/microbus-io/errors"

// The marker the engine attaches to a cancellation error. It is a property rather than a Go type because onErr
// is read back from JSON, which does not restore types. The engine builds the error in internal/cancelmarker;
// this package imports nothing internal, so it keeps its own copy, pinned to the engine's by TestIsCancelled.
const (
	errorTypeProperty  = "dwarf_type"
	errorTypeCancelled = "cancelled"
)

// IsCancelled reports whether err is a cancellation delivered by the engine's Cancel operation: the error an
// onError handler reads as onErr when its step was cancelled, or the error flow.Subgraph returns when the
// child flow was cancelled. Returning that error from a task keeps it a cancellation.
func IsCancelled(err error) bool {
	var te *errors.TracedError
	// A nil *TracedError passed as an error is a non-nil interface that As matches, so te can still be nil.
	if !errors.As(err, &te) || te == nil {
		return false
	}
	kind, _ := te.Properties[errorTypeProperty].(string)
	return kind == errorTypeCancelled
}
