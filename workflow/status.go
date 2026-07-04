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

const (
	StatusCreated     = "created"     // Flow/step exists but has not been started
	StatusPending     = "pending"     // Step is awaiting execution
	StatusRunning     = "running"     // Flow is actively executing a task
	StatusInterrupted = "interrupted" // Flow is paused, waiting for external input
	StatusCompleted   = "completed"   // Flow has finished successfully
	StatusFailed      = "failed"      // Flow has failed with an error
	StatusCancelled   = "cancelled"   // Flow was cancelled by the user
)

// IsValidStatus reports whether s is one of the defined flow/step statuses. Callers that inline a
// caller-supplied status into a query (the engine's List/Purge filter) gate on this first, so only a
// known constant - never arbitrary input - reaches the SQL string.
func IsValidStatus(s string) bool {
	switch s {
	case StatusCreated, StatusPending, StatusRunning, StatusInterrupted,
		StatusCompleted, StatusFailed, StatusCancelled:
		return true
	}
	return false
}
