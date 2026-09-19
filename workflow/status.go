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
	StatusCreated     = "created"     // Step exists but has not been started; a flow's own status is never reported as created - Create returns it already running
	StatusPending     = "pending"     // Step is awaiting execution
	StatusRunning     = "running"     // Flow is actively executing a task
	StatusInterrupted = "interrupted" // Flow is paused, waiting for external input
	StatusCompleted   = "completed"   // Flow has finished successfully
	StatusFailed      = "failed"      // Flow has failed with an error
	StatusTerminated  = "terminated"  // Flow was forcefully, unconditionally stopped by Terminate; in-flight work was abandoned, not awaited
	StatusCancelled   = "cancelled"   // Reserved for a future graceful-cancellation operation; not yet produced by any current operation
)

// IsValidStatus reports whether s is one of the defined flow/step statuses.
func IsValidStatus(s string) bool {
	switch s {
	case StatusCreated, StatusPending, StatusRunning, StatusInterrupted,
		StatusCompleted, StatusFailed, StatusTerminated, StatusCancelled:
		return true
	}
	return false
}

// TerminalStatuses lists every flow/step status that is terminal (immutable, no further advancement).
var TerminalStatuses = []string{StatusCompleted, StatusFailed, StatusTerminated, StatusCancelled}

// IsTerminalStatus reports whether s is one of the terminal flow/step statuses.
func IsTerminalStatus(s string) bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusTerminated, StatusCancelled:
		return true
	}
	return false
}
