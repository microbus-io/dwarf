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

const (
	StatusCreated     = "created"     // Flow/step exists but has not been started
	StatusPending     = "pending"     // Step is awaiting execution
	StatusRunning     = "running"     // Flow is actively executing a task
	StatusInterrupted = "interrupted" // Flow is paused, waiting for external input
	StatusCompleted   = "completed"   // Flow has finished successfully
	StatusFailed      = "failed"      // Flow has failed with an error
	StatusCancelled   = "cancelled"   // Flow was cancelled by the user
)
