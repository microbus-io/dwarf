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

package engine

import (
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestTerminalStatusesSQL_MatchesIsTerminalStatus pins that the terminal-status list the engine inlines into
// SQL holds exactly the statuses workflow.IsTerminalStatus calls terminal. A status added to one and not the
// other would let a guarded write rewrite a closed flow, or keep an open one out of a selection.
func TestTerminalStatusesSQL_MatchesIsTerminalStatus(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, s := range []string{
		workflow.StatusCreated, workflow.StatusPending, workflow.StatusRunning, workflow.StatusInterrupted,
		workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
	} {
		inSQL := strings.Contains(terminalStatusesSQL, "'"+s+"'")
		assert.Equal(workflow.IsTerminalStatus(s), inSQL, "status %q", s)
	}
}
