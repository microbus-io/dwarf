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

package database

import (
	"slices"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestSQLList_TerminalStatusesAreTheClosedOnes pins that TerminalStatuses and TerminalStatusesSQL hold exactly the
// statuses workflow.IsTerminalStatus calls terminal. A status added to one and not the other would let a guarded write
// rewrite a closed flow, or keep an open one out of a selection.
func TestSQLList_TerminalStatusesAreTheClosedOnes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, s := range []string{
		workflow.StatusCreated, workflow.StatusPending, workflow.StatusRunning, workflow.StatusInterrupted,
		workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
	} {
		assert.Equal(workflow.IsTerminalStatus(s), slices.Contains(TerminalStatuses, s), "list: status %q", s)
		assert.Equal(workflow.IsTerminalStatus(s), strings.Contains(TerminalStatusesSQL, "'"+s+"'"), "SQL: status %q", s)
	}
}

// TestSQLList_IntList pins IntList's spelling: the ids in order, comma-separated, nothing for none.
func TestSQLList_IntList(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal("", IntList(nil))
	assert.Equal("7", IntList([]int{7}))
	assert.Equal("3,1,2", IntList([]int{3, 1, 2}))
}
