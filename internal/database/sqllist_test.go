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

// TestSQLList_IDList pins IDList's lengths: a list padded up to the first of 4, 20, 100, 500 and MaxListIDs that holds
// it, by repeating its last id, so a statement takes one of five texts; and a list of none or of more than MaxListIDs
// refused.
func TestSQLList_IDList(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ph, args := IDList([]int{7})
	assert.Equal("?,?,?,?", ph)
	assert.Equal([]any{7, 7, 7, 7}, args)
	ph, args = IDList([]int{3, 1, 2, 9, 5})
	assert.Equal(20, strings.Count(ph, "?"))
	assert.Equal([]any{3, 1, 2, 9, 5}, args[:5])
	for _, a := range args[5:] {
		assert.Equal(5, a)
	}
	for n, arity := range map[int]int{4: 4, 21: 100, 100: 100, 101: 500, 501: MaxListIDs, MaxListIDs: MaxListIDs} {
		ids := make([]int, n)
		for i := range ids {
			ids[i] = i + 1
		}
		ph, args := IDList(ids)
		assert.Equal(arity, strings.Count(ph, "?"), "%d ids", n)
		assert.Len(args, arity, "%d ids", n)
	}
	panics := func(ids []int) (p bool) {
		defer func() { p = recover() != nil }()
		IDList(ids)
		return false
	}
	assert.True(panics([]int{}), "no ids")
	assert.True(panics(make([]int, MaxListIDs+1)), "more than MaxListIDs")
}
