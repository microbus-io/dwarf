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
	"strconv"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
)

// TerminalStatuses are the statuses of a closed flow or step: the ones workflow.IsTerminalStatus calls terminal.
// Read-only.
var TerminalStatuses = []string{
	workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
}

// TerminalStatusesSQL is TerminalStatuses as a list of quoted literals, to inline into a WHERE clause:
// "status IN (" + TerminalStatusesSQL + ")" or "status NOT IN (" + TerminalStatusesSQL + ")". Inline it, never bind a
// status: a bound status defeats the SQL Server and SQLite filtered indexes.
const TerminalStatusesSQL = "'" + workflow.StatusCompleted + "', '" + workflow.StatusFailed + "', '" +
	workflow.StatusTerminated + "', '" + workflow.StatusCancelled + "'"

// IntList renders ids as a comma-separated list of literals, to inline into an IN (...) clause. Inlined rather than
// bound, so it is for ids the caller holds as ints - read from the database - and never for text.
func IntList(ids []int) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(id))
	}
	return b.String()
}
