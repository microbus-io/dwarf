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

// MaxListIDs is the most ids one bound list holds (IDList): a longer list goes out in chunks of at most this many. A
// statement that binds more than about 100 values beside its list chunks smaller, at one of IDList's lengths, since
// SQL Server takes at most 2,100 parameters a statement.
const MaxListIDs = 2000

// listArities are the lengths a bound list takes, shortest first: a list is padded up to the first that holds it.
var listArities = [...]int{4, 20, 100, 500, MaxListIDs}

// IDList returns the placeholders and the arguments of a bound IN (...) or NOT IN (...) list over 1 to MaxListIDs ids,
// padded to one of a few lengths by repeating the last id. Bind every id through it; never write one into a statement.
func IDList[T any](ids []T) (placeholders string, args []any) {
	if len(ids) == 0 || len(ids) > MaxListIDs {
		panic("database.IDList: " + strconv.Itoa(len(ids)) + " ids, not 1 to " + strconv.Itoa(MaxListIDs))
	}
	arity := MaxListIDs
	for _, a := range listArities {
		if len(ids) <= a {
			arity = a
			break
		}
	}
	args = make([]any, arity)
	for i := range args {
		args[i] = ids[min(i, len(ids)-1)]
	}
	return strings.Repeat("?,", arity-1) + "?", args
}
