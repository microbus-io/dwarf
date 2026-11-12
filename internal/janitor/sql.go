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

package janitor

import (
	"strconv"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
)

// tables names the pair of tables a statement runs against: the live tables, or one archive day's.
type tables struct {
	flows, steps string
}

// live is the live tables.
var live = tables{flows: "dwarf_flows", steps: "dwarf_steps"}

// terminalStatuses are the closed statuses, which TestSQL_TerminalStatusesAreTheClosedOnes pins against
// workflow.IsTerminalStatus.
var terminalStatuses = []string{
	workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
}

// terminalStatusesSQL is terminalStatuses as an IN list of inlined literals: a bound status defeats the filtered
// indexes.
var terminalStatusesSQL = "'" + strings.Join(terminalStatuses, "', '") + "'"

// intCSV joins ids as a comma-separated list of literals.
func intCSV(ids []int) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(id))
	}
	return b.String()
}
