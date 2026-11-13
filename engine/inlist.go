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
	"cmp"
	"slices"

	"github.com/microbus-io/dwarf/internal/database"
)

// inListChunk is the most ids one bound IN (...) list carries in the engine: an id list whose length the workload
// sets - a cohort's members, a subgraph tree's flows - goes out in chunks of this many. It is one of database.IDList's
// lengths, below database.MaxListIDs, because some statements bind more beside the list - a CASE arm of two binds per
// id, about 1,500 binds a chunk - and SQL Server takes at most 2,100 parameters a statement.
const inListChunk = 500

// inList returns the placeholders and arguments of an IN (...) list over one chunk of at most inListChunk ids, padded
// to one of database.IDList's few lengths.
func inList[T any](ids []T) (placeholders string, args []any) {
	return database.IDList(ids)
}

// lockOrder returns flow ids ascending, as a copy, for statements that take row locks chunk by chunk. One
// statement over the whole list locks in its plan's order, but chunks lock in turn, so two transactions over
// overlapping sets that chunk in different orders can each hold rows the other's next chunk waits on - a deadlock
// a single statement could not form. Ascending order makes every chunked lock pass take its rows in one order. A
// copy, because callers keep the original aligned with other per-flow slices.
func lockOrder(flowIDs []any) []any {
	out := slices.Clone(flowIDs)
	slices.SortFunc(out, func(a, b any) int { return cmp.Compare(a.(int), b.(int)) })
	return out
}
