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
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
)

// stealGrace is how long past due a root waits for the dispatcher whose residue class it is before any dispatcher
// takes it.
const stealGrace = 30 * time.Second

// dueRoot is a due root a scan returned, by its key in the scan's order.
type dueRoot struct {
	updatedAt   time.Time
	flowID      int
	createdHour int
}

// byClose orders due roots by (updated_at, flow_id): the order closed roots became due in.
func byClose(a, b dueRoot) int {
	if c := a.updatedAt.Compare(b.updatedAt); c != 0 {
		return c
	}
	return cmp.Compare(a.flowID, b.flowID)
}

// rootIDs returns the flow ids of roots, in their order.
func rootIDs(roots []dueRoot) []int {
	ids := make([]int, len(roots))
	for i, r := range roots {
		ids[i] = r.flowID
	}
	return ids
}

// closedDueSQL is the condition, on a live flows row, that it is a closed root due grace after its close and not
// stamped for deletion, and its argument. A batch rechecks it under the root's lock.
func closedDueSQL(grace time.Duration) (cond string, args []any) {
	return "status IN (" + terminalStatusesSQL + ") AND surgraph_flow_id=0 AND delete_after_ms=0" +
		" AND updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?)", []any{-grace.Milliseconds()}
}

// closedScanSQL selects up to limit closed roots due grace after their close, restricted by where (a SQL fragment and
// its arguments; empty for none), as rows of (flow_id, updated_at, age in milliseconds, created_hour).
//
// One branch per terminal status, each a range of idx_dwarf_flows_status (status, updated_at) that stops at the limit:
// one scan over all four statuses has no index order, so it would read and sort every due root on every batch,
// O(backlog) per batch. The status is inlined, never bound: a bound status defeats the filtered indexes and the
// estimates. Each root's age comes with it, from the row the scan reads anyway: the first of each branch is that
// status's oldest due root, which is the archive lag. flow_id breaks ties of updated_at, so the order is total: the
// index carries the primary key after updated_at on MySQL, SQL Server and SQLite, and PostgreSQL sorts only each tie
// group (an incremental sort).
func closedScanSQL(grace time.Duration, where string, whereArgs []any, limit int) (string, []any) {
	var q string
	var args []any
	for i, status := range terminalStatuses {
		if i > 0 {
			q += " UNION ALL "
		}
		q += "SELECT flow_id, updated_at, age_ms, created_hour FROM (SELECT flow_id, updated_at, created_hour," +
			" DATE_DIFF_MILLIS(NOW_UTC(), updated_at) AS age_ms" +
			" FROM dwarf_flows WHERE status='" + status + "'" +
			" AND updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?) AND surgraph_flow_id=0 AND delete_after_ms=0" +
			where + " ORDER BY updated_at, flow_id LIMIT_OFFSET(?, 0)) due_" + status
		args = append(args, -grace.Milliseconds())
		args = append(args, whereArgs...)
		args = append(args, limit)
	}
	return q, args
}

// closedResidency restricts a closed scan to this replica's share of the shard's dispatchers, flow_id % count =
// ordinal, plus every root overdue by the steal grace past the archive grace. Two janitors then take disjoint roots
// while both keep up. A position-ordered scan steals only the overdue roots ahead of its position - when its job is
// behind - so a slow peer's roots, once behind it, are a sweep's. It fails open, as the piston's partition does: an
// unknown or invalid pair, or a solo dispatcher, restricts nothing.
func closedResidency(count, ordinal int, ok bool, grace, steal time.Duration) (clause string, args []any) {
	if !ok || count < 2 || ordinal < 0 || ordinal >= count {
		return "", nil
	}
	return " AND (flow_id % ? = ? OR updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?))",
		[]any{count, ordinal, -(grace + steal).Milliseconds()}
}

// afterClose restricts a closed scan to the roots after root in close order, by an explicit timestamp seek rather
// than an OR across its leading column. Nothing for a zero root.
func afterClose(driver string, root dueRoot) (clause string, args []any) {
	if root.flowID == 0 {
		return "", nil
	}
	ts, arg := database.SQLTime(driver, root.updatedAt)
	return " AND updated_at>=" + ts + " AND NOT (updated_at=" + ts + " AND flow_id<=?)", []any{arg, arg, root.flowID}
}

// scanClosed selects up to limit due closed roots in close order (closedScanSQL), merging its branches: their first
// limit are exactly the order's first, since each branch holds its own first limit. It returns the age of the oldest
// it read (0 for none), and fewer than limit only when it saw every due root.
func scanClosed(ctx context.Context, q sequel.Executor, seams *seamster.Seamster, grace time.Duration, where string, whereArgs []any, limit int) (roots []dueRoot, oldest time.Duration, err error) {
	stmt, args := closedScanSQL(grace, where, whereArgs, limit)
	rows, err := q.QueryContext(ctx, stmt, args...)
	// diagnostics.FaultReapSelectErr fails the scan, so a test proves a round logs and goes on without removing anything.
	if err == nil && seams.IsFault(diagnostics.FaultReapSelectErr) {
		rows.Close()
		err = errors.New("injected fault: " + diagnostics.FaultReapSelectErr)
	}
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var r dueRoot
		// float64: DATE_DIFF_MILLIS is fractional on SQLite, where an int64 scan fails.
		var ageMs float64
		if err := rows.Scan(&r.flowID, &r.updatedAt, &ageMs, &r.createdHour); err != nil {
			return nil, 0, errors.Trace(err)
		}
		// The position is bound back as this instant (afterClose): a MySQL driver with a DSN loc other than UTC places
		// the stored wall clock in that location, and binding it as found would seek hours off.
		r.updatedAt = database.StoredTime(r.updatedAt)
		roots = append(roots, r)
		oldest = max(oldest, time.Duration(ageMs*float64(time.Millisecond)))
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Trace(err)
	}
	slices.SortFunc(roots, byClose)
	return roots[:min(limit, len(roots))], oldest, nil
}
