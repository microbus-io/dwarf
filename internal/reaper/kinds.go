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

package reaper

import (
	"cmp"
	"slices"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
)

// kind is one population of due roots: how they are found, in what order, and what the batch that takes them
// re-checks.
type kind struct {
	name string
	// selectDue returns the statement and arguments that select up to limit due roots on a database of driverName,
	// restricted to residency (a SQL fragment over flow_id and its arguments; empty for none), as rows of (flow_id,
	// updated_at, age in milliseconds, created_hour).
	// The statement is one or more branches, each in the kind's order and each limited to limit, so the traversal is
	// the branches merged in that order (scanDue).
	selectDue func(driverName, residency string, residencyArgs []any, limit int) (string, []any)
	// order is the kind's traversal order over its due roots.
	order func(a, b dueRoot) int
	// due re-checks a locked root, with its arguments.
	due     string
	dueArgs []any
	// stealWhere matches a root overdue by the steal grace, which any dispatcher takes whatever its residency.
	stealWhere string
	stealArgs  []any
	// lagged makes this kind's fast scan report archive lag past grace.
	lagged bool
	grace  time.Duration
}

// deletionDue is the roots DeleteOnCompletion and Delete stamped: they are deleted, never archived. Only roots ever
// carry delete_after_ms > 0, and the surgraph_flow_id=0 guard makes that explicit.
func deletionDue(steal time.Duration) kind {
	const due = "delete_after_ms>0 AND surgraph_flow_id=0 AND DATE_ADD_MILLIS(updated_at, delete_after_ms)<=NOW_UTC()"
	return kind{
		name: KindDeleted,
		selectDue: func(driverName, residency string, args []any, limit int) (string, []any) {
			// ORDER BY flow_id is REQUIRED because LIMIT_OFFSET compiles to OFFSET/FETCH on SQL Server, which
			// rejects it without a preceding ORDER BY. It walks idx_dwarf_flows_delete_after, keyed on flow_id over
			// the stamped roots only, in its own order. Without that index every dialect walks the primary key past
			// every unstamped root ahead of the first stamped one - an archive outage's whole held backlog, on every
			// sweep from the front (PostgreSQL: 1.29 s at 1.2M held roots, 0.35 ms with it). MySQL and MariaDB have
			// no partial index, so their index is the full (delete_after_ms) one and the scan is told to use it: it
			// then reads every stamped root and sorts them (~250 ms at 220k stamped), bounded by what is stamped
			// rather than by what is held; left to itself MariaDB walked the primary key (2.05 s).
			from := "dwarf_flows"
			if driverName == "mysql" || driverName == "mariadb" {
				from += " FORCE INDEX (idx_dwarf_flows_delete_after)"
			}
			return "SELECT flow_id, updated_at, DATE_DIFF_MILLIS(NOW_UTC(), updated_at), created_hour FROM " + from +
				" WHERE " + due + residency + " ORDER BY flow_id LIMIT_OFFSET(?, 0)", append(slices.Clone(args), limit)
		},
		order:      byFlowID,
		due:        due,
		stealWhere: "DATE_ADD_MILLIS(updated_at, delete_after_ms)<=DATE_ADD_MILLIS(NOW_UTC(), ?)",
		stealArgs:  []any{-steal.Milliseconds()},
	}
}

// closedDue is the roots that ended grace ago and were not stamped for deletion: they are archived, or deleted at a
// retention of 0.
func closedDue(steal, grace time.Duration) kind {
	graceMs := grace.Milliseconds()
	return kind{
		name: KindClosed,
		selectDue: func(_, residency string, args []any, limit int) (string, []any) {
			// One branch per terminal status, each a range of idx_dwarf_flows_status (status, updated_at) that stops
			// at the batch: one scan over all four statuses has no index order, so it would read and sort every due
			// root on every batch, O(backlog) per batch. The status is inlined, never bound: a bound status defeats
			// the filtered indexes and the estimates. Each root's age comes with it, from the row the scan reads
			// anyway: the first of each branch is that status's oldest due root, which is the archive lag. flow_id
			// breaks ties of updated_at, so the order is total: the index carries the primary key after updated_at on
			// MySQL, SQL Server and SQLite, and PostgreSQL sorts only each tie group (an incremental sort).
			var q string
			var all []any
			for i, status := range terminalStatuses {
				if i > 0 {
					q += " UNION ALL "
				}
				q += "SELECT flow_id, updated_at, age_ms, created_hour FROM (SELECT flow_id, updated_at, created_hour," +
					" DATE_DIFF_MILLIS(NOW_UTC(), updated_at) AS age_ms" +
					" FROM dwarf_flows WHERE status='" + status + "'" +
					" AND updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?) AND surgraph_flow_id=0 AND delete_after_ms=0" +
					residency + " ORDER BY updated_at, flow_id LIMIT_OFFSET(?, 0)) due_" + status
				all = append(all, -graceMs)
				all = append(all, args...)
				all = append(all, limit)
			}
			return q, all
		},
		order: byClose,
		due: "status IN (" + terminalStatusesSQL + ") AND surgraph_flow_id=0 AND delete_after_ms=0" +
			" AND updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?)",
		dueArgs:    []any{-graceMs},
		stealWhere: "updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?)",
		stealArgs:  []any{-(grace + steal).Milliseconds()},
		lagged:     true,
		grace:      grace,
	}
}

// retentionDue is the closed kind restricted to roots that closed before cutoff, in its scan and in the recheck under
// the root lock alike, so a root that is not past the cutoff is never deleted whatever the scan saw.
func retentionDue(driver string, cutoff time.Time, steal, grace time.Duration) kind {
	ts, arg := database.SQLTime(driver, cutoff)
	k := closedDue(steal, grace)
	k.name, k.lagged = KindRetention, false
	selectDue := k.selectDue
	k.selectDue = func(driverName, where string, args []any, limit int) (string, []any) {
		return selectDue(driverName, where+" AND updated_at<"+ts, append(slices.Clone(args), arg), limit)
	}
	k.due += " AND updated_at<" + ts
	k.dueArgs = append(slices.Clone(k.dueArgs), arg)
	return k
}

// residencyClause restricts a due-root scan to this replica's share of the shard's dispatchers, flow_id % count =
// ordinal, plus every root overdue by the steal grace, and returns the pair (count 0 when it restricts nothing). Two
// reapers then take disjoint roots while both keep up, and a slow or departed one's roots are taken after the grace.
// It fails open, as the piston's partition does: an unknown or invalid pair, or a solo dispatcher, restricts nothing.
func residencyClause(count, ordinal int, ok bool, k kind) (clause string, args []any) {
	if !ok || count < 2 || ordinal < 0 || ordinal >= count {
		return "", nil
	}
	return " AND (flow_id % ? = ? OR " + k.stealWhere + ")", append([]any{count, ordinal}, k.stealArgs...)
}

// dueRoot is a due root a scan returned, by its key in the traversal order.
type dueRoot struct {
	updatedAt   time.Time
	flowID      int
	createdHour int
}

// byClose orders due roots by (updated_at, flow_id), the closed kind's order: the order they became due in.
func byClose(a, b dueRoot) int {
	if c := a.updatedAt.Compare(b.updatedAt); c != 0 {
		return c
	}
	return cmp.Compare(a.flowID, b.flowID)
}

// byFlowID orders due roots by flow_id, the stamped kind's order.
func byFlowID(a, b dueRoot) int {
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

// walkRange keeps the timestamp range seek explicit instead of using an OR across its leading column.
func walkRange(driver string, k kind, after, end dueRoot) (string, []any) {
	var q string
	var args []any
	if after.flowID != 0 {
		if k.name == KindDeleted {
			q += " AND flow_id>?"
			args = append(args, after.flowID)
		} else {
			ts, arg := database.SQLTime(driver, after.updatedAt)
			q += " AND updated_at>=" + ts + " AND NOT (updated_at=" + ts + " AND flow_id<=?)"
			args = append(args, arg, arg, after.flowID)
		}
	}
	if !end.updatedAt.IsZero() {
		// Bound both axes: even a database clock moving backwards cannot admit an endless stream of new ids.
		q += " AND flow_id<=?"
		args = append(args, end.flowID)
		if k.name != KindDeleted {
			ts, arg := database.SQLTime(driver, end.updatedAt)
			q += " AND updated_at<=" + ts
			args = append(args, arg)
		}
	}
	return q, args
}
