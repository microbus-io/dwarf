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

type route int

const (
	routeMove  route = iota // moved, or deleted at a retention of 0, in one transaction
	routePass               // left where it is: every candidate has an archive attempt, large-tree archiving's
	routeStart              // its archive attempt started, for large-tree archiving: a tree over the staging threshold
	routeStamp              // stamped for deletion in chunks: at a retention of 0, a tree too large for a batch
)

// dueRoot is a due root a scan returned, by its key in the scan's order.
type dueRoot struct {
	updatedAt   time.Time
	flowID      int
	createdHour int
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
	return "status IN (" + database.TerminalStatusesSQL + ") AND surgraph_flow_id=0 AND delete_after_ms=0" +
		" AND updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?)", []any{-grace.Milliseconds()}
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

// scanClosed selects up to limit closed roots due grace after their close, restricted by where (a SQL
// fragment and its arguments; empty for none), in close order - (updated_at, flow_id) - and returns the age of the
// oldest it read, 0 for none. It returns fewer than limit only when it saw every due root.
//
// One branch per terminal status, each a range of idx_dwarf_flows_status (status, updated_at) that stops at the limit:
// one scan over all four statuses has no index order, so it would read and sort every due root on every batch,
// O(backlog) per batch. The status is inlined, never bound: a bound status defeats the filtered indexes and the
// estimates. Each root's age comes with it, from the row the scan reads anyway: the first of each branch is that
// status's oldest due root. flow_id breaks ties of updated_at, so the order is total: the index carries the primary key
// after updated_at on MySQL, SQL Server and SQLite, and PostgreSQL sorts only each tie group (an incremental sort). The
// branches are merged here: their first limit are exactly the order's first, since each branch holds its own first
// limit.
func scanClosed(ctx context.Context, q sequel.Executor, seams *seamster.Seamster, grace time.Duration, where string, whereArgs []any, limit int) (roots []dueRoot, oldest time.Duration, err error) {
	var stmt string
	var args []any
	for i, status := range database.TerminalStatuses {
		if i > 0 {
			stmt += " UNION ALL "
		}
		stmt += "SELECT flow_id, updated_at, age_ms, created_hour FROM (SELECT flow_id, updated_at, created_hour," +
			" DATE_DIFF_MILLIS(NOW_UTC(), updated_at) AS age_ms" +
			" FROM dwarf_flows WHERE status='" + status + "'" +
			" AND updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?) AND surgraph_flow_id=0 AND delete_after_ms=0" +
			where + " ORDER BY updated_at, flow_id LIMIT_OFFSET(?, 0)) due_" + status
		args = append(args, -grace.Milliseconds())
		args = append(args, whereArgs...)
		args = append(args, limit)
	}
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
		// The position is bound back as this instant (restriction): a MySQL driver with a DSN loc other than UTC places
		// the stored wall clock in that location, and binding it as found would seek hours off.
		r.updatedAt = database.StoredTime(r.updatedAt)
		roots = append(roots, r)
		oldest = max(oldest, time.Duration(ageMs*float64(time.Millisecond)))
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Trace(err)
	}
	slices.SortFunc(roots, func(a, b dueRoot) int {
		if c := a.updatedAt.Compare(b.updatedAt); c != 0 {
			return c
		}
		return cmp.Compare(a.flowID, b.flowID)
	})
	return roots[:min(limit, len(roots))], oldest, nil
}

// cutClosed returns the roots among candidates, in order, that one batch takes, and what it does with them. A root with an
// archive attempt is left out, and passed. Of the rest it takes a leading run whose trees fit a batch (cutWithin) -
// rows, bytes and flows - and always the first: the first sized up to what decides its path - over a batch, and over
// the staging threshold when staging - and each later one up to what is left of the batch. Sizing is a batching hint;
// the move locks only the roots it takes. A cut empty of roots means every candidate had an attempt.
func cutClosed(ctx context.Context, h handle, seams *seamster.Seamster, candidates []int, archiving bool, batchRows, batchBytes, stageRows, stageBytes int) (cut []int, via route, err error) {
	eligible := candidates
	if archiving {
		attempted, err := attemptsAmong(ctx, h, candidates)
		if err != nil {
			return nil, routeMove, err
		}
		eligible = slices.DeleteFunc(slices.Clone(candidates), func(root int) bool { return attempted[root] })
		if len(eligible) == 0 {
			return nil, routePass, nil
		}
	}
	batch := capOf(batchRows, batchBytes)
	cutLimit := batch
	cutLimit.flows = MaxBatchFlows
	firstRows := batch.rows + 1
	stage, staging := capOf(stageRows, stageBytes), archiving && stageRows > 0
	if staging {
		// A batch holds no more than the threshold, so a tree over it is alone in its batch, whatever the batch's own
		// size.
		cutLimit = cutLimit.tighter(stage)
		firstRows = max(firstRows, stage.rows+1)
	}
	cut, first, err := cutWithin(eligible, cutLimit, firstRows, func(group, limits []int) ([]size, error) {
		if err := targetedFault(seams, diagnostics.FaultReapSizeErr, group...); err != nil {
			return nil, err
		}
		return treesSizesUpTo(ctx, h, "", group, limits)
	})
	switch {
	case err != nil:
		return nil, routeMove, err
	case len(cut) > 1:
		return cut, routeMove, nil
	case staging && stage.over(first):
		return cut, routeStart, nil
	case !archiving && batch.over(first):
		return cut, routeStamp, nil
	}
	return cut, routeMove, nil
}

// stampClosed marks a closed root for deletion, due at once, if it still matches due (and its arguments) - a closed
// root due for deletion: what a retention of 0, and live retention, do with a tree too large to delete in one batch,
// which is then deleted in chunks, each authorized by the stamp. The stamp clears any archive attempt, as every stamp
// but a publication does. The root is locked first as a move locks it, so a root another transaction holds - a Fork
// cloning the tree - is skipped on PostgreSQL and MySQL rather than waited for. It reports whether it stamped it.
func stampClosed(ctx context.Context, h handle, root int, due string, dueArgs []any) (stamped bool, err error) {
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		stamped = false
		locked, err := lockRoots(ctx, tx, "", true, "flow_id=? AND "+due, append([]any{root}, dueArgs...)...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		none, noneArg := NoAttemptSQL(tx.DriverName())
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at="+none+" WHERE flow_id=?", noneArg, root); err != nil {
			return errors.Trace(err)
		}
		stamped = true
		return nil
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	return stamped, nil
}
