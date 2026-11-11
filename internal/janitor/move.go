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
	"context"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
)

// filingDay is the archive day a round archives into, and until when, by this process's clock, it archives into it: a
// margin before the day ends (quietMargin), so a round near midnight leaves its trees to the first round of the next
// day. A zero stop never stops - a day too short for a margin, or Settings.NoQuietMargin - where a move after midnight
// is stamped within its day all the same. notWhole is true when tomorrow's tables are not whole, which also stops every
// new archive.
type filingDay struct {
	day      int
	stop     time.Time
	notWhole bool
	// archive is the shard's, whose directory hours say which moved flows the directory must name.
	archive *archiver.Archive
}

// startable reports whether a new archive may start into the day now.
func (d *filingDay) startable() bool {
	return !d.notWhole && (d.stop.IsZero() || time.Now().Before(d.stop))
}

// quietMargin is how long before a day ends archiving into it stops, for days of dayLength: a minute, or an eighth of
// a shorter day - and none for a day under 16 seconds, where the margin would be within two of the archive clock's
// whole-second steps (TodayLeft rounds down), which can leave no moment of the day to archive in. A move that commits
// after midnight still stamps the day it is filed in (archivedAtSQL), so the margin only makes that rare; it is not what
// keeps a day's rows in order.
func quietMargin(dayLength time.Duration) time.Duration {
	if dayLength < 16*time.Second {
		return 0
	}
	return min(time.Minute, dayLength/8)
}

// refreshDay reads today from the database clock into d, and when to stop archiving into it. The janitor makes no
// table: today and the days ahead are the archive's maintenance's to make. Tomorrow not whole is an error, an archive
// failure like a failed move: a row goes into a day only once the day after it is whole, which a locate relies on. d
// is filled even then, with notWhole set. Today missing fails the move itself. The clock is read on q.
func refreshDay(ctx context.Context, q sequel.Executor, a *archiver.Archive, noQuietMargin bool, d *filingDay) error {
	today, left, err := a.TodayLeftOn(ctx, q)
	if err != nil {
		return errors.Trace(err)
	}
	d.day, d.archive = today, a
	d.stop = time.Time{}
	if quiet := quietMargin(a.DayLength()); quiet > 0 && !noQuietMargin {
		d.stop = time.Now().Add(left - quiet)
	}
	d.notWhole = !a.Whole(today + 1)
	if d.notWhole {
		return errors.New("tomorrow's archive tables (day %d) are not whole", today+1)
	}
	return nil
}

// noAttempt is archive_attempt_at's value for a live root with no archive attempt: the column's default.
var noAttempt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// noAttemptSQL returns the SQL of archive_attempt_at's "no attempt" value on driver, holding one placeholder, and the
// argument to bind to it.
func noAttemptSQL(driver string) (expr string, arg string) {
	return database.SQLTime(driver, noAttempt)
}

// publishedSQL is the condition, on a live flows row, that its tree is published to the archive: stamped with its
// archive attempt still set, or carrying the archive time its publication copied, which Delete clearing the attempt
// keeps. Accounting only; nothing is authorized by it. And its arguments.
func publishedSQL(driver string) (string, []any) {
	none, noneArg := noAttemptSQL(driver)
	sentinel, arg := database.SQLTime(driver, database.MaxSQLTime().Truncate(time.Second))
	return "((delete_after_ms>0 AND archive_attempt_at>" + none + ") OR archived_at<" + sentinel + ")", []any{noneArg, arg}
}

// attemptsAmong returns the roots among roots that have an archive attempt: one primary-key read of the roots.
func attemptsAmong(ctx context.Context, q sequel.Executor, roots []int) (map[int]bool, error) {
	none, arg := noAttemptSQL(q.DriverName())
	rows, err := q.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE flow_id IN ("+intCSV(roots)+")"+
		" AND archive_attempt_at>"+none, arg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	attempted := map[int]bool{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Trace(err)
		}
		attempted[id] = true
	}
	return attempted, errors.Trace(rows.Err())
}

// stampClosed marks a closed root for deletion, due at once, if it is still a closed root due grace after its close:
// what a retention of 0 does with a tree too large to delete in one batch, leaving it to the deletion of stamped trees
// in chunks. The stamp clears any archive attempt, as every stamp but a publication does. The root is locked first as a
// move locks it, so a root another transaction holds - a Fork cloning the tree - is skipped on PostgreSQL and MySQL
// rather than waited for. It reports whether it stamped it.
func stampClosed(ctx context.Context, h Handle, root int, grace time.Duration) (stamped bool, err error) {
	id := strconv.Itoa(root)
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		stamped = false
		due, dueArgs := closedDueSQL(grace)
		locked, err := lockRoots(ctx, tx, live.flows, true, "flow_id="+id+" AND "+due, dueArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		none, noneArg := noAttemptSQL(tx.DriverName())
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at="+none+" WHERE flow_id="+id, noneArg); err != nil {
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

// removal is what one transaction removed from the live tables: flow rows, and of those, the rows of trees deleted
// without having been archived - every tree but a published one, whose live copy is deleted after its archive copy
// was published.
type removal struct {
	flows, unarchived int
}

// move removes the trees of the roots it can lock among roots, in one transaction on h: moved into d's archive tables,
// or deleted when d is nil. Each root is locked (lockRoots) and rechecked under the lock by due, with dueArgs - and,
// archiving, for no archive attempt, which is the staged path's whatever the batch's selection saw. PostgreSQL and
// MySQL skip a held root; SQLite and SQL Server wait for it. Transact retries contention, straggler lock inversions
// included.
func move(ctx context.Context, h Handle, seams *seamster.Seamster, roots []int, due string, dueArgs []any, d *filingDay) (removed removal, err error) {
	roots = slices.Sorted(slices.Values(roots))
	dueArgs = slices.Clone(dueArgs)
	if d != nil {
		none, noneArg := noAttemptSQL(h.DriverName())
		due, dueArgs = due+" AND archive_attempt_at<="+none, append(dueArgs, noneArg)
	}
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		removed = removal{}
		locked, err := lockRoots(ctx, tx, live.flows, true, "flow_id IN ("+intCSV(roots)+") AND "+due, dueArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		seams.Checkpoint(ctx, CheckpointBatchLocked)
		// ids are trusted integers embedded as literals to dodge the per-driver bind-param ceiling.
		ids := intCSV(locked)
		trees := "SELECT flow_id FROM dwarf_flows WHERE root_flow_id IN (" + ids + ")"
		published := 0
		if d == nil {
			// Counted under the root locks: a published tree's live copy is deleted, not reaped.
			cond, args := publishedSQL(tx.DriverName())
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id IN (SELECT flow_id FROM"+
				" dwarf_flows WHERE flow_id IN ("+ids+") AND "+cond+")", args...).Scan(&published); err != nil {
				return errors.Trace(err)
			}
		}
		steps := "DELETE FROM dwarf_steps WHERE flow_id IN (" + trees + ")"
		flows := "DELETE FROM dwarf_flows WHERE root_flow_id IN (" + ids + ")"
		var flowsArgs []any
		if d != nil {
			// FaultContention fails every attempt of an archive move on lock contention.
			if seams.IsFault(FaultContention) {
				return errors.Trace(&pgconn.PgError{Code: "40P01", Message: "injected fault: " + FaultContention})
			}
			// FaultArchiveErr fails an archive move with an error that is not lock contention.
			if seams.IsFault(FaultArchiveErr) {
				return errors.New("injected fault: " + FaultArchiveErr)
			}
			steps, flows, flowsArgs = archiveStatements(ctx, tx, d, ids, trees, steps, flows)
		}
		// Steps before flows: the subquery reads the flow rows.
		if _, err := tx.ExecContext(ctx, steps); err != nil {
			return errors.Trace(err)
		}
		// FaultMidTree aborts after the steps go but before the flows do, so the whole batch rolls back: a failure mid
		// tree leaves the tree whole, never a flow with no steps.
		if seams.IsFault(FaultMidTree) {
			return errors.New("injected fault: " + FaultMidTree)
		}
		if err := targetedFault(seams, FaultMidTree, locked...); err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, flows, flowsArgs...)
		if err != nil {
			return errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		removed.flows = int(n)
		if d == nil {
			removed.unarchived = removed.flows - published
		}
		return nil
	})
	if err != nil {
		return removal{}, errors.Trace(err)
	}
	return removed, nil
}

// archiveStatements returns the statements that move the trees whose flow ids trees selects, the roots ids, into d:
// the steps' and the flows', and the flows' arguments. A flow a locate would not find by its creation hour alone -
// archived two or more days after its creation day, or on a day before it (a straggler created while a batch begun
// before midnight ran) - is named in the directory, in the same transaction, from the same rows the move takes.
//
// On PostgreSQL each table moves in one statement, so what is archived is exactly what is deleted, whatever a
// descendant does between statements - only the roots are locked. RETURNING * is in column order, and the day tables
// carry the live columns in the same order; the flows name theirs, to stamp archived_at. Elsewhere the rows are copied
// here, then deleted by the plain statements: a descendant that changes between the copy and the delete - only a
// fan-out straggler settling, or an orphan a bug left - is archived as it was at the copy.
func archiveStatements(ctx context.Context, tx *sequel.Tx, d *filingDay, ids, trees, steps, flows string) (stepsStmt, flowsStmt string, flowsArgs []any) {
	archivedAt, archivedAtArgs := archivedAtSQL(tx.DriverName(), d.archive.DayEnds(d.day))
	before, from := d.archive.DirectoryHours(d.day)
	directory := "INSERT INTO " + archiver.DirectoryTable + " (flow_id, day) SELECT flow_id, " + strconv.Itoa(d.day) +
		" FROM %s WHERE (created_hour<" + strconv.Itoa(before) + " OR created_hour>=" + strconv.Itoa(from) + ")"
	if tx.DriverName() == "pgx" {
		stepsStmt = "WITH moved AS (" + steps + " RETURNING *) INSERT INTO " + migrations.StepsTable(d.day) + " SELECT * FROM moved"
		flowsStmt = "WITH moved AS (" + flows + " RETURNING *)," +
			" named AS (" + fmt.Sprintf(directory, "moved") + ")" +
			" INSERT INTO " + migrations.FlowsTable(d.day) + " (" + archivedFlowColumns + ", archived_at)" +
			" SELECT " + archivedFlowColumns + ", " + archivedAt + " FROM moved"
		return stepsStmt, flowsStmt, archivedAtArgs
	}
	// An error here surfaces from the statements that follow, which a failed statement aborts on every dialect that
	// gets here, or from the commit.
	tx.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(d.day)+" ("+archivedFlowColumns+", archived_at)"+
		" SELECT "+archivedFlowColumns+", "+archivedAt+" FROM dwarf_flows WHERE root_flow_id IN ("+ids+")", archivedAtArgs...)
	tx.ExecContext(ctx, "INSERT INTO "+migrations.StepsTable(d.day)+" SELECT * FROM dwarf_steps WHERE flow_id IN ("+trees+")")
	tx.ExecContext(ctx, fmt.Sprintf(directory, "dwarf_flows")+" AND root_flow_id IN ("+ids+")")
	return steps, flows, nil
}

// liveOnlyColumn is the one column of dwarf_flows the day tables do not have: a live root's archive attempt, which
// means nothing once the tree is archived.
const liveOnlyColumn = "archive_attempt_at"

// archivedFlowColumns are the columns of dwarf_flows but archived_at and liveOnlyColumn, in table order. A move names
// them, so the archive's row takes its archive time in place of the live row's "not archived" value. A column added
// to dwarf_flows must be added here too, or the archive drops it: TestMove_NamesEveryFlowsColumn compares the list with
// the table.
const archivedFlowColumns = "flow_id, flow_token, workflow_url, workflow_name, graph, graph_bytes, graph_hash, payload_ver," +
	" baggage, baggage_bytes, status, stopped_step_id, surgraph_flow_id, surgraph_step_id, root_flow_id, forked_from_step," +
	" trace_parent, delete_on_completion, delete_after_ms, final_state, final_state_bytes, error, cancel_reason," +
	" cancel_watermark, priority, fairness_key, fairness_weight, time_budget_ms, engine_id, created_hour, created_at," +
	" started_at, updated_at, touch"

// archivedAtSQL returns the archive time a move into the day that ends at dayEnds stamps, and its arguments: the
// database's now, or the day's last millisecond if the move commits after midnight - so a day holds only rows archived
// on it, and a later day only later ones: the days are in archive order. On PostgreSQL now is the transaction's start,
// so every tree a batch moves is stamped alike. CASE rather than LEAST: SQL Server 2019 has none.
func archivedAtSQL(driverName string, dayEnds time.Time) (expr string, args []any) {
	last, arg := database.SQLTime(driverName, dayEnds.Add(-time.Millisecond))
	return "CASE WHEN NOW_UTC()<" + last + " THEN NOW_UTC() ELSE " + last + " END", []any{arg, arg}
}

// lockRoots locks the rows of flows that match where and returns their flow ids, ascending. The rows are about to be
// deleted, so on PostgreSQL and MySQL the lock is FOR UPDATE, the strength a DELETE takes - which the engine's Fork,
// locking a source root FOR NO KEY UPDATE, conflicts with; SQLite and SQL Server take it by flipping touch. With
// skipHeld, a row another transaction holds is left out rather than waited for (SKIP LOCKED) on PostgreSQL and MySQL.
// SQL Server and SQLite wait as without it: SQLite has one writer, so nothing there is held. On SQL Server a READPAST
// here does skip the row, but the move's own statements then wait on it - under locking READ COMMITTED a DELETE that
// scans waits on every locked row it passes, one it would not delete included - and READPAST on those could leave a
// tree's row out of its copy.
func lockRoots(ctx context.Context, tx *sequel.Tx, flows string, skipHeld bool, where string, args ...any) ([]int, error) {
	var stmt string
	switch tx.DriverName() {
	case "pgx", "mysql", "mariadb":
		stmt = "SELECT flow_id FROM " + flows + " WHERE " + where + " ORDER BY flow_id FOR UPDATE"
		if skipHeld {
			stmt += " SKIP LOCKED"
		}
	case "mssql":
		stmt = "UPDATE " + flows + " SET touch=1-touch OUTPUT inserted.flow_id WHERE " + where
	default:
		stmt = "UPDATE " + flows + " SET touch=1-touch WHERE " + where + " RETURNING flow_id"
	}
	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	var locked []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Trace(err)
		}
		locked = append(locked, id)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	slices.Sort(locked)
	return locked, nil
}

// targetedFault fails when fault is injected targeted at any of roots.
func targetedFault(seams *seamster.Seamster, fault string, roots ...int) error {
	if !seams.Enabled() {
		return nil
	}
	for _, root := range roots {
		if seams.IsFault(join(fault, strconv.Itoa(root))) {
			return errors.New("injected fault: " + fault)
		}
	}
	return nil
}
