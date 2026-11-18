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
	"github.com/microbus-io/dwarf/internal/diagnostics"
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
// is filled even then, with notWhole set. Today missing fails the move itself.
func refreshDay(ctx context.Context, a *archiver.Archive, noQuietMargin bool, d *filingDay) error {
	today, left, err := a.TodayLeft(ctx)
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

// publishedSQL is the condition, on a live flows row, that its tree is published to the archive: stamped with its
// archive attempt still set, or carrying the archive time its publication copied, which Delete clearing the attempt
// keeps. Accounting only; nothing is authorized by it. And its arguments.
func publishedSQL(driver string) (string, []any) {
	none, noneArg := NoAttemptSQL(driver)
	sentinel, arg := database.SQLTime(driver, database.MaxSQLTime().Truncate(time.Second))
	return "((delete_after_ms>0 AND archive_attempt_at>" + none + ") OR archived_at<" + sentinel + ")", []any{noneArg, arg}
}

// removal is what one transaction removed from the live tables: flow rows, and of those, the rows of trees deleted
// without having been archived - every tree but a published one, whose live copy is deleted after its archive copy
// was published.
type removal struct {
	flows, unarchived int
}

// MoveTrees moves the closed trees of roots, due with no grace, into day's archive tables in one transaction, as a
// round archiving into day would, and returns how many flow rows it moved. It is for a test that files a tree on a day
// of its choosing.
func MoveTrees(ctx context.Context, db *sequel.DB, seams *seamster.Seamster, archive *archiver.Archive, day int, roots []int) (moved int, err error) {
	due, dueArgs := closedDueSQL(0)
	removed, err := move(ctx, db, seams, roots, due, dueArgs, &filingDay{day: day, archive: archive})
	return removed.flows, err
}

// move removes the trees of the roots it can lock among roots, in one transaction on h: moved into d's archive tables,
// or deleted when d is nil. Each root is locked (lockRoots) and rechecked under the lock by due, with dueArgs - and,
// archiving, for no archive attempt, which is the staged path's whatever the batch's selection saw. PostgreSQL and
// MySQL skip a held root; SQLite and SQL Server wait for it. Transact retries contention, straggler lock inversions
// included.
func move(ctx context.Context, h handle, seams *seamster.Seamster, roots []int, due string, dueArgs []any, d *filingDay) (removed removal, err error) {
	roots = slices.Sorted(slices.Values(roots))
	dueArgs = slices.Clone(dueArgs)
	if d != nil {
		none, noneArg := NoAttemptSQL(h.DriverName())
		due, dueArgs = due+" AND archive_attempt_at<="+none, append(dueArgs, noneArg)
	}
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		removed = removal{}
		rootsPh, rootsArgs := database.IDList(roots)
		locked, err := lockRoots(ctx, tx, "", true, "flow_id IN ("+rootsPh+") AND "+due, append(rootsArgs, dueArgs...)...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		seams.Checkpoint(ctx, diagnostics.CheckpointReapBatchLocked)
		ids, idArgs := database.IDList(locked)
		trees := "SELECT flow_id FROM dwarf_flows WHERE root_flow_id IN (" + ids + ")"
		published := 0
		if d == nil {
			// Counted under the root locks: a published tree's live copy is deleted, not reaped.
			cond, args := publishedSQL(tx.DriverName())
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id IN (SELECT flow_id FROM"+
				" dwarf_flows WHERE flow_id IN ("+ids+") AND "+cond+")", append(slices.Clone(idArgs), args...)...).Scan(&published); err != nil {
				return errors.Trace(err)
			}
		}
		steps := database.DeleteSQL(tx.DriverName(), "dwarf_steps", database.SeekIndex("dwarf_steps", "flow_id", "flow_id"), "flow_id IN ("+trees+")")
		flows := database.DeleteSQL(tx.DriverName(), "dwarf_flows", database.SeekIndex("dwarf_flows", "root", "root_flow_id"), "root_flow_id IN ("+ids+")")
		stepsArgs, flowsArgs := idArgs, idArgs
		if d != nil {
			// diagnostics.FaultReapContention fails every attempt of an archive move on lock contention.
			if seams.IsFault(diagnostics.FaultReapContention) {
				return errors.Trace(&pgconn.PgError{Code: "40P01", Message: "injected fault: " + diagnostics.FaultReapContention})
			}
			// diagnostics.FaultReapArchiveErr fails an archive move with an error that is not lock contention.
			if seams.IsFault(diagnostics.FaultReapArchiveErr) {
				return errors.New("injected fault: " + diagnostics.FaultReapArchiveErr)
			}
			steps, stepsArgs, flows, flowsArgs = archiveStatements(ctx, tx, d, ids, idArgs, trees, steps, flows)
		}
		// Steps before flows: the subquery reads the flow rows.
		if _, err := tx.ExecContext(ctx, steps, stepsArgs...); err != nil {
			return errors.Trace(err)
		}
		// diagnostics.FaultReapMidTree aborts after the steps go but before the flows do, so the whole batch rolls back: a failure mid
		// tree leaves the tree whole, never a flow with no steps.
		if seams.IsFault(diagnostics.FaultReapMidTree) {
			return errors.New("injected fault: " + diagnostics.FaultReapMidTree)
		}
		if err := targetedFault(seams, diagnostics.FaultReapMidTree, locked...); err != nil {
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
		seams.Checkpoint(ctx, diagnostics.CheckpointReapDeleted)
		return nil
	})
	if err != nil {
		return removal{}, errors.Trace(err)
	}
	if seams.Enabled() {
		for _, root := range roots {
			if seams.IsFault(diagnostics.Join(diagnostics.FaultJanitorLostResponse, strconv.Itoa(root))) {
				return removal{}, errors.New("injected fault: " + diagnostics.FaultJanitorLostResponse)
			}
		}
	}
	return removed, nil
}

// archiveStatements returns the statements that move the trees whose flow ids trees selects, the roots the bound list
// ids holds (idArgs), into d: the steps' and the flows', with their arguments. The directory's day and hours are written
// into the statement: they change once a day, so a day's statements take one text. A flow a locate would not find by its creation hour alone -
// archived two or more days after its creation day, or on a day before it (a straggler created while a batch begun
// before midnight ran) - is named in the directory, in the same transaction, from the same rows the move takes.
//
// On PostgreSQL each table moves in one statement, so what is archived is exactly what is deleted, whatever a
// descendant does between statements - only the roots are locked. Both tables name their columns
// (archivedStepColumns, archivedFlowColumns), so neither depends on the tables' column order; the flows' list leaves
// out archived_at, to stamp it. Elsewhere the rows are copied
// here, then deleted by the plain statements: a descendant that changes between the copy and the delete - only a
// fan-out straggler settling, or an orphan a bug left - is archived as it was at the copy.
func archiveStatements(ctx context.Context, tx *sequel.Tx, d *filingDay, ids string, idArgs []any, trees, steps, flows string) (stepsStmt string, stepsArgs []any, flowsStmt string, flowsArgs []any) {
	archivedAt, archivedAtArgs := archivedAtSQL(tx.DriverName(), d.archive.DayEnds(d.day))
	before, from := d.archive.DirectoryHours(d.day)
	directory := "INSERT INTO " + archiver.DirectoryTable + " (flow_id, day) SELECT flow_id, " + strconv.Itoa(d.day) +
		" FROM %s WHERE (created_hour<" + strconv.Itoa(before) + " OR created_hour>=" + strconv.Itoa(from) + ")"
	if tx.DriverName() == "pgx" {
		stepsStmt = "WITH moved AS (" + steps + " RETURNING " + archivedStepColumns + ") INSERT INTO " + migrations.StepsTable(d.day) +
			" (" + archivedStepColumns + ") SELECT " + archivedStepColumns + " FROM moved"
		flowsStmt = "WITH moved AS (" + flows + " RETURNING *)," +
			" named AS (" + fmt.Sprintf(directory, "moved") + ")" +
			" INSERT INTO " + migrations.FlowsTable(d.day) + " (" + archivedFlowColumns + ", archived_at)" +
			" SELECT " + archivedFlowColumns + ", " + archivedAt + " FROM moved"
		return stepsStmt, idArgs, flowsStmt, append(slices.Clone(idArgs), archivedAtArgs...)
	}
	// An error here surfaces from the statements that follow, which a failed statement aborts on every dialect that
	// gets here, or from the commit.
	tx.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(d.day)+" ("+archivedFlowColumns+", archived_at)"+
		" SELECT "+archivedFlowColumns+", "+archivedAt+" FROM dwarf_flows WHERE root_flow_id IN ("+ids+")",
		append(slices.Clone(archivedAtArgs), idArgs...)...)
	tx.ExecContext(ctx, "INSERT INTO "+migrations.StepsTable(d.day)+" ("+archivedStepColumns+") SELECT "+archivedStepColumns+
		" FROM dwarf_steps WHERE flow_id IN ("+trees+")", idArgs...)
	tx.ExecContext(ctx, fmt.Sprintf(directory, "dwarf_flows")+" AND root_flow_id IN ("+ids+")", idArgs...)
	return steps, idArgs, flows, idArgs
}

// liveOnlyColumn is the one column of dwarf_flows the day tables do not have: a live root's archive attempt, which
// means nothing once the tree is archived.
const liveOnlyColumn = "archive_attempt_at"

// archivedStepColumns are the columns of dwarf_steps, every one, which the day's steps table has too. A move and a
// staged copy name them, so a row is copied by name: an existing table whose added columns sit at its end - where
// PostgreSQL, SQL Server and SQLite add a column - copies into a day made with them in place. A column added to
// dwarf_steps must be added here too, or the archive drops it: TestMove_NamesEveryColumn compares the list with the
// table.
const archivedStepColumns = "step_id, flow_id, step_depth, step_token, task_name, task_url, state, state_bytes, changes," +
	" changes_bytes, interrupt_payload, interrupt_payload_bytes, state_refs, state_reduced, payload_deferrals," +
	" status, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures," +
	" cohort_cancellations, fan_out_ordinal, predecessor_id, successor_id, priority, fairness_key," +
	" fairness_weight, interrupt_done, resume_data, resume_data_bytes, subgraph_done, subgraph_result," +
	" subgraph_result_bytes, subgraph_error, parked, parked_child_flow_id, cancelling, advancing," +
	" advance_goto, advance_sleep_until, advance_on_error, not_before, lease_expires, lease_seq, engine_id," +
	" flow_created_hour, created_at, started_at, updated_at"

// archivedFlowColumns are the columns of dwarf_flows but archived_at and liveOnlyColumn, in table order. A move names
// them, so the archive's row takes its archive time in place of the live row's "not archived" value. A column added
// to dwarf_flows must be added here too, or the archive drops it: TestMove_NamesEveryColumn compares the list with
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
// SQL Server and SQLite wait as without it: SQLite has one writer, so nothing there is held. Do not put READPAST on
// the statements that copy and delete a locked tree on SQL Server: it would leave a held row out of the copy. where
// must restrict flow_id, by = or IN: on SQL Server the rows are reached by a seek on the primary key.
func lockRoots(ctx context.Context, tx *sequel.Tx, sfx string, skipHeld bool, where string, args ...any) ([]int, error) {
	var stmt string
	switch tx.DriverName() {
	case "pgx", "mysql", "mariadb":
		stmt = "SELECT flow_id FROM dwarf_flows" + sfx + " WHERE " + where + " ORDER BY flow_id FOR UPDATE"
		if skipHeld {
			stmt += " SKIP LOCKED"
		}
	case "mssql":
		stmt = database.UpdateSQL("mssql", "dwarf_flows"+sfx, database.SeekPrimaryKey("flow_id"), "touch=1-touch OUTPUT inserted.flow_id", where)
	default:
		stmt = "UPDATE dwarf_flows" + sfx + " SET touch=1-touch WHERE " + where + " RETURNING flow_id"
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
		if seams.IsFault(diagnostics.Join(fault, strconv.Itoa(root))) {
			return errors.New("injected fault: " + fault)
		}
	}
	return nil
}
