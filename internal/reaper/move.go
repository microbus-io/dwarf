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
	"github.com/microbus-io/sequel"
)

// filingDay is the archive day a pass writes into, and until when, by this process's clock, it archives into it: a margin
// before the day ends (quietMargin), so a pass near midnight leaves its trees to the first pass of the next day. A zero
// stop never stops - a day too short for a margin, or NoQuietMargin - where a move after midnight is stamped within its
// day all the same. notWhole is true when tomorrow's tables are not whole, which also stops every new archive. Neither
// stops a staged archive already started, which goes on into its own day.
type filingDay struct {
	day      int
	stop     time.Time
	notWhole bool
	// archive is the shard's, whose directory hours say which moved flows the directory must name.
	archive *archiver.Archive
}

// startable reports whether a new archive - an atomic move, or a staged attempt - may start into the day now.
func (d *filingDay) startable() bool {
	return !d.notWhole && (d.stop.IsZero() || time.Now().Before(d.stop))
}

// quietMargin is how long before a day ends the reaper stops archiving into it, for days of dayLength: a minute, or an
// eighth of a shorter day - and none for a day under 16 seconds, where the margin would be within two of the archive
// clock's whole-second steps (TodayLeft rounds down), which can leave no moment of the day to archive in. A move that
// commits after midnight still stamps the day it is filed in (archivedAtSQL), so the margin only makes that rare; it is
// not what keeps a day's rows in order.
func quietMargin(dayLength time.Duration) time.Duration {
	if dayLength < 16*time.Second {
		return 0
	}
	return min(time.Minute, dayLength/8)
}

// refreshDay reads today from the database clock into d, and when to stop archiving into it. The reaper makes no
// table: today and the days ahead are the archive's maintenance's to make. Tomorrow not whole is an error, an archive
// failure like a failed move: a row goes into a day only once the day after it is whole, which a locate relies on. d
// is filled even then, with notWhole set, so a staged archive already started goes on into its own day. Today missing
// fails the move itself.
func (p *pass) refreshDay(ctx context.Context, d *filingDay) error {
	a := p.shard.Archive
	attemptCtx, cancel := p.attemptCtx(ctx)
	defer cancel()
	today, left, err := a.TodayLeft(attemptCtx)
	if err != nil {
		return errors.Trace(err)
	}
	d.day, d.archive = today, a
	d.stop = time.Time{}
	if quiet := quietMargin(a.DayLength()); quiet > 0 && !p.cfg.NoQuietMargin {
		d.stop = time.Now().Add(left - quiet)
	}
	d.notWhole = !a.Whole(today + 1)
	if d.notWhole {
		return errors.New("tomorrow's archive tables (day %d) are not whole", today+1)
	}
	return nil
}

// scanDue selects up to fetch due roots of k in its traversal order (k.order), and the age of the oldest of them for a
// lagged kind (0 for none). The statement's branches - one per terminal status, for the closed kind - are merged here;
// their first fetch are exactly the kind's first fetch due roots, since each branch holds its own first fetch. It
// reports fewer than fetch only when every branch did, which is when it saw every due root.
func (p *pass) scanDue(ctx context.Context, k kind, residency string, residencyArgs []any, fetch int) (roots []dueRoot, oldest time.Duration, err error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	db := p.shard.DB
	stmt, args := k.selectDue(db.DriverName(), residency, residencyArgs, fetch)
	rows, err := db.QueryContext(ctx, stmt, args...)
	// FaultSelectErr treats the scan as failed, so a test proves a pass logs and bails without deleting, and the next
	// pass reaps cleanly.
	if err == nil && p.r.seams.IsFault(FaultSelectErr) {
		rows.Close()
		err = errors.New("injected fault: " + FaultSelectErr)
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
		roots = append(roots, r)
		oldest = max(oldest, time.Duration(ageMs*float64(time.Millisecond)))
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.Trace(err)
	}
	slices.SortFunc(roots, k.order)
	return roots[:min(fetch, len(roots))], oldest, nil
}

// batchOf is move under the attempt deadline, on the pass's shard.
func (p *pass) batchOf(ctx context.Context, rootIDs []int, k kind, d *filingDay) (removal, error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	return p.r.move(ctx, p.shard.DB, rootIDs, k, d)
}

// MoveTrees moves the closed trees rooted at roots from the live tables into the archive tables of day d, in one
// transaction, on the caller's context: what a pass does to one batch of due closed roots, into a day the caller
// chooses. A root that is not a closed root, or is stamped for deletion, is left where it is. It returns how many
// flows it moved. The caller provides a non-nil archive, and makes sure the tables of d and of the day after it are
// whole: MoveTrees checks neither, and a locate relies on the day after being whole. It locks each root as Pass does.
func (r *Reaper) MoveTrees(ctx context.Context, db *sequel.DB, archive *archiver.Archive, d int, roots []int) (moved int, err error) {
	n, err := r.move(ctx, db, roots, closedDue(0, 0), &filingDay{day: d, archive: archive})
	return n.flows, err
}

// move removes the trees of the roots it can lock among rootIDs, in any order, in one transaction: moved into d's
// archive tables unless d is nil, deleted otherwise. It returns how many flow rows it removed.
//
// Root locks and eligibility rechecks arbitrate overlapping replicas. PostgreSQL and MySQL skip held roots; elsewhere
// the attempt waits under its deadline. Transact retries contention, including straggler lock inversions.
func (r *Reaper) move(ctx context.Context, db *sequel.DB, rootIDs []int, k kind, d *filingDay) (removal, error) {
	rootIDs = slices.Sorted(slices.Values(rootIDs))
	due, dueArgs := k.due, k.dueArgs
	if d != nil {
		// A root whose staged archive has started is the staged path's, whatever this batch's selection saw.
		none, noneArg := NoAttemptSQL(db.DriverName())
		due, dueArgs = due+" AND archive_attempt_at<="+none, append(slices.Clone(dueArgs), noneArg)
	}
	var moved removal
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		moved = removal{}
		locked, err := lockRoots(ctx, tx, live.flows, true, "flow_id IN ("+intCSV(rootIDs)+") AND "+due, dueArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		r.seams.Checkpoint(ctx, CheckpointBatchLocked)
		// ids are trusted integers embedded as literals to dodge the per-driver bind-param ceiling.
		ids := intCSV(locked)
		trees := "SELECT flow_id FROM dwarf_flows WHERE root_flow_id IN (" + ids + ")"
		published := 0
		if d == nil {
			cond, args := publishedSQL(tx.DriverName())
			// Only an atomic batch is counted here, under its root locks; large trees use chunks.
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id IN (SELECT flow_id FROM dwarf_flows WHERE flow_id IN ("+ids+") AND "+cond+")", args...).Scan(&published); err != nil {
				return errors.Trace(err)
			}
		}
		steps := "DELETE FROM dwarf_steps WHERE flow_id IN (" + trees + ")"
		flows := "DELETE FROM dwarf_flows WHERE root_flow_id IN (" + ids + ")"
		// FaultContention fails every attempt of an archive move on lock contention.
		if d != nil && r.seams.IsFault(FaultContention) {
			return errors.Trace(&pgconn.PgError{Code: "40P01", Message: "injected fault: " + FaultContention})
		}
		// FaultArchiveErr fails an archive move with an error that is not lock contention.
		if d != nil && r.seams.IsFault(FaultArchiveErr) {
			return errors.New("injected fault: " + FaultArchiveErr)
		}
		// A flow a locate would not find by its creation hour alone - archived two or more days after its creation
		// day, or on a day before it (a straggler created while a batch begun before midnight ran) - is named in the
		// directory, in this transaction, from the same rows the move takes.
		var directory, archivedAt string
		var archivedAtArgs []any
		if d != nil {
			archivedAt, archivedAtArgs = archivedAtSQL(tx.DriverName(), d.archive.DayEnds(d.day))
			before, from := d.archive.DirectoryHours(d.day)
			directory = "INSERT INTO " + archiver.DirectoryTable + " (flow_id, day) SELECT flow_id, " + strconv.Itoa(d.day) +
				" FROM %s WHERE (created_hour<" + strconv.Itoa(before) + " OR created_hour>=" + strconv.Itoa(from) + ")"
		}
		var flowsStmt string
		var flowsArgs []any
		switch {
		case d == nil:
			flowsStmt = flows
		case tx.DriverName() == "pgx":
			// Each table moves in one statement, so what is archived is exactly what is deleted, whatever a
			// descendant does between statements - only the roots are locked. RETURNING * is in column order, and
			// the day tables carry the live columns in the same order; the flows name theirs, to stamp archived_at.
			// The directory's rows come from the same moved rows, in the same statement.
			steps = "WITH moved AS (" + steps + " RETURNING *)" +
				" INSERT INTO " + migrations.StepsTable(d.day) + " SELECT * FROM moved"
			flowsStmt = "WITH moved AS (" + flows + " RETURNING *)," +
				" named AS (" + fmt.Sprintf(directory, "moved") + ")" +
				" INSERT INTO " + migrations.FlowsTable(d.day) + " (" + archivedFlowColumns + ", archived_at)" +
				" SELECT " + archivedFlowColumns + ", " + archivedAt + " FROM moved"
			flowsArgs = archivedAtArgs
		default:
			// Copied, then deleted. A descendant that changes between the copy and the delete - only a fan-out
			// straggler settling, or an orphan a bug left - is archived as it was at the copy.
			tx.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(d.day)+" ("+archivedFlowColumns+", archived_at)"+
				" SELECT "+archivedFlowColumns+", "+archivedAt+" FROM dwarf_flows WHERE root_flow_id IN ("+ids+")", archivedAtArgs...)
			tx.ExecContext(ctx, "INSERT INTO "+migrations.StepsTable(d.day)+
				" SELECT * FROM dwarf_steps WHERE flow_id IN ("+trees+")")
			tx.ExecContext(ctx, fmt.Sprintf(directory, "dwarf_flows")+" AND root_flow_id IN ("+ids+")")
			flowsStmt = flows
		}
		// Steps before flows: the subquery reads the flow rows.
		if _, err := tx.ExecContext(ctx, steps); err != nil {
			return errors.Trace(err)
		}
		// FaultMidTree aborts after the steps go but before the flows do, so the whole batch rolls back atomically: a
		// mid-tree failure leaves the tree intact, never a flow with no steps, and the next pass removes it cleanly.
		if r.seams.Enabled() {
			for _, root := range locked {
				if r.seams.IsFault(join(FaultMidTree, strconv.Itoa(root))) {
					return errors.New("injected fault: " + FaultMidTree)
				}
			}
		}
		if r.seams.IsFault(FaultMidTree) {
			return errors.New("injected fault: " + FaultMidTree)
		}
		res, err := tx.ExecContext(ctx, flowsStmt, flowsArgs...)
		if err != nil {
			return errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		moved.flows = int(n)
		if d == nil {
			moved.unarchived = moved.flows - published
		}
		return nil
	})
	if err != nil {
		return removal{}, errors.Trace(err)
	}
	return moved, nil
}

// liveOnlyColumn is the one column of dwarf_flows the day tables do not have: a live root's archive attempt, which
// means nothing once the tree is archived.
const liveOnlyColumn = "archive_attempt_at"

// archivedFlowColumns are the columns of dwarf_flows but archived_at and liveOnlyColumn, in table order. A move names
// them, so the archive's row takes its archive time in place of the live row's "not archived" value. A column added to dwarf_flows
// must be added here too, or the archive drops it: TestMove_NamesEveryFlowsColumn compares the list with the table.
const archivedFlowColumns = "flow_id, flow_token, workflow_url, workflow_name, graph, graph_hash, payload_ver, baggage," +
	" status, stopped_step_id, surgraph_flow_id, surgraph_step_id, root_flow_id, forked_from_step, trace_parent," +
	" delete_on_completion, delete_after_ms, final_state, error, cancel_reason, cancel_watermark, priority, fairness_key," +
	" fairness_weight, time_budget_ms, engine_id, created_hour, created_at, started_at, updated_at, touch"

// archivedAtSQL returns the archive time a move into the day that ends at dayEnds stamps, and its arguments: the
// database's now, or the day's last millisecond if the move commits after midnight - so a day holds only rows archived
// on it, and a later day only later ones: the days are in archive order. On PostgreSQL now is the transaction's start,
// so every tree a batch moves is stamped alike. CASE rather than LEAST: SQL Server 2019 has none.
func archivedAtSQL(driverName string, dayEnds time.Time) (expr string, args []any) {
	last, arg := database.SQLTime(driverName, dayEnds.Add(-time.Millisecond))
	return "CASE WHEN NOW_UTC()<" + last + " THEN NOW_UTC() ELSE " + last + " END", []any{arg, arg}
}

// lockRoots locks the rows of flows that match where and returns their flow ids, ascending. The rows are about to be
// deleted, so on PostgreSQL and MySQL the lock is FOR UPDATE, the strength a DELETE takes; SQLite and SQL Server take
// it by flipping touch. With skipHeld, a row another transaction holds is left out rather
// than waited for (SKIP LOCKED) on PostgreSQL and MySQL. SQL Server and SQLite wait as without it: SQLite has one
// writer, so nothing there is held. On SQL Server a READPAST here does skip the row, but the move's own statements then
// wait on it - under locking READ COMMITTED a DELETE that scans waits on every locked row it passes, one it would not
// delete included - and READPAST on those could leave a tree's row out of its copy. SQL Server's lock wait is bounded
// by the caller's context.
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

// rootsWithinRows returns the leading roots of roots, in the order given, that one batch takes, with the first tree's
// size and, when archiving, the archive attempts among roots by their age. A root with an attempt, or one whose atomic
// archive ran into its deadline, is archived in stages, so when archiving it takes a batch of its own and is not
// sized: first, it is the cut alone; later, the cut ends before it.
// The others are sized by capped counts in their order (cutWithinRows): the first up to what decides its path - over
// the batch, and over StageRows when staging - and each later one up to what is left of the batch. Sizing is a
// batching hint outside the transaction; the batch locks only the roots it takes, and its transaction still opens with
// a write, which SQLite needs.
func (p *pass) rootsWithinRows(ctx context.Context, roots []int, archiving bool) (cut []int, firstSize int, attempts map[int]time.Duration, err error) {
	// staged is whether a root takes the staged path whatever its size: it has an attempt, or its atomic archive ran
	// into its deadline (preferences).
	staged := func(root int) bool {
		_, ok := attempts[root]
		return ok || p.r.prefer.has(stageKey{p.shard.Index, root})
	}
	if archiving {
		if attempts, err = p.attemptsAmong(ctx, roots); err != nil {
			return nil, 0, nil, errors.Trace(err)
		}
		if staged(roots[0]) {
			return roots[:1], 0, attempts, nil
		}
	}
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	firstLimit := max(p.cfg.BatchRows, 1) + 1
	if archiving && p.cfg.StageRows > 0 {
		firstLimit = max(firstLimit, p.cfg.StageRows+1)
	}
	var stop func(int) bool
	if archiving {
		stop = staged
	}
	cut, firstSize, err = cutWithinRows(roots, p.cfg.BatchRows, firstLimit, stop,
		func(group, limits []int) ([]int, error) {
			if p.r.seams.Enabled() {
				for _, root := range group {
					if p.r.seams.IsFault(join(FaultSizeErr, strconv.Itoa(root))) {
						return nil, errors.New("injected fault: " + FaultSizeErr)
					}
				}
			}
			return treesRowsUpTo(ctx, p.shard.DB, live, group, limits)
		})
	return cut, firstSize, attempts, errors.Trace(err)
}

// cutWithinRows returns the leading roots whose trees fit in about rows rows, and at least the first, with the first's
// size. It sizes the roots in groups - the first of firstGroup, then of sizeGroup - each by one call of sizes, which
// counts each tree up to its limit: the first root up to firstLimit, every other up to one past what was left of rows
// when its group was sent, which is all it takes to tell whether it fits. So no count reads a large tree whole, and a
// batch of small trees takes a few calls, not one a tree. The cut ends before a root for which stop is true.
func cutWithinRows(roots []int, rows, firstLimit int, stop func(root int) bool, sizes func(group, limits []int) ([]int, error)) (cut []int, firstSize int, err error) {
	rows = max(rows, 1)
	total, i := 0, 0
	for i < len(roots) {
		n := sizeGroup
		if i == 0 {
			n = firstGroup
		}
		group := roots[i:min(i+n, len(roots))]
		for k, root := range group {
			if (i+k > 0) && stop != nil && stop(root) {
				group = group[:k]
				break
			}
		}
		if len(group) == 0 {
			return roots[:i], firstSize, nil
		}
		limits := make([]int, len(group))
		for k := range group {
			limits[k] = rows - total + 1
		}
		if i == 0 {
			limits[0] = firstLimit
		}
		counts, err := sizes(group, limits)
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		for k, c := range counts {
			if i+k == 0 {
				firstSize = c
			}
			total += c
			if total > rows {
				return roots[:max(i+k, 1)], firstSize, nil
			}
		}
		i += len(group)
		if i < len(roots) && stop != nil && stop(roots[i]) {
			return roots[:i], firstSize, nil
		}
	}
	return roots, firstSize, nil
}

// The groups cutWithinRows sizes in one call: a small first one, since its first tree may be large and the batch then
// takes it alone, then larger ones.
const (
	firstGroup = 16
	sizeGroup  = 64
)
