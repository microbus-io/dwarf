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
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// The batch sizes are Engine fields (reapBatch, reapBatchRows) so a test can drive a pass of several batches. A
// batch considers up to reapBatch due roots, which keeps each IN (...) literal list plannable, as purgeChunk does, and
// moves about reapBatchRows rows - a tree's joined steps - so it takes many small trees or few large ones in a
// transaction of about the same size, and always at least one tree. For as long as a batch's transaction runs,
// vacuum cannot remove any row that died after it began.

// reaperLoop removes closed trees from the live tables on its own plain ticker: it archives every root that ended
// archiveGrace ago, and deletes, without archiving, every root whose delete_after_ms window has elapsed. A single
// goroutine is inherently non-overlapping - a slow pass just delays the next tick. It is latency-tolerant, so there
// is no wake/startup/shutdown special pass: a tree that came due while a replica was down is removed on the next tick
// here, or by a peer replica's reaper. Only the shards this replica dispatches are reaped: every shard always has a
// dispatcher, so each is still covered, and a replica that only reads a shard would otherwise run the sweep through
// its two-connection reader pool once per replica. It ends on ctx, which drainRuntime cancels, abandoning a pass in
// flight: each batch is one transaction, so an abandoned batch lands whole or not at all, and what is left is reaped
// by a peer or the next start.
//
// The expiration pass (expireOn) is a stage of the same pass, after the reap, on the first tick and on any tick at
// least expireInterval after the last that expired: it has no deadline to keep, and its own time budget bounds how long
// it holds back the next reap. The first is at once, not an interval in, so that a replica living shorter than the
// interval still expires.
func (e *Engine) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(e.reapInterval)
	defer ticker.Stop()
	var lastExpire time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			expire := time.Since(lastExpire) >= e.expireInterval
			if expire {
				lastExpire = time.Now()
			}
			e.reapPass(ctx, e.archiveGrace, expire)
		}
	}
}

// reapKind is one population of due roots: how they are found, in what order, and what the batch that takes them
// re-checks.
type reapKind struct {
	name string
	// selectDue returns the statement and arguments that select up to limit due roots, restricted to residency (a SQL
	// fragment over flow_id and its arguments; empty for none), as rows of (flow_id, updated_at, age in milliseconds, created_hour).
	// The statement is one or more branches, each in the kind's order and each limited to limit, so the traversal is
	// the branches merged in that order (scanDue).
	selectDue func(residency string, residencyArgs []any, limit int) (string, []any)
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

// deletionDue is the roots DeleteOnCompletion and Delete stamped: they are deleted, never archived. Only
// roots ever carry delete_after_ms > 0, and the surgraph_flow_id=0 guard makes that explicit.
func (e *Engine) deletionDue(steal time.Duration) reapKind {
	const due = "delete_after_ms>0 AND surgraph_flow_id=0 AND DATE_ADD_MILLIS(updated_at, delete_after_ms)<=NOW_UTC()"
	return reapKind{
		name: "deleted",
		selectDue: func(residency string, args []any, limit int) (string, []any) {
			// ORDER BY flow_id is REQUIRED because LIMIT_OFFSET compiles to OFFSET/FETCH on SQL Server, which
			// rejects it without a preceding ORDER BY. The scan reads the partial index of stamped roots only.
			return "SELECT flow_id, updated_at, DATE_DIFF_MILLIS(NOW_UTC(), updated_at), created_hour FROM dwarf_flows WHERE " + due + residency +
				" ORDER BY flow_id LIMIT_OFFSET(?, 0)", append(slices.Clone(args), limit)
		},
		order:      byFlowID,
		due:        due,
		stealWhere: "DATE_ADD_MILLIS(updated_at, delete_after_ms)<=DATE_ADD_MILLIS(NOW_UTC(), ?)",
		stealArgs:  []any{-steal.Milliseconds()},
	}
}

// closedDue is the roots that ended archiveGrace ago and were not stamped for deletion: they are archived, or deleted
// at a retention of 0.
func (e *Engine) closedDue(steal time.Duration) reapKind {
	return e.closedDueAfter(steal, e.archiveGrace)
}

// closedDueAfter is closedDue for roots that ended grace ago.
func (e *Engine) closedDueAfter(steal, grace time.Duration) reapKind {
	graceMs := grace.Milliseconds()
	statuses := []string{
		workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
	}
	return reapKind{
		name: "closed",
		selectDue: func(residency string, args []any, limit int) (string, []any) {
			// One branch per terminal status, each a range of idx_dwarf_flows_status (status, updated_at) that stops
			// at the batch: one scan over all four statuses has no index order, so it would read and sort every due
			// root on every batch, O(backlog) per batch. The status is inlined, never bound: a bound status defeats
			// the filtered indexes and the estimates. Each root's age comes with it, from the row the scan reads
			// anyway: the first of each branch is that status's oldest due root, which is the archive lag. flow_id
			// breaks ties of updated_at, so the order is total: the index carries the primary key after updated_at on
			// MySQL, SQL Server and SQLite, and PostgreSQL sorts only each tie group (an incremental sort).
			var q string
			var all []any
			for i, status := range statuses {
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

// reapStealGrace is how long past due a root waits for the dispatcher whose residency it is before any dispatcher
// takes it: two reaper ticks, so a slow or departed peer's roots wait about three ticks rather than forever.
func (e *Engine) reapStealGrace() time.Duration {
	return 2 * e.reapInterval
}

// reapResidency restricts a due-root scan to this replica's share of the shard's dispatchers, flow_id % count =
// ordinal, plus every root overdue by the steal grace, and returns the pair (count 0 when it restricts nothing). Two
// reapers then take disjoint roots while both keep up, and a slow or departed one's roots are taken after the grace.
// It fails open, as the piston's partition does: an unknown or invalid pair, or a solo dispatcher, restricts nothing.
func (e *Engine) reapResidency(shard int, kind reapKind) (clause string, args []any, count, ordinal int) {
	count, ordinal, ok := e.partitionOn(shard)
	return residencyClause(count, ordinal, ok, kind)
}

// residencyClause is reapResidency's clause for the shard's partition pair.
func residencyClause(count, ordinal int, ok bool, kind reapKind) (clause string, args []any, c, o int) {
	if !ok || count < 2 || ordinal < 0 || ordinal >= count {
		return "", nil, 0, 0
	}
	return " AND (flow_id % ? = ? OR " + kind.stealWhere + ")", append([]any{count, ordinal}, kind.stealArgs...),
		count, ordinal
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

// reapDay is the archive day a pass writes into, and until when, by this process's clock, it archives into it: a margin
// before the day ends (reapQuietMargin), so a pass near midnight leaves its trees to the first pass of the next day.
// A zero stop never stops - test mode, or a day too short for a margin - where a move after midnight is stamped within
// its day all the same.
type reapDay struct {
	day  int
	stop time.Time
	// archive is the shard's, whose directory hours say which moved flows the directory must name.
	archive *archiver.Archive
}

// reapQuietMargin is how long before a day ends the reaper stops archiving into it, for days of dayLength: a minute,
// or an eighth of a shorter day - and none for a day under 16 seconds, where the margin would be within two of the
// archive clock's whole-second steps (TodayLeft rounds down), which can leave no moment of the day to archive in.
// A move that commits after midnight still stamps the day it is filed in (archivedAtSQL), so the margin only makes
// that rare; it is not what keeps a day's rows in order.
func reapQuietMargin(dayLength time.Duration) time.Duration {
	if dayLength < 16*time.Second {
		return 0
	}
	return min(time.Minute, dayLength/8)
}

// reapDueFlows is a reaper pass that does not expire: what a test drives to reap what is due.
func (e *Engine) reapDueFlows(ctx context.Context) {
	e.reapPass(ctx, e.archiveGrace, false)
}

// reapPass removes, per shard this replica dispatches, every due tree, archiving the closed roots that ended grace ago
// - the archive grace, or 0 for a test archiving what it just ended (engineseams.ArchiveNow) - and, with expire, then
// runs the expiration pass. Each shard's pass is one turn and a sequence of stages (reapShard), one after the other;
// a stage that fails or has nothing to do ends itself, never the stages after it.
func (e *Engine) reapPass(ctx context.Context, grace time.Duration, expire bool) {
	set := e.archives.Load()
	steal := e.reapStealGrace()
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			// A replica that resumes dispatching later saw none of what happened meanwhile, so it starts afresh: an
			// old cursor is only a hint about work seen before the handoff.
			e.forgetReaping(shard)
			return nil // see reaperLoop
		}
		// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
		// connections like any other caller instead of taking them from work already under way.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		e.reapShard(ctx, db, shard, set, steal, grace)
		if expire && e.flowExpiration > 0 {
			e.expireOn(ctx, db, shard)
		}
		return nil
	})
}

// reapShard gives each maintenance walker a slice on the same goroutine. Healthy backlog uses the remaining pass
// budget; failures and empty scans yield until the next tick. Atomic tree operations recheck eligibility under lock.
func (e *Engine) reapShard(ctx context.Context, db *sequel.DB, shard int, set *archiveSet, steal, grace time.Duration) {
	start := time.Now()
	defer func() { e.metricReapPass(ctx, shard, time.Since(start)) }()
	// Retention cleanup is independent of archive table health and runs once per tick.
	e.reapRetained(ctx, db, shard, set, steal, grace)
	for {
		moreDeleted := e.reapDue(ctx, db, shard, e.deletionDue(steal), func() (*reapDay, bool) { return nil, true }, reapReasonDeleted)
		moreClosed := e.reapClosed(ctx, db, shard, set, steal, grace)
		if ctx.Err() != nil || time.Since(start) >= e.reapBudget || (!moreDeleted && !moreClosed) {
			return
		}
	}
}

func (e *Engine) reapClosed(ctx context.Context, db *sequel.DB, shard int, set *archiveSet, steal, grace time.Duration) bool {
	var day *reapDay
	if e.retentionDays > 0 {
		if set == nil || set.byShard[shard] == nil {
			return false
		}
		day = &reapDay{}
		if err := e.refreshReapDay(ctx, set.byShard[shard], day); err != nil {
			e.archiveFailed(ctx, shard, "reading today's archive day", err)
			return false
		}
	}
	return e.reapDue(ctx, db, shard, e.closedDueAfter(steal, grace), func() (*reapDay, bool) {
		if day != nil && !day.stop.IsZero() && !time.Now().Before(day.stop) {
			return nil, false
		}
		return day, true
	}, reapReasonNoRetention)
}

// refreshReapDay reads today from the database clock into day, and when to stop archiving into it. The reaper makes
// no table: today and the days ahead are the archive's maintenance's to make, at Startup and on every pass. Tomorrow
// not whole is an error, an archive failure like a failed move: a row goes into a day only once the day after it is
// whole, which a locate relies on. Today missing fails the move itself.
func (e *Engine) refreshReapDay(ctx context.Context, a *archiver.Archive, day *reapDay) error {
	attemptCtx, cancel := e.reapAttemptCtx(ctx)
	defer cancel()
	today, left, err := a.TodayLeft(attemptCtx)
	if err != nil {
		return errors.Trace(err)
	}
	if !a.Whole(today + 1) {
		return errors.New("tomorrow's archive tables (day %d) are not whole", today+1)
	}
	day.day, day.archive = today, a
	day.stop = time.Time{}
	if quiet := reapQuietMargin(a.DayLength()); quiet > 0 && !e.noReapQuiet {
		day.stop = time.Now().Add(left - quiet)
	}
	return nil
}

// archiveFailed records an archive failure. Only the first is logged; repair logs individual held roots.
func (e *Engine) archiveFailed(ctx context.Context, shard int, what string, err error) {
	if ctx.Err() != nil {
		return
	}
	if e.archiveFailures.fail(shard, time.Now()) {
		e.logger.ErrorContext(ctx, "Reaper: "+what+" failed; retained trees stay in the live tables", "shard", shard, "error", err)
	}
}

type archiveFailures struct {
	mu   sync.Mutex
	byID map[int]archiveFailure
}
type archiveFailure struct{ since, last time.Time }

func (f *archiveFailures) fail(shard int, now time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.byID == nil {
		f.byID = map[int]archiveFailure{}
	}
	rec, failing := f.byID[shard]
	if !failing {
		rec.since = now
	}
	rec.last = now
	f.byID[shard] = rec
	return !failing
}

// clearUnlessFailedSince forgets the shard's failures, unless one was recorded at or after start: the pass that began
// at start archived without failing.
func (f *archiveFailures) clearUnlessFailedSince(shard int, start time.Time) {
	f.mu.Lock()
	if rec, ok := f.byID[shard]; ok && rec.last.Before(start) {
		delete(f.byID, shard)
	}
	f.mu.Unlock()
}

// failingFor returns how long archiving has been failing on shard at now, or 0.
func (f *archiveFailures) failingFor(shard int, now time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	rec, ok := f.byID[shard]
	if !ok {
		return 0
	}
	return max(now.Sub(rec.since), 0)
}

// forget forgets shard's failures.
func (f *archiveFailures) forget(shard int) {
	f.mu.Lock()
	delete(f.byID, shard)
	f.mu.Unlock()
}

// reset forgets every shard's failures.
func (f *archiveFailures) reset() {
	f.mu.Lock()
	f.byID = nil
	f.mu.Unlock()
}

// archiveLag is, per shard, how long past the archive grace the oldest due closed root had waited at the reaper's last
// scan: the dwarf_archive_lag_seconds reading. It lives as long as the engine's run.
type archiveLag struct {
	mu   sync.Mutex
	byID map[int]time.Duration
}

// set records shard's reading.
func (l *archiveLag) set(shard int, lag time.Duration) {
	l.mu.Lock()
	if l.byID == nil {
		l.byID = map[int]time.Duration{}
	}
	l.byID[shard] = lag
	l.mu.Unlock()
}

// of returns shard's last reading, and false before its first.
func (l *archiveLag) of(shard int) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	lag, ok := l.byID[shard]
	return lag, ok
}

// forget forgets shard's reading.
func (l *archiveLag) forget(shard int) {
	l.mu.Lock()
	delete(l.byID, shard)
	l.mu.Unlock()
}

// forgetReaping forgets what the reaper recorded about shard: its archive failures, its lag and where its expiration
// walk resumes.
func (e *Engine) forgetReaping(shard int) {
	e.archiveFailures.forget(shard)
	e.archiveLag.forget(shard)
	e.expirePositions.forget(shard)
	e.reapWalks.forget(shard)
}

// reset forgets every shard's reading.
func (l *archiveLag) reset() {
	l.mu.Lock()
	l.byID = nil
	l.mu.Unlock()
}

// scanDue selects up to fetch due roots of kind in its traversal order (kind.order), and the age of the oldest of them
// for a lagged kind (0 for none). The statement's branches - one per terminal status, for the closed kind - are merged
// here; their first fetch are exactly the kind's first fetch due roots, since each branch holds its own first fetch.
// It reports fewer than fetch only when every branch did, which is when it saw every due root.
func (e *Engine) scanDue(
	ctx context.Context, db *sequel.DB, kind reapKind, residency string, residencyArgs []any, fetch int,
) (roots []dueRoot, oldest time.Duration, err error) {
	ctx, cancel := e.reapAttemptCtx(ctx)
	defer cancel()
	stmt, args := kind.selectDue(residency, residencyArgs, fetch)
	rows, err := db.QueryContext(ctx, stmt, args...)
	// engineseams.FaultReapSelectErr treats this due-root scan as errored (sibling to engineseams.FaultReapMidTree,
	// which covers the delete half), so the test proves the pass logs and bails without deleting and the NEXT pass
	// reaps cleanly - the reaper's resilience to a transient SELECT blip.
	if err == nil && e.seams.IsFault(engineseams.FaultReapSelectErr) {
		rows.Close()
		err = errors.New("injected fault: " + engineseams.FaultReapSelectErr)
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
	slices.SortFunc(roots, kind.order)
	return roots[:min(fetch, len(roots))], oldest, nil
}

// reapBatchOf removes the trees of the roots it can lock among rootIDs, in any order, in one transaction: moved into
// day's archive tables unless day is nil, deleted otherwise. It returns how many flow rows it removed.
//
// Root locks and eligibility rechecks arbitrate overlapping replicas. PostgreSQL and MySQL skip held roots;
// elsewhere the attempt waits under its deadline. Transact retries contention, including straggler lock inversions.
func (e *Engine) reapBatchOf(
	ctx context.Context, db *sequel.DB, rootIDs []int, kind reapKind, day *reapDay,
) (int, error) {
	ctx, cancel := e.reapAttemptCtx(ctx)
	defer cancel()
	rootIDs = slices.Sorted(slices.Values(rootIDs))
	var moved int
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		moved = 0
		locked, err := lockDueRoots(ctx, tx, rootIDs, kind)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		e.seams.Checkpoint(ctx, engineseams.CheckpointReapBatchLocked)
		// ids are trusted integers embedded as literals to dodge the per-driver bind-param ceiling.
		ids := intCSV(locked)
		trees := "SELECT flow_id FROM dwarf_flows WHERE root_flow_id IN (" + ids + ")"
		steps := "DELETE FROM dwarf_steps WHERE flow_id IN (" + trees + ")"
		flows := "DELETE FROM dwarf_flows WHERE root_flow_id IN (" + ids + ")"
		// engineseams.FaultReapContention fails every attempt of an archive move on lock contention.
		if day != nil && e.seams.IsFault(engineseams.FaultReapContention) {
			return errors.Trace(&pgconn.PgError{Code: "40P01", Message: "injected fault: " + engineseams.FaultReapContention})
		}
		// engineseams.FaultReapArchiveErr fails an archive move with an error that is not lock contention.
		if day != nil && e.seams.IsFault(engineseams.FaultReapArchiveErr) {
			return errors.New("injected fault: " + engineseams.FaultReapArchiveErr)
		}
		// A flow a locate would not find by its creation hour alone - archived two or more days after its creation
		// day, or on a day before it (a straggler created while a batch begun before midnight ran) - is named in the
		// directory, in this transaction, from the same rows the move takes.
		var directory, archivedAt string
		var archivedAtArgs []any
		if day != nil {
			archivedAt, archivedAtArgs = archivedAtSQL(tx.DriverName(), day.archive.DayEnds(day.day))
			before, from := day.archive.DirectoryHours(day.day)
			directory = "INSERT INTO " + archiver.DirectoryTable + " (flow_id, day) SELECT flow_id, " + strconv.Itoa(day.day) +
				" FROM %s WHERE (created_hour<" + strconv.Itoa(before) + " OR created_hour>=" + strconv.Itoa(from) + ")"
		}
		var flowsStmt string
		var flowsArgs []any
		switch {
		case day == nil:
			flowsStmt = flows
		case tx.DriverName() == "pgx":
			// Each table moves in one statement, so what is archived is exactly what is deleted, whatever a
			// descendant does between statements - only the roots are locked. RETURNING * is in column order, and
			// the day tables carry the live columns in the same order; the flows name theirs, to stamp archived_at.
			// The directory's rows come from the same moved rows, in the same statement.
			steps = "WITH moved AS (" + steps + " RETURNING *)" +
				" INSERT INTO " + migrations.StepsTable(day.day) + " SELECT * FROM moved"
			flowsStmt = "WITH moved AS (" + flows + " RETURNING *)," +
				" named AS (" + fmt.Sprintf(directory, "moved") + ")" +
				" INSERT INTO " + migrations.FlowsTable(day.day) + " (" + archivedFlowColumns + ", archived_at)" +
				" SELECT " + archivedFlowColumns + ", " + archivedAt + " FROM moved"
			flowsArgs = archivedAtArgs
		default:
			// Copied, then deleted. A descendant that changes between the copy and the delete - only a fan-out
			// straggler settling, or an orphan a bug left - is archived as it was at the copy.
			tx.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(day.day)+" ("+archivedFlowColumns+", archived_at)"+
				" SELECT "+archivedFlowColumns+", "+archivedAt+" FROM dwarf_flows WHERE root_flow_id IN ("+ids+")", archivedAtArgs...)
			tx.ExecContext(ctx, "INSERT INTO "+migrations.StepsTable(day.day)+
				" SELECT * FROM dwarf_steps WHERE flow_id IN ("+trees+")")
			tx.ExecContext(ctx, fmt.Sprintf(directory, "dwarf_flows")+" AND root_flow_id IN ("+ids+")")
			flowsStmt = flows
		}
		// Steps before flows: the subquery reads the flow rows.
		if _, err := tx.ExecContext(ctx, steps); err != nil {
			return errors.Trace(err)
		}
		// engineseams.FaultReapMidTree aborts after the steps go but before the flows do, so the whole batch rolls
		// back atomically. The test proves a mid-tree failure leaves the tree intact (not a half-deleted flow with
		// no steps) and the next reap pass removes it cleanly.
		if e.seams.Enabled() {
			for _, root := range locked {
				if e.seams.IsFault(engineseams.Join(engineseams.FaultReapMidTree, strconv.Itoa(root))) {
					return errors.New("injected fault: " + engineseams.FaultReapMidTree)
				}
			}
		}
		if e.seams.IsFault(engineseams.FaultReapMidTree) {
			return errors.New("injected fault: " + engineseams.FaultReapMidTree)
		}
		res, err := tx.ExecContext(ctx, flowsStmt, flowsArgs...)
		if err != nil {
			return errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		moved = int(n)
		return nil
	})
	return moved, errors.Trace(err)
}

// archivedFlowColumns are the columns of dwarf_flows but archived_at, in table order. A move names them, so the
// archive's row takes its archive time in place of the live row's "not archived" value. A column added to dwarf_flows
// must be added here too, or the archive drops it: TestArchiving_TheMoveNamesEveryFlowsColumn compares the list with
// the table.
const archivedFlowColumns = "flow_id, flow_token, workflow_url, workflow_name, graph, graph_hash, payload_ver, baggage," +
	" status, stopped_step_id, surgraph_flow_id, surgraph_step_id, root_flow_id, forked_from_step, trace_parent," +
	" delete_on_completion, delete_after_ms, final_state, error, cancel_reason, cancel_watermark, priority, fairness_key," +
	" fairness_weight, time_budget_ms, engine_id, created_hour, created_at, started_at, updated_at, touch"

// archivedAtSQL returns the archive time a move into the day that ends at dayEnds stamps, and its arguments: the
// database's now, or the day's last millisecond if the move commits after midnight - so a day holds only rows archived
// on it, and a later day only later ones: the days are in archive order. On PostgreSQL now is
// the transaction's start, so every tree a batch moves is stamped alike. CASE rather than LEAST: SQL Server 2019 has
// none.
func archivedAtSQL(driverName string, dayEnds time.Time) (expr string, args []any) {
	last, arg := sqlTime(driverName, dayEnds.Add(-time.Millisecond))
	return "CASE WHEN NOW_UTC()<" + last + " THEN NOW_UTC() ELSE " + last + " END", []any{arg, arg}
}

// lockDueRoots locks the roots among rootIDs that are still due and that no other transaction holds, and returns them
// ascending.
//
// PostgreSQL and MySQL skip held roots; other dialects wait under the attempt deadline. Repair revisits both.
func lockDueRoots(ctx context.Context, tx *sequel.Tx, rootIDs []int, kind reapKind) ([]int, error) {
	return lockRootsIn(ctx, tx, liveTables.flows, true, "flow_id IN ("+intCSV(rootIDs)+") AND "+kind.due, kind.dueArgs...)
}

// lockRootsIn locks the rows of flows that match where and returns their flow ids, ascending. The rows are about to be
// deleted, so on PostgreSQL and MySQL the lock is FOR UPDATE, the strength a DELETE takes; SQLite and SQL Server take
// it by flipping touch, as lockFlowRows does. It serializes with Fork's lock on the same root (lockRows), which on
// PostgreSQL is FOR NO KEY UPDATE and conflicts with it. With skipHeld, a row another transaction holds is left out
// rather than waited for (SKIP LOCKED) on PostgreSQL and MySQL. SQL Server and SQLite wait as without it: SQLite has
// one writer, so nothing there is held. On SQL Server a READPAST here does skip the row, but the move's own
// statements then wait on it - under locking READ COMMITTED a DELETE that scans waits on every locked row it passes,
// one it would not delete included - and READPAST on those could leave a tree's row out of its copy. SQL Server's
// lock wait is bounded by the caller's context.
func lockRootsIn(ctx context.Context, tx *sequel.Tx, flows string, skipHeld bool, where string, args ...any) ([]int, error) {
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

// rootsWithinRows returns the leading roots of roots, in the order given, whose trees fit in about reapBatchRows rows,
// and at least the first. A tree's rows are counted as its flows' joined steps, one for a flow with none. Sizing is a
// batching hint outside the transaction; the batch locks only the roots it takes, and its
// transaction still opens with a write, which SQLite needs.
func (e *Engine) rootsWithinRows(ctx context.Context, db *sequel.DB, roots []int) ([]int, error) {
	ctx, cancel := e.reapAttemptCtx(ctx)
	defer cancel()
	if e.seams.Enabled() {
		for _, root := range roots {
			if e.seams.IsFault(engineseams.Join(engineseams.FaultReapSizeErr, strconv.Itoa(root))) {
				return nil, errors.New("injected fault: " + engineseams.FaultReapSizeErr)
			}
		}
	}
	return e.rootsWithinRowsIn(ctx, db, liveTables, roots)
}

// rootsWithinRowsIn is rootsWithinRows over the given tables: the live ones, or an archive day's.
func (e *Engine) rootsWithinRowsIn(ctx context.Context, db *sequel.DB, t flowTables, roots []int) ([]int, error) {
	rows, err := db.QueryContext(ctx, "SELECT f.root_flow_id, COUNT(*) FROM "+t.flows+" f"+
		" LEFT JOIN "+t.steps+" s ON s.flow_id=f.flow_id WHERE f.root_flow_id IN ("+intCSV(roots)+") GROUP BY f.root_flow_id")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	size := make(map[int]int, len(roots))
	for rows.Next() {
		var root, n int
		if err := rows.Scan(&root, &n); err != nil {
			return nil, errors.Trace(err)
		}
		size[root] = n
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return withinRows(roots, size, e.reapBatchRows), nil
}

// withinRows returns the leading roots whose sizes sum to at most limit, and at least the first.
func withinRows(roots []int, size map[int]int, limit int) []int {
	total := 0
	for i, root := range roots {
		total += size[root]
		if total > limit && i > 0 {
			return roots[:i]
		}
	}
	return roots
}
