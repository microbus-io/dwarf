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
// The expiration pass (expireFlows) rides the same goroutine, after the reap of the first tick and of any tick at least
// expireInterval after the last: it has no deadline to keep, and its own time budget bounds how long it holds back the
// next reap. The first is at once, not an interval in, so that a replica living shorter than the interval still expires.
func (e *Engine) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(e.reapInterval)
	defer ticker.Stop()
	var lastExpire time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.reapDueFlows(ctx)
			if time.Since(lastExpire) >= e.expireInterval {
				lastExpire = time.Now()
				e.expireFlows(ctx)
			}
		}
	}
}

// reapKind is one population of due roots: how they are found, and what the batch that takes them re-checks.
type reapKind struct {
	name string
	// selectDue returns the statement and arguments that select up to limit due roots, oldest first, restricted to
	// residency (a SQL fragment over flow_id and its arguments; empty for none).
	selectDue func(residency string, residencyArgs []any, limit int) (string, []any)
	// due re-checks a locked root, with its arguments.
	due     string
	dueArgs []any
	// stealWhere matches a root overdue by the steal grace, which any dispatcher takes whatever its residency.
	stealWhere string
	stealArgs  []any
	// lagged is set when selectDue's second column is each root's age in milliseconds, oldest first per branch, so
	// a pass's first scan reads the archive lag past grace (archiveLag); otherwise the column is 0.
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
			return "SELECT flow_id, 0 FROM dwarf_flows WHERE " + due + residency +
				" ORDER BY flow_id LIMIT_OFFSET(?, 0)", append(slices.Clone(args), limit)
		},
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
			// anyway: the first of each branch is that status's oldest due root, which is the archive lag.
			var q string
			var all []any
			for i, status := range statuses {
				if i > 0 {
					q += " UNION ALL "
				}
				q += "SELECT flow_id, age_ms FROM (SELECT flow_id, DATE_DIFF_MILLIS(NOW_UTC(), updated_at) AS age_ms" +
					" FROM dwarf_flows WHERE status='" + status + "'" +
					" AND updated_at<=DATE_ADD_MILLIS(NOW_UTC(), ?) AND surgraph_flow_id=0 AND delete_after_ms=0" +
					residency + " ORDER BY updated_at LIMIT_OFFSET(?, 0)) due_" + status
				all = append(all, -graceMs)
				all = append(all, args...)
				all = append(all, limit)
			}
			return q, all
		},
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

// reapFetch is how many candidates a scan asks for to fill a batch of batch roots. Once a backlog is older than the
// steal grace the clause admits every class, in no order of class, and a scan of count x batch rows holds this
// replica's own class batch times only on average - short about half the time, which makes the batch take a
// peer's roots while its own wait, and the two reapers meet on them. Twice that leaves the own class short only far
// out in the tail, as it does for the piston's fetch.
func reapFetch(count, batch int) int {
	if count < 2 {
		return batch
	}
	return 2 * count * batch
}

// ownFirst returns up to n of ids with this replica's own class (id % count = ordinal) ahead of the others, so a
// batch takes a peer's roots only for the room its own leave. With no pair every id is its own.
func ownFirst(ids []int, count, ordinal, n int) []int {
	if count >= 2 {
		slices.SortStableFunc(ids, func(a, b int) int {
			ownA, ownB := a%count == ordinal, b%count == ordinal
			switch {
			case ownA == ownB:
				return 0
			case ownA:
				return -1
			}
			return 1
		})
	}
	return ids[:min(n, len(ids))]
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

// reapDueFlows removes, per shard this replica dispatches, every due tree: first the roots stamped for deletion,
// then the roots that ended archiveGrace ago, archived into today's day tables (or deleted, at a retention of 0).
// A tree is a root and every subgraph descendant, found by root_flow_id, and goes in one transaction with its steps.
// Each kind loops batches until its due set drains, all on one turn.
//
// The tree delete is unconditional on descendant status: it removes every flow with root_flow_id IN(ids)
// regardless of whether a descendant is still non-terminal. A due root is always terminal - but a descendant can
// still be a live orphan (a running child whose parent already terminalized, which only a bug leaves; see
// recoverOrphanedSubgraphChildren) or a fan-out straggler still settling. Removing it is safe: a worker
// mid-dispatch on it no-ops via the lease fence (its claim/write matches zero rows once the row is gone), so the
// reaper does not reguard on descendant status.
func (e *Engine) reapDueFlows(ctx context.Context) {
	e.reapPass(ctx, e.archiveGrace)
}

// reapPass is reapDueFlows archiving the roots that ended grace ago: the archive grace, or 0 for a test archiving
// what it just ended (engineseams.ArchiveNow).
func (e *Engine) reapPass(ctx context.Context, grace time.Duration) {
	set := e.archives.Load()
	steal := e.reapStealGrace()
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			// A replica that resumes dispatching later saw none of what happened meanwhile, so it starts afresh: an
			// old failure would read as past the grace at its first failure, and delete unarchived at once.
			e.forgetReaping(shard)
			return nil // see reaperLoop
		}
		// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
		// connections like any other caller instead of taking them from work already under way.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		passStart := time.Now()
		defer func() { e.metricReapPass(ctx, shard, time.Since(passStart)) }()
		// A failure of the stamped kind, logged, does not hold back the closed kind.
		e.reapDue(ctx, db, shard, e.deletionDue(steal), func() (*reapDay, bool) { return nil, true }, reapReasonDeleted)
		var day *reapDay
		unarchived := reapReasonNoRetention
		if e.retentionDays > 0 {
			if set == nil || set.byShard[shard] == nil {
				return nil // nowhere to archive into; never fall back to deleting
			}
			day = &reapDay{}
			if err := e.refreshReapDay(ctx, set.byShard[shard], day); err != nil {
				if !e.archiveFailed(ctx, shard, "reading today's archive day", err) {
					return nil
				}
				day, unarchived = nil, reapReasonArchiveFailed
			}
		}
		archiving := day != nil
		endedStart := time.Now()
		ok := e.reapDue(ctx, db, shard, e.closedDueAfter(steal, grace), func() (*reapDay, bool) {
			if day != nil && !day.stop.IsZero() && !time.Now().Before(day.stop) {
				return nil, false // the day's last moments: the trees wait for the next day's first pass
			}
			return day, true
		}, unarchived)
		if ok && archiving {
			// Only a pass that met no failure ends one: a pass that deleted after the grace failed too.
			e.archiveFailures.clearUnlessFailedSince(shard, endedStart)
		}
		return nil
	})
}

// refreshReapDay reads today from the database clock into day, and when to stop archiving into it. The reaper makes
// no table: today and the days ahead are the archive's maintenance's to make, at Startup and on every pass. Tomorrow
// not whole is an error, an archive failure like a failed move: a row goes into a day only once the day after it is
// whole, which a locate relies on. Today missing fails the move itself.
func (e *Engine) refreshReapDay(ctx context.Context, a *archiver.Archive, day *reapDay) error {
	today, left, err := a.TodayLeft(ctx)
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

// archiveFailed records that archiving failed on shard and reports whether to delete the closed trees unarchived
// instead: only once archiving has been failing for archiveFailureGrace, which keeps the live tables from filling
// while an archive fault lasts, at the cost of those trees' archive. Until then the caller ends its pass and the trees
// wait. It logs the first failure and the start of the deletions, never every tick. A stop is not a failure.
func (e *Engine) archiveFailed(ctx context.Context, shard int, what string, err error) (deleteInstead bool) {
	if ctx.Err() != nil {
		return false
	}
	first, expired, firstExpired := e.archiveFailures.fail(shard, time.Now(), e.archiveFailureGrace)
	switch {
	case first:
		e.logger.ErrorContext(ctx, "Reaper: "+what+" failed; closed trees wait in the live tables, and are deleted"+
			" without archiving if archiving still fails after the grace", "shard", shard, "grace", e.archiveFailureGrace,
			"error", err)
	case firstExpired:
		e.logger.ErrorContext(ctx, "Reaper: archiving has failed for the grace; deleting closed trees without archiving"+
			" until it succeeds", "shard", shard, "grace", e.archiveFailureGrace, "error", err)
	}
	return expired
}

// archiveFailures is, per shard, since when archiving has been failing, cleared by a pass that archives without
// failing. It lives as long as the engine's run.
type archiveFailures struct {
	mu   sync.Mutex
	byID map[int]archiveFailure
}

type archiveFailure struct {
	since    time.Time // the first failure since the last clean pass
	last     time.Time // the latest failure
	deleting bool      // the grace has passed, and the start of the deletions was logged
}

// fail records a failure on shard at now and reports whether it is the first since the last success, whether
// archiving has been failing for at least grace, and whether this is the first failure to find it so.
func (f *archiveFailures) fail(shard int, now time.Time, grace time.Duration) (first, expired, firstExpired bool) {
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
	expired = failing && now.Sub(rec.since) >= grace
	firstExpired = expired && !rec.deleting
	rec.deleting = rec.deleting || expired
	f.byID[shard] = rec
	return !failing, expired, firstExpired
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

// forgetReaping forgets what the reaper recorded about shard: its archive failures and its lag.
func (e *Engine) forgetReaping(shard int) {
	e.archiveFailures.forget(shard)
	e.archiveLag.forget(shard)
}

// reset forgets every shard's reading.
func (l *archiveLag) reset() {
	l.mu.Lock()
	l.byID = nil
	l.mu.Unlock()
}

// reapDue removes the due roots of one kind on one shard, batch by batch. dayOf returns the archive day each batch
// moves into, or nil to delete only, and false to end the pass (an archive failure within its grace). unarchived is
// why a batch dayOf gives no day is deleted, for dwarf_flows_reaped. It reports false when it stopped on an error, or
// on an archive failure within its grace.
//
// One scan feeds several batches: the candidates it returns, own class first, are consumed a batch at a time, and
// only an exhausted list is scanned again. A batch whose move fails ends the pass, and is deleted unarchived instead
// once archiving has been failing for the grace (archiveFailed). A batch that cannot be deleted either ends the pass,
// and the next tick tries again.
func (e *Engine) reapDue(
	ctx context.Context, db *sequel.DB, shard int, kind reapKind, dayOf func() (*reapDay, bool), unarchived string,
) bool {
	residency, residencyArgs, count, ordinal := e.reapResidency(shard, kind)
	fetch := reapFetch(count, e.reapBatch)
	for scans := 0; ; scans++ {
		if ctx.Err() != nil {
			return false
		}
		candidates, oldest, err := e.scanDue(ctx, db, kind, residency, residencyArgs, fetch)
		if err != nil {
			e.logSweepError(ctx, "Reaper: selecting due flows", "shard", shard, "kind", kind.name, "error", err)
			return false
		}
		// Only the pass's first scan sees the oldest due root; a later one sees what the batches left.
		if kind.lagged && scans == 0 {
			e.archiveLag.set(shard, max(oldest-kind.grace, 0))
		}
		if len(candidates) == 0 {
			return true
		}
		exhausted := len(candidates) < fetch
		candidates = ownFirst(candidates, count, ordinal, len(candidates))
		progress := false
		for len(candidates) > 0 {
			if ctx.Err() != nil {
				return false
			}
			day, ok := dayOf()
			if !ok {
				return false
			}
			batch := slices.Sorted(slices.Values(candidates[:min(e.reapBatch, len(candidates))]))
			cut, err := e.rootsWithinRows(ctx, db, batch)
			if err == nil {
				var moved int
				moved, err = e.reapBatchOf(ctx, db, cut, kind, day)
				progress = progress || moved > 0
				if err == nil && day != nil {
					e.metricFlowsArchived(ctx, shard, moved)
				} else if err == nil {
					e.metricFlowsReaped(ctx, shard, unarchived, moved)
				}
			}
			// A batch that cannot be archived waits for the next tick, and is deleted unarchived once archiving has been
			// failing for the grace (archiveFailed).
			if err != nil && day != nil && len(cut) > 0 && ctx.Err() == nil {
				if !e.archiveFailed(ctx, shard, "archiving closed trees", err) {
					return false
				}
				var moved int
				moved, err = e.reapBatchOf(ctx, db, cut, kind, nil)
				progress = progress || moved > 0
				if err == nil {
					e.metricFlowsReaped(ctx, shard, reapReasonArchiveFailed, moved)
				}
			}
			if err != nil {
				e.logSweepError(ctx, "Reaper: removing due trees", "shard", shard, "kind", kind.name, "error", err)
				return false
			}
			candidates = slices.DeleteFunc(candidates, func(id int) bool { return slices.Contains(cut, id) })
		}
		// A scan that came back short saw every due root; one whose batches moved nothing found them all changed
		// since, and would only see the same ones again.
		if exhausted || !progress {
			return true
		}
	}
}

// scanDue selects up to fetch due roots of kind (per terminal status, for the closed kind), and the age of the oldest
// of them for a lagged kind (0 for none).
func (e *Engine) scanDue(
	ctx context.Context, db *sequel.DB, kind reapKind, residency string, residencyArgs []any, fetch int,
) (ids []int, oldest time.Duration, err error) {
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
		var id int
		// float64: DATE_DIFF_MILLIS is fractional on SQLite, where an int64 scan fails.
		var ageMs float64
		if err := rows.Scan(&id, &ageMs); err != nil {
			return nil, 0, errors.Trace(err)
		}
		ids = append(ids, id)
		oldest = max(oldest, time.Duration(ageMs*float64(time.Millisecond)))
	}
	return ids, oldest, errors.Trace(rows.Err())
}

// reapBatchOf removes the trees of the roots it can lock among rootIDs (ascending) in one transaction: moved into
// day's archive tables unless day is nil, deleted otherwise. It returns how many flow rows it removed.
//
// The roots are locked first, in flow_id order and re-checked, so two reapers that meet on a root - a stolen one, or
// a dispatcher handoff - wait on each other rather than deadlock, and the later finds the root gone.
func (e *Engine) reapBatchOf(
	ctx context.Context, db *sequel.DB, rootIDs []int, kind reapKind, day *reapDay,
) (int, error) {
	var moved int
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		moved = 0
		locked, err := lockDueRoots(ctx, tx, rootIDs, kind)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
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

// lockDueRoots locks the roots among rootIDs that are still due, and returns them ascending.
func lockDueRoots(ctx context.Context, tx *sequel.Tx, rootIDs []int, kind reapKind) ([]int, error) {
	return lockRootsIn(ctx, tx, liveTables.flows, "flow_id IN ("+intCSV(rootIDs)+") AND "+kind.due, kind.dueArgs...)
}

// lockRootsIn locks the rows of flows that match where and returns their flow ids, ascending. The rows are about to be
// deleted, so on PostgreSQL and MySQL the lock is FOR UPDATE, the strength a DELETE takes; SQLite and SQL Server take
// it by flipping touch, as lockFlowRows does. It serializes with Fork's lock on the same root (lockRows), which on
// PostgreSQL is FOR NO KEY UPDATE and conflicts with it.
func lockRootsIn(ctx context.Context, tx *sequel.Tx, flows, where string, args ...any) ([]int, error) {
	var stmt string
	switch tx.DriverName() {
	case "pgx", "mysql":
		stmt = "SELECT flow_id FROM " + flows + " WHERE " + where + " ORDER BY flow_id FOR UPDATE"
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

// rootsWithinRows returns the leading roots of roots (ascending) whose trees fit in about reapBatchRows rows, and at
// least the first. A tree's rows are counted as its flows' joined steps, one for a flow with none. It reads outside
// the batch's transaction - a closed tree's size does not move - so the batch locks only the roots it takes, and its
// transaction still opens with a write, which SQLite needs.
func (e *Engine) rootsWithinRows(ctx context.Context, db *sequel.DB, roots []int) ([]int, error) {
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
