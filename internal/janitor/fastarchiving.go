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
	"strconv"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobFastArchiving is the name the engine adds its fast archiving job under.
const JobFastArchiving = "fast_archiving"

// The reason a closed tree is deleted without archiving, the reason attribute of dwarf_flows_reaped.
const ReasonNoRetention = "no_retention"

// The default settings, the engine's.
const (
	defaultGrace      = time.Minute
	defaultBatchRoots = 512
	defaultBatchRows  = 4096
	defaultBatchBytes = 16 << 20
)

// probeInterval is how often, at most, a busy fast archiving job reads the shard's oldest due closed root, for the
// archive lag and the end of a failure episode. An idle one reads it at every round, which its idle wait paces.
const probeInterval = time.Second

// noSeams is consulted where a fault must not fire: a probe's scan, which an armed scan fault meant for a round's
// would otherwise consume.
var noSeams = seamster.New(false)

// FastArchiving is the job that archives closed trees as they come due: one batch a round, the closed roots after its
// position in close order, the position then past them whatever became of them. At a retention of 0 it deletes them
// instead. It passes the roots that are not its own - one with an archive attempt, one over the staging threshold, and
// at a retention of 0 one too large for a batch, which it stamps for deletion in chunks - and the roots it fails on or
// finds held, leaving them behind its position. It reports dwarf_archive_lag_seconds and
// dwarf_archive_failing_seconds while its janitor is active.
type FastArchiving struct {
	archive       *archiver.Archive
	retentionDays int
	grace         time.Duration
	batchRoots    int
	batchRows     int
	batchBytes    int
	stageRows     int
	stageBytes    int
	noQuietMargin bool

	// j is the janitor the job is added to; the instruments are created on its meter, and record under shardAttr.
	j            *Janitor
	shardAttr    attribute.KeyValue
	archived     metric.Int64Counter
	moveFailures metric.Int64Counter
	reaped       metric.Int64Counter
	gauges       metric.Registration

	// after is the position: the last root a round was done with. count, ordinal and partitioned are the residue
	// class the position was walked in; a round in another starts over from the beginning, since the roots of a
	// replica that left, which were not this one's, are behind it. probedAt is when the last probe ran.
	after       dueRoot
	count       int
	ordinal     int
	partitioned bool
	probedAt    time.Time

	mu sync.Mutex
	// lagSince is when the shard's oldest due closed root at the last probe passed the grace, on this process's clock,
	// zero for none; lagOK is false before the first probe.
	lagSince time.Time
	lagOK    bool
	// failSince is when archiving began failing, zero while it is not; failLast is when it last failed.
	failSince, failLast time.Time
}

// NewFastArchiving returns a fast archiving job into archive - nil when the shard has none, and closed roots then stay
// live at a positive retention - keeping retentionDays of archive: 0 keeps none, and closed trees are deleted. Its
// settings are the engine's defaults until set; set them before adding it to a janitor.
func NewFastArchiving(archive *archiver.Archive, retentionDays int) *FastArchiving {
	return &FastArchiving{
		archive:       archive,
		retentionDays: retentionDays,
		grace:         defaultGrace,
		batchRoots:    defaultBatchRoots,
		batchRows:     defaultBatchRows,
		batchBytes:    defaultBatchBytes,
	}
}

// SetGrace sets how long after its close a closed root is archived (a minute).
func (f *FastArchiving) SetGrace(d time.Duration) {
	f.grace = d
}

// SetBatch sets one batch: how many due roots it considers (512), and about how many rows, flows and steps alike
// (4,096), and payload bytes (16 MiB; 0 sets no byte limit) one transaction moves - at least one tree.
func (f *FastArchiving) SetBatch(roots, rows, bytes int) {
	f.batchRoots, f.batchRows, f.batchBytes = roots, rows, bytes
}

// SetStaging sets the size above which a closed tree is archived in stages rather than in one transaction, and passed
// by fast archiving: in rows (0, none staged), and in payload bytes beside it (0, no byte threshold).
func (f *FastArchiving) SetStaging(rows, bytes int) {
	f.stageRows, f.stageBytes = rows, bytes
}

// SetQuietMargin sets whether archiving stops a margin before its day ends (on): off, a round archives into its day
// however near the day's end. For tests.
func (f *FastArchiving) SetQuietMargin(on bool) {
	f.noQuietMargin = !on
}

// The ways a round takes its cut.
type route int

const (
	routeMove  route = iota // moved, or deleted at a retention of 0, in one transaction
	routePass               // left where it is: every candidate has an archive attempt, large-tree archiving's
	routeStart              // its archive attempt started, for large-tree archiving: a tree over the staging threshold
	routeStamp              // stamped for deletion in chunks: at a retention of 0, a tree too large for a batch
)

// attach creates the job's instruments on j's meter, and registers its gauges.
func (f *FastArchiving) attach(j *Janitor, name string) {
	f.j = j
	f.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	meter := j.diags.Meter()
	errs := make([]error, 6)
	f.archived, errs[0] = meter.Int64Counter("dwarf_flows_archived", metric.WithDescription(archivedDescription))
	f.moveFailures, errs[1] = meter.Int64Counter("dwarf_archive_move_failures", metric.WithDescription(moveFailuresDescription))
	f.reaped, errs[2] = meter.Int64Counter("dwarf_flows_reaped", metric.WithDescription(reapedDescription))
	lag, err := meter.Float64ObservableGauge("dwarf_archive_lag_seconds", metric.WithUnit("s"),
		metric.WithDescription("Per shard: how long archiving is behind - how long past the archive grace the shard's oldest closed root not yet archived has waited, every replica's and one being archived in stages included - read about once a second while this replica dispatches the shard, and aging between readings. Aggregate with max."))
	errs[3] = err
	failing, err := meter.Float64ObservableGauge("dwarf_archive_failing_seconds", metric.WithUnit("s"),
		metric.WithDescription(failingDescription))
	errs[4] = err
	shard := metric.WithAttributes(f.shardAttr)
	jobShard := metric.WithAttributes(f.shardAttr, attribute.String("job", name))
	f.gauges, errs[5] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if !j.isActive() {
			return nil
		}
		if l, ok := f.lagReading(); ok {
			o.ObserveFloat64(lag, l.Seconds(), shard)
		}
		o.ObserveFloat64(failing, f.failingFor(time.Now()).Seconds(), jobShard)
		return nil
	}, lag, failing)
	if err := errors.Join(errs...); err != nil {
		j.diags.Logger().Error("Creating the fast archiving job's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach unregisters the job's gauges.
func (f *FastArchiving) detach() {
	if f.gauges != nil {
		_ = f.gauges.Unregister()
		f.gauges = nil
	}
}

// idleWait is how long the job waits after a round without progress.
func (f *FastArchiving) idleWait() time.Duration {
	return time.Second
}

// round runs one batch.
func (f *FastArchiving) round(ctx context.Context, batch batchFunc) (progress bool) {
	archiving := f.retentionDays > 0
	if archiving && f.archive == nil {
		return false // closed roots stay live: there is nowhere to archive them
	}
	err := batch(ctx, func(ctx context.Context, h handle) error {
		progress = f.batch(ctx, h)
		return nil
	})
	if err != nil {
		// The batch did not run: its connection could not be taken.
		if archiving {
			f.failed(ctx, "taking a connection", err)
		} else {
			f.logError(ctx, "Janitor: taking a connection", "error", err)
		}
	}
	return progress
}

// batch scans, sizes and removes one batch of closed trees on h, and reports whether it made progress: removed a tree,
// or passed roots that are not this job's.
func (f *FastArchiving) batch(ctx context.Context, h handle) bool {
	seams := f.j.diags.Seams()
	archiving := f.retentionDays > 0
	var d *filingDay
	refreshed := true
	if archiving {
		d = &filingDay{}
		if err := refreshDay(ctx, h, f.archive, f.noQuietMargin, d); err != nil {
			f.failed(ctx, "reading today's archive day", err)
			refreshed = false
			if d.archive == nil {
				return false
			}
		}
	}
	// A failure episode ends only in a round that could archive: one whose day was read and is open. A closed day
	// fails every round, so ending its episode in between would report it as starting anew each time.
	canEnd := refreshed && (d == nil || d.startable())
	probed := false
	if time.Since(f.probedAt) >= probeInterval {
		f.probe(ctx, h, canEnd)
		probed = true
	}
	where, whereArgs := f.restriction(h.DriverName())
	candidates, _, err := f.scanClosed(ctx, h, seams, where, whereArgs, max(f.batchRoots, 1))
	if err != nil {
		f.logError(ctx, "Janitor: selecting closed trees", "error", err)
		if archiving {
			f.failed(ctx, "selecting closed trees", err)
		}
		return false
	}
	if len(candidates) == 0 {
		if !probed {
			f.probe(ctx, h, canEnd)
		}
		return false
	}
	if d != nil && !d.startable() {
		return false
	}

	start := time.Now()
	cut, via, err := f.cut(ctx, h, rootIDs(candidates), archiving)
	last := len(candidates) - 1 // a cut that could not be read passes every candidate
	if len(cut) > 0 {
		last = slices.IndexFunc(candidates, func(c dueRoot) bool { return c.flowID == cut[len(cut)-1] })
	}
	var removed removal
	if err == nil {
		switch via {
		case routeMove:
			due, dueArgs := closedDueSQL(f.grace)
			removed, err = move(ctx, h, seams, cut, due, dueArgs, d)
		case routeStart:
			due, dueArgs := closedDueSQL(f.grace)
			_, _, err = claimAttempt(ctx, h, cut[0], due, dueArgs, d, time.Time{})
		case routeStamp:
			_, err = f.stampClosed(ctx, h, cut[0])
		}
	}
	// Counted before the stop check: a batch that committed as the job was stopped still counts.
	switch {
	case err != nil:
	case archiving && removed.flows > 0:
		f.archived.Add(ctx, int64(removed.flows), metric.WithAttributes(f.shardAttr))
	case !archiving && removed.unarchived > 0:
		f.reaped.Add(ctx, int64(removed.unarchived),
			metric.WithAttributes(f.shardAttr, attribute.String("reason", ReasonNoRetention)))
	}
	if ctx.Err() != nil {
		return false // stopped: no failure, and the position stays
	}
	f.after = candidates[last]
	switch {
	case err != nil && archiving:
		f.moveFailures.Add(ctx, 1, metric.WithAttributes(f.shardAttr))
		f.failed(ctx, "archiving closed trees", err)
		return false
	case err != nil:
		f.logError(ctx, "Janitor: removing closed trees", "error", err)
		return false
	case archiving && removed.flows > 0:
		f.clearUnlessFailedSince(start)
	}
	return removed.flows > 0 || via != routeMove
}

// probe reads the shard's oldest due closed root - every replica's, behind the position or ahead of it, with an archive
// attempt or without - for the archive lag. During a failure episode, and when canEnd, it also reads the oldest without
// an attempt, and ends the episode once there is none, or that one came due more than endMargin after the last
// failure: everything due then has left the live tables. A tree being archived in stages is not a failure, so it does not hold an episode open. Ages are compared
// on the database's clock and the failure's on this process's, so neither clock's offset matters.
func (f *FastArchiving) probe(ctx context.Context, h handle, canEnd bool) {
	f.probedAt = time.Now()
	due, oldest, err := f.scanClosed(ctx, h, noSeams, "", nil, 1)
	if err != nil {
		f.logError(ctx, "Janitor: reading the oldest closed root", "error", err)
		return
	}
	f.mu.Lock()
	f.lagSince, f.lagOK = time.Time{}, true
	if len(due) > 0 {
		f.lagSince = f.probedAt.Add(-max(oldest-f.grace, 0))
	}
	failLast := f.failLast
	f.mu.Unlock()
	if failLast.IsZero() || !canEnd || f.retentionDays <= 0 {
		return
	}
	none, noneArg := noAttemptSQL(h.DriverName())
	unattempted, oldestUnattempted, err := f.scanClosed(ctx, h, noSeams, " AND archive_attempt_at<="+none, []any{noneArg}, 1)
	if err != nil {
		f.logError(ctx, "Janitor: reading the oldest closed root", "error", err)
		return
	}
	// A root due at the last failure had waited the grace by then, and has waited since.
	if len(unattempted) == 0 || oldestUnattempted+endMargin < time.Since(failLast)+f.grace {
		f.clearUnlessFailedSince(failLast.Add(time.Nanosecond))
	}
}

// endMargin is how long after the last failure the oldest unattempted root must have come due for a probe to end the
// episode. The two sides are read at different instants - the root's age on the database's clock, in milliseconds, when
// the probe's statement runs; the time since the failure on this process's after it returns - so with no margin a root
// that failed just after it came due can read as having come due after its own failure, and the episode ends while it
// is still live. A root due within the margin is the job's to archive next, and that move ends the episode.
const endMargin = time.Second

// cut returns the roots among candidates, in order, that one batch takes, and what it does with them. A root with an
// archive attempt is left out, and passed. Of the rest it takes a leading run whose trees fit a batch (cutWithin) -
// rows, bytes and flows - and always the first: the first sized up to what decides its path - over a batch, and over
// the staging threshold when staging - and each later one up to what is left of the batch. Sizing is a batching hint;
// the move locks only the roots it takes. A cut empty of roots means every candidate had an attempt.
func (f *FastArchiving) cut(ctx context.Context, h handle, candidates []int, archiving bool) (cut []int, via route, err error) {
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
	batch := capOf(f.batchRows, f.batchBytes)
	cutLimit := batch
	cutLimit.flows = MaxBatchFlows
	firstRows := batch.rows + 1
	stage, staging := capOf(f.stageRows, f.stageBytes), archiving && f.stageRows > 0
	if staging {
		// A batch holds no more than the threshold, so a tree over it is alone in its batch, whatever the batch's own
		// size.
		cutLimit = cutLimit.tighter(stage)
		firstRows = max(firstRows, stage.rows+1)
	}
	cut, first, err := cutWithin(eligible, cutLimit, firstRows, func(group, limits []int) ([]size, error) {
		if err := targetedFault(f.j.diags.Seams(), diagnostics.FaultReapSizeErr, group...); err != nil {
			return nil, err
		}
		return treesSizesUpTo(ctx, h, group, limits)
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

// restriction is a round's restriction of its scan: this replica's residue class of the roots, as the shard reports
// it, and every root overdue by the steal grace; and the range after the position. A residue class other than the one
// the position was walked in starts the walk over from the beginning.
func (f *FastArchiving) restriction(driver string) (where string, args []any) {
	count, ordinal, partitioned := f.j.shard.Residency()
	if count != f.count || ordinal != f.ordinal || partitioned != f.partitioned {
		f.count, f.ordinal, f.partitioned, f.after = count, ordinal, partitioned, dueRoot{}
	}
	where, args = closedResidency(count, ordinal, partitioned, f.grace, stealGrace)
	if f.after.flowID != 0 {
		// After the position in close order, by an explicit timestamp seek rather than an OR across its leading column.
		ts, arg := database.SQLTime(driver, f.after.updatedAt)
		where += " AND updated_at>=" + ts + " AND NOT (updated_at=" + ts + " AND flow_id<=?)"
		args = append(args, arg, arg, f.after.flowID)
	}
	return where, args
}

// scanClosed selects up to limit closed roots due the job's grace after their close, restricted by where (a SQL
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
func (f *FastArchiving) scanClosed(ctx context.Context, q sequel.Executor, seams *seamster.Seamster, where string, whereArgs []any, limit int) (roots []dueRoot, oldest time.Duration, err error) {
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
		args = append(args, -f.grace.Milliseconds())
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

// stampClosed marks a closed root for deletion, due at once, if it is still a closed root due the job's grace after its close:
// what a retention of 0 does with a tree too large to delete in one batch, leaving it to the deletion of stamped trees
// in chunks. The stamp clears any archive attempt, as every stamp but a publication does. The root is locked first as a
// move locks it, so a root another transaction holds - a Fork cloning the tree - is skipped on PostgreSQL and MySQL
// rather than waited for. It reports whether it stamped it.
func (f *FastArchiving) stampClosed(ctx context.Context, h handle, root int) (stamped bool, err error) {
	id := strconv.Itoa(root)
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		stamped = false
		due, dueArgs := closedDueSQL(f.grace)
		locked, err := lockRoots(ctx, tx, true, "flow_id="+id+" AND "+due, dueArgs...)
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

// failed records an archive failure, unless ctx ended: a stop is no failure. Only the first failure of an episode is
// logged.
func (f *FastArchiving) failed(ctx context.Context, what string, err error) {
	if ctx.Err() != nil {
		return
	}
	now := time.Now()
	f.mu.Lock()
	first := f.failSince.IsZero()
	if first {
		f.failSince = now
	}
	f.failLast = now
	f.mu.Unlock()
	if first {
		f.j.diags.Logger().ErrorContext(ctx, "Janitor: "+what+" failed; retained trees stay in the live tables",
			"shard", f.j.shard.Index(), "error", err)
	}
}

// clearUnlessFailedSince ends the failure episode, unless a failure was recorded at or after start: the work that
// began at start - a move that archived something, or a probe that found nothing due at the last failure - failed
// nothing.
func (f *FastArchiving) clearUnlessFailedSince(start time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failSince.IsZero() && f.failLast.Before(start) {
		f.failSince, f.failLast = time.Time{}, time.Time{}
	}
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (f *FastArchiving) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	f.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", f.j.shard.Index()}, args...)...)
}

// reset forgets where the job stood, and what it observed: the janitor resumes as one that knows nothing.
func (f *FastArchiving) reset() {
	f.after, f.count, f.ordinal, f.partitioned, f.probedAt = dueRoot{}, 0, 0, false, time.Time{}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lagSince, f.lagOK = time.Time{}, false
	f.failSince, f.failLast = time.Time{}, time.Time{}
}

// lagReading returns the lag now - how long archiving is behind - and false before the first probe. It goes on aging
// after the probe that found the oldest due root, so the reading says how long that root has waited, not how long it
// had when last looked at; 0 when the last probe found none due.
func (f *FastArchiving) lagReading() (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.lagOK || f.lagSince.IsZero() {
		return 0, f.lagOK
	}
	return max(time.Since(f.lagSince), 0), true
}

// failingFor returns how long archiving has been failing at now, or 0.
func (f *FastArchiving) failingFor(now time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSince.IsZero() {
		return 0
	}
	return max(now.Sub(f.failSince), 0)
}
