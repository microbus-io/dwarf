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
	"database/sql"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobLargeArchiving is the kind of the engine's large-tree archiving jobs, each added under
// ShareName(JobLargeArchiving, k).
const JobLargeArchiving = "large_archiving"

// stageGroup is how many of a tree's descendant flows share one archive time in a staged copy. It is a fixed constant,
// never the batch, so a flow's archive time is a function of the tree alone.
const stageGroup = 4096

// maxStages bounds the copies whose position the job remembers. A position forgotten is read back from what the
// attempt's day already holds, so the bound costs a reread, never a row.
const maxStages = 64

// LargeArchiving is the job that archives the trees whose staged archive attempt has started - trees too large to move
// in one transaction, whose attempt fast archiving starts - in stages: each copied into its attempt's day over as many
// transactions as it takes, invisible there, then published in one small one, its live tree stamped for deletion with
// the attempt kept. One tree at a time, by flow_id, a chunk a batch until it publishes, fails, is found held or waits
// for its day's next day; then the next. A tree whose chunk fails keeps its attempt and is taken again when the
// position comes round to it.
type LargeArchiving struct {
	archive       *archiver.Archive
	retentionDays int
	batchRows     int
	batchBytes    int
	noQuietMargin bool
	share         share

	// j is the janitor the job is added to; the instruments are created on its meter, and record under shardAttr.
	j         *Janitor
	shardAttr attribute.KeyValue
	archived  metric.Int64Counter
	staged    metric.Int64Counter
	gauges    metric.Registration

	// current is the tree being copied, 0 between trees; after is the last tree the job finished with - published,
	// failed, held or waiting - which the next is taken after. stages holds each copy's position. probedAt is when the
	// last probe ran.
	current  int
	after    int
	stages   map[int]stagePos
	probedAt time.Time

	mu sync.Mutex
	// oldestSince is when the oldest unpublished attempt at the last probe began, on this process's clock, zero for
	// none; oldestOK is false before the first probe.
	oldestSince time.Time
	oldestOK    bool
	// failSince is when the job began failing, zero while it is not. lapFailed is whether a chunk failed since the
	// position last wrapped: a lap over every started archive of the share that fails none ends the episode.
	failSince time.Time
	lapFailed bool
}

// stagePos is where the staged copy of one tree stands. at is the attempt it belongs to; seen counts the descendants
// copied whole, in flow_id order up to after; of flow, part of whose steps are copied (a descendant, or the root at the
// end), every step up to stepAfter is. A position for another attempt is not used: a fresh one is read back from the
// attempt's day.
type stagePos struct {
	at              time.Time
	seen, after     int
	flow, stepAfter int
}

// NewLargeArchiving returns a large-tree archiving job into archive, keeping retentionDays of archive - at 0 it archives
// nothing, and a tree an attempt was started for is fast archiving's to delete - with the engine's default batch until
// SetBatch; set it before adding the job to a janitor.
func NewLargeArchiving(archive *archiver.Archive, retentionDays int) *LargeArchiving {
	return &LargeArchiving{archive: archive, retentionDays: retentionDays, batchRows: defaultBatchRows, batchBytes: defaultBatchBytes}
}

// SetBatch sets one chunk of a copy: about how many rows (4,000) and payload bytes (16 MiB; 0 sets no byte limit) one
// transaction copies - at least one row.
func (l *LargeArchiving) SetBatch(rows, bytes int) {
	l.batchRows, l.batchBytes = rows, bytes
}

// SetShare sets which of a shard's large-tree archiving jobs this one is, the k-th of of, from 1 (1 of 1, every
// attempt): a shard's jobs of k from 1 to of each copy the started archives of their share of its roots, and none
// copies another's. A share is by flow_id alone, with no residue class: every replica runs every share.
func (l *LargeArchiving) SetShare(k, of int) {
	l.share = share{k, of}
}

// SetQuietMargin sets whether an attempt replacing one whose day was dropped starts only up to a margin before the
// day's end (on, the default), so its stamp groups have room in the day.
func (l *LargeArchiving) SetQuietMargin(on bool) {
	l.noQuietMargin = !on
}

// attach creates the job's instruments on j's meter, and registers its gauge.
func (l *LargeArchiving) attach(j *Janitor, name string) {
	l.j = j
	l.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	if l.stages == nil {
		l.stages = map[int]stagePos{}
	}
	meter := j.diags.Meter()
	errs := make([]error, 5)
	l.archived, errs[0] = meter.Int64Counter("dwarf_flows_archived", metric.WithDescription(archivedDescription))
	l.staged, errs[1] = meter.Int64Counter("dwarf_archive_staged_rows", metric.WithDescription(stagedRowsDescription))
	oldest, err := meter.Float64ObservableGauge("dwarf_archive_oldest_attempt_seconds", metric.WithUnit("s"),
		metric.WithDescription("Per shard and large-tree archiving job: age of the oldest staged archive attempt of the job's share not yet published, read about once a second while this replica dispatches the shard; zero for none. A tree too large to archive in one transaction is copied in stages and then published; a value that keeps growing means one is not publishing, and the logs say why. Read beside dwarf_archive_staged_rows: growing while that moves is a large tree copying, growing while it is flat is a copy that is stuck. A replaced attempt starts its age again. Aggregate with max."))
	errs[2] = err
	failing, err := meter.Float64ObservableGauge("dwarf_archive_failing_seconds", metric.WithUnit("s"),
		metric.WithDescription(failingDescription))
	errs[3] = err
	jobShard := metric.WithAttributes(l.shardAttr, attribute.String("job", name))
	l.gauges, errs[4] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if !j.isActive() {
			return nil
		}
		now := time.Now()
		if age, ok := l.oldestAge(now); ok {
			o.ObserveFloat64(oldest, age.Seconds(), jobShard)
		}
		o.ObserveFloat64(failing, l.failingFor(now).Seconds(), jobShard)
		return nil
	}, oldest, failing)
	if err := errors.Join(errs...); err != nil {
		j.diags.Logger().Error("Creating the large-tree archiving job's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach unregisters the job's gauge.
func (l *LargeArchiving) detach() {
	if l.gauges != nil {
		_ = l.gauges.Unregister()
		l.gauges = nil
	}
}

// idleWait is how long the job waits after a round without progress.
func (l *LargeArchiving) idleWait() time.Duration {
	return repairIdle
}

// reset forgets where the job stood: its position, every copy's position, and its reading.
func (l *LargeArchiving) reset() {
	l.current, l.after, l.probedAt = 0, 0, time.Time{}
	l.stages = map[int]stagePos{}
	l.mu.Lock()
	l.oldestSince, l.oldestOK = time.Time{}, false
	l.failSince, l.lapFailed = time.Time{}, false
	l.mu.Unlock()
}

// failed records a failure of the job, unless ctx ended: a stop is no failure.
func (l *LargeArchiving) failed(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failSince.IsZero() {
		l.failSince = time.Now()
	}
	l.lapFailed = true
}

// lapped records that the position wrapped: a lap over every started archive of the share ended, and one that failed
// none ends the failure episode - every tree the failures were on was taken again, and copied, published, held or gone.
func (l *LargeArchiving) lapped() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.lapFailed {
		l.failSince = time.Time{}
	}
	l.lapFailed = false
}

// failingFor returns how long the job has been failing at now, or 0.
func (l *LargeArchiving) failingFor(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failSince.IsZero() {
		return 0
	}
	return max(now.Sub(l.failSince), 0)
}

// oldestAge is the age at now of the oldest unpublished attempt the last probe found, 0 for none, and false before the
// first probe. It goes on growing after the probe, so a copy whose chunk blocks - and the probes with it - still shows
// its attempt aging.
func (l *LargeArchiving) oldestAge(now time.Time) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.oldestOK || l.oldestSince.IsZero() {
		return 0, l.oldestOK
	}
	return max(now.Sub(l.oldestSince), 0), true
}

// round takes one chunk a batch - of the tree being copied, or else the next with an attempt - until a batch makes
// progress or a full lap makes none: a tree that fails, is held or waits gives way to the next in the same round, so it
// does not put the job to sleep while another has work.
func (l *LargeArchiving) round(ctx context.Context, batch batchFunc) bool {
	seen := map[int]bool{}
	for ctx.Err() == nil {
		var progress bool
		var root int
		if err := batch(ctx, func(ctx context.Context, h handle) error {
			progress, root = l.batch(ctx, h, seen)
			return nil
		}); err != nil {
			return false // the batch did not run
		}
		if progress {
			return true
		}
		if root == 0 || seen[root] {
			return false // nothing has an attempt, or a lap made no progress
		}
		seen[root] = true
	}
	return false
}

// batch takes one chunk of the tree after the position whose attempt has started, on h, and reports whether it made
// progress - copied rows, published, or replaced an attempt whose day was dropped - and the root it took, 0 for none.
// A root in lap, already taken this round, is not taken again: the round's lap is over.
func (l *LargeArchiving) batch(ctx context.Context, h handle, lap map[int]bool) (progress bool, root int) {
	if l.archive == nil || l.retentionDays <= 0 {
		return false, 0 // nothing is archived: a tree with an attempt is fast archiving's to delete
	}
	// A copy goes on into its own day whatever today's state: tomorrow not whole stops only new attempts, which a
	// replacement below checks for itself.
	d := &filingDay{}
	if err := refreshDay(ctx, l.archive, l.noQuietMargin, d); err != nil && d.archive == nil {
		l.logError(ctx, "Janitor: reading today's archive day", "error", err)
		return false, 0
	}
	probed := false
	if time.Since(l.probedAt) >= probeInterval {
		l.probe(ctx, h)
		probed = true
	}
	root, at, ok, wrapped, err := l.next(ctx, h)
	if err != nil {
		l.logError(ctx, "Janitor: selecting started archives", "error", err)
		return false, 0
	}
	if !ok {
		l.lapped() // nothing has an attempt: none is failing
		if !probed {
			l.probe(ctx, h)
		}
		return false, 0
	}
	if lap[root] {
		return false, root // the round's lap is over; the next round's takes the tree, and counts the wrap
	}
	if wrapped {
		l.lapped()
	}
	// The tree is the job's until it publishes, fails, is found held or waits for its day's next day: then the next.
	l.current = root
	finish := func() {
		l.current, l.after = 0, root
	}
	pos, kept := l.stages[root]
	if !kept || !pos.at.Equal(at) {
		pos, kept = stagePos{at: at}, false
	}
	published, copied, done, err := l.stageChunk(ctx, h, root, &pos)
	if err == nil && !done && published == 0 && copied == 0 && kept {
		// Every row the chunk read was there already: another replica copied past this position. Read the copy back
		// from the attempt's day and go on from there, in this batch, rather than re-read a chunk's worth under the
		// root's lock on every turn.
		delete(l.stages, root)
		pos = stagePos{at: at}
		published, copied, done, err = l.stageChunk(ctx, h, root, &pos)
	}
	if copied > 0 {
		l.staged.Add(ctx, int64(copied), metric.WithAttributes(l.shardAttr))
	}
	if sequel.IsUndefinedTableError(err) {
		// The attempt's day was dropped, and its rows with it: a fresh attempt into today, in a transaction of its own,
		// since the statement that found the day missing ended this one.
		delete(l.stages, root)
		due, dueArgs := closedDueSQL(0)
		_, replaced, err := claimAttempt(ctx, h, root, due, dueArgs, d, at)
		if err != nil {
			l.failed(ctx)
			l.logError(ctx, "Janitor: replacing an archive attempt", "root", root, "error", err)
		}
		if !replaced {
			finish()
		}
		return replaced, root
	}
	if err != nil {
		// pos advanced only if the chunk committed - a lost response - so it is kept either way.
		finish()
		l.remember(root, pos)
		l.failed(ctx)
		l.logError(ctx, "Janitor: staging an archive", "root", root, "error", err)
		return false, root
	}
	l.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointReapStageChunkDone)
	switch {
	case published > 0:
		finish()
		l.archived.Add(ctx, int64(published), metric.WithAttributes(l.shardAttr))
		delete(l.stages, root)
		return true, root
	case done:
		// Held, no longer this copy's, or waiting for its day's next day: forgotten only once the tree is not the copy's
		// at all, and kept while another transaction merely holds its root or the copy waits.
		finish()
		if treeGone(ctx, h, root) {
			delete(l.stages, root)
		} else {
			l.remember(root, pos)
		}
		return false, root
	case copied == 0:
		finish()
		return false, root
	}
	l.remember(root, pos)
	return true, root
}

// remember keeps pos as root's copy position, making room by forgetting another when the job holds maxStages.
func (l *LargeArchiving) remember(root int, pos stagePos) {
	if _, ok := l.stages[root]; !ok && len(l.stages) >= maxStages {
		for r := range l.stages {
			delete(l.stages, r)
			break
		}
	}
	l.stages[root] = pos
}

// next returns the tree being copied, while its attempt is still there and unpublished, or else the first live root of
// the job's share after the last one finished whose archive attempt has started and is not published, with the
// attempt, wrapping to the first when none is after it; wrapped is true when it read from the first - a lap ended, or none is left. It walks idx_dwarf_flows_attempt: keyed on flow_id over the
// roots with an attempt alone on PostgreSQL, SQL Server and SQLite, and named with FORCE INDEX on MySQL and MariaDB,
// whose full index on the attempt reads every attempted root and sorts them - bounded by what has an attempt.
func (l *LargeArchiving) next(ctx context.Context, q sequel.Executor) (root int, at time.Time, ok, wrapped bool, err error) {
	driver := q.DriverName()
	table := "dwarf_flows"
	if driver == "mysql" || driver == "mariadb" {
		table += " FORCE INDEX (idx_dwarf_flows_attempt)"
	}
	inShare, shareArgs := l.share.sql(0, 0, false)
	first := func(op string, from int) (int, time.Time, bool, error) {
		var id int
		var t time.Time
		err := q.QueryRowContext(ctx, "SELECT flow_id, archive_attempt_at FROM "+table+" WHERE "+AttemptedSQL(driver)+
			" AND delete_after_ms=0 AND surgraph_flow_id=0 AND flow_id"+op+"?"+inShare+" ORDER BY flow_id LIMIT_OFFSET(1, 0)",
			append([]any{from}, shareArgs...)...).Scan(&id, &t)
		if err == sql.ErrNoRows {
			return 0, time.Time{}, false, nil
		}
		return id, database.StoredTime(t), err == nil, errors.Trace(err)
	}
	// From the position: the current tree itself, or the first after the last one finished. Only reading from the start,
	// when that finds nothing, is a wrap - never inferred from where the position happens to be.
	op, from := ">", l.after
	if l.current != 0 {
		op, from = ">=", l.current
	}
	if root, at, ok, err = first(op, from); err != nil || ok {
		return root, at, ok, false, err
	}
	root, at, ok, err = first(">", 0)
	return root, at, ok, err == nil, err
}

// probe reads the age of the oldest unpublished archive attempt of the job's share, for
// dwarf_archive_oldest_attempt_seconds, through idx_dwarf_flows_attempt (named on MySQL and MariaDB, as next does).
func (l *LargeArchiving) probe(ctx context.Context, q sequel.Executor) {
	l.probedAt = time.Now()
	var ageMs sql.NullFloat64 // fractional on SQLite; null for no attempt
	driver := q.DriverName()
	from := "dwarf_flows"
	if driver == "mysql" || driver == "mariadb" {
		from += " FORCE INDEX (idx_dwarf_flows_attempt)"
	}
	inShare, shareArgs := l.share.sql(0, 0, false)
	err := q.QueryRowContext(ctx, "SELECT DATE_DIFF_MILLIS(NOW_UTC(), MIN(archive_attempt_at)) FROM "+from+
		" WHERE "+AttemptedSQL(driver)+" AND delete_after_ms=0 AND surgraph_flow_id=0"+inShare, shareArgs...).Scan(&ageMs)
	if err != nil {
		l.logError(ctx, "Janitor: reading the oldest archive attempt", "error", err)
		return
	}
	l.mu.Lock()
	l.oldestSince, l.oldestOK = time.Time{}, true
	if ageMs.Valid {
		l.oldestSince = l.probedAt.Add(-max(time.Duration(ageMs.Float64*float64(time.Millisecond)), 0))
	}
	l.mu.Unlock()
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (l *LargeArchiving) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	l.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointJanitorFailed)
	l.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", l.j.shard.Index()}, args...)...)
}

// treeGone reports whether root's tree is no longer a staged copy's to finish: its live root missing or stamped. A read
// that fails is taken for no, so the position is kept.
func treeGone(ctx context.Context, q sequel.Executor, root int) bool {
	var stamp int
	err := q.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp)
	return err == sql.ErrNoRows || (err == nil && stamp > 0)
}

// stageChunk copies, in one transaction under the live root's lock, up to about a batch of root's tree into the day of
// the attempt pos holds, from pos on: descendants in flow_id order, each flow's steps then its flow row and its
// directory row; then the root's steps; then, when nothing is left, it publishes. Every write inserts only what is not
// there, so a chunk run again writes the same rows. It returns the flows archived when it published, the rows it
// inserted, and done when the tree cannot be copied now: stamped, its attempt changed, held, or every row copied while
// the day after the attempt's is not whole - a root published there could not be located. pos advances only when the
// chunk commits.
func (l *LargeArchiving) stageChunk(ctx context.Context, h handle, root int, pos *stagePos) (published, copied int, done bool, err error) {
	a := l.archive
	seams := l.j.diags.Seams()
	driver := h.DriverName()
	day := archiver.DayOf(pos.at, a.DayLength())
	c := &stageCopy{
		driver: driver, at: pos.at, dayEnds: a.DayEnds(day), day: day,
		flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day),
	}
	c.dirBefore, c.dirFrom = a.DirectoryHours(day)
	atExpr, atArg := database.SQLTime(driver, pos.at)
	var at stagePos
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		published, done, at = 0, false, *pos
		c.copied = 0
		if err := boundReads(ctx, tx); err != nil {
			return err
		}
		locked, err := lockRoots(ctx, tx, "", true, "flow_id=? AND status IN ("+database.TerminalStatusesSQL+")"+
			" AND surgraph_flow_id=0 AND delete_after_ms=0 AND archive_attempt_at="+atExpr, root, atArg)
		if err != nil {
			return errors.Trace(err)
		}
		if len(locked) == 0 {
			done = true
			return nil
		}
		if err := targetedFault(seams, diagnostics.FaultReapStageChunk, root); err != nil {
			return err
		}
		// FaultReapArchiveErr fails every archive write, a staging chunk as a move.
		if seams.IsFault(diagnostics.FaultReapArchiveErr) {
			return errors.New("injected fault: " + diagnostics.FaultReapArchiveErr)
		}
		if at == (stagePos{at: pos.at}) {
			// A fresh position - a restart, another replica, a position forgotten - resumes after what the day already
			// holds of this attempt: descendants are copied in flow_id order and each flow row after its steps, so every
			// descendant up to the largest there is whole, and their count numbers the next one's stamp group.
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(MAX(flow_id), 0) FROM "+c.flows+
				" WHERE root_flow_id=? AND flow_id<>?", root, root).Scan(&at.seen, &at.after); err != nil {
				return errors.Trace(err)
			}
			children, _, open, err := descendantsAfter(ctx, tx, root, at.after)
			if err != nil {
				return errors.Trace(err)
			}
			if open {
				return errors.New("tree %d holds a flow that is not closed", root)
			}
			at.flow = root
			if len(children) > 0 {
				at.flow = children[0]
			}
			if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(step_id), 0) FROM "+c.steps+" WHERE flow_id=?", at.flow).Scan(&at.stepAfter); err != nil {
				return errors.Trace(err)
			}
			// No partial child: keep the ordinary batched descendant path.
			if at.flow != root && at.stepAfter == 0 {
				at.flow = 0
			}
		}
		budget := capOf(l.batchRows, l.batchBytes)
		left := budget
		for !left.spent() && at.flow != root {
			if at.flow != 0 {
				// A descendant part copied by an earlier chunk: go on from its position. Its flow row goes with its last
				// steps when it fits beside them, or first in the next chunk.
				fresh := left == budget
				sz, last, more, err := c.stepsFrom(ctx, tx, at.flow, at.stepAfter, left, fresh)
				if err != nil {
					return errors.Trace(err)
				}
				own, err := flowBytes(ctx, tx, "", at.flow)
				if err != nil {
					return errors.Trace(err)
				}
				if more || !rowFits(left, fresh, sz, own) {
					at.stepAfter = last
					return nil
				}
				if err := c.flowRows(ctx, tx, []int{at.flow}, at.seen); err != nil {
					return errors.Trace(err)
				}
				left = left.minus(sz.plus(size{rows: 1, bytes: own, flows: 1}))
				at.seen, at.after, at.flow, at.stepAfter = at.seen+1, at.flow, 0, 0
				continue
			}
			children, childBytes, open, err := descendantsAfter(ctx, tx, root, at.after)
			if err != nil {
				return errors.Trace(err)
			}
			if open {
				return errors.New("tree %d holds a flow that is not closed", root)
			}
			if len(children) == 0 {
				at.flow, at.stepAfter = root, 0
				break
			}
			// The budget is charged what the run reads, whether or not a row is there already.
			run, sz, err := fitRun(ctx, tx, "", children, childBytes, left)
			if err != nil {
				return errors.Trace(err)
			}
			if run == 0 {
				// The next descendant alone is over what is left: part of its steps now, the rest - and its flow row - in
				// the next chunk.
				_, last, _, err := c.stepsFrom(ctx, tx, children[0], 0, left, left == budget)
				if err != nil {
					return errors.Trace(err)
				}
				at.flow, at.stepAfter = children[0], last
				return nil
			}
			list, args := database.IDList(children[:run])
			res, err := tx.ExecContext(ctx, "INSERT INTO "+c.steps+" ("+archivedStepColumns+") SELECT "+archivedStepColumns+
				" FROM dwarf_steps s WHERE s.flow_id IN ("+list+")"+
				" AND NOT EXISTS (SELECT 1 FROM "+c.steps+" a WHERE a.step_id=s.step_id)", args...)
			if err != nil {
				return errors.Trace(err)
			}
			c.inserted(res)
			if err := c.flowRows(ctx, tx, children[:run], at.seen); err != nil {
				return errors.Trace(err)
			}
			left = left.minus(sz)
			at.seen, at.after = at.seen+run, children[run-1]
		}
		if left.spent() {
			return nil
		}
		// The root's steps, then the root itself: the publication, when the root's row fits beside them, or first in the
		// next chunk.
		fresh := left == budget
		sz, last, more, err := c.stepsFrom(ctx, tx, root, at.stepAfter, left, fresh)
		if err != nil {
			return errors.Trace(err)
		}
		if more {
			at.stepAfter = last
			return nil
		}
		if sz.rows > 0 {
			at.stepAfter = last
		}
		rootBytes, err := flowBytes(ctx, tx, "", root)
		if err != nil {
			return errors.Trace(err)
		}
		if !rowFits(left, fresh, sz, rootBytes) {
			return nil
		}
		if !a.Whole(day + 1) {
			// A locate finds a flow only where the day after its own is whole: publish once it is. Unless the attempt's
			// own day is gone, which a statement on it proves (the archive's record may be stale) and which replaces the
			// attempt rather than waiting for a day that is never made again.
			var one int
			err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.flows+" WHERE flow_id=?", root).Scan(&one)
			if err != nil {
				return errors.Trace(err)
			}
			done = true
			return nil
		}
		groups := (at.seen + stageGroup - 1) / stageGroup
		if err := c.flowRows(ctx, tx, []int{root}, groups*stageGroup); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archived_at=(SELECT archived_at FROM "+c.flows+" WHERE flow_id=?) WHERE flow_id=?", root, root); err != nil {
			return errors.Trace(err)
		}
		published = at.seen + 1
		return nil
	})
	if err != nil {
		return 0, 0, false, errors.Trace(err)
	}
	*pos = at
	if seams.Enabled() && seams.IsFault(diagnostics.Join(diagnostics.FaultReapStageLost, strconv.Itoa(root))) {
		return 0, c.copied, false, errors.New("injected fault: " + diagnostics.FaultReapStageLost)
	}
	return published, c.copied, done, nil
}

// stageCopy is what a staging chunk writes into: the attempt's day, its tables, and when it ends.
type stageCopy struct {
	driver             string
	at, dayEnds        time.Time
	day                int
	flows, steps       string
	dirBefore, dirFrom int
	// copied counts the rows the chunk inserted, for dwarf_archive_staged_rows.
	copied int
}

// inserted adds the rows an insert wrote to the chunk's count.
func (c *stageCopy) inserted(res sql.Result) {
	if n, err := res.RowsAffected(); err == nil {
		c.copied += int(n)
	}
}

// stepsFrom copies steps of flow after the step after, in step_id order, up to budget - at least one, whatever its
// bytes, when fresh - and returns their size, the last one's id, and more when the budget, not the flow, ended them.
// The ids are read first and copied by bound lists of at most database.MaxListIDs.
func (c *stageCopy) stepsFrom(ctx context.Context, tx *sequel.Tx, flow, after int, budget limit, fresh bool) (sz size, last int, more bool, err error) {
	ids, sz, more, err := stepsWithin(ctx, tx, "", flow, after, budget, fresh)
	if err != nil || len(ids) == 0 {
		return size{}, after, more, errors.Trace(err)
	}
	for chunk := range slices.Chunk(ids, database.MaxListIDs) {
		ph, args := database.IDList(chunk)
		res, err := tx.ExecContext(ctx, "INSERT INTO "+c.steps+" ("+archivedStepColumns+") SELECT "+archivedStepColumns+
			" FROM dwarf_steps s WHERE s.step_id IN ("+ph+")"+
			" AND NOT EXISTS (SELECT 1 FROM "+c.steps+" a WHERE a.step_id=s.step_id)", args...)
		if err != nil {
			return size{}, after, false, errors.Trace(err)
		}
		c.inserted(res)
	}
	return sz, ids[len(ids)-1], more, nil
}

// flowRows copies the flow rows of flows, the first of which is the tree's flow number first in flow_id order (the root
// being numbered past every descendant group), each stamped with its stamp group's archive time, at + its group in
// milliseconds, held at the day's last millisecond; and writes the directory's rows for those a locate would not find
// by their creation hour.
func (c *stageCopy) flowRows(ctx context.Context, tx *sequel.Tx, flows []int, first int) error {
	last := c.dayEnds.Add(-time.Millisecond)
	for i := 0; i < len(flows); {
		g := (first + i) / stageGroup
		j := min(len(flows), (g+1)*stageGroup-first) // the next group's first flow
		stamp := c.at.Add(time.Duration(g) * time.Millisecond)
		if stamp.After(last) {
			stamp = last
		}
		expr, arg := database.SQLTime(c.driver, stamp)
		list, args := database.IDList(flows[i:j])
		res, err := tx.ExecContext(ctx, "INSERT INTO "+c.flows+" ("+archivedFlowColumns+", archived_at)"+
			" SELECT "+archivedFlowColumns+", "+expr+" FROM dwarf_flows f WHERE f.flow_id IN ("+list+")"+
			" AND NOT EXISTS (SELECT 1 FROM "+c.flows+" a WHERE a.flow_id=f.flow_id)", append([]any{arg}, args...)...)
		if err != nil {
			return errors.Trace(err)
		}
		c.inserted(res)
		// A replaced attempt moves a flow's directory row from the dropped day to this one.
		if _, err := tx.ExecContext(ctx, database.DeleteSQL(tx.DriverName(), archiver.DirectoryTable, database.SeekPrimaryKey("flow_id"),
			"flow_id IN ("+list+") AND day<>"+strconv.Itoa(c.day)), args...); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) SELECT flow_id, "+
			strconv.Itoa(c.day)+" FROM dwarf_flows f WHERE f.flow_id IN ("+list+")"+
			" AND (f.created_hour<"+strconv.Itoa(c.dirBefore)+" OR f.created_hour>="+strconv.Itoa(c.dirFrom)+")"+
			" AND NOT EXISTS (SELECT 1 FROM "+archiver.DirectoryTable+" x WHERE x.flow_id=f.flow_id)", args...); err != nil {
			return errors.Trace(err)
		}
		i = j
	}
	return nil
}
