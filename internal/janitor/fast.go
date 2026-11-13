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
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
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
	// lag is how long past the grace the shard's oldest due closed root had waited at the last probe; lagOK is false
	// before the first.
	lag   time.Duration
	lagOK bool
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
	routePass               // left where it is: a tree over the staging threshold
	routeStamp              // stamped for deletion in chunks: at a retention of 0, a tree too large for a batch
)

// attach creates the job's instruments on j's meter, and registers its gauges.
func (f *FastArchiving) attach(j *Janitor) {
	f.j = j
	f.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	meter := j.diags.Meter()
	errs := make([]error, 6)
	f.archived, errs[0] = meter.Int64Counter("dwarf_flows_archived",
		metric.WithDescription("Counts flows moved from the live tables into the archive, by shard: each root and every subgraph flow of its tree. Its rate plus dwarf_flows_reaped's, against dwarf_flows_closed, is archiving keeping up - a closed root waits the archive grace (a minute) in the live tables, so the two track each other a minute apart."))
	f.moveFailures, errs[1] = meter.Int64Counter("dwarf_archive_move_failures",
		metric.WithDescription("Failed archive attempts, including sizing, contention and deadlines; counts attempts, not distinct trees."))
	f.reaped, errs[2] = meter.Int64Counter("dwarf_flows_reaped", metric.WithDescription(reapedDescription))
	lag, err := meter.Float64ObservableGauge("dwarf_archive_lag_seconds", metric.WithUnit("s"),
		metric.WithDescription("Per shard: how long past the archive grace the shard's oldest closed root not yet archived has waited - every replica's, and one being archived in stages - read about once a second while this replica dispatches the shard. Aggregate with max."))
	errs[3] = err
	failing, err := meter.Float64ObservableGauge("dwarf_archive_failing_seconds", metric.WithUnit("s"),
		metric.WithDescription("Per shard: time since archiving began failing, cleared by a later move that archives something, or once every root due at the last failure has left the live tables. Retained trees remain live. Aggregate with max."))
	errs[4] = err
	shard := metric.WithAttributes(f.shardAttr)
	f.gauges, errs[5] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if !j.isActive() {
			return nil
		}
		if l, ok := f.lagReading(); ok {
			o.ObserveFloat64(lag, l.Seconds(), shard)
		}
		o.ObserveFloat64(failing, f.failingFor(time.Now()).Seconds(), shard)
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
	candidates, _, err := scanClosed(ctx, h, seams, f.grace, where, whereArgs, max(f.batchRoots, 1))
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
		case routeStamp:
			_, err = stampClosed(ctx, h, cut[0], f.grace)
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
// an attempt, and ends the episode once that one came due after the last failure: everything due then has left the
// live tables. A tree being archived in stages is not a failure, so it does not hold an episode open. Ages are compared
// on the database's clock and the failure's on this process's, so neither clock's offset matters.
func (f *FastArchiving) probe(ctx context.Context, h handle, canEnd bool) {
	f.probedAt = time.Now()
	_, oldest, err := scanClosed(ctx, h, noSeams, f.grace, "", nil, 1)
	if err != nil {
		f.logError(ctx, "Janitor: reading the oldest closed root", "error", err)
		return
	}
	f.mu.Lock()
	f.lag, f.lagOK = max(oldest-f.grace, 0), true
	failLast := f.failLast
	f.mu.Unlock()
	if failLast.IsZero() || !canEnd || f.retentionDays <= 0 {
		return
	}
	none, noneArg := noAttemptSQL(h.DriverName())
	_, oldestUnattempted, err := scanClosed(ctx, h, noSeams, f.grace, " AND archive_attempt_at<="+none, []any{noneArg}, 1)
	if err != nil {
		f.logError(ctx, "Janitor: reading the oldest closed root", "error", err)
		return
	}
	// A root due at the last failure had waited the grace by then, and has waited since.
	if oldestUnattempted < time.Since(failLast)+f.grace {
		f.clearUnlessFailedSince(failLast.Add(time.Nanosecond))
	}
}

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
		return treesSizesUpTo(ctx, h, live, group, limits)
	})
	switch {
	case err != nil:
		return nil, routeMove, err
	case len(cut) > 1:
		return cut, routeMove, nil
	case staging && stage.over(first):
		return cut, routePass, nil
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
	after, afterArgs := afterClose(driver, f.after)
	return where + after, append(args, afterArgs...)
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
	f.lag, f.lagOK = 0, false
	f.failSince, f.failLast = time.Time{}, time.Time{}
}

// lagReading returns the lag at the last probe, and false before the first.
func (f *FastArchiving) lagReading() (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lag, f.lagOK
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
