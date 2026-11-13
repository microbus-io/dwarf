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
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobSweepArchiving is the name the engine adds its sweep archiving job under.
const JobSweepArchiving = "sweep_archiving"

// DefaultSweepOverdue is how long past its due time a closed root waits before sweep archiving takes it: the steal
// grace, long enough for what failed a root - a Fork holding it, contention, a connection lost - to have cleared.
const DefaultSweepOverdue = 30 * time.Second

// kindClosed is the kind attribute of the sweep gauges for closed trees.
const kindClosed = "closed"

// sweepFailureLimit is how many batches a round may fail before it gives way to the job's idle wait.
const sweepFailureLimit = 8

// heldLogLimit is how many held roots a sweep logs; the gauge counts the rest. A sweep through an archive outage holds
// every root it takes, up to sweepFailureLimit a second.
const heldLogLimit = 8

// The descriptions of the sweep gauges, which the engine also reports, for the kinds of trees the janitor does not
// sweep yet, word for word the same: two instruments of one name and different descriptions are exported as two streams
// of that name.
const (
	HeldRootsDescription  = "Roots a sweep failed on singly: the larger of the current sweep's count and the last complete one's. Per replica, shard and kind; aggregate with max, not sum. Resets at restart or handoff."
	OldestHeldDescription = "Age of the oldest root waiting for the last sweep when it began, increasing until resampled; includes roots not yet attempted. Per shard and kind; aggregate with max."
	SweepAgeDescription   = "Age of the current sweep, or zero between sweeps. Per shard and kind; aggregate with max."
)

// SweepArchiving is the job that archives the closed roots fast archiving left behind: one it failed on, found held or
// could not start an attempt for, one whose close committed late behind its position, one of a slow peer's class. Each
// sweep captures fast archiving's position as it begins and walks the closed roots at or behind it in close order,
// taking those overdue by its window past the archive grace - what fast archiving has passed and is no longer coming
// back for. It routes them as fast archiving does: moved, a tree over the staging threshold given its attempt for
// large-tree archiving, one with an attempt passed, and at a retention of 0 deleted or stamped. A failed batch is
// taken again a root at a time; a root that fails alone is held - counted, logged and passed - and the next sweep
// comes back to it.
type SweepArchiving struct {
	fast          *FastArchiving
	archive       *archiver.Archive
	retentionDays int
	grace         time.Duration
	overdue       time.Duration
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

	// sweeping is whether a sweep is under way: end is fast archiving's boundary when it began - its position and
	// residue class - after the sweep's own position, size how many roots its next batch takes - 1 after a failure,
	// doubling after a success - and front whether its first scan is still to come.
	sweeping bool
	end      boundary
	after    dueRoot
	size     int
	front    bool

	mu sync.Mutex
	// started is when the current sweep began, zero between sweeps; failed counts the roots it held, lastFailed the
	// last complete sweep's. frontSince is when the oldest due root at the last sweep's front closed, zero for none.
	started            time.Time
	failed, lastFailed int
	frontSince         time.Time
	// failSince is when the job began failing to archive, zero while it is not; sweepFailed is whether the current
	// sweep failed: a sweep that completes without failing ends the episode.
	failSince   time.Time
	sweepFailed bool
}

// NewSweepArchiving returns a sweep archiving job behind fast, the shard's fast archiving job, whose position bounds
// each sweep, into archive - nil when the shard has none - keeping retentionDays of archive: 0 keeps none, and closed
// trees are deleted. Its settings are the engine's defaults until set; set them before adding it to a janitor.
func NewSweepArchiving(fast *FastArchiving, archive *archiver.Archive, retentionDays int) *SweepArchiving {
	return &SweepArchiving{
		fast:          fast,
		archive:       archive,
		retentionDays: retentionDays,
		grace:         defaultGrace,
		overdue:       DefaultSweepOverdue,
		batchRoots:    defaultBatchRoots,
		batchRows:     defaultBatchRows,
		batchBytes:    defaultBatchBytes,
		size:          1,
	}
}

// SetGrace sets the archive grace: how long a closed root waits in the live tables before it is due.
func (s *SweepArchiving) SetGrace(d time.Duration) {
	s.grace = d
}

// SetOverdue sets the sweep's window: how long past due a root waits before the sweep takes it (30 s).
func (s *SweepArchiving) SetOverdue(d time.Duration) {
	s.overdue = d
}

// SetBatch sets one batch at most: how many roots (512), and about how many rows (4,096) and payload bytes (16 MiB; 0
// sets no byte limit) one transaction moves - at least one tree.
func (s *SweepArchiving) SetBatch(roots, rows, bytes int) {
	s.batchRoots, s.batchRows, s.batchBytes = roots, rows, bytes
}

// SetStaging sets the staging threshold: a tree over rows rows or bytes payload bytes is given an archive attempt for
// large-tree archiving rather than moved. 0 rows sets no threshold.
func (s *SweepArchiving) SetStaging(rows, bytes int) {
	s.stageRows, s.stageBytes = rows, bytes
}

// SetQuietMargin sets whether archiving into a day stops a margin before it ends (on, the default).
func (s *SweepArchiving) SetQuietMargin(on bool) {
	s.noQuietMargin = !on
}

// attach creates the job's instruments on j's meter, and registers its gauges.
func (s *SweepArchiving) attach(j *Janitor, name string) {
	s.j = j
	s.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	meter := j.diags.Meter()
	errs := make([]error, 8)
	s.archived, errs[0] = meter.Int64Counter("dwarf_flows_archived", metric.WithDescription(archivedDescription))
	s.moveFailures, errs[1] = meter.Int64Counter("dwarf_archive_move_failures", metric.WithDescription(moveFailuresDescription))
	s.reaped, errs[2] = meter.Int64Counter("dwarf_flows_reaped", metric.WithDescription(reapedDescription))
	held, err := meter.Int64ObservableGauge("dwarf_archive_held_roots", metric.WithUnit("{root}"),
		metric.WithDescription(HeldRootsDescription))
	errs[3] = err
	oldest, err := meter.Float64ObservableGauge("dwarf_archive_oldest_held_seconds", metric.WithUnit("s"),
		metric.WithDescription(OldestHeldDescription))
	errs[4] = err
	age, err := meter.Float64ObservableGauge("dwarf_archive_sweep_seconds", metric.WithUnit("s"),
		metric.WithDescription(SweepAgeDescription))
	errs[5] = err
	failing, err := meter.Float64ObservableGauge("dwarf_archive_failing_seconds", metric.WithUnit("s"),
		metric.WithDescription(failingDescription))
	errs[6] = err
	kind := metric.WithAttributes(s.shardAttr, attribute.String("kind", kindClosed))
	jobShard := metric.WithAttributes(s.shardAttr, attribute.String("job", name))
	s.gauges, errs[7] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if !j.isActive() {
			return nil
		}
		r := s.reading(time.Now())
		o.ObserveInt64(held, int64(r.held), kind)
		o.ObserveFloat64(oldest, r.oldestHeld.Seconds(), kind)
		o.ObserveFloat64(age, r.sweep.Seconds(), kind)
		o.ObserveFloat64(failing, r.failing.Seconds(), jobShard)
		return nil
	}, held, oldest, age, failing)
	if err := errors.Join(errs...); err != nil {
		j.diags.Logger().Error("Creating the sweep archiving job's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach unregisters the job's gauges.
func (s *SweepArchiving) detach() {
	if s.gauges != nil {
		_ = s.gauges.Unregister()
		s.gauges = nil
	}
}

// idleWait is how long the job waits after a round without progress.
func (s *SweepArchiving) idleWait() time.Duration {
	return repairIdle
}

// reset forgets where the job stood and what it observed: a replica that sweeps the shard again saw nothing of what
// happened meanwhile.
func (s *SweepArchiving) reset() {
	s.sweeping, s.end, s.after, s.size, s.front = false, boundary{}, dueRoot{}, 1, false
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started, s.failed, s.lastFailed, s.frontSince = time.Time{}, 0, 0, time.Time{}
	s.failSince, s.sweepFailed = time.Time{}, false
}

// sweepReading is what the sweep gauges report at an instant.
type sweepReading struct {
	held                       int
	oldestHeld, sweep, failing time.Duration
}

// reading returns the sweep gauges' values at now.
func (s *SweepArchiving) reading(now time.Time) sweepReading {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := sweepReading{held: max(s.failed, s.lastFailed)}
	if !s.frontSince.IsZero() {
		r.oldestHeld = max(now.Sub(s.frontSince), 0)
	}
	if !s.started.IsZero() {
		r.sweep = max(now.Sub(s.started), 0)
	}
	if !s.failSince.IsZero() {
		r.failing = max(now.Sub(s.failSince), 0)
	}
	return r
}

// outcome is what one batch of a sweep came to.
type outcome int

const (
	idle     outcome = iota // nothing done: nothing to take, the day closed to archiving, or a read failed
	moved                   // progress: trees moved, deleted, stamped or given an attempt
	passed                  // roots passed and nothing done: each held, or with an attempt - the round takes another
	faltered                // a batch failed: the round takes another at once, up to sweepFailureLimit
)

// round runs the sweep's batches until one makes progress, the sweep finds nothing to do, or sweepFailureLimit
// batches have failed. Passing roots is not progress, though the round goes on past them: a root whose copy waits or
// fails keeps its attempt behind fast archiving's position, and every sweep passes it again.
func (s *SweepArchiving) round(ctx context.Context, batch batchFunc) bool {
	archiving := s.retentionDays > 0
	if archiving && s.archive == nil {
		return false // closed roots stay live: there is nowhere to archive them
	}
	for failures := 0; failures < sweepFailureLimit; {
		var o outcome
		err := batch(ctx, func(ctx context.Context, h handle) error {
			o = s.batch(ctx, h)
			return nil
		})
		if err != nil {
			// The batch did not run: its connection could not be taken. An archive failure only where there is an
			// archive: at a retention of 0 the job deletes.
			if archiving {
				s.failure(ctx, "taking a connection", err)
			} else {
				s.logError(ctx, "Janitor: taking a connection", "error", err)
			}
			return false
		}
		if ctx.Err() != nil {
			return false
		}
		switch o {
		case faltered:
			failures++
		case passed:
		default:
			return o == moved
		}
	}
	return false
}

// batch takes one batch of the sweep on h, beginning a sweep when none is under way.
func (s *SweepArchiving) batch(ctx context.Context, h handle) outcome {
	end, ok := s.fast.position()
	if !ok {
		return idle // fast archiving has not run a round: there is no boundary yet
	}
	if s.sweeping && (end.count != s.end.count || end.ordinal != s.end.ordinal || end.partitioned != s.end.partitioned) {
		// Fast archiving started over in another residue class, and so does the sweep. The dropped sweep's held roots
		// still count, and it ends no failure episode: it did not complete.
		s.sweeping = false
		s.mu.Lock()
		s.lastFailed = max(s.lastFailed, s.failed)
		s.mu.Unlock()
	}
	if !s.sweeping {
		s.sweeping, s.end, s.after, s.size, s.front = true, end, dueRoot{}, 1, true
		s.mu.Lock()
		s.started, s.failed = time.Now(), 0
		s.mu.Unlock()
	}
	seams := s.j.diags.Seams()
	archiving := s.retentionDays > 0
	var d *filingDay
	refreshed := true
	if archiving {
		d = &filingDay{}
		if err := refreshDay(ctx, h, s.archive, s.noQuietMargin, d); err != nil {
			s.failure(ctx, "reading today's archive day", err)
			refreshed = false
			if d.archive == nil {
				return idle
			}
		}
	}
	// A sweep completes when nothing is left behind its end: at once when fast archiving has passed nothing. One that
	// failed nothing ends a failure episode - but only where it could archive: a closed day fails every round, and
	// ending its episode in between would report it as starting anew each time.
	complete := func() outcome {
		s.sweeping = false
		s.mu.Lock()
		defer s.mu.Unlock()
		s.lastFailed, s.failed, s.started = s.failed, 0, time.Time{}
		if !s.sweepFailed && refreshed && (d == nil || d.startable()) {
			s.failSince = time.Time{}
		}
		s.sweepFailed = false
		return idle
	}
	if s.end.after.flowID == 0 {
		s.mu.Lock()
		s.frontSince = time.Time{}
		s.mu.Unlock()
		return complete()
	}
	where, args := s.restriction(h.DriverName(), archiving)
	candidates, oldest, err := scanClosed(ctx, h, seams, s.grace+s.overdue, where, args, max(min(s.size, s.batchRoots), 1))
	if err != nil {
		s.logError(ctx, "Janitor: selecting closed trees to sweep", "error", err)
		if archiving {
			s.failure(ctx, "selecting closed trees to sweep", err)
		}
		return idle
	}
	if s.front {
		s.front = false
		s.mu.Lock()
		s.frontSince = time.Time{}
		if len(candidates) > 0 {
			s.frontSince = time.Now().Add(-oldest)
		}
		s.mu.Unlock()
	}
	if len(candidates) == 0 {
		return complete()
	}
	if d != nil && !d.startable() {
		return idle
	}

	cut, via, err := cutClosed(ctx, h, seams, rootIDs(candidates), archiving, s.batchRows, s.batchBytes, s.stageRows, s.stageBytes)
	last := len(candidates) - 1 // a cut that could not be read passes every candidate
	if len(cut) > 0 {
		last = slices.IndexFunc(candidates, func(c dueRoot) bool { return c.flowID == cut[len(cut)-1] })
	}
	var removed removal
	did := false // a tree moved, deleted, stamped or given its attempt
	if err == nil {
		due, dueArgs := closedDueSQL(s.grace)
		switch via {
		case routeMove:
			removed, err = move(ctx, h, seams, cut, due, dueArgs, d)
			did = removed.flows > 0
		case routeStart:
			_, did, err = claimAttempt(ctx, h, cut[0], due, dueArgs, d, time.Time{})
		case routeStamp:
			did, err = stampClosed(ctx, h, cut[0], s.grace)
		}
	}
	// Counted before the stop check: a batch that committed as the job was stopped still counts.
	switch {
	case err != nil:
	case archiving && removed.flows > 0:
		s.archived.Add(ctx, int64(removed.flows), metric.WithAttributes(s.shardAttr))
	case !archiving && removed.unarchived > 0:
		s.reaped.Add(ctx, int64(removed.unarchived),
			metric.WithAttributes(s.shardAttr, attribute.String("reason", ReasonNoRetention)))
	}
	if ctx.Err() != nil {
		return idle // stopped: no failure, and the position stays
	}
	if err != nil {
		if archiving {
			s.moveFailures.Add(ctx, 1, metric.WithAttributes(s.shardAttr))
			s.failure(ctx, "sweeping closed trees", err)
		}
		if len(cut) != 1 && len(candidates) > 1 {
			s.size = 1 // taken again a root at a time, from the same position
			return faltered
		}
		// A root that fails alone is held: counted, logged and passed, with the roots before it that had an attempt.
		s.held(ctx, candidates[last], err)
		s.after = candidates[last]
		return faltered
	}
	s.after = candidates[last]
	s.size = min(2*s.size, max(s.batchRoots, 1)) // nothing failed
	if !did {
		return passed
	}
	return moved
}

// restriction is a sweep batch's restriction of its scan: the residue class of its end, with every root overdue by
// the steal grace past the sweep's window; the range after the sweep's position; the range at or behind its end; and,
// when archiving, no root with an archive attempt - large-tree archiving's, which every sweep would otherwise pass
// again, a batch at a time, for as long as its copy runs.
func (s *SweepArchiving) restriction(driver string, archiving bool) (where string, args []any) {
	where, args = closedResidency(s.end.count, s.end.ordinal, s.end.partitioned, s.grace+s.overdue, stealGrace)
	if archiving {
		none, noneArg := noAttemptSQL(driver)
		where += " AND archive_attempt_at<=" + none
		args = append(args, noneArg)
	}
	if s.after.flowID != 0 {
		ts, arg := database.SQLTime(driver, s.after.updatedAt)
		where += " AND updated_at>=" + ts + " AND NOT (updated_at=" + ts + " AND flow_id<=?)"
		args = append(args, arg, arg, s.after.flowID)
	}
	ts, arg := database.SQLTime(driver, s.end.after.updatedAt)
	where += " AND updated_at<=" + ts + " AND NOT (updated_at=" + ts + " AND flow_id>?)"
	args = append(args, arg, arg, s.end.after.flowID)
	return where, args
}

// failure records an archive failure, unless ctx ended: a stop is no failure. Only the first failure of an episode is
// logged.
func (s *SweepArchiving) failure(ctx context.Context, what string, err error) {
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	first := s.failSince.IsZero()
	if first {
		s.failSince = time.Now()
	}
	s.sweepFailed = true
	s.mu.Unlock()
	if first {
		s.j.diags.Logger().ErrorContext(ctx, "Janitor: "+what+" failed; retained trees stay in the live tables",
			"shard", s.j.shard.Index(), "error", err)
	}
}

// held counts a root the sweep failed on alone, which it passes; the next sweep comes back to it. The first
// heldLogLimit of a sweep are logged, by the token-free correlation id, never by a flow key.
func (s *SweepArchiving) held(ctx context.Context, root dueRoot, err error) {
	s.mu.Lock()
	s.failed++
	n := s.failed
	s.mu.Unlock()
	if n > heldLogLimit {
		return
	}
	s.logError(ctx, "Janitor: a sweep could not remove a tree; the next sweep takes it again",
		"correlation_id", keys.CorrelationID(s.j.shard.Index(), root.createdHour, root.flowID), "error", err)
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (s *SweepArchiving) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	s.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", s.j.shard.Index()}, args...)...)
}
