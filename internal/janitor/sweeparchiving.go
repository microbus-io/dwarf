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
	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobSweepArchiving is the kind of the engine's sweep archiving jobs, the one behind ShareName(JobFastArchiving, k)
// added under ShareName(JobSweepArchiving, k).
const JobSweepArchiving = "sweep_archiving"

// SweepArchiving is the job that archives the closed roots its fast archiving job left behind: one it failed on, found
// held or could not start an attempt for, one whose close committed late behind its position, one of a slow peer's
// class. Each sweep captures the fast job's position as it begins and walks the closed roots of its share at or behind
// it in close order, taking those overdue by its window past the archive grace - what fast archiving has passed and is
// no longer coming back for. It routes them as fast archiving does: moved, a tree over the staging threshold given its
// attempt for large-tree archiving, one with an attempt passed, and at a retention of 0 deleted or stamped. A failed
// batch is taken again a root at a time; a root that fails alone is held - counted, logged and passed - and the next
// sweep comes back to it.
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
	j         *Janitor
	shardAttr attribute.KeyValue
	archived  metric.Int64Counter
	reaped    metric.Int64Counter
	gauges    metric.Registration

	// w is the sweeps' account; end is fast archiving's boundary when the sweep under way began - its position and
	// residue class - and after the sweep's own position.
	w     sweep
	end   boundary
	after dueRoot

	mu sync.Mutex
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
		w:             sweep{size: 1},
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

// SetBatch sets one batch at most: how many roots (500, at most database.MaxListIDs), and about how many rows (4,000) and payload bytes (16 MiB; 0
// sets no byte limit) one transaction moves - at least one tree.
func (s *SweepArchiving) SetBatch(roots, rows, bytes int) {
	s.batchRoots, s.batchRows, s.batchBytes = min(roots, database.MaxListIDs), rows, bytes
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
	errs := make([]error, 5)
	s.archived, errs[0] = meter.Int64Counter("dwarf_flows_archived", metric.WithDescription(archivedDescription))
	s.reaped, errs[1] = meter.Int64Counter("dwarf_flows_reaped", metric.WithDescription(reapedDescription))
	gauges, err := newSweepGauge(meter)
	errs[2] = err
	failing, err := meter.Float64ObservableGauge("dwarf_archive_failing_seconds", metric.WithUnit("s"),
		metric.WithDescription(failingDescription))
	errs[3] = err
	kind := metric.WithAttributes(s.shardAttr, attribute.String("kind", kindClosed), attribute.String("job", name))
	jobShard := metric.WithAttributes(s.shardAttr, attribute.String("job", name))
	s.gauges, errs[4] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if !j.isActive() {
			return nil
		}
		now := time.Now()
		gauges.observe(o, &s.w, kind)
		o.ObserveFloat64(failing, s.failingFor(now).Seconds(), jobShard)
		return nil
	}, gauges.held, failing)
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
	s.w.forget()
	s.end, s.after = boundary{}, dueRoot{}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failSince, s.sweepFailed = time.Time{}, false
}

// failingFor is how long the job has been failing to archive at now, 0 while it is not.
func (s *SweepArchiving) failingFor(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSince.IsZero() {
		return 0
	}
	return max(now.Sub(s.failSince), 0)
}

// round runs the sweep's batches (sweepRound).
func (s *SweepArchiving) round(ctx context.Context, batch batchFunc) bool {
	if s.retentionDays > 0 && s.archive == nil {
		return false // closed roots stay live: there is nowhere to archive them
	}
	return sweepRound(ctx, batch, s.batch)
}

// batch takes one batch of the sweep on h, beginning a sweep when none is under way.
func (s *SweepArchiving) batch(ctx context.Context, h handle) outcome {
	end, ok := s.fast.position()
	if !ok {
		return idle // fast archiving has not run a round: there is no boundary yet
	}
	if s.w.sweeping && !end.sameClass(s.end) {
		s.w.drop() // fast archiving started over in another residue class, and so does the sweep; it ends no episode
	}
	if !s.w.sweeping {
		s.w.begin()
		s.end, s.after = end, dueRoot{}
	}
	seams := s.j.diags.Seams()
	archiving := s.retentionDays > 0
	var d *filingDay
	refreshed := true
	if archiving {
		d = &filingDay{}
		if err := refreshDay(ctx, s.archive, s.noQuietMargin, d); err != nil {
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
		s.w.complete()
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.sweepFailed && refreshed && (d == nil || d.startable()) {
			s.failSince = time.Time{}
		}
		s.sweepFailed = false
		return idle
	}
	if s.end.after.flowID == 0 {
		return complete()
	}
	where, args := s.restriction(h.DriverName(), archiving)
	candidates, _, err := scanClosed(ctx, h, seams, s.grace+s.overdue, where, args, s.w.limit(s.batchRoots))
	if err != nil {
		s.logError(ctx, "Janitor: selecting closed trees to sweep", "error", err)
		if archiving {
			s.failure(ctx, "selecting closed trees to sweep", err)
		}
		return idle
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
			did, err = stampClosed(ctx, h, cut[0], due, dueArgs)
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
			s.failure(ctx, "sweeping closed trees", err)
		}
		if len(cut) != 1 && len(candidates) > 1 {
			s.w.size = 1 // taken again a root at a time, from the same position
			return faltered
		}
		// A root that fails alone is held: counted, logged and passed.
		s.w.hold(ctx, s.j, candidates[last], err)
		s.after = candidates[last]
		return faltered
	}
	s.after = candidates[last]
	s.w.grow(s.batchRoots)
	if !did {
		return passed
	}
	return moved
}

// restriction is a sweep batch's restriction of its scan: the share and residue class of its end, with every root of
// the share overdue by the steal grace past the sweep's window; the range after the sweep's position; the range at or
// behind its end; and, when archiving, no root with an archive attempt - large-tree archiving's, which every sweep would
// otherwise pass again, a batch at a time, for as long as its copy runs.
func (s *SweepArchiving) restriction(driver string, archiving bool) (where string, args []any) {
	where, args = closedResidency(s.end.count, s.end.ordinal, s.end.partitioned, s.end.share, s.grace+s.overdue, stealGrace)
	if archiving {
		none, noneArg := NoAttemptSQL(driver)
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

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (s *SweepArchiving) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	s.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", s.j.shard.Index()}, args...)...)
}
