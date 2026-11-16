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
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobSweepDeletion is the kind of the engine's sweep deletion jobs, the one behind ShareName(JobFastDeletion, k) added
// under ShareName(JobSweepDeletion, k).
const JobSweepDeletion = "sweep_deletion"

// SweepDeletion is the job that deletes the stamped trees its fast deletion job left behind: one too large for a batch,
// one it failed on or found held, one whose stamp came due behind its position, one of a slow peer's class. Each sweep
// captures the fast job's position as it begins and walks the stamped roots of its share at or behind it in flow_id
// order, taking those overdue by its window past their stamp's due time. A tree a batch holds is deleted with the others of its
// batch; a larger one in chunks, one tree at a time, a chunk a batch, until it is gone or a chunk fails. A failed batch
// is taken again a root at a time; a root that fails alone is held - counted and passed - and the next sweep comes back
// to it.
type SweepDeletion struct {
	fast       *FastDeletion
	overdue    time.Duration
	batchRoots int
	batchRows  int
	batchBytes int

	// j is the janitor the job is added to; the instruments are created on its meter, and record under shardAttr.
	j         *Janitor
	shardAttr attribute.KeyValue
	reaped    metric.Int64Counter
	gauges    metric.Registration

	// w is the sweeps' account; end is fast deletion's boundary when the sweep under way began - its position and
	// residue class - after the flow id of the sweep's own position, and current the large tree being deleted in
	// chunks, at pos, the zero root for none.
	w       sweep
	end     boundary
	after   int
	current dueRoot
	pos     chunkPos
}

// NewSweepDeletion returns a sweep deletion job behind fast, the shard's fast deletion job, whose position bounds each
// sweep. Its settings are the engine's defaults until set; set them before adding it to a janitor.
func NewSweepDeletion(fast *FastDeletion) *SweepDeletion {
	return &SweepDeletion{
		fast:       fast,
		overdue:    DefaultSweepOverdue,
		batchRoots: defaultBatchRoots,
		batchRows:  defaultBatchRows,
		batchBytes: defaultBatchBytes,
		w:          sweep{size: 1},
	}
}

// SetOverdue sets the sweep's window: how long past its stamp's due time a root waits before the sweep takes it (30 s).
func (s *SweepDeletion) SetOverdue(d time.Duration) {
	s.overdue = d
}

// SetBatch sets one batch at most: how many roots (500, at most database.MaxListIDs), and about how many rows (4,000) and payload bytes (16 MiB; 0
// sets no byte limit) one transaction deletes - at least one tree, and one chunk of a larger one.
func (s *SweepDeletion) SetBatch(roots, rows, bytes int) {
	s.batchRoots, s.batchRows, s.batchBytes = min(roots, database.MaxListIDs), rows, bytes
}

// attach creates the job's instruments on j's meter, and registers its gauges.
func (s *SweepDeletion) attach(j *Janitor, name string) {
	s.j = j
	s.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	meter := j.diags.Meter()
	var errs [3]error
	s.reaped, errs[0] = meter.Int64Counter("dwarf_flows_reaped", metric.WithDescription(reapedDescription))
	gauges, err := newSweepGauge(meter)
	errs[1] = err
	kind := metric.WithAttributes(s.shardAttr, attribute.String("kind", kindDeleted), attribute.String("job", name))
	s.gauges, errs[2] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if j.isActive() {
			gauges.observe(o, &s.w, kind)
		}
		return nil
	}, gauges.held)
	if err := errors.Join(errs[:]...); err != nil {
		j.diags.Logger().Error("Creating the sweep deletion job's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach unregisters the job's gauges.
func (s *SweepDeletion) detach() {
	if s.gauges != nil {
		_ = s.gauges.Unregister()
		s.gauges = nil
	}
}

// idleWait is how long the job waits after a round without progress.
func (s *SweepDeletion) idleWait() time.Duration {
	return repairIdle
}

// reset forgets where the job stood and what it observed: a replica that sweeps the shard again saw nothing of what
// happened meanwhile.
func (s *SweepDeletion) reset() {
	s.w.forget()
	s.end, s.after, s.current, s.pos = boundary{}, 0, dueRoot{}, chunkPos{}
}

// round runs the sweep's batches (sweepRound).
func (s *SweepDeletion) round(ctx context.Context, batch batchFunc) bool {
	return sweepRound(ctx, batch, s.batch)
}

// batch takes one batch of the sweep on h, beginning a sweep when none is under way: a chunk of the large tree in
// progress, or the next roots behind the sweep's end.
func (s *SweepDeletion) batch(ctx context.Context, h handle) outcome {
	end, ok := s.fast.position()
	if !ok {
		return idle // fast deletion has not run a round: there is no boundary yet
	}
	if s.w.sweeping && !end.sameClass(s.end) {
		s.w.drop() // fast deletion started over in another residue class, and so does the sweep
		s.current, s.pos = dueRoot{}, chunkPos{}
	}
	if !s.w.sweeping {
		s.w.begin()
		s.end, s.after = end, 0
	}
	seams := s.j.diags.Seams()
	budget := capOf(s.batchRows, s.batchBytes)
	if s.current.flowID != 0 {
		return s.chunk(ctx, h, budget)
	}
	if s.end.after.flowID == 0 {
		s.w.complete() // fast deletion has passed nothing: nothing is behind it
		return idle
	}
	where, args := s.restriction()
	candidates, err := scanStamped(ctx, h, seams, where, args, s.w.limit(s.batchRoots))
	if err != nil {
		s.logError(ctx, "Janitor: selecting stamped trees to sweep", "error", err)
		return idle
	}
	if len(candidates) == 0 {
		s.w.complete()
		return idle
	}
	cut, first, err := cutTrees(ctx, h, seams, "", rootIDs(candidates), budget, budget.rows+1)
	last := len(candidates) - 1 // a cut that could not be read passes every candidate
	if len(cut) > 0 {
		last = slices.IndexFunc(candidates, func(c dueRoot) bool { return c.flowID == cut[len(cut)-1] })
	}
	if err == nil && len(cut) == 1 && budget.over(first) {
		// A tree too large for a batch: deleted in chunks, one tree at a time, from the next batch on.
		s.current, s.pos = candidates[last], chunkPos{}
		return passed
	}
	var removed removal
	if err == nil {
		removed, err = move(ctx, h, seams, cut, stampedDueSQL, nil, nil)
	}
	if err == nil {
		// Counted before the stop check: a batch that committed as the job was stopped still counts.
		s.addReaped(ctx, removed.unarchived)
	}
	if ctx.Err() != nil {
		return idle // stopped: no failure, and the position stays
	}
	if err != nil {
		if len(cut) != 1 && len(candidates) > 1 {
			s.w.size = 1 // taken again a root at a time, from the same position
			return faltered
		}
		s.w.hold(ctx, s.j, candidates[last], err)
		s.after = candidates[last].flowID
		return faltered
	}
	s.after = candidates[last].flowID
	s.w.grow(s.batchRoots)
	if removed.flows == 0 {
		return passed // every root held, or no longer due
	}
	return moved
}

// chunk deletes a chunk of the large tree in progress. The sweep moves past the tree once it is gone, no longer
// stamped and due, held by another transaction, or a chunk failed - held, as a root that fails alone is.
func (s *SweepDeletion) chunk(ctx context.Context, h handle, budget limit) outcome {
	deleted, done, err := deleteChunk(ctx, h, s.j.diags.Seams(), "", s.current.flowID, budget, &s.pos)
	if err == nil {
		s.addReaped(ctx, deleted.unarchived)
		s.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointReapChunkDone)
	}
	if ctx.Err() != nil {
		return idle // stopped: the tree stays current, and its next chunk is taken when the job runs again
	}
	if err != nil {
		s.w.hold(ctx, s.j, s.current, err)
	}
	if err != nil || done {
		s.after, s.current, s.pos = s.current.flowID, dueRoot{}, chunkPos{}
	}
	switch {
	case err != nil:
		return faltered
	case deleted.flows > 0 || !done:
		return moved // a chunk not done deleted rows, if only part of one flow's steps
	}
	return passed // done with nothing deleted: held, gone or no longer due
}

// restriction is a sweep batch's restriction of its scan: the share and residue class of its end, with every root of
// the share overdue by the steal grace past the sweep's window; the stamps overdue by the window; and the flow ids
// after the sweep's position and at or behind its end.
func (s *SweepDeletion) restriction() (where string, args []any) {
	where, args = stampedResidency(s.end.count, s.end.ordinal, s.end.partitioned, s.end.share, s.overdue+stealGrace)
	where += " AND DATE_ADD_MILLIS(updated_at, delete_after_ms)<=DATE_ADD_MILLIS(NOW_UTC(), ?) AND flow_id>? AND flow_id<=?"
	args = append(args, -s.overdue.Milliseconds(), s.after, s.end.after.flowID)
	return where, args
}

// addReaped counts trees deleted without having been archived, under dwarf_flows_reaped{reason="deleted"}.
func (s *SweepDeletion) addReaped(ctx context.Context, unarchived int) {
	if unarchived > 0 {
		s.reaped.Add(ctx, int64(unarchived), metric.WithAttributes(s.shardAttr, attribute.String("reason", ReasonDeleted)))
	}
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (s *SweepDeletion) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	s.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointJanitorFailed)
	s.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", s.j.shard.Index()}, args...)...)
}
