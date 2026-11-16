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
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobArchiveDeletionSweep is the name the engine adds its archive deletion sweep under.
const JobArchiveDeletionSweep = "archive_deletion_sweep"

// deletionsPendingDescription is the archive deletion sweep's gauge's description.
const deletionsPendingDescription = "Archived trees marked for deletion that the last archive deletion sweep found and could not delete - held by another transaction, or failing - by shard. Hidden from reads. A Delete or Purge only marks, so marked trees wait for the next sweep; a count that stays above zero across sweeps is the one to look at. A retained day the sweep could not read is logged, and its trees are not counted."

// DeleteMarked is the archive deletion sweep: the job that deletes the archived trees a Delete or a Purge marked for
// deletion - every one of them, the calls only mark - and the archive copy of a published tree a Delete took back. Each
// sweep goes through the retained days, oldest first, finding each day's marked roots by a probe of the day's deletion
// index; the trees a batch holds are deleted together, a larger one in chunks, one tree at a time. A failed batch is
// taken again a root at a time; a root that fails alone is held - counted and passed - and the next sweep comes back
// to it. No residue class: replicas meeting on a root skip it on PostgreSQL and MySQL.
type DeleteMarked struct {
	archive       *archiver.Archive
	retentionDays int
	batchRoots    int
	batchRows     int
	batchBytes    int

	// j is the janitor the job is added to; the gauges are registered on its meter.
	j      *Janitor
	gauges metric.Registration

	// w is the sweeps' account; days are the retained days the sweep under way walks, at the index of the day it is on,
	// after the last root of that day it is past; current the large tree being deleted in chunks on currentDay, at pos,
	// the zero root for none.
	w          sweep
	days       []int
	at         int
	after      int
	current    dueRoot
	currentDay int
	pos        chunkPos

	mu sync.Mutex
	// pendingNow counts the roots left in the sweep under way, pending the last complete sweep's; the gauge reports the
	// larger, so a failure shows as it happens, not only once the sweep completes. observed is whether a sweep has
	// completed.
	pendingNow, pending int
	observed            bool
}

// NewDeleteMarked returns an archive deletion sweep over the shard's archive, which keeps retentionDays of days. Its
// settings are the engine's defaults until set; set them before adding it to a janitor.
func NewDeleteMarked(archive *archiver.Archive, retentionDays int) *DeleteMarked {
	return &DeleteMarked{
		archive:       archive,
		retentionDays: retentionDays,
		batchRoots:    defaultBatchRoots,
		batchRows:     defaultBatchRows,
		batchBytes:    defaultBatchBytes,
		w:             sweep{size: 1},
	}
}

// SetBatch sets one batch at most: how many roots (500, at most database.MaxListIDs), and about how many rows (4,000) and payload bytes (16 MiB; 0
// sets no byte limit) one transaction deletes - at least one tree, and one chunk of a larger one.
func (d *DeleteMarked) SetBatch(roots, rows, bytes int) {
	d.batchRoots, d.batchRows, d.batchBytes = min(roots, database.MaxListIDs), rows, bytes
}

// attach registers the job's gauges on j's meter.
func (d *DeleteMarked) attach(j *Janitor, _ string) {
	d.j = j
	shard := metric.WithAttributes(attribute.String("shard", strconv.Itoa(j.shard.Index())))
	meter := j.diags.Meter()
	var errs [2]error
	pending, err := meter.Int64ObservableGauge("dwarf_archive_deletions_pending", metric.WithUnit("{tree}"),
		metric.WithDescription(deletionsPendingDescription))
	errs[0] = err
	d.gauges, errs[1] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if !j.isActive() {
			return nil
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		// Nothing until a sweep completes - zero would claim every day examined - but a failure as soon as it is met.
		if d.observed || d.pendingNow > 0 {
			o.ObserveInt64(pending, int64(max(d.pending, d.pendingNow)), shard)
		}
		return nil
	}, pending)
	if err := errors.Join(errs[:]...); err != nil {
		j.diags.Logger().Error("Creating the archive deletion sweep's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach unregisters the job's gauges.
func (d *DeleteMarked) detach() {
	if d.gauges != nil {
		_ = d.gauges.Unregister()
		d.gauges = nil
	}
}

// idleWait is how long the job waits after a round without progress.
func (d *DeleteMarked) idleWait() time.Duration {
	return repairIdle
}

// reset forgets where the job stood and what it observed: a replica that sweeps the shard again saw nothing of what
// happened meanwhile.
func (d *DeleteMarked) reset() {
	d.w.forget()
	d.days, d.at, d.after, d.current, d.currentDay, d.pos = nil, 0, 0, dueRoot{}, 0, chunkPos{}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pendingNow, d.pending, d.observed = 0, 0, false
}

// round runs the sweep's batches (sweepRound).
func (d *DeleteMarked) round(ctx context.Context, batch batchFunc) bool {
	if d.archive == nil {
		return false
	}
	return sweepRound(ctx, batch, d.batch)
}

// batch takes one batch of the sweep on h, beginning a sweep when none is under way: a chunk of the large tree in
// progress, or the next marked roots of the day the sweep is on.
func (d *DeleteMarked) batch(ctx context.Context, h handle) outcome {
	seams := d.j.diags.Seams()
	budget := capOf(d.batchRows, d.batchBytes)
	if d.current.flowID != 0 {
		return d.chunk(ctx, h, budget)
	}
	if !d.w.sweeping {
		today, err := d.archive.Today(ctx)
		if err != nil {
			d.logError(ctx, "Janitor: reading today for the marked archive trees", "error", err)
			return idle
		}
		d.w.begin()
		d.days = d.archive.DaysIn(today-max(d.retentionDays, 0), today+1)
		d.at, d.after = 0, 0
		d.mu.Lock()
		d.pendingNow = 0
		d.mu.Unlock()
	}
	// The next day with marked roots after the position; a day probed empty, dropped or unreadable is passed.
	var roots []dueRoot
	for ; d.at < len(d.days); d.at, d.after = d.at+1, 0 {
		var err error
		roots, err = markedIn(ctx, h, seams, d.days[d.at], d.after, d.w.limit(d.batchRoots))
		if ctx.Err() != nil {
			return idle // stopped: the position stays
		}
		if sequel.IsUndefinedTableError(err) {
			continue // dropped since the archive's record: its trees with it
		}
		if err != nil {
			d.logError(ctx, "Janitor: finding marked archive trees", "day", d.days[d.at], "error", err)
			continue
		}
		if len(roots) > 0 {
			break
		}
	}
	if d.at >= len(d.days) {
		d.w.complete()
		d.mu.Lock()
		d.pending, d.pendingNow, d.observed = d.pendingNow, 0, true
		d.mu.Unlock()
		return idle
	}
	day := d.days[d.at]
	sfx := daySuffix(day)
	cut, first, err := cutTrees(ctx, h, seams, sfx, rootIDs(roots), budget, budget.rows+1)
	if err == nil && len(cut) == 1 && budget.over(first) {
		// A tree too large for a batch: deleted in chunks, one tree at a time, from the next batch on.
		d.current, d.currentDay, d.pos = roots[0], day, chunkPos{} // the cut's one root is the first
		return passed
	}
	deleted := 0
	if err == nil {
		deleted, err = deleteMarkedTrees(ctx, h, seams, sfx, cut)
	}
	if ctx.Err() != nil {
		return idle // stopped: the position stays
	}
	if sequel.IsUndefinedTableError(err) {
		d.at, d.after = d.at+1, 0 // dropped since its probe: its trees with it
		return passed
	}
	if err != nil {
		if len(cut) != 1 && len(roots) > 1 {
			d.w.size = 1 // taken again a root at a time, from the same position
			return faltered
		}
		d.w.hold(ctx, d.j, roots[0], err)
		d.count(1)
		d.after = roots[0].flowID
		return faltered
	}
	if deleted < len(cut) {
		d.count(stillMarked(ctx, h, sfx, cut)) // held by another transaction, rather than deleted by another replica
	}
	d.after = cut[len(cut)-1]
	d.w.grow(d.batchRoots)
	if deleted == 0 {
		return passed
	}
	return moved
}

// count adds roots left to the sweep under way's count, which the gauge reports at once.
func (d *DeleteMarked) count(roots int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pendingNow += roots
}

// chunk deletes a chunk of the large tree in progress. The sweep moves past the tree once it is gone, no longer marked,
// held by another transaction, or a chunk failed - held, as a root that fails alone is.
func (d *DeleteMarked) chunk(ctx context.Context, h handle, budget limit) outcome {
	sfx := daySuffix(d.currentDay)
	deleted, done, err := deleteChunk(ctx, h, d.j.diags.Seams(), sfx, d.current.flowID, budget, &d.pos)
	if err == nil {
		d.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointReapChunkDone)
	}
	if ctx.Err() != nil {
		return idle // stopped: the tree stays current, and its next chunk is taken when the job runs again
	}
	gone := sequel.IsUndefinedTableError(err) // the day dropped: its trees with it
	switch {
	case gone:
		err = nil
		done = true
	case err != nil:
		d.w.hold(ctx, d.j, d.current, err)
		d.count(1)
	case done && deleted.flows == 0:
		d.count(stillMarked(ctx, h, sfx, []int{d.current.flowID})) // held by another transaction
	}
	if err != nil || done {
		d.after, d.current, d.currentDay, d.pos = d.current.flowID, dueRoot{}, 0, chunkPos{}
	}
	switch {
	case err != nil:
		return faltered
	case deleted.flows > 0 || !done:
		return moved // a chunk not done deleted rows, if only part of one flow's steps
	}
	return passed
}

// markedIn returns up to limit roots marked for deletion in day after the root after, ascending, each with its
// created_hour for its correlation id: a probe of the day's
// deletion index, which holds an entry only for a marked root on PostgreSQL, SQLite and SQL Server, and is named on
// MySQL and MariaDB, where it is full and the planner may otherwise walk the primary key. A day made before the index
// has none, and a hint naming a missing index is an error there, so the probe without it - a read of the day's flows
// table - follows.
func markedIn(ctx context.Context, q sequel.Executor, seams *seamster.Seamster, day, after, limit int) ([]dueRoot, error) {
	if seams.IsFault(diagnostics.FaultJanitorMarkedProbe) {
		return nil, errors.New("injected fault: " + diagnostics.FaultJanitorMarkedProbe)
	}
	flows := "dwarf_flows" + daySuffix(day)
	where := " WHERE " + markedSQL + " AND flow_id>? ORDER BY flow_id LIMIT_OFFSET(?, 0)"
	if d := q.DriverName(); d == "mysql" || d == "mariadb" {
		// The index there is the full (delete_after_ms): an equality, with the primary key InnoDB appends, seeks in
		// flow_id order, where a range reads and sorts every mark of the day for each batch. Every mark is 1 (a Delete's,
		// a Purge's, a published copy's taken back).
		where = " WHERE delete_after_ms=1 AND surgraph_flow_id=0 AND flow_id>? ORDER BY flow_id LIMIT_OFFSET(?, 0)"
	}
	var rows *sequel.Rows
	var err error
	if d := q.DriverName(); d == "mysql" || d == "mariadb" {
		rows, err = q.QueryContext(ctx, "SELECT flow_id, created_hour FROM "+flows+" FORCE INDEX (idx_"+flows+"_delete_after)"+where, after, limit)
		if err != nil && !sequel.IsUndefinedTableError(err) && ctx.Err() == nil {
			rows, err = q.QueryContext(ctx, "SELECT flow_id, created_hour FROM "+flows+where, after, limit)
		}
	} else {
		rows, err = q.QueryContext(ctx, "SELECT flow_id, created_hour FROM "+flows+where, after, limit)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	var roots []dueRoot
	for rows.Next() {
		var r dueRoot
		if err := rows.Scan(&r.flowID, &r.createdHour); err != nil {
			return nil, errors.Trace(err)
		}
		roots = append(roots, r)
	}
	return roots, errors.Trace(rows.Err())
}

// deleteMarkedTrees deletes, in one transaction on h, the trees in the day tables of sfx whose roots are among roots
// and still marked: the roots locked first - a held one skipped on PostgreSQL and MySQL - then the directory's rows for
// their flows, their steps and their flows. It returns how many roots it deleted.
func deleteMarkedTrees(ctx context.Context, h handle, seams *seamster.Seamster, sfx string, roots []int) (deleted int, err error) {
	roots = slices.Sorted(slices.Values(roots))
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		deleted = 0
		rootsPh, rootsArgs := database.IDList(roots)
		locked, err := lockRoots(ctx, tx, sfx, true, "flow_id IN ("+rootsPh+") AND "+markedSQL, rootsArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		ids, idArgs := database.IDList(locked)
		trees := "SELECT flow_id FROM dwarf_flows" + sfx + " WHERE root_flow_id IN (" + ids + ")"
		// The directory and the steps before the flows: their subqueries read the flow rows.
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+archiver.DirectoryTable+" WHERE flow_id IN ("+trees+")", idArgs...); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_steps"+sfx+" WHERE flow_id IN ("+trees+")", idArgs...); err != nil {
			return errors.Trace(err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_flows"+sfx+" WHERE root_flow_id IN ("+ids+")", idArgs...); err != nil {
			return errors.Trace(err)
		}
		// FaultArchiveDelete, targeted at a root, fails the transaction that would delete it, after its statements ran.
		if err := targetedFault(seams, diagnostics.FaultArchiveDelete, locked...); err != nil {
			return err
		}
		deleted = len(locked)
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err) // rolled back: nothing deleted
	}
	seams.Checkpoint(ctx, diagnostics.CheckpointReapArchiveDeleted)
	return deleted, nil
}

// stillMarked counts how many of roots the day tables of sfx still hold marked: the roots a deletion found held, rather
// than gone. A read that fails counts them all.
func stillMarked(ctx context.Context, q sequel.Executor, sfx string, roots []int) int {
	var n int
	ph, args := database.IDList(roots)
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows"+sfx+" WHERE flow_id IN ("+ph+") AND "+markedSQL, args...).Scan(&n)
	if sequel.IsUndefinedTableError(err) {
		return 0
	}
	if err != nil {
		return len(roots)
	}
	return n
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (d *DeleteMarked) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	d.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointJanitorFailed)
	d.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", d.j.shard.Index()}, args...)...)
}
