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

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobFastDeletion is the name the engine adds its fast deletion job under.
const JobFastDeletion = "fast_deletion"

// ReasonDeleted is the reason a stamped tree is deleted, the reason attribute of dwarf_flows_reaped.
const ReasonDeleted = "deleted"

// reapedDescription is dwarf_flows_reaped's description. Both jobs create the counter, and internal/reaper creates it
// too, with this text word for word: two instruments of one name and different descriptions are exported as two
// streams of that name.
const reapedDescription = "Counts flows deleted from the live tables WITHOUT archiving them, by shard and by reason: deleted (a tree Delete or DeleteOnCompletion scheduled for deletion, or a tree a retention of 0 stamped for deletion as too large for one transaction), no_retention (a retention of 0 keeps no archive), and archive_failed (an unarchived tree reached its retention cutoff). archive_failed is records LOST from the archive and must stay zero; dwarf_archive_failing_seconds says since when it has been failing, and the logs say why."

// FastDeletion is the job that deletes the stamped trees - a Delete's, a DeleteOnCompletion's, a published tree's live
// copy - as they come due: one batch a round, the stamped roots after its position in flow_id order, the position then
// past them whatever became of them. It passes a tree too large for a batch, left to the deletion in chunks, and the
// roots it fails on or finds held, leaving them behind its position.
type FastDeletion struct {
	batchRoots int
	batchRows  int
	batchBytes int

	// j is the janitor the job is added to; the counter is created on its meter, and records under shardAttr.
	j         *Janitor
	shardAttr attribute.KeyValue
	reaped    metric.Int64Counter

	// after is the position: the flow id of the last root a round was done with. count, ordinal and partitioned are
	// the residue class the position was walked in; a round in another starts over.
	after       int
	count       int
	ordinal     int
	partitioned bool
}

// NewFastDeletion returns a fast deletion job with the engine's default batch until SetBatch; set it before adding the
// job to a janitor.
func NewFastDeletion() *FastDeletion {
	return &FastDeletion{batchRoots: defaultBatchRoots, batchRows: defaultBatchRows, batchBytes: defaultBatchBytes}
}

// SetBatch sets one batch: how many due roots it considers (512), and about how many rows (4,096) and payload bytes
// (16 MiB; 0 sets no byte limit) one transaction deletes - at least one tree. A tree over a batch is passed, left to the
// deletion in chunks.
func (f *FastDeletion) SetBatch(roots, rows, bytes int) {
	f.batchRoots, f.batchRows, f.batchBytes = roots, rows, bytes
}

// attach creates the job's counter on j's meter.
func (f *FastDeletion) attach(j *Janitor) {
	f.j = j
	f.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	var err error
	f.reaped, err = j.diags.Meter().Int64Counter("dwarf_flows_reaped", metric.WithDescription(reapedDescription))
	if err != nil {
		j.diags.Logger().Error("Creating the fast deletion job's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach has nothing to release.
func (f *FastDeletion) detach() {}

// idleWait is how long the job waits after a round without progress.
func (f *FastDeletion) idleWait() time.Duration {
	return time.Second
}

// stampedDueSQL is the condition, on a live flows row, that it is a root stamped for deletion whose stamp is due. Only
// roots carry a stamp, and the surgraph_flow_id=0 guard makes that explicit. A batch rechecks it under the root's lock.
const stampedDueSQL = "delete_after_ms>0 AND surgraph_flow_id=0 AND DATE_ADD_MILLIS(updated_at, delete_after_ms)<=NOW_UTC()"

// round runs one batch.
func (f *FastDeletion) round(ctx context.Context, batch batchFunc) (progress bool) {
	err := batch(ctx, func(ctx context.Context, h handle) error {
		progress = f.batch(ctx, h)
		return nil
	})
	if err != nil {
		f.logError(ctx, "Janitor: taking a connection", "error", err) // the batch did not run
	}
	return progress
}

// batch scans, sizes and deletes one batch of stamped trees on h, and reports whether it made progress: deleted a
// tree, or passed one too large for a batch.
func (f *FastDeletion) batch(ctx context.Context, h handle) bool {
	seams := f.j.diags.Seams()
	where, whereArgs := f.restriction()
	candidates, err := scanStamped(ctx, h, seams, where, whereArgs, max(f.batchRoots, 1))
	if err != nil {
		f.logError(ctx, "Janitor: selecting stamped trees", "error", err)
		return false
	}
	if len(candidates) == 0 {
		return false
	}
	batchLimit := capOf(f.batchRows, f.batchBytes)
	cut, first, err := cutWithin(candidates, batchLimit, batchLimit.rows+1, func(group, limits []int) ([]size, error) {
		if err := targetedFault(seams, diagnostics.FaultReapSizeErr, group...); err != nil {
			return nil, err
		}
		return treesSizesUpTo(ctx, h, group, limits)
	})
	passed := err == nil && len(cut) == 1 && batchLimit.over(first)
	var removed removal
	if err == nil && !passed {
		removed, err = move(ctx, h, seams, cut, stampedDueSQL, nil, nil)
	}
	if err == nil && removed.unarchived > 0 {
		// Counted before the stop check: a batch that committed as the job was stopped still counts.
		f.reaped.Add(ctx, int64(removed.unarchived),
			metric.WithAttributes(f.shardAttr, attribute.String("reason", ReasonDeleted)))
	}
	if ctx.Err() != nil {
		return false // stopped: no failure, and the position stays
	}
	f.after = candidates[len(candidates)-1] // a cut that could not be read passes every candidate
	if len(cut) > 0 {
		f.after = cut[len(cut)-1]
	}
	if err != nil {
		f.logError(ctx, "Janitor: removing stamped trees", "error", err)
		return false
	}
	return removed.flows > 0 || passed
}

// restriction is a round's restriction of its scan: this replica's residue class of the roots, as the shard reports
// it, and every root overdue by the steal grace; and the range after the position. A residue class other than the one
// the position was walked in starts the walk over from the beginning.
func (f *FastDeletion) restriction() (where string, args []any) {
	count, ordinal, partitioned := f.j.shard.Residency()
	if count != f.count || ordinal != f.ordinal || partitioned != f.partitioned {
		f.count, f.ordinal, f.partitioned, f.after = count, ordinal, partitioned, 0
	}
	where, args = stampedResidency(count, ordinal, partitioned, stealGrace)
	if f.after != 0 {
		where += " AND flow_id>?"
		args = append(args, f.after)
	}
	return where, args
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (f *FastDeletion) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	f.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", f.j.shard.Index()}, args...)...)
}

// reset forgets where the job stood.
func (f *FastDeletion) reset() {
	f.after, f.count, f.ordinal, f.partitioned = 0, 0, 0, false
}

// stampedResidency restricts a stamped scan to this replica's share of the shard's dispatchers, as closedResidency
// does, a root overdue by the steal grace past its stamp's due time taken by any.
func stampedResidency(count, ordinal int, ok bool, steal time.Duration) (clause string, args []any) {
	if !ok || count < 2 || ordinal < 0 || ordinal >= count {
		return "", nil
	}
	return " AND (flow_id % ? = ? OR DATE_ADD_MILLIS(updated_at, delete_after_ms)<=DATE_ADD_MILLIS(NOW_UTC(), ?))",
		[]any{count, ordinal, -steal.Milliseconds()}
}

// scanStamped selects up to limit stamped roots due for deletion, restricted by where, in flow_id order.
//
// ORDER BY flow_id is required: LIMIT_OFFSET compiles to OFFSET/FETCH on SQL Server, which rejects it without one. It
// walks idx_dwarf_flows_delete_after, keyed on flow_id over the stamped roots only, in its own order. Without that index
// every dialect walks the primary key past every unstamped root ahead of the first stamped one - an archive outage's
// whole held backlog, on every round (PostgreSQL: 1.29 s at 1.2M held roots, 0.35 ms with it). MySQL and MariaDB have
// no partial index, so their index is the full (delete_after_ms) one and the scan is told to use it: it then reads
// every stamped root and sorts them, bounded by what is stamped rather than by what is held; left to itself MariaDB
// walked the primary key.
func scanStamped(ctx context.Context, q sequel.Executor, seams *seamster.Seamster, where string, whereArgs []any, limit int) ([]int, error) {
	stmt, args := stampedScanSQL(q.DriverName(), where, whereArgs, limit)
	rows, err := q.QueryContext(ctx, stmt, args...)
	if err == nil && seams.IsFault(diagnostics.FaultReapSelectErr) {
		rows.Close()
		err = errors.New("injected fault: " + diagnostics.FaultReapSelectErr)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	var roots []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Trace(err)
		}
		roots = append(roots, id)
	}
	return roots, errors.Trace(rows.Err())
}

// stampedScanSQL is scanStamped's statement.
func stampedScanSQL(driver, where string, whereArgs []any, limit int) (string, []any) {
	from := "dwarf_flows"
	if driver == "mysql" || driver == "mariadb" {
		from += " FORCE INDEX (idx_dwarf_flows_delete_after)"
	}
	return "SELECT flow_id FROM " + from + " WHERE " + stampedDueSQL + where + " ORDER BY flow_id LIMIT_OFFSET(?, 0)",
		append(slices.Clone(whereArgs), limit)
}
