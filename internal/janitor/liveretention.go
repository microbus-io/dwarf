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
	"strconv"
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

// JobLiveRetention is the name the engine adds its live retention job under.
const JobLiveRetention = "live_retention"

// ReasonArchiveFailed is the reason attribute of dwarf_flows_reaped for the flows of a closed tree past its retention
// that archiving never moved: records lost from the archive.
const ReasonArchiveFailed = "archive_failed"

// LiveRetention is the job that gives up on the closed trees archiving never moved once their retention has passed:
// archiving failing for the whole retention keeps them live, and they would otherwise stay forever. It stamps them for
// deletion, due at once, and leaves the deletion to the deletion of stamped trees, as a Delete does. A tree is past
// its retention when its filing day - the day of its close plus the archive grace - ended retentionDays days before
// today, on the database's clock and the archive's day length; the cutoff is rechecked under the root's lock, so an
// archive failure never becomes permission to delete sooner. Each sweep walks the closed roots before the cutoff in
// close order, a batch of roots a transaction. A failed batch is taken again a root at a time; a root that fails alone
// is held - counted and passed - and the next sweep comes back to it. No residue class: every root past its retention
// is overdue by the steal grace, so a class would restrict nothing, and a stamp is one small statement under the root
// lock.
type LiveRetention struct {
	archive       *archiver.Archive
	retentionDays int
	grace         time.Duration
	batchRoots    int

	// j is the janitor the job is added to; the instruments are created on its meter, and record under shardAttr.
	j         *Janitor
	shardAttr attribute.KeyValue
	reaped    metric.Int64Counter
	gauges    metric.Registration

	// w is the sweeps' account, and after the position of the sweep under way.
	w     sweep
	after dueRoot
}

// NewLiveRetention returns a live retention job over the shard's archive - for its days, not its tables - keeping
// retentionDays of archive: at 0 nothing is retained and the job does nothing, closed trees being deleted as they come
// due. Its settings are the engine's defaults until set; set them before adding it to a janitor.
func NewLiveRetention(archive *archiver.Archive, retentionDays int) *LiveRetention {
	return &LiveRetention{
		archive:       archive,
		retentionDays: retentionDays,
		grace:         defaultGrace,
		batchRoots:    defaultBatchRoots,
		w:             sweep{size: 1},
	}
}

// SetGrace sets the archive grace: how long a closed root waits in the live tables before it is due, which with its
// close sets its filing day.
func (l *LiveRetention) SetGrace(d time.Duration) {
	l.grace = d
}

// SetBatch sets how many roots one transaction stamps at most (512).
func (l *LiveRetention) SetBatch(roots int) {
	l.batchRoots = roots
}

// attach creates the job's instruments on j's meter, and registers its gauges.
func (l *LiveRetention) attach(j *Janitor, _ string) {
	l.j = j
	l.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	meter := j.diags.Meter()
	var errs [3]error
	l.reaped, errs[0] = meter.Int64Counter("dwarf_flows_reaped", metric.WithDescription(reapedDescription))
	gauges, err := newSweepGauges(meter)
	errs[1] = err
	kind := metric.WithAttributes(l.shardAttr, attribute.String("kind", kindRetention))
	l.gauges, errs[2] = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		if j.isActive() {
			gauges.observe(o, l.w.reading(time.Now()), kind)
		}
		return nil
	}, gauges.instruments()...)
	if err := errors.Join(errs[:]...); err != nil {
		j.diags.Logger().Error("Creating the live retention job's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach unregisters the job's gauges.
func (l *LiveRetention) detach() {
	if l.gauges != nil {
		_ = l.gauges.Unregister()
		l.gauges = nil
	}
}

// idleWait is how long the job waits after a round without progress.
func (l *LiveRetention) idleWait() time.Duration {
	return repairIdle
}

// reset forgets where the job stood and what it observed: a replica that sweeps the shard again saw nothing of what
// happened meanwhile.
func (l *LiveRetention) reset() {
	l.w.forget()
	l.after = dueRoot{}
}

// round runs the sweep's batches (sweepRound).
func (l *LiveRetention) round(ctx context.Context, batch batchFunc) bool {
	if l.retentionDays <= 0 || l.archive == nil {
		return false
	}
	progress, err := sweepRound(ctx, batch, l.batch)
	if err != nil {
		l.logError(ctx, "Janitor: taking a connection", "error", err) // the batch did not run
	}
	return progress
}

// batch stamps one batch of the roots past their retention on h, beginning a sweep when none is under way.
func (l *LiveRetention) batch(ctx context.Context, h handle) outcome {
	if !l.w.sweeping {
		l.w.begin()
		l.after = dueRoot{}
	}
	today, _, err := l.archive.TodayLeftOn(ctx, h)
	if err != nil {
		l.logError(ctx, "Janitor: reading the retention's day", "error", err)
		return idle
	}
	if l.retentionDays >= today {
		l.w.sawFront(false, 0)
		l.w.complete() // no day is that old
		return idle
	}
	// A root whose filing day ended retentionDays before today: closed before that day's end, less the grace. Day
	// arithmetic, not a duration, so a large retention cannot overflow one.
	cutoff := l.archive.DayEnds(today - l.retentionDays - 1).Add(-l.grace)
	seams := l.j.diags.Seams()
	where, args := l.restriction(h.DriverName(), cutoff)
	candidates, oldest, err := scanClosed(ctx, h, seams, l.grace, where, args, l.w.limit(l.batchRoots))
	if err != nil {
		l.logError(ctx, "Janitor: selecting trees past their retention", "error", err)
		return idle
	}
	l.w.sawFront(len(candidates) > 0, oldest)
	if len(candidates) == 0 {
		l.w.complete()
		return idle
	}
	due, dueArgs := l.dueSQL(h.DriverName(), cutoff)
	stamped, flows, err := stampPastRetention(ctx, h, seams, rootIDs(candidates), due, dueArgs)
	if err == nil && flows > 0 {
		// Counted before the stop check: a batch that committed as the job was stopped still counts.
		l.reaped.Add(ctx, int64(flows), metric.WithAttributes(l.shardAttr, attribute.String("reason", ReasonArchiveFailed)))
	}
	if ctx.Err() != nil {
		return idle // stopped: no failure, and the position stays
	}
	last := len(candidates) - 1
	if err != nil {
		if len(candidates) > 1 {
			l.w.size = 1 // taken again a root at a time, from the same position
			return faltered
		}
		l.w.hold(ctx, l.j, candidates[last], err)
		l.after = candidates[last]
		return faltered
	}
	l.after = candidates[last]
	l.w.grow(l.batchRoots)
	if stamped == 0 {
		return passed // every root held, or no longer past its retention
	}
	return moved
}

// stampPastRetention stamps the roots among roots it can lock that still match due, in one transaction on h: each
// stamped for deletion, due at once, its archive attempt cleared, as every stamp but a publication does, and its
// updated_at set to now, so its wait for deletion - the deleted kind's gauges, sweep deletion's window - is counted
// from when it was given up rather than from its close, days ago. It returns how many roots it stamped and how many
// flows their trees hold - the flows lost from the archive - counted before the transaction, so no root is held
// through a read of whole trees: a closed tree gains no flow, and one that leaves the live tables in between is not
// locked. PostgreSQL and MySQL skip a held root.
func stampPastRetention(ctx context.Context, h handle, seams *seamster.Seamster, roots []int, due string, dueArgs []any) (stamped, flows int, err error) {
	treeFlows, err := flowsOf(ctx, h, roots)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		stamped, flows = 0, 0
		locked, err := lockRoots(ctx, tx, "", true, "flow_id IN ("+database.IntList(roots)+") AND "+due, dueArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		if err := targetedFault(seams, diagnostics.FaultJanitorRetentionStamp, locked...); err != nil {
			return err
		}
		none, noneArg := NoAttemptSQL(tx.DriverName())
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at="+none+
			", updated_at=NOW_UTC() WHERE flow_id IN ("+database.IntList(locked)+")", noneArg); err != nil {
			return errors.Trace(err)
		}
		stamped = len(locked)
		for _, root := range locked {
			flows += treeFlows[root]
		}
		return nil
	})
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	return stamped, flows, nil
}

// flowsOf returns how many live flows the tree of each of roots holds.
func flowsOf(ctx context.Context, q sequel.Executor, roots []int) (map[int]int, error) {
	rows, err := q.QueryContext(ctx, "SELECT root_flow_id, COUNT(*) FROM dwarf_flows WHERE root_flow_id IN ("+
		database.IntList(roots)+") GROUP BY root_flow_id")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	n := map[int]int{}
	for rows.Next() {
		var root, count int
		if err := rows.Scan(&root, &count); err != nil {
			return nil, errors.Trace(err)
		}
		n[root] = count
	}
	return n, errors.Trace(rows.Err())
}

// dueSQL is the condition, on a live flows row, that it is a closed root due for archiving that closed before cutoff:
// past its retention. A batch rechecks it under the root's lock.
func (l *LiveRetention) dueSQL(driver string, cutoff time.Time) (cond string, args []any) {
	cond, args = closedDueSQL(l.grace)
	ts, arg := database.SQLTime(driver, cutoff)
	return cond + " AND updated_at<" + ts, append(args, arg)
}

// restriction is a sweep batch's restriction of its scan: the roots closed before cutoff, and the range after the
// sweep's position.
func (l *LiveRetention) restriction(driver string, cutoff time.Time) (where string, args []any) {
	ts, arg := database.SQLTime(driver, cutoff)
	where, args = " AND updated_at<"+ts, []any{arg}
	if l.after.flowID != 0 {
		ts, arg := database.SQLTime(driver, l.after.updatedAt)
		where += " AND updated_at>=" + ts + " AND NOT (updated_at=" + ts + " AND flow_id<=?)"
		args = append(args, arg, arg, l.after.flowID)
	}
	return where, args
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (l *LiveRetention) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	l.j.diags.Logger().ErrorContext(ctx, msg, append([]any{"shard", l.j.shard.Index()}, args...)...)
}
