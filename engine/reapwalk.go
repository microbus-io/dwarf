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
	"slices"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// reapAttemptCtx bounds a database attempt. The parent distinguishes shutdown from an attempt's own timeout.
func (e *Engine) reapAttemptCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, e.reapAttempt)
}

type reapWalkKey struct {
	shard  int
	kind   string
	repair bool
}
type reapWalk struct {
	after, end         dueRoot
	started            time.Time
	batch              int
	failed, lastFailed int
	oldest             time.Duration
	observed           time.Time
}

// reapWalks holds scheduling hints, never ownership. Production has one walker per shard; the mutex also lets
// metrics read its observations and test-driven passes overlap without data races.
type reapWalks struct {
	mu    sync.Mutex
	byKey map[reapWalkKey]reapWalk
}

func (w *reapWalks) get(k reapWalkKey) reapWalk {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.byKey[k]
}
func (w *reapWalks) put(k reapWalkKey, v reapWalk) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.byKey == nil {
		w.byKey = map[reapWalkKey]reapWalk{}
	}
	w.byKey[k] = v
}
func (w *reapWalks) forget(shard int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for k := range w.byKey {
		if k.shard == shard {
			delete(w.byKey, k)
		}
	}
}
func (w *reapWalks) reset() { w.mu.Lock(); w.byKey = nil; w.mu.Unlock() }

// reapDue runs a fast slice and a repair slice. Repair covers ALL due roots, independently of the fast cursor.
// Each slice finishes at most one repair sweep; a permanently bad root cannot cause a tight wrap/retry loop.
func (e *Engine) reapDue(ctx context.Context, db *sequel.DB, shard int, kind reapKind,
	dayOf func() (*reapDay, bool), reason string,
) bool {
	more := e.reapWalk(ctx, db, shard, kind, dayOf, reason, false)
	repairMore := e.reapWalk(ctx, db, shard, kind, dayOf, reason, true)
	return more || repairMore
}

// reapEnd freezes a sweep's inclusive endpoint independently of the fast cursor. A transaction committing later
// with an old timestamp, or a root becoming due behind the position, belongs to the next sweep.
func (e *Engine) reapEnd(ctx context.Context, db *sequel.DB) (end dueRoot, err error) {
	ctx, cancel := e.reapAttemptCtx(ctx)
	defer cancel()
	const query = "SELECT NOW_UTC(), COALESCE(MAX(flow_id), 0) FROM dwarf_flows"
	if db.DriverName() == "sqlite" {
		var stamp string
		err = db.QueryRowContext(ctx, query).Scan(&stamp, &end.flowID)
		if err == nil {
			end.updatedAt, err = time.Parse("2006-01-02 15:04:05.000", stamp)
		}
	} else {
		err = db.QueryRowContext(ctx, query).Scan(&end.updatedAt, &end.flowID)
	}
	return end, errors.Trace(err)
}

// reapRange keeps the timestamp range seek explicit instead of using an OR across its leading column.
func reapRange(driver string, kind reapKind, after, end dueRoot) (string, []any) {
	var q string
	var args []any
	if after.flowID != 0 {
		if kind.name == "deleted" {
			q += " AND flow_id>?"
			args = append(args, after.flowID)
		} else {
			ts, arg := sqlTime(driver, after.updatedAt)
			q += " AND updated_at>=" + ts + " AND NOT (updated_at=" + ts + " AND flow_id<=?)"
			args = append(args, arg, arg, after.flowID)
		}
	}
	if !end.updatedAt.IsZero() {
		// Bound both axes: even a database clock moving backwards cannot admit an endless stream of new ids.
		q += " AND flow_id<=?"
		args = append(args, end.flowID)
		if kind.name != "deleted" {
			ts, arg := sqlTime(driver, end.updatedAt)
			q += " AND updated_at<=" + ts
			args = append(args, arg)
		}
	}

	return q, args
}

// reapWalk spends a slice walking forward. Fast skips a failed batch. Repair retries a failed batch singly, passes
// a failed single, and grows again on successful removal. No error classification or durable failure markers.
func (e *Engine) reapWalk(ctx context.Context, db *sequel.DB, shard int, kind reapKind,
	dayOf func() (*reapDay, bool), reason string, repair bool,
) bool {
	key := reapWalkKey{shard, kind.name, repair}
	w := e.reapWalks.get(key)
	defer func() { e.reapWalks.put(key, w) }()
	start := time.Now()
	if w.batch < 1 {
		w.batch = 1
	}
	if repair && w.started.IsZero() {
		end, err := e.reapEnd(ctx, db)
		if err != nil {
			e.logSweepError(ctx, "Reaper: reading sweep endpoint", "shard", shard, "error", err)
			return false
		}
		w.end, w.after, w.started, w.failed = end, dueRoot{}, start, 0
	}
	residency, residencyArgs, _, _ := e.reapResidency(shard, kind)
	failures, attempts := 0, 0
	for ctx.Err() == nil {
		if attempts > 0 && time.Since(start) >= e.reapSlice {
			return failures == 0
		}
		span, spanArgs := reapRange(db.DriverName(), kind, w.after, w.end)
		args := append(slices.Clone(residencyArgs), spanArgs...)
		batch := e.reapBatch
		if repair {
			batch = min(w.batch, e.reapBatch)
		}
		candidates, oldest, err := e.scanDue(ctx, db, kind, residency+span, args, batch)
		if err != nil {
			e.logSweepError(ctx, "Reaper: selecting due flows", "shard", shard, "kind", kind.name, "error", err)
			// An archive failure only where there is an archive: at a retention of 0 the closed kind deletes.
			if kind.lagged && !repair && e.retentionDays > 0 {
				e.archiveFailed(ctx, shard, "selecting closed trees", err)
			}
			return false
		}
		if attempts == 0 {
			if kind.lagged && !repair {
				e.archiveLag.set(shard, max(oldest-kind.grace, 0))
			}
			if repair && w.after.flowID == 0 {
				w.oldest, w.observed = oldest, time.Now()
			}
		}
		if len(candidates) == 0 {
			if repair {
				if kind.lagged && w.failed == 0 {
					e.archiveFailures.clearUnlessFailedSince(shard, w.started)
				}
				w.lastFailed, w.failed, w.started = w.failed, 0, time.Time{}
			}
			return false
		}
		day, ok := dayOf()
		if !ok {
			return false
		}
		cut, err := e.rootsWithinRows(ctx, db, rootIDs(candidates))
		if err != nil {
			cut = rootIDs(candidates)
		} // a known range, even when its count fails
		moved := 0
		attemptStart := time.Now()
		if err == nil {
			moved, err = e.reapBatchOf(ctx, db, cut, kind, day)
		}
		if ctx.Err() != nil {
			return false
		}
		attempts++
		if err != nil {
			failures++
			if day != nil {
				e.metricArchiveMoveFailure(ctx, shard)
				if !repair {
					e.archiveFailed(ctx, shard, "archiving closed trees", err)
				}
			}
			if repair {
				w.batch = 1
				if len(cut) == 1 {
					w.failed++
					e.logHeldRoot(ctx, shard, kind, candidates[0], err)
				}
			} else if day == nil {
				// A failed archive move is logged by archiveFailed, once per failure episode, not per attempt.
				e.logSweepError(ctx, "Reaper: removing due trees", "shard", shard, "kind", kind.name, "error", err)
			}
		} else {
			failures = 0
			if day != nil {
				e.metricFlowsArchived(ctx, shard, moved)
				if !repair && moved > 0 {
					e.archiveFailures.clearUnlessFailedSince(shard, attemptStart)
				}
			} else {
				e.metricFlowsReaped(ctx, shard, reason, moved)
			}
			if repair && moved > 0 {
				w.batch = min(e.reapBatch, 2*w.batch)
			}
		}
		if !repair || err == nil || len(cut) == 1 {
			w.after = candidates[len(cut)-1]
		}
		// Publish running counts even when an outage makes a sweep take many passes.
		e.reapWalks.put(key, w)
		if failures >= 8 {
			return false
		}
	}
	return false
}

func (e *Engine) logHeldRoot(ctx context.Context, shard int, kind reapKind, root dueRoot, err error) {
	e.logSweepError(ctx, "Reaper: retained tree attempt failed", "shard", shard, "kind", kind.name,
		"correlation_id", keys.CorrelationID(shard, root.createdHour, root.flowID), "error", err)
}

// reapRetained deletes only trees whose hypothetical filing day (close plus grace) is older than retention.
// Day arithmetic avoids multiplying an arbitrary accepted retention by a duration. It needs no archive tables.
func (e *Engine) reapRetained(ctx context.Context, db *sequel.DB, shard int, set *archiveSet, steal, grace time.Duration) {
	if e.retentionDays == 0 {
		return
	}
	length := archiver.DefaultDayLength
	if set != nil {
		length = set.dayLength
	} else if d, err := e.archiveDayLength(ctx); err == nil {
		length = d
	}
	a := archiver.New(db, length, migrations.Archive())
	attemptCtx, cancel := e.reapAttemptCtx(ctx)
	today, err := a.Today(attemptCtx)
	cancel()
	if err != nil {
		e.logSweepError(ctx, "Reaper: reading retention day", "shard", shard, "error", err)
		return
	}
	if e.retentionDays >= today {
		return
	}
	cutoff := a.DayEnds(today - e.retentionDays - 1).Add(-grace)
	kind := e.retentionDue(db.DriverName(), cutoff, steal, grace)
	e.reapWalk(ctx, db, shard, kind, func() (*reapDay, bool) { return nil, true }, reapReasonArchiveFailed, true)
}

// retentionDue is the closed kind restricted to roots that closed before cutoff, in its scan and in the recheck under
// the root lock alike, so a root that is not past the cutoff is never deleted whatever the scan saw.
func (e *Engine) retentionDue(driver string, cutoff time.Time, steal, grace time.Duration) reapKind {
	ts, arg := sqlTime(driver, cutoff)
	kind := e.closedDueAfter(steal, grace)
	kind.name, kind.lagged = "retention", false
	selectDue := kind.selectDue
	kind.selectDue = func(driverName, where string, args []any, limit int) (string, []any) {
		return selectDue(driverName, where+" AND updated_at<"+ts, append(slices.Clone(args), arg), limit)
	}
	kind.due += " AND updated_at<" + ts
	kind.dueArgs = append(slices.Clone(kind.dueArgs), arg)
	return kind
}
