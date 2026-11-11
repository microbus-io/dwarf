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

package reaper

import (
	"context"
	"slices"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
)

// pass is one Pass on one shard: the Reaper, the pass's settings and the shard.
type pass struct {
	r     *Reaper
	cfg   Config
	shard Shard
}

// attemptCtx bounds a database attempt. The parent distinguishes a stop from an attempt's own timeout.
func (p *pass) attemptCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.cfg.Attempt)
}

// run gives each walker a slice on the same goroutine. Healthy backlog uses the remaining pass budget; failures and
// empty scans yield until the next pass. Atomic tree operations recheck eligibility under lock.
func (p *pass) run(ctx context.Context) {
	start := time.Now()
	defer func() { p.r.metrics.reapPass(ctx, p.shard.Index, time.Since(start)) }()
	// Retention cleanup is independent of archive table health and runs once per pass.
	p.retained(ctx)
	for {
		// Closed trees first, then the stamped: a tree published in a round - by a staged copy's last chunk - is a
		// stamped tree, and the deletion walk after it removes its live copy in the same round.
		moreClosed := p.closed(ctx)
		moreDeleted := p.due(ctx, deletionDue(p.cfg.StealGrace), func() (*filingDay, bool) { return nil, true }, ReasonDeleted)
		if ctx.Err() != nil || time.Since(start) >= p.cfg.Budget || (!moreDeleted && !moreClosed) {
			return
		}
	}
}

// closed archives the closed roots into today, or deletes them at a retention of 0.
func (p *pass) closed(ctx context.Context) bool {
	var d *filingDay
	if p.cfg.RetentionDays > 0 {
		if p.shard.Archive == nil {
			return false
		}
		d = &filingDay{}
		if err := p.refreshDay(ctx, d); err != nil {
			p.archiveFailed(ctx, "reading today's archive day", err)
			if d.archive == nil {
				return false
			}
		}
	}
	return p.due(ctx, closedDue(p.cfg.StealGrace, p.cfg.Grace), func() (*filingDay, bool) {
		return d, d == nil || d.startable()
	}, ReasonNoRetention)
}

// archiveFailed records an archive failure. Only the first of an episode is logged; repair logs individual held roots.
func (p *pass) archiveFailed(ctx context.Context, what string, err error) {
	if ctx.Err() != nil {
		return
	}
	if p.r.failures.fail(p.shard.Index, time.Now()) {
		p.r.logger.ErrorContext(ctx, "Reaper: "+what+" failed; retained trees stay in the live tables",
			"shard", p.shard.Index, "error", err)
	}
}

// logError logs a failed statement - unless ctx is done, which means the pass was stopped on purpose.
func (p *pass) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	p.r.logger.ErrorContext(ctx, msg, args...)
}

// due runs a fast slice and a repair slice. Repair covers ALL due roots, independently of the fast cursor. Each slice
// finishes at most one repair sweep, so a root that always fails cannot cause a tight wrap-and-retry loop.
func (p *pass) due(ctx context.Context, k kind, dayOf func() (*filingDay, bool), reason string) bool {
	more := p.walk(ctx, k, dayOf, reason, false)
	repairMore := p.walk(ctx, k, dayOf, reason, true)
	return more || repairMore
}

// end freezes a sweep's inclusive endpoint independently of the fast cursor. A transaction committing later with an
// old timestamp, or a root becoming due behind the position, belongs to the next sweep.
func (p *pass) end(ctx context.Context) (end dueRoot, err error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	db := p.shard.DB
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

// residency is the walk's residency clause on the shard, re-read on every walk.
func (p *pass) residency(k kind) (clause string, args []any) {
	if p.shard.Residency == nil {
		return "", nil
	}
	count, ordinal, ok := p.shard.Residency()
	clause, args, _, _ = residencyClause(count, ordinal, ok, k)
	return clause, args
}

// walk spends a slice walking forward. Fast skips a failed batch. Repair retries a failed batch singly, passes a
// failed single, and grows again on successful removal. No error classification or durable failure markers.
func (p *pass) walk(ctx context.Context, k kind, dayOf func() (*filingDay, bool), reason string, repair bool) bool {
	shard := p.shard.Index
	key := walkKey{shard, k.name, repair}
	w := p.r.walks.get(key)
	defer func() { p.r.walks.put(key, w) }()
	start := time.Now()
	if w.batch < 1 {
		w.batch = 1
	}
	if repair && w.started.IsZero() {
		end, err := p.end(ctx)
		if err != nil {
			p.logError(ctx, "Reaper: reading sweep endpoint", "shard", shard, "error", err)
			return false
		}
		w.end, w.after, w.started, w.failed = end, dueRoot{}, start, 0
	}
	residency, residencyArgs := p.residency(k)
	failures, attempts := 0, 0
	for ctx.Err() == nil {
		if attempts > 0 && time.Since(start) >= p.cfg.Slice {
			return failures == 0
		}
		span, spanArgs := walkRange(p.shard.DB.DriverName(), k, w.after, w.end)
		args := append(slices.Clone(residencyArgs), spanArgs...)
		batch := p.cfg.Batch
		if repair {
			batch = min(w.batch, p.cfg.Batch)
		}
		candidates, oldest, err := p.scanDue(ctx, k, residency+span, args, batch)
		if err != nil {
			p.logError(ctx, "Reaper: selecting due flows", "shard", shard, "kind", k.name, "error", err)
			// An archive failure only where there is an archive: at a retention of 0 the closed kind deletes.
			if k.lagged && !repair && p.cfg.RetentionDays > 0 {
				p.archiveFailed(ctx, "selecting closed trees", err)
			}
			return false
		}
		if attempts == 0 {
			if k.lagged && !repair {
				p.r.lag.set(shard, max(oldest-k.grace, 0))
			}
			if repair && w.after.flowID == 0 {
				w.oldest, w.observed = oldest, time.Now()
			}
		}
		if len(candidates) == 0 {
			if repair {
				if k.lagged && w.failed == 0 {
					p.r.failures.clearUnlessFailedSince(shard, w.started)
				}
				w.lastFailed, w.failed, w.started = w.failed, 0, time.Time{}
				w.lastAttemptAge, w.lastAttemptSeen, w.attemptAge, w.attemptSeen = w.attemptAge, w.attemptSeen, 0, time.Time{}
			}
			return false
		}
		d, startable := dayOf()
		if !startable {
			// The scan above still reports the lag; the slice goes to the trees whose staged archive has started.
			if d != nil {
				p.resumeStarted(ctx, k, &w, d, residency, residencyArgs, start)
			}
			return false
		}
		cut, firstSize, attemptAges, err := p.rootsWithinRows(ctx, rootIDs(candidates), d != nil)
		if err != nil {
			cut = rootIDs(candidates) // a known range, even when its count fails
		}
		if repair && d != nil {
			// The oldest unpublished attempt this sweep has seen, for its gauge.
			w.observeAttempts(attemptAges, time.Now())
		}
		_, attempted := attemptAges[cut[0]]
		attempted = attempted || (d != nil && p.r.prefer.has(stageKey{shard, cut[0]}))
		moved := 0
		var removed removal
		attemptStart := time.Now()
		// finished is false when a tree deleted in chunks is not gone yet and nothing failed: the walk keeps its
		// position on it. A failed chunk is passed like any failed batch.
		finished := true
		large := err == nil && d == nil && len(cut) == 1 && firstSize > max(p.cfg.BatchRows, 1)
		staged := err == nil && d != nil && len(cut) == 1 &&
			(attempted || (p.cfg.StageRows > 0 && firstSize > p.cfg.StageRows))
		switch {
		case err != nil:
		case staged:
			// A tree too large to archive in one transaction is copied over several, and published in the last.
			moved, finished, err = p.stage(ctx, k, cut[0], d, start)
		case large:
			// A tree to delete that is larger than a batch is deleted in chunks rather than in one transaction, which
			// would grow with the tree until it could not finish within its attempt. It counts its own chunks.
			moved, finished, err = p.deleteLarge(ctx, k, cut[0], reason, start)
		default:
			removed, err = p.batchOf(ctx, cut, k, d)
			moved = removed.flows
		}
		if ctx.Err() != nil {
			return false
		}
		attempts++
		if err != nil {
			failures++
			if d != nil {
				p.r.metrics.archiveMoveFailure(ctx, shard)
				if !repair {
					p.archiveFailed(ctx, "archiving closed trees", err)
				}
			}
			if d != nil && !staged && len(cut) == 1 && p.cfg.StageRows > 0 && ctx.Err() == nil &&
				time.Since(attemptStart) >= p.cfg.Attempt {
				// A tree that ran into its deadline in one transaction takes the staged path from its next visit,
				// whatever its size: the claim waits for the next visit, since a timed-out statement can hold the
				// root a while yet (MySQL runs it on after the driver gives up). A lock wait that ran out stages a
				// tree that did not need it, which costs only the transactions.
				p.preferStaging(ctx, cut[0])
			}
			if repair {
				w.batch = 1
				if len(cut) == 1 {
					w.failed++
					p.logHeldRoot(ctx, k, candidates[0], err)
				}
			} else if d == nil {
				// A failed archive move is logged by archiveFailed, once per failure episode, not per attempt.
				p.logError(ctx, "Reaper: removing due trees", "shard", shard, "kind", k.name, "error", err)
			}
		} else {
			failures = 0
			if d != nil {
				p.r.metrics.flowsArchived(ctx, shard, moved)
				if !repair && moved > 0 {
					p.r.failures.clearUnlessFailedSince(shard, attemptStart)
				}
			} else if !large {
				p.r.metrics.flowsReaped(ctx, shard, reason, removed.unarchived)
			}
			if !staged {
				for _, root := range cut {
					p.r.prefer.drop(stageKey{shard, root}) // removed by the batch, or not this walk's
				}
			}
			if repair && moved > 0 {
				w.batch = min(p.cfg.Batch, 2*w.batch)
			}
		}
		if (finished || err != nil) && (!repair || err == nil || len(cut) == 1) {
			w.after = candidates[len(cut)-1]
		}
		// Publish running counts even when an outage makes a sweep take many passes.
		p.r.walks.put(key, w)
		if failures >= 8 {
			return false
		}
	}
	return false
}

func (p *pass) logHeldRoot(ctx context.Context, k kind, root dueRoot, err error) {
	p.logError(ctx, "Reaper: retained tree attempt failed", "shard", p.shard.Index, "kind", k.name,
		"correlation_id", keys.CorrelationID(p.shard.Index, root.createdHour, root.flowID), "error", err)
}

// retained deletes only trees whose hypothetical filing day (close plus grace) is older than retention. Day arithmetic
// avoids multiplying an arbitrary accepted retention by a duration. It needs no archive tables.
func (p *pass) retained(ctx context.Context) {
	if p.cfg.RetentionDays == 0 {
		return
	}
	length := p.cfg.DayLength
	if p.shard.Archive != nil {
		length = p.shard.Archive.DayLength()
	}
	if length <= 0 {
		length = archiver.DefaultDayLength
	}
	a := archiver.New(p.shard.DB, length, migrations.Archive())
	attemptCtx, cancel := p.attemptCtx(ctx)
	today, err := a.Today(attemptCtx)
	cancel()
	if err != nil {
		p.logError(ctx, "Reaper: reading retention day", "shard", p.shard.Index, "error", err)
		return
	}
	if p.cfg.RetentionDays >= today {
		return
	}
	cutoff := a.DayEnds(today - p.cfg.RetentionDays - 1).Add(-p.cfg.Grace)
	k := retentionDue(p.shard.DB.DriverName(), cutoff, p.cfg.StealGrace, p.cfg.Grace)
	p.walk(ctx, k, func() (*filingDay, bool) { return nil, true }, ReasonArchiveFailed, true)
}

// resumeStarted spends a slice, while no new archive may start into the day, on the trees whose staged archive has
// started: each goes on into its own day. It scans the due roots from where its last scan stopped (startedAfter, which
// wraps at the end), reading only which have an attempt, and goes on with the first it finds. The walk's own position
// is left where it is, on the trees it archives once the day opens.
func (p *pass) resumeStarted(ctx context.Context, k kind, w *walk, d *filingDay, residency string, residencyArgs []any, start time.Time) {
	shard := p.shard.Index
	// At least one scan, however the walk's slice went before it, as a walk gives its first batch a try.
	for first := true; ctx.Err() == nil && (first || time.Since(start) < p.cfg.Slice); first = false {
		span, spanArgs := walkRange(p.shard.DB.DriverName(), k, w.startedAfter, dueRoot{})
		candidates, _, err := p.scanDue(ctx, k, residency+span, append(slices.Clone(residencyArgs), spanArgs...), p.cfg.Batch)
		if err != nil {
			p.logError(ctx, "Reaper: selecting due flows", "shard", shard, "kind", k.name, "error", err)
			return
		}
		if len(candidates) == 0 {
			// The scan wrapped: what it saw is a complete reading of the started attempts, as a repair sweep's is,
			// which this walk does not finish while no new archive may start.
			w.startedAfter = dueRoot{}
			w.lastAttemptAge, w.lastAttemptSeen, w.attemptAge, w.attemptSeen = w.attemptAge, w.attemptSeen, 0, time.Time{}
			return
		}
		before := w.startedAfter
		started, err := p.attemptsAmong(ctx, rootIDs(candidates))
		if err != nil {
			p.logError(ctx, "Reaper: reading archive attempts", "shard", shard, "error", err)
			return
		}
		w.observeAttempts(started, time.Now())
		i := slices.IndexFunc(candidates, func(c dueRoot) bool { _, ok := started[c.flowID]; return ok })
		if i < 0 {
			w.startedAfter = candidates[len(candidates)-1]
			continue
		}
		moved, finished, err := p.stage(ctx, k, candidates[i].flowID, d, start)
		// Past the tree once it is done with or failed; on it still while its copy goes on, so the next slice comes
		// back to it rather than wrapping past it first.
		w.startedAfter = candidates[i]
		if err == nil && !finished {
			w.startedAfter = before
			if i > 0 {
				w.startedAfter = candidates[i-1]
			}
		}
		if err != nil {
			p.r.metrics.archiveMoveFailure(ctx, shard)
			p.logError(ctx, "Reaper: staging an archive", "shard", shard, "error", err)
			return
		}
		p.r.metrics.flowsArchived(ctx, shard, moved)
		return
	}
}

// attemptsAmong returns the roots among roots that have an archive attempt, each with the attempt's age on the
// database clock: one primary-key read of the roots, before anything sizes or copies them.
func (p *pass) attemptsAmong(ctx context.Context, roots []int) (map[int]time.Duration, error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	cond, args := AttemptedSQL(p.shard.DB.DriverName())
	rows, err := p.shard.DB.QueryContext(ctx, "SELECT flow_id, DATE_DIFF_MILLIS(NOW_UTC(), archive_attempt_at) FROM dwarf_flows"+
		" WHERE flow_id IN ("+intCSV(roots)+") AND "+cond, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	out := map[int]time.Duration{}
	for rows.Next() {
		var id int
		var ageMs float64 // fractional on SQLite
		if err := rows.Scan(&id, &ageMs); err != nil {
			return nil, errors.Trace(err)
		}
		out[id] = max(time.Duration(ageMs*float64(time.Millisecond)), time.Nanosecond)
	}
	return out, errors.Trace(rows.Err())
}

// preferStaging remembers root as preferring the staged path (preferences), making room first by dropping the
// remembered roots that are no longer live, unstamped roots - which one read of them tells - and no other: a pending
// preference is never evicted for a new one. A preference that finds no room is not kept, and the tree times out once
// more before it is remembered.
func (p *pass) preferStaging(ctx context.Context, root int) {
	key := stageKey{p.shard.Index, root}
	if p.r.prefer.add(key) {
		return
	}
	held := p.r.prefer.roots(p.shard.Index)
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	live := map[int]bool{}
	rows, err := p.shard.DB.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE flow_id IN ("+intCSV(held)+") AND delete_after_ms=0")
	if err != nil {
		return
	}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return // a partial read would make a pending preference look gone
		}
		live[id] = true
	}
	if err := rows.Close(); err != nil || rows.Err() != nil {
		return
	}
	for _, r := range held {
		if !live[r] {
			p.r.prefer.drop(stageKey{p.shard.Index, r})
		}
	}
	p.r.prefer.add(key)
}
