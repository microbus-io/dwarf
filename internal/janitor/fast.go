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
	"sync"
	"time"
)

// JobFastArchiving is the fast archiving job's name.
const JobFastArchiving = "fast_archiving"

// fastArchiving is the job that archives closed trees as they come due: one batch a round, the closed roots after its
// position in close order, the position then past them whatever became of them. At a retention of 0 it deletes them
// instead. It passes the roots that are not its own - one with an archive attempt, one over the staging threshold, and
// at a retention of 0 one too large for a batch, which it stamps for deletion in chunks - and the roots it fails on or
// finds held, leaving them behind its position.
type fastArchiving struct {
	j *Janitor

	// after is the position: the last root a round was done with. residency is the residue class the position was
	// walked in; a round in another starts over from the beginning, since the roots of a replica that left, which
	// were not this one's, are behind it.
	after     dueRoot
	residency residency

	mu sync.Mutex
	// lag is how long past the grace the oldest due root after the position had waited at the last scan; lagOK is
	// false before the first.
	lag   time.Duration
	lagOK bool
	// failSince is when archiving began failing, zero while it is not; failLast is when it last failed.
	failSince, failLast time.Time
}

// The ways a round takes its cut.
type route int

const (
	routeMove  route = iota // moved, or deleted at a retention of 0, in one transaction
	routePass               // left where it is: a tree over the staging threshold
	routeStamp              // stamped for deletion in chunks: at a retention of 0, a tree too large for a batch
)

// round runs one batch.
func (f *fastArchiving) round(ctx context.Context, batch batchFunc) (progress bool) {
	s, res := f.j.current()
	archiving := f.j.retentionDays > 0
	if archiving && f.j.archive == nil {
		return false // closed roots stay live: there is nowhere to archive them
	}
	err := batch(ctx, func(ctx context.Context, h handle) error {
		progress = f.batch(ctx, h, s, res)
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
func (f *fastArchiving) batch(ctx context.Context, h handle, s settings, res residency) bool {
	shard, seams := f.j.shard, f.j.seams
	archiving := f.j.retentionDays > 0
	var d *filingDay
	if archiving {
		d = &filingDay{}
		if err := refreshDay(ctx, h, f.j.archive, s.noQuietMargin, d); err != nil {
			f.failed(ctx, "reading today's archive day", err)
			if d.archive == nil {
				return false
			}
		}
	}
	where, whereArgs := f.restriction(h.DriverName(), s.grace, res)
	candidates, oldest, err := scanClosed(ctx, h, seams, s.grace, where, whereArgs, max(s.batchRoots, 1))
	if err != nil {
		f.logError(ctx, "Janitor: selecting closed trees", "error", err)
		if archiving {
			f.failed(ctx, "selecting closed trees", err)
		}
		return false
	}
	f.mu.Lock()
	f.lag, f.lagOK = max(oldest-s.grace, 0), true
	f.mu.Unlock()
	if len(candidates) == 0 || (d != nil && !d.startable()) {
		return false
	}

	start := time.Now()
	cut, via, err := f.cut(ctx, h, s, rootIDs(candidates), archiving)
	last := len(candidates) - 1 // a cut that could not be read passes every candidate
	if len(cut) > 0 {
		last = slices.IndexFunc(candidates, func(c dueRoot) bool { return c.flowID == cut[len(cut)-1] })
	}
	var removed removal
	if err == nil {
		switch via {
		case routeMove:
			due, dueArgs := closedDueSQL(s.grace)
			removed, err = move(ctx, h, seams, cut, due, dueArgs, d)
		case routeStamp:
			_, err = stampClosed(ctx, h, cut[0], s.grace)
		}
	}
	// Counted before the stop check: a batch that committed as the job was stopped still counts.
	switch {
	case err == nil && archiving:
		f.j.metrics.flowsArchived(ctx, shard, removed.flows)
	case err == nil:
		f.j.metrics.flowsReaped(ctx, shard, ReasonNoRetention, removed.unarchived)
	}
	if ctx.Err() != nil {
		return false // stopped: no failure, and the position stays
	}
	f.after = candidates[last]
	switch {
	case err != nil && archiving:
		f.j.metrics.archiveMoveFailure(ctx, shard)
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

// cut returns the roots among candidates, in order, that one batch takes, and what it does with them. A root with an
// archive attempt is left out, and passed. Of the rest it takes a leading run whose trees fit a batch (cutWithin) -
// rows, bytes and flows - and always the first: the first sized up to what decides its path - over a batch, and over
// the staging threshold when staging - and each later one up to what is left of the batch. Sizing is a batching hint;
// the move locks only the roots it takes. A cut empty of roots means every candidate had an attempt.
func (f *fastArchiving) cut(ctx context.Context, h handle, s settings, candidates []int, archiving bool) (cut []int, via route, err error) {
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
	batch := capOf(s.batchRows, s.batchBytes)
	cutLimit := batch
	cutLimit.flows = MaxBatchFlows
	firstRows := batch.rows + 1
	stage, staging := capOf(s.stageRows, s.stageBytes), archiving && s.stageRows > 0
	if staging {
		// A batch holds no more than the threshold, so a tree over it is alone in its batch, whatever the batch's own
		// size.
		cutLimit = cutLimit.tighter(stage)
		firstRows = max(firstRows, stage.rows+1)
	}
	cut, first, err := cutWithin(eligible, cutLimit, firstRows, func(group, limits []int) ([]size, error) {
		if err := targetedFault(f.j.seams, FaultSizeErr, group...); err != nil {
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

// restriction is a round's restriction of its scan: this replica's residue class of the roots, and every root
// overdue by the steal grace; and the range after the position. A residue class other than the last round's starts the
// walk over from the beginning.
func (f *fastArchiving) restriction(driver string, grace time.Duration, res residency) (where string, args []any) {
	if res != f.residency {
		f.residency, f.after = res, dueRoot{}
	}
	where, args = closedResidency(res.count, res.ordinal, res.ok, grace, stealGrace)
	after, afterArgs := afterClose(driver, f.after)
	return where + after, append(args, afterArgs...)
}

// failed records an archive failure, unless ctx ended: a stop is no failure. Only the first failure of an episode is
// logged.
func (f *fastArchiving) failed(ctx context.Context, what string, err error) {
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
		f.j.logger.ErrorContext(ctx, "Janitor: "+what+" failed; retained trees stay in the live tables",
			"shard", f.j.shard, "error", err)
	}
}

// clearUnlessFailedSince ends the failure episode, unless a failure was recorded at or after start: the work that
// began at start - a move that archived something, or a sweep of every due closed root - failed nothing. An empty scan
// ends nothing: it is after the position, and a root a failure left behind is not.
func (f *fastArchiving) clearUnlessFailedSince(start time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.failSince.IsZero() && f.failLast.Before(start) {
		f.failSince, f.failLast = time.Time{}, time.Time{}
	}
}

// logError logs a failed statement - unless ctx ended, which means the job was stopped on purpose.
func (f *fastArchiving) logError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	f.j.logger.ErrorContext(ctx, msg, append([]any{"shard", f.j.shard}, args...)...)
}

// reset forgets where the job stood, and what it observed: the janitor resumes as one that knows nothing.
func (f *fastArchiving) reset() {
	f.after, f.residency = dueRoot{}, residency{}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lag, f.lagOK = 0, false
	f.failSince, f.failLast = time.Time{}, time.Time{}
}

// lagReading returns the lag at the last scan, and false before the first.
func (f *fastArchiving) lagReading() (time.Duration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lag, f.lagOK
}

// failingFor returns how long archiving has been failing at now, or 0.
func (f *fastArchiving) failingFor(now time.Time) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failSince.IsZero() {
		return 0
	}
	return max(now.Sub(f.failSince), 0)
}
