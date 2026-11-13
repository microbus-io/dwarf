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
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/reaper"
	"github.com/microbus-io/sequel"
)

// reaperLoop removes closed trees from the live tables on its own plain ticker: it archives every root that ended
// archiveGrace ago, and deletes, without archiving, every root whose delete_after_ms window has elapsed. A single
// goroutine is inherently non-overlapping - a slow pass just delays the next tick. It is latency-tolerant, so there
// is no wake/startup/shutdown special pass: a tree that came due while a replica was down is removed on the next tick
// here, or by a peer replica's reaper. Only the shards this replica dispatches are reaped: every shard always has a
// dispatcher, so each is still covered, and a replica that only reads a shard would otherwise run the sweep through
// its two-connection reader pool once per replica. It ends on ctx, which drainRuntime cancels, abandoning a pass in
// flight: each batch is one transaction, so an abandoned batch lands whole or not at all, and what is left is reaped
// by a peer or the next start.
//
// Two stages of the same pass run on the first tick and on any tick at least expireInterval after the last that ran
// them: the deletion of archived trees marked for deletion that the call marking them did not finish (DeleteMarked),
// and the expiration pass (expireOn). Neither has a deadline to keep, and their own time budgets bound how long they
// hold back the next reap. The first is at once, not an interval in, so that a replica living shorter than the
// interval still runs them.
func (e *Engine) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(e.reapInterval)
	defer ticker.Stop()
	var lastExpire time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			periodic := time.Since(lastExpire) >= e.expireInterval
			if periodic {
				lastExpire = time.Now()
			}
			e.reapPass(ctx, e.archiveGrace, periodic)
		}
	}
}

// reapDueFlows is what a test drives to reap what is due: the janitors' jobs run until they make no progress.
func (e *Engine) reapDueFlows(ctx context.Context) {
	e.drainJanitors(ctx, e.archiveGrace)
}

// reaperConfig is one pass's settings, from the engine's fields, read anew for every pass since tests change them
// on a running engine. The steal grace is two reaper ticks, so a slow or departed peer's roots wait about three ticks
// rather than forever. The day's length is the run's; before a run it is resolved as at Startup, and only when
// retention needs it.
func (e *Engine) reaperConfig(ctx context.Context, grace time.Duration) reaper.Config {
	cfg := reaper.Config{
		Batch:         e.reapBatch,
		BatchRows:     e.reapBatchRows,
		BatchBytes:    e.reapBatchBytes,
		StageRows:     e.reapStageRows,
		StageBytes:    e.reapStageBytes,
		Attempt:       e.reapAttempt,
		Budget:        e.reapBudget,
		Grace:         grace,
		StealGrace:    2 * e.reapInterval,
		RetentionDays: e.retentionDays,
		NoQuietMargin: e.noReapQuiet,
	}
	switch {
	case e.dayLength() > 0:
		cfg.DayLength = e.dayLength()
	case e.retentionDays > 0:
		cfg.DayLength = archiver.DefaultDayLength
		if d, err := e.archiveDayLength(ctx); err == nil {
			cfg.DayLength = d
		}
	}
	return cfg
}

// reapPass, with periodic, deletes the marked archive trees left and runs the expiration pass, per shard this replica
// dispatches, in one turn each; with or without, it forgets what it recorded about the shards this replica no longer
// dispatches. Janitor step 7: every walk of the live tables is the janitor's, so the reaper's own pass runs nothing and
// is not called; grace goes into the configuration DeleteMarked reads.
func (e *Engine) reapPass(ctx context.Context, grace time.Duration, periodic bool) {
	r := e.reaper.Load()
	cfg := e.reaperConfig(ctx, grace)
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			// A replica that resumes dispatching later saw none of what happened meanwhile, so it starts afresh: an
			// old position is only a hint about work seen before the handoff.
			e.forgetReaping(shard)
			return nil // see reaperLoop
		}
		if !periodic {
			return nil
		}
		// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
		// connections like any other caller instead of taking them from work already under way.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		a := e.archiveOn(shard)
		// Janitor step 7: the reaper's pass runs nothing. Kept, commented out, as the reference.
		// r.Pass(ctx, cfg, reaper.Shard{Index: shard, DB: db, Archive: a, Residency: func() (int, int, bool) {
		// 	return e.partitionOn(shard)
		// }})
		r.DeleteMarked(ctx, cfg, reaper.Shard{Index: shard, DB: db, Archive: a})
		if e.flowExpiration > 0 {
			e.expireOn(ctx, db, shard)
		}
		return nil
	})
}

// forgetReaping forgets what the reaper and the expiration walk recorded about shard.
func (e *Engine) forgetReaping(shard int) {
	e.reaper.Load().Forget(shard)
	e.expirePositions.forget(shard)
}
