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

	"github.com/microbus-io/sequel"
)

// reaperLoop runs the expiration pass on its own plain ticker: on the first tick, and on any tick at least
// expireInterval after the last that ran it, per shard this replica dispatches; on every tick it forgets the shards this
// replica no longer dispatches. The first run is at once, not an interval in, so that a replica living shorter than the
// interval still expires flows. It ends on ctx, which drainRuntime cancels. Removing trees from the live tables and
// from the archive is the janitor's.
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
			e.reapPass(ctx, periodic)
		}
	}
}

// reapDueFlows is what a test drives to reap what is due: the janitors' jobs run until they make no progress.
func (e *Engine) reapDueFlows(ctx context.Context) {
	e.drainJanitors(ctx, e.archiveGrace)
}

// reapPass, with periodic, runs the expiration pass, per shard this replica dispatches, in one turn each; with or
// without, it forgets what it recorded about the shards this replica no longer dispatches.
func (e *Engine) reapPass(ctx context.Context, periodic bool) {
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			// A replica that resumes dispatching later saw none of what happened meanwhile, so it starts afresh: an
			// old position is only a hint about work seen before the handoff.
			e.forgetReaping(shard)
			return nil // see reaperLoop
		}
		if !periodic || e.flowExpiration <= 0 {
			return nil
		}
		// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
		// connections like any other caller instead of taking them from work already under way.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		// Janitor step 8: the archive deletion sweep is the janitor's. Kept, commented out, as the reference.
		// r.DeleteMarked(ctx, cfg, reaper.Shard{Index: shard, DB: db, Archive: a})
		e.expireOn(ctx, db, shard)
		return nil
	})
}

// forgetReaping forgets what the reaper and the expiration walk recorded about shard.
func (e *Engine) forgetReaping(shard int) {
	e.reaper.Load().Forget(shard)
	e.expirePositions.forget(shard)
}
