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

// reaperLoop forgets, on every tick of its plain ticker, what the reaper recorded about the shards this replica no
// longer dispatches. It ends on ctx, which drainRuntime cancels. Removing trees from the live tables and from the
// archive, and expiration, are the janitor's.
func (e *Engine) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(e.reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.reapPass(ctx)
		}
	}
}

// reapDueFlows is what a test drives to reap what is due: the janitors' jobs run until they make no progress.
func (e *Engine) reapDueFlows(ctx context.Context) {
	e.drainJanitors(ctx, e.archiveGrace)
}

// reapPass forgets what the reaper recorded about the shards this replica no longer dispatches: a replica that resumes
// dispatching one later saw none of what happened meanwhile, so it starts afresh.
func (e *Engine) reapPass(ctx context.Context) {
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			e.reaper.Load().Forget(shard)
		}
		return nil
	})
}
