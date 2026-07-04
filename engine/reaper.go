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

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// reaperLoop deletes flows whose delete_after_ms window has elapsed, on its own plain ticker. A single
// goroutine is inherently non-overlapping - a slow pass just delays the next tick. Deletion is
// latency-tolerant, so there is no wake/startup/shutdown special pass: a flow that came due while a replica
// was down is removed on the next tick here, or by a peer replica's reaper. Drained via reaperStop in
// drainRuntime; a pass in progress finishes its current tree-delete (checked between batches) and exits.
func (e *Engine) reaperLoop(ctx context.Context) {
	ticker := time.NewTicker(reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.reaperStop:
			return
		case <-ticker.C:
			e.reapDueFlows(ctx)
		}
	}
}

// reapDueFlows removes, per shard, the whole subgraph tree of every root whose delete_after_ms window has
// elapsed. Set-based (no N+1): one SELECT of due roots + one two-statement tree delete per batch, looped in
// reapBatch-sized chunks until the due set drains. The reapBatch cap keeps each `IN (...)` literal list
// plannable (same rationale as purgeCap). Only roots ever carry delete_after_ms > 0 (DeleteOnCompletion is
// root-only; Delete/Purge stamp roots), and the surgraph_flow_id=0 guard makes that explicit, so deleting by
// root_flow_id removes each root plus its descendants. The between-batch reaperStop check lets Shutdown abort
// a long drain promptly - between whole-tree deletes, never mid-statement.
func (e *Engine) reapDueFlows(ctx context.Context) {
	const reapBatch = 4096
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		for {
			select {
			case <-e.reaperStop:
				return nil
			default:
			}
			rows, err := db.QueryContext(ctx,
				"SELECT flow_id FROM dwarf_flows WHERE delete_after_ms>0 AND surgraph_flow_id=0"+
					" AND DATE_ADD_MILLIS(updated_at, delete_after_ms)<=NOW_UTC() LIMIT_OFFSET(?, 0)",
				reapBatch,
			)
			if err != nil {
				e.logger.ErrorContext(ctx, "Reaper: selecting due flows", "shard", shard, "error", err)
				return nil
			}
			var rootIDs []int
			for rows.Next() {
				var fid int
				if err := rows.Scan(&fid); err != nil {
					rows.Close()
					e.logger.ErrorContext(ctx, "Reaper: scanning due flow id", "shard", shard, "error", err)
					return nil
				}
				rootIDs = append(rootIDs, fid)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				e.logger.ErrorContext(ctx, "Reaper: iterating due flows", "shard", shard, "error", err)
				return nil
			}
			if len(rootIDs) == 0 {
				return nil
			}

			// Steps before flows (the subquery reads the flow rows first). Trees keyed on root_flow_id, so a
			// root plus all its subgraph descendants go in one shot. ids are trusted integers embedded as
			// literals to dodge the per-driver bind-param ceiling.
			ids := intCSV(rootIDs)
			err = db.Transact(ctx, func(tx *sequel.Tx) error {
				if _, err := tx.ExecContext(ctx,
					"DELETE FROM dwarf_steps WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id IN ("+ids+"))",
				); err != nil {
					return errors.Trace(err)
				}
				if _, err := tx.ExecContext(ctx,
					"DELETE FROM dwarf_flows WHERE root_flow_id IN ("+ids+")",
				); err != nil {
					return errors.Trace(err)
				}
				return nil
			})
			if err != nil {
				e.logger.ErrorContext(ctx, "Reaper: deleting due trees", "shard", shard, "error", err)
				return nil
			}
			if len(rootIDs) < reapBatch {
				return nil // drained this shard
			}
		}
	})
}
