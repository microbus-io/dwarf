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
	ticker := time.NewTicker(e.reapInterval)
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
//
// The tree delete is unconditional on descendant status: it removes every flow with root_flow_id IN(ids)
// regardless of whether a descendant is still non-terminal. deleteFlow's 409 guards only the root's own status
// (a non-terminal root is never stamped), so a due tree's root is always terminal - but a descendant can still
// be a live orphan (the residue of the Cancel-vs-spawn race, a running child whose parent already terminalized;
// see recoverOrphanedSubgraphChildren). Deleting that running orphan is safe: it is a bug-state row the wedge
// sweep would cancel anyway, and a worker mid-dispatch on it simply no-ops via the lease fence (its claim/write
// matches zero rows once the row is gone). So the reaper does not reguard on descendant status; whichever of the
// reaper and the wedge sweep reaches the orphan first wins, both removing it cleanly. This is a deliberate
// behavior change from the old inline deleteFlow, which 409'd if any descendant was running.
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
				// faultReapMidTree aborts after the steps delete but before the flows delete, so the whole
				// tree-delete rolls back atomically. The test proves a mid-tree failure leaves the tree intact
				// (not a half-deleted flow with no steps) and the next reap pass removes it cleanly.
				if e.isFault(faultReapMidTree) {
					return errors.New("injected fault: " + faultReapMidTree)
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
