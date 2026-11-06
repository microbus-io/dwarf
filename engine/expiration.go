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
	"net/http"
	"time"

	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// SetFlowExpiration sets how long after its creation a flow may still be running or interrupted. A flow still running or
// interrupted once it is older than d is terminated, with its subgraphs, and its TerminateReason says it expired; it is
// then archived like any other terminated flow. The check runs every 5 minutes, so a flow is terminated up to about 5
// minutes after it expires. 0, the default, never expires a flow.
//
// A flow created by Continue or Fork counts from its own creation. Use the same value on every replica sharing a
// database: any replica dispatching a shard may expire its flows, so the smallest value in the fleet wins. A value
// under a millisecond counts as one. Construction-time only.
func (e *Engine) SetFlowExpiration(d time.Duration) error {
	if e.started.Load() {
		return errSetAfterStartup("flow expiration")
	}
	if d < 0 {
		return errors.New("flow expiration must not be negative", http.StatusBadRequest)
	}
	e.flowExpiration = d
	return nil
}

// expirationReason is the TerminateReason of a flow the expiration pass ended.
func expirationReason(d time.Duration) string {
	return "expired: still running or interrupted after the flow expiration (" + d.String() + ")"
}

// expireFlows terminates, per shard this replica dispatches, every root that has not closed and was created longer
// than the flow expiration ago. It reads no index of its own: flow_id is assigned in creation order, and every flow
// row's created_at is the column default (Fork's clones and Continue's flows included), so the oldest flows by id are
// the oldest by creation, and the walk stops at the first row that has not expired - one row when none has. Ids
// are assigned, and created_at stamped, at different moments of a flow's insert - as long as a Fork's clone of a whole
// tree takes, on PostgreSQL, whose NOW() is the transaction's start - which a lifetime of hours or days does not see. An index on (status, created_at)
// would cost an entry, and its WAL, on every flow insert and status change.
//
// A shard's pass starts no termination once it has run expireBudget, after the first, so a backlog - expiration
// turned on over months of abandoned flows - drains over several passes rather than holding back the reaper, whose
// goroutine this runs on. Two dispatchers of a shard may meet on a root; the later finds it ended and does nothing.
// A root that fails to terminate is logged and passed over: every pass walks from the oldest, so one failing every
// time would otherwise stop expiration on the shard for good.
func (e *Engine) expireFlows(ctx context.Context) {
	if e.flowExpiration <= 0 {
		return
	}
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			return nil
		}
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		e.expireOn(ctx, db, shard)
		return nil
	})
}

// expireOn is expireFlows on one shard.
func (e *Engine) expireOn(ctx context.Context, db *sequel.DB, shard int) {
	start := time.Now()
	reason := expirationReason(e.flowExpiration)
	ended, attempts := 0, 0
	defer func() { e.metricFlowsExpired(ctx, shard, ended) }()
	after := 0
	for {
		// Checked per page as well as per root: a page holding no open root - a large tree's subgraph children - would
		// otherwise walk on past the budget, and past a stop.
		if ctx.Err() != nil {
			return
		}
		if attempts > 0 && time.Since(start) >= e.expireBudget {
			e.logger.InfoContext(ctx, "Expiring flows: the pass reached its time budget; the next pass goes on",
				"shard", shard, "expired", ended, "budget", e.expireBudget)
			return
		}
		roots, last, young, err := e.expiredPage(ctx, db, after)
		if err != nil {
			e.logSweepError(ctx, "Expiring flows; the next pass retries", "shard", shard, "error", err)
			return
		}
		for _, r := range roots {
			if ctx.Err() != nil {
				return
			}
			if attempts > 0 && time.Since(start) >= e.expireBudget {
				e.logger.InfoContext(ctx, "Expiring flows: the pass reached its time budget; the next pass goes on",
					"shard", shard, "expired", ended, "budget", e.expireBudget)
				return
			}
			attempts++
			done, err := e.terminateSubtree(ctx, shard, r.flowID, r.hour, r.token, reason, engineseams.FaultExpireTerminate)
			if err != nil {
				if ctx.Err() == nil {
					e.metricExpireFailed(ctx, shard)
				}
				e.logSweepError(ctx, "Expiring flows: terminating an expired flow; the next pass retries", "shard", shard,
					"flow", r.flowID, "error", err)
				continue
			}
			if done {
				ended++
			}
		}
		if young || last == 0 {
			return
		}
		after = last
	}
}

// expiredRoot is a root the expiration pass terminates.
type expiredRoot struct {
	flowID, hour int
	token        string
}

// expiredPage reads the page of flow rows after flowID after, in flow_id order, up to the first that has not expired,
// and returns the open roots among them, the last id it read (0 for none), and whether it reached a row that has
// not expired. Closed flows waiting for the reaper, and subgraph children - ended with their roots - are passed over.
func (e *Engine) expiredPage(ctx context.Context, db *sequel.DB, after int) (roots []expiredRoot, last int, young bool, err error) {
	// ORDER BY flow_id is REQUIRED: LIMIT_OFFSET is OFFSET/FETCH on SQL Server, which needs one. The age is compared
	// on the database clock, never bound as a Go time.
	rows, err := db.QueryContext(ctx, "SELECT flow_id, flow_token, created_hour, status, surgraph_flow_id,"+
		" CASE WHEN created_at<=DATE_ADD_MILLIS(NOW_UTC(), ?) THEN 1 ELSE 0 END"+
		" FROM dwarf_flows WHERE flow_id>? ORDER BY flow_id LIMIT_OFFSET(?, 0)",
		-max(e.flowExpiration.Milliseconds(), 1), after, e.expirePage)
	if err != nil {
		return nil, 0, false, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var r expiredRoot
		var status string
		var surgraphFlowID, expired int
		if err := rows.Scan(&r.flowID, &r.token, &r.hour, &status, &surgraphFlowID, &expired); err != nil {
			return nil, 0, false, errors.Trace(err)
		}
		if expired == 0 {
			return roots, last, true, nil
		}
		last = r.flowID
		if surgraphFlowID == 0 && (status == workflow.StatusRunning || status == workflow.StatusInterrupted) {
			roots = append(roots, r)
		}
	}
	return roots, last, false, errors.Trace(rows.Err())
}
