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

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// SetFlowExpiration sets how long after its creation a flow may still be running or interrupted. A flow still running or
// interrupted once it is older than d is terminated, with its subgraphs, and its TerminateReason says it expired; it is
// then deleted, not archived: Snapshot and Await report the terminated outcome until the deletion, within a few
// minutes, and a not-found error after it. The check runs every hour, or every half of d when that is shorter (at least a minute
// apart), so a flow is terminated up to that long after it expires. 0, the default, never expires a flow.
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

// expirePass terminates, on a shard this replica dispatches, the roots that have not closed and were created longer
// than the flow expiration ago, walking the flows after the flow_id after; it marks each for deletion as it terminates
// it, so fast and sweep deletion delete the tree rather than archive it. It returns the flow_id the next pass goes on
// after - 0 once the walk has swept the shard - and whether it made progress: ended a root, or stopped at its budget.
// The janitor's expiration job runs it, under a turn on the shard, and keeps the position.
//
// It reads no index of its own: flow_id is assigned in creation order, and every flow
// row's created_at is the column default (Fork's clones and Continue's flows included), so the oldest flows by id are
// the oldest by creation, and the walk stops at the first row that has not expired - one row when none has. Ids
// are assigned, and created_at stamped, at different moments of a flow's insert - as long as a Fork's clone of a whole
// tree takes, on PostgreSQL, whose NOW() is the transaction's start - which a lifetime of hours or days does not see. An index on (status, created_at)
// would cost an entry, and its WAL, on every flow insert and status change.
//
// A pass reads no page and starts no termination once it has run expireBudget, after its first page and its first
// termination, so a backlog - expiration turned on over months of abandoned flows, or a long run of closed flows
// waiting for archiving - drains over several passes, each on a turn of its own, rather than holding one turn
// unbounded. Each pass goes on from where the last stopped: from the oldest, a prefix larger than one budget would cost
// every pass its whole budget and never reach an expired root behind it. A walk that reaches a flow that has not
// expired, or the newest flow, has swept the shard, and the next pass starts again from the oldest. Two dispatchers of
// a shard may meet on a root; the later finds it ended and does nothing. A root that fails to terminate is logged and
// passed over, and retried by the next sweep.
func (e *Engine) expirePass(ctx context.Context, shard, after int) (next int, progress bool) {
	start := time.Now()
	reason := expirationReason(e.flowExpiration)
	ended, attempts, pages := 0, 0, 0
	defer func() { e.metricFlowsExpired(ctx, shard, ended) }()
	sh, err := e.db.Shard(shard)
	if err != nil {
		e.logSweepError(ctx, "Expiring flows; the next pass retries", "shard", shard, "error", err)
		return after, false
	}
	db := sh.DB()
	overBudget := func() bool {
		if time.Since(start) < e.expireBudget {
			return false
		}
		e.diags.Logger().InfoContext(ctx, "Expiring flows: the pass reached its time budget; the next pass goes on",
			"shard", shard, "expired", ended, "budget", e.expireBudget)
		return true
	}
	for {
		// Checked per page as well as per root: a page holding no open root - closed flows waiting for archiving, a
		// large tree's subgraph children - would otherwise walk on past the budget, and past a stop.
		if ctx.Err() != nil {
			return after, false
		}
		if pages > 0 && overBudget() {
			return after, true
		}
		roots, last, swept, err := e.expiredPage(ctx, db, after)
		if err != nil {
			e.logSweepError(ctx, "Expiring flows; the next pass retries", "shard", shard, "error", err)
			return after, ended > 0
		}
		pages++
		for _, r := range roots {
			// A stop before a root leaves the position just short of it, never past the rest of the page.
			if ctx.Err() != nil {
				return r.flowID - 1, false
			}
			if attempts > 0 && overBudget() {
				return r.flowID - 1, true
			}
			attempts++
			after = r.flowID
			done, err := e.terminateSubtree(ctx, shard, r.flowID, r.hour, r.token, reason, diagnostics.FaultExpireTerminate, true)
			if err != nil {
				if ctx.Err() == nil {
					e.metricExpireFailed(ctx, shard)
				}
				e.logSweepError(ctx, "Expiring flows: terminating an expired flow; the next sweep retries", "shard", shard,
					"flow", r.flowID, "error", err)
				continue
			}
			if done {
				ended++
			}
		}
		if swept {
			return 0, ended > 0
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
// and returns the open roots among them, the last id it read (0 for none), and whether the walk has swept the shard:
// it reached a row that has not expired, or read fewer rows than a page, the newest flow included. Closed flows
// waiting for archiving, and subgraph children - ended with their roots - are passed over.
func (e *Engine) expiredPage(ctx context.Context, db *sequel.DB, after int) (roots []expiredRoot, last int, swept bool, err error) {
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
	n := 0
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
		n++
		last = r.flowID
		if surgraphFlowID == 0 && (status == workflow.StatusRunning || status == workflow.StatusInterrupted) {
			roots = append(roots, r)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, 0, false, errors.Trace(err)
	}
	return roots, last, n < e.expirePage, nil
}
