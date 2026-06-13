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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// Probe schedule constants for the per-task 404 ack-timeout breaker.
const (
	breakerInitialProbeDelay = 100 * time.Millisecond
	breakerProbeMultiplier   = 2.0
	breakerMaxProbeDelay     = 1 * time.Minute
)

// Cause labels for the breaker metrics.
const (
	breakerCauseAckTimeout  = "ack_timeout"
	breakerCauseUnavailable = "unavailable"
	breakerCauseOverloaded  = "overloaded"
)

// taskBreaker is the per-task circuit breaker state. Zero trippedAt = closed (admitting).
type taskBreaker struct {
	trippedAt    time.Time
	probeAttempt int
	nextProbeAt  time.Time
	cause        string
}

// breakerProbeBackoff returns the wait until the n-th probe attempt (1-indexed),
// capped at breakerMaxProbeDelay.
func breakerProbeBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(breakerInitialProbeDelay)
	for i := 1; i < attempt; i++ {
		d *= breakerProbeMultiplier
		if d >= float64(breakerMaxProbeDelay) {
			return breakerMaxProbeDelay
		}
	}
	return time.Duration(d)
}

// parkTrippedSteps scans the pending steps of flowID, finds those whose task has a locally-tripped
// breaker, and parks them (parked=2) within the given transaction. Cheap shortcut when no
// breakers are tripped on this replica - skips the SELECT entirely. Used by Start (CREATED ->
// PENDING transition) and Retry (which inserts a fresh PENDING row). One additional UPDATE per
// affected task; off the hot path.
func (e *Engine) parkTrippedSteps(ctx context.Context, tx sequel.Executor, flowID int) error {
	e.breakersLock.RLock()
	if len(e.breakers) == 0 {
		e.breakersLock.RUnlock()
		return nil
	}
	trippedTasks := make(map[string]struct{}, len(e.breakers))
	for name, b := range e.breakers {
		if !b.trippedAt.IsZero() {
			trippedTasks[name] = struct{}{}
		}
	}
	e.breakersLock.RUnlock()
	if len(trippedTasks) == 0 {
		return nil
	}
	// Pull only the task names that actually appear in this flow's pending set so we
	// avoid running a per-task UPDATE for tasks that aren't in this graph.
	rows, err := tx.QueryContext(ctx,
		"SELECT DISTINCT task_name FROM dwarf_steps WHERE flow_id=? AND status=? AND parked=?",
		flowID, workflow.StatusPending, parkedNone,
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()
	affectedTasks := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return errors.Trace(err)
		}
		if _, ok := trippedTasks[name]; ok {
			affectedTasks = append(affectedTasks, name)
		}
	}
	if err := rows.Err(); err != nil {
		return errors.Trace(err)
	}
	for _, name := range affectedTasks {
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET parked=?, updated_at=NOW_UTC() WHERE flow_id=? AND task_name=? AND status=? AND parked=?",
			parkedBreaker, flowID, name, workflow.StatusPending, parkedNone,
		)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// reconstituteBreakers re-arms the in-memory breaker map after a restart by scanning each shard
// for tasks that still have parked=parkedBreaker rows. For each such task it calls breakerTrip
// locally - schedule starts fresh on this replica (probeAttempt=0, first probe at now+100ms)
// regardless of where peers are in their own schedules. The DB-side state (parked rows + the
// elevated probe per shard) is already correct from the prior replica's breakerBulkPark, so no
// SQL writes are needed. Local-only: does NOT broadcast TripBreaker - peers already have their
// own in-memory state and the gossip merge would clobber their accumulated probeAttempt with
// our fresh "now" timestamp. Cause defaults to ack_timeout (the most common reason rows survive
// a restart).
func (e *Engine) reconstituteBreakers(ctx context.Context) error {
	return e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		rows, err := db.QueryContext(ctx,
			"SELECT DISTINCT task_name FROM dwarf_steps WHERE parked=? AND status=?",
			parkedBreaker, workflow.StatusPending,
		)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		for rows.Next() {
			var taskName string
			if err := rows.Scan(&taskName); err != nil {
				return errors.Trace(err)
			}
			e.breakerTrip(taskName, breakerCauseAckTimeout)
		}
		return errors.Trace(rows.Err())
	})
}
