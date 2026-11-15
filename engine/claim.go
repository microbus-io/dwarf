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
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// claimRow receives the columns processStep's claim reads back: the step's in claimCols order, then the
// flow's. The claim scans here and processStep copies the values into its locals, because a local whose
// address goes into a Scan escapes to the heap - one allocation per column on every step, where this is one.
type claimRow struct {
	stepDepth                                  int
	taskName, stepToken                        string
	stateJSON, priorChangesJSON, stateRefsJSON []byte
	attempt, lineageID, flowID, timeBudgetMs   int
	interruptDone                              bool
	resumeDataJSON                             []byte
	subgraphDone                               bool
	subgraphResultJSON                         []byte
	subgraphErrorJSON                          []byte
	stepCreatedAt                              time.Time
	leaseSeq, fanOutOrdinal                    int
	stepCancelling, replaying                  bool
	replayGoto                                 string
	replaySleepLeftMs                          float64
	replayOnError                              bool
	payloadDeferrals, createdHour              int
	flowToken, flowStatus, workflowURL         string
	baggageJSON                                []byte
	traceParent                                string
	flowCreatedAt, flowUpdatedAt               time.Time
	flowPriority                               int
	flowFairnessKey                            string
	flowFairnessWeight                         int
	flowTimeBudgetMs                           int
	flowCancelReason, graphHash                string
	flowPayloadVer                             int
	dest                                       [claimStepCols + claimFlowCols]any
}

const (
	claimStepCols = 25
	claimFlowCols = 14
)

// destinations points dest at the fields and returns the step's and the flow's scan destinations, which
// share one backing array so a claim reading both scans dest whole.
func (r *claimRow) destinations() (step, flow []any) {
	r.dest = [claimStepCols + claimFlowCols]any{
		&r.stepDepth, &r.taskName, &r.stepToken, &r.stateJSON, &r.priorChangesJSON, &r.stateRefsJSON, &r.attempt,
		&r.lineageID, &r.flowID, &r.timeBudgetMs, &r.interruptDone, &r.resumeDataJSON, &r.subgraphDone,
		&r.subgraphResultJSON, &r.subgraphErrorJSON, &r.stepCreatedAt, &r.leaseSeq, &r.fanOutOrdinal,
		&r.stepCancelling, &r.replaying, &r.replayGoto, &r.replaySleepLeftMs, &r.replayOnError, &r.payloadDeferrals, &r.createdHour,
		&r.flowToken, &r.flowStatus, &r.workflowURL, &r.baggageJSON, &r.traceParent, &r.flowCreatedAt,
		&r.flowUpdatedAt, &r.flowPriority, &r.flowFairnessKey, &r.flowFairnessWeight, &r.flowTimeBudgetMs,
		&r.flowCancelReason, &r.graphHash, &r.flowPayloadVer,
	}
	return r.dest[:claimStepCols], r.dest[claimStepCols:]
}

// lockFlowRowsSQL takes the write lock on the dwarf_flows rows that match where and changes nothing else. Read its
// result with lockFlowRows, or lockedFlowRows in a batch: a locking read counts the rows it returns, the UPDATE
// its rows affected.
//
// On PostgreSQL and MySQL it is a locking read. An UPDATE writes a new row version even when it changes nothing,
// and the flow row is locked on every step: on PostgreSQL that is a row version per step on a row that otherwise
// changes a few times per flow, filling its pages faster than they are pruned, so its writes stop being HOT. FOR
// NO KEY UPDATE is the lock
// a non-key UPDATE takes, so it conflicts with exactly what the UPDATE did.
//
// SQLite and SQL Server flip the non-indexed touch column. SQLite must take its write lock up front: a read takes
// a SHARED lock, and two readers deadlock on the upgrade. On SQL Server a locking read is UPDLOCK, whose reads
// under READ_COMMITTED_SNAPSHOT are unverified.
func lockFlowRowsSQL(driverName string, where string) string {
	return lockRowsSQL(driverName, liveTables.flows, where)
}

// lockRowsSQL is lockFlowRowsSQL over the given flows table: the live one, or an archive day's.
func lockRowsSQL(driverName string, flows string, where string) string {
	switch driverName {
	case "pgx":
		return "SELECT 1 FROM " + flows + " WHERE " + where + " FOR NO KEY UPDATE"
	case "mysql", "mariadb":
		return "SELECT 1 FROM " + flows + " WHERE " + where + " FOR UPDATE"
	}
	return "UPDATE " + flows + " SET touch=1-touch WHERE " + where
}

// locksByReading reports whether lockFlowRowsSQL is a locking read on the driver.
func locksByReading(driverName string) bool {
	return driverName == "pgx" || driverName == "mysql" || driverName == "mariadb"
}

// lockFlowRows takes the write lock on the dwarf_flows rows that match where and returns how many it locked.
func lockFlowRows(ctx context.Context, tx *sequel.Tx, where string, args ...any) (int64, error) {
	return lockRows(ctx, tx, liveTables.flows, where, args...)
}

// lockRows is lockFlowRows over the given flows table: the live one, or an archive day's. Fork locks its source root
// with it, and that lock must keep conflicting with the janitor's, which takes every root it removes FOR UPDATE (or by
// the touch flip): it is what serializes a Fork with the archiving or deletion of the tree it clones.
func lockRows(ctx context.Context, tx *sequel.Tx, flows string, where string, args ...any) (int64, error) {
	query := lockRowsSQL(tx.DriverName(), flows, where)
	if !locksByReading(tx.DriverName()) {
		res, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		return n, nil
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()
	var n int64
	for rows.Next() {
		n++
	}
	return n, errors.Trace(rows.Err())
}

// lockedFlowRows reads a lockFlowRowsSQL statement queued on a batch and returns how many rows it locked.
func lockedFlowRows(driverName string, res *sequel.BatchResults) (int64, error) {
	if !locksByReading(driverName) {
		result, err := res.Exec()
		if err != nil {
			return 0, errors.Trace(err)
		}
		n, _ := result.RowsAffected()
		return n, nil
	}
	rows, err := res.Query()
	if err != nil {
		return 0, errors.Trace(err)
	}
	var n int64
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, errors.Trace(err)
	}
	return n, errors.Trace(rows.Close())
}

// liveFlowWhere matches the flow row by flow_id while it is not terminal.
var liveFlowWhere = "flow_id=? AND status NOT IN (" + database.TerminalStatusesSQL + ")"

// claimEligible is the claim's test that the step is still claimable - pending, unparked, due, lease lapsed -
// wrapped in CASE so that no index can serve it, leaving the primary key on step_id as the claim's only index
// path. Written as plain conjuncts, the test implies status='pending', which makes every partial index on
// pending rows (idx_dwarf_steps_due, _selection) a candidate, and a scan of one of them filtered on
// step_id walks the whole backlog. Measured on Postgres: after an ANALYZE taken at a small backlog, both the
// generic and the custom plan claimed through a partial index on pending rows, every claim cost O(backlog),
// and a 5,000 steps/s run fell to ~800 steps/s with its backlog at the cap until an ANALYZE taken at a large
// backlog flipped the plan back. The status stays a literal (the inline-status rule); parked is the caller's
// bind. alias is the table alias with its dot ("s.") or "".
func claimEligible(alias string) string {
	return "CASE WHEN " + alias + "status='" + workflow.StatusPending + "' AND " + alias + "parked=? AND " +
		alias + "not_before<=NOW_UTC() AND " + alias + "lease_expires<=NOW_UTC() THEN 1 END=1"
}

// claimSet is the claim's SET clause: take the lease, bump its generation to nextSeq, and stamp started_at unless
// the dispatch continues an earlier one (a retry, a resumed park, a replay). alias qualifies the step table's columns
// ("s." or ""), since an assignment's right-hand side may need it. Binds: the running status, the lease margin in
// ms, the engine ID.
func claimSet(alias, nextSeq string) string {
	return "status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), " + alias + "time_budget_ms + ?), lease_seq=" + nextSeq + ", engine_id=?, updated_at=NOW_UTC()," +
		" started_at=CASE WHEN " + alias + "attempt>0 OR " + alias + "subgraph_done=1 OR " + alias + "interrupt_done=1 OR " + alias + "advancing=1 THEN " + alias + "started_at ELSE NOW_UTC() END"
}

// claimReadCols is the step columns the claim reads back, in the order processStep scans them.
var claimReadCols = []string{"step_depth", "task_name", "step_token", "state", "changes", "state_refs", "attempt",
	"lineage_id", "flow_id", "time_budget_ms", "interrupt_done", "resume_data", "subgraph_done", "subgraph_result",
	"subgraph_error", "created_at", "lease_seq", "fan_out_ordinal", "cancelling", "advancing", "advance_goto",
	"advance_sleep_until", "advance_on_error", "payload_deferrals", "flow_created_hour"}

// claimCols is claimReadCols as a select list, each qualified by prefix ("s.", "INSERTED." or ""). A recorded
// sleep is read as what remains of it, on the database clock.
func claimCols(prefix string) string {
	cols := make([]string, len(claimReadCols))
	for i, c := range claimReadCols {
		cols[i] = prefix + c
		if c == "advance_sleep_until" {
			cols[i] = "DATE_DIFF_MILLIS(" + prefix + c + ", NOW_UTC())"
		}
	}
	return strings.Join(cols, ", ")
}

// The claim, per dialect, built once: it runs on every step. Binds: the running status, the lease margin in ms,
// the engine ID, the step ID, the unparked value. claimSQLPostgres is the CAS on the step row, its read, and the
// flow row joined in, in one round trip; SQLite and SQL Server read the step back with RETURNING/OUTPUT; MySQL has
// neither, so it claims and then reads (claimReadMySQL, bound to the step ID and the generation the claim took).
//
// MySQL's claim wraps the new generation in LAST_INSERT_ID(expr), which hands it back in the UPDATE's own OK
// packet (sql.Result.LastInsertId, MySQL 8.4 and MariaDB 10.11 alike), so the read can be fenced on it at no extra
// round trip. Do not read the generation back unfenced: the two statements are separate, so between them the lease
// can expire, recovery can return the step to pending, and a later claim (this engine's or a peer's) can take it -
// an unfenced read then adopts THAT claim's generation and row, every lease_seq fence after it passes, and the
// transition is committed twice. Every claim raises lease_seq and nothing lowers it, so the generation names one
// claim.
var (
	claimSQLSQLite   = "UPDATE dwarf_steps SET " + claimSet("", "lease_seq+1") + " WHERE step_id=? AND " + claimEligible("") + " RETURNING " + claimCols("")
	claimSQLMSSQL    = "UPDATE dwarf_steps SET " + claimSet("", "lease_seq+1") + " OUTPUT " + claimCols("INSERTED.") + " WHERE step_id=? AND " + claimEligible("")
	claimSQLMySQL    = "UPDATE dwarf_steps SET " + claimSet("", "LAST_INSERT_ID(lease_seq+1)") + " WHERE step_id=? AND " + claimEligible("")
	claimReadMySQL   = "SELECT " + claimCols("") + " FROM dwarf_steps WHERE step_id=? AND lease_seq=?"
	claimSQLPostgres = "UPDATE dwarf_steps s SET " + claimSet("s.", "s.lease_seq+1") +
		" FROM dwarf_flows f" +
		" WHERE s.step_id=? AND " + claimEligible("s.") + " AND f.flow_id=s.flow_id" +
		" RETURNING " + claimCols("s.") + "," +
		" f.flow_token, f.status, f.workflow_url, f.baggage, f.trace_parent, f.created_at, f.updated_at, f.priority, f.fairness_key, f.fairness_weight, f.time_budget_ms, f.cancel_reason, f.graph_hash, f.payload_ver"
)
