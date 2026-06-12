/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

func (e *Engine) history(ctx context.Context, flowKey string) ([]workflow.FlowStep, error) {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var exists int
	err = db.QueryRowContext(ctx, "SELECT 1 FROM microbus_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e.historyBeforeStep(ctx, shardNum, flowID, 0)
}

func (e *Engine) historyBeforeStep(ctx context.Context, shardNum int, flowID int, beforeStepDepth int) ([]workflow.FlowStep, error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var rows *sql.Rows
	if beforeStepDepth > 0 {
		rows, err = db.QueryContext(ctx,
			"SELECT step_id, step_token, step_depth, task_name, attempt, status, error, created_at, started_at, updated_at, predecessor_id, successor_id FROM microbus_steps WHERE flow_id=? AND step_depth<? ORDER BY step_depth, step_id",
			flowID, beforeStepDepth,
		)
	} else {
		rows, err = db.QueryContext(ctx,
			"SELECT step_id, step_token, step_depth, task_name, attempt, status, error, created_at, started_at, updated_at, predecessor_id, successor_id FROM microbus_steps WHERE flow_id=? ORDER BY step_depth, step_id",
			flowID,
		)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	return e.scanHistorySteps(ctx, shardNum, rows)
}

func (e *Engine) scanHistorySteps(ctx context.Context, shardNum int, rows *sql.Rows) ([]workflow.FlowStep, error) {
	var steps []workflow.FlowStep
	for rows.Next() {
		var step workflow.FlowStep
		var stepID int
		var stepToken, errMsg string
		err := rows.Scan(&stepID, &stepToken, &step.StepDepth, &step.TaskName, &step.Attempt, &step.Status, &errMsg, &step.CreatedAt, &step.StartedAt, &step.UpdatedAt, &step.PredecessorID, &step.SuccessorID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		step.StepID = stepID
		step.StepKey = fmt.Sprintf("%d-%d-%s", shardNum, stepID, strings.TrimSpace(stepToken))
		step.Status = strings.TrimSpace(step.Status)
		step.Error = strings.TrimSpace(errMsg)
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	for i := range steps {
		subWorkflowName, subHistory, err := e.subgraphHistory(ctx, shardNum, steps[i].StepID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if len(subHistory) > 0 {
			steps[i].Subgraph = true
			steps[i].SubWorkflowName = subWorkflowName
			steps[i].SubHistory = subHistory
		}
	}
	return steps, nil
}

func (e *Engine) subgraphHistory(ctx context.Context, shardNum int, surgraphStepID int) (string, []workflow.FlowStep, error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	var subFlowID int
	var subWorkflowName string
	err = db.QueryRowContext(ctx, "SELECT flow_id, workflow_name FROM microbus_flows WHERE surgraph_step_id=?", surgraphStepID).Scan(&subFlowID, &subWorkflowName)
	if err == sql.ErrNoRows {
		return "", nil, nil
	}
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	subWorkflowName = strings.TrimSpace(subWorkflowName)
	steps, err := e.historyBeforeStep(ctx, shardNum, subFlowID, 0)
	return subWorkflowName, steps, errors.Trace(err)
}

func (e *Engine) step(ctx context.Context, stepKey string) (*workflow.FlowStep, error) {
	shardNum, stepID, stepToken, err := parseStepKey(stepKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var taskName, statusStr, errMsg string
	var stateJSON, changesJSON, interruptJSON string
	var stepDepth, attempt, predID, succID int
	var createdAt, updatedAt time.Time
	err = db.QueryRowContext(ctx,
		"SELECT step_depth, task_name, attempt, state, changes, interrupt_payload, status, error, created_at, updated_at, predecessor_id, successor_id FROM microbus_steps WHERE step_id=? AND step_token=?",
		stepID, stepToken,
	).Scan(&stepDepth, &taskName, &attempt, &stateJSON, &changesJSON, &interruptJSON, &statusStr, &errMsg, &createdAt, &updatedAt, &predID, &succID)
	if err == sql.ErrNoRows {
		return nil, errors.New("step not found", http.StatusNotFound)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	fs := &workflow.FlowStep{
		StepKey:       stepKey,
		StepID:        stepID,
		StepDepth:     stepDepth,
		TaskName:      taskName,
		Attempt:       attempt,
		PredecessorID: predID,
		SuccessorID:   succID,
		Status:        strings.TrimSpace(statusStr),
		Error:         strings.TrimSpace(errMsg),
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}
	err = json.Unmarshal([]byte(stateJSON), &fs.State)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = json.Unmarshal([]byte(changesJSON), &fs.Changes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if interruptJSON != "" {
		err = json.Unmarshal([]byte(interruptJSON), &fs.InterruptPayload)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// Navigation skips the surgraph wrapper entirely: it's a routing/structural
	// step (parked while the subgraph runs) and isn't on the execution timeline
	// the user wants to walk. We resolve the effective neighbor in three steps:
	//
	//   1. Start with the intra-flow predecessor_id / successor_id.
	//   2. If the current step is a subgraph entry/exit (predID/succID == 0),
	//      stitch across the seam to the *parent's* surgraph-step's intra-flow
	//      neighbor (skipping the wrapper itself).
	//   3. If the current step is itself a surgraph (has a child flow attached),
	//      jump straight to that child flow's entry on successor.
	//   4. Repeat the "neighbor is a surgraph -> descend" walk until the
	//      effective neighbor is a regular step. Nested subgraphs naturally
	//      unwrap in one direction or the other.
	effectivePredID := predID
	effectiveSuccID := succID
	if predID == 0 || succID == 0 {
		// We may be inside a subgraph - look up our own flow's surgraph linkage.
		var surgraphStepID int
		err = db.QueryRowContext(ctx,
			"SELECT f.surgraph_step_id FROM microbus_steps s JOIN microbus_flows f ON s.flow_id = f.flow_id WHERE s.step_id=?",
			stepID,
		).Scan(&surgraphStepID)
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if surgraphStepID > 0 {
			// Cross-flow ascend: skip the surgraph wrapper and jump to its
			// intra-flow neighbor in the parent flow.
			var parentPred, parentSucc int
			err = db.QueryRowContext(ctx,
				"SELECT predecessor_id, successor_id FROM microbus_steps WHERE step_id=?",
				surgraphStepID,
			).Scan(&parentPred, &parentSucc)
			if err != nil && err != sql.ErrNoRows {
				return nil, errors.Trace(err)
			}
			if effectivePredID == 0 && parentPred > 0 {
				effectivePredID = parentPred
			}
			if effectiveSuccID == 0 && parentSucc > 0 {
				effectiveSuccID = parentSucc
			}
		}
	}
	// If the current step itself is a surgraph, descend on the successor side
	// (entry of its subgraph) so navigation enters the child instead of skipping
	// past it.
	var ownChildFlow int
	err = db.QueryRowContext(ctx,
		"SELECT flow_id FROM microbus_flows WHERE surgraph_step_id=?",
		stepID,
	).Scan(&ownChildFlow)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Trace(err)
	}
	if ownChildFlow > 0 {
		var entry int
		err = db.QueryRowContext(ctx,
			"SELECT step_id FROM microbus_steps WHERE flow_id=? AND predecessor_id=0 ORDER BY step_id LIMIT_OFFSET(1, 0)",
			ownChildFlow,
		).Scan(&entry)
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if entry > 0 {
			effectiveSuccID = entry
		}
	}
	// Walk past any surgraph wrapper that the effective neighbor lands on,
	// descending into the appropriate end of the subgraph (entry for forward,
	// exit for backward). Loop in case of nested subgraphs.
	effectiveSuccID, err = e.skipSurgraphForward(ctx, db, effectiveSuccID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	effectivePredID, err = e.skipSurgraphBackward(ctx, db, effectivePredID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Resolve predecessor/successor step keys for navigation. The cross-flow
	// fallbacks above still land on the same shard (subgraph flows have shard
	// affinity with their parent), so one IN query fetches both rows.
	if effectivePredID > 0 || effectiveSuccID > 0 {
		var ids []any
		if effectivePredID > 0 {
			ids = append(ids, effectivePredID)
		}
		if effectiveSuccID > 0 && effectiveSuccID != effectivePredID {
			ids = append(ids, effectiveSuccID)
		}
		placeholders := strings.Repeat("?,", len(ids))
		placeholders = placeholders[:len(placeholders)-1]
		nrows, err := db.QueryContext(ctx,
			"SELECT step_id, step_token FROM microbus_steps WHERE step_id IN ("+placeholders+")",
			ids...,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer nrows.Close()
		for nrows.Next() {
			var nid int
			var ntoken string
			if err := nrows.Scan(&nid, &ntoken); err != nil {
				return nil, errors.Trace(err)
			}
			key := fmt.Sprintf("%d-%d-%s", shardNum, nid, strings.TrimSpace(ntoken))
			if nid == effectivePredID {
				fs.PrevKey = key
			}
			if nid == effectiveSuccID {
				fs.NextKey = key
			}
		}
	}
	return fs, nil
}

// skipSurgraphForward walks past a surgraph wrapper to the entry step of its child
// subgraph (predecessor_id=0). Loops for nested subgraphs.
func (e *Engine) skipSurgraphForward(ctx context.Context, db *sequel.DB, id int) (int, error) {
	for id > 0 {
		var childFlow int
		err := db.QueryRowContext(ctx,
			"SELECT flow_id FROM microbus_flows WHERE surgraph_step_id=?",
			id,
		).Scan(&childFlow)
		if err == sql.ErrNoRows {
			return id, nil
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		if childFlow == 0 {
			return id, nil
		}
		var entry int
		err = db.QueryRowContext(ctx,
			"SELECT step_id FROM microbus_steps WHERE flow_id=? AND predecessor_id=0 ORDER BY step_id LIMIT_OFFSET(1, 0)",
			childFlow,
		).Scan(&entry)
		if err != nil {
			if err == sql.ErrNoRows {
				return id, nil
			}
			return 0, errors.Trace(err)
		}
		id = entry
	}
	return id, nil
}

// skipSurgraphBackward is the backward counterpart: if id is a surgraph
// wrapper, return the subgraph's exit step (completed, with successor_id=0).
// Loops for nested subgraphs.
func (e *Engine) skipSurgraphBackward(ctx context.Context, db *sequel.DB, id int) (int, error) {
	for id > 0 {
		var childFlow int
		err := db.QueryRowContext(ctx,
			"SELECT flow_id FROM microbus_flows WHERE surgraph_step_id=?",
			id,
		).Scan(&childFlow)
		if err == sql.ErrNoRows {
			return id, nil
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		if childFlow == 0 {
			return id, nil
		}
		var exit int
		err = db.QueryRowContext(ctx,
			"SELECT step_id FROM microbus_steps WHERE flow_id=? AND successor_id=0 AND status='completed' ORDER BY step_id DESC LIMIT_OFFSET(1, 0)",
			childFlow,
		).Scan(&exit)
		if err != nil {
			if err == sql.ErrNoRows {
				return id, nil
			}
			return 0, errors.Trace(err)
		}
		id = exit
	}
	return id, nil
}

func (e *Engine) fingerprint(ctx context.Context, flowKey string) (string, string, error) {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	var status string
	err = db.QueryRowContext(ctx, "SELECT status FROM microbus_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&status)
	if err == sql.ErrNoRows {
		return "", "", errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return "", "", errors.Trace(err)
	}
	status = strings.TrimSpace(status)

	flowIDs := []any{flowID}
	descendants, err := e.allDescendantSubgraphFlows(ctx, db, flowID)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	for _, id := range descendants {
		flowIDs = append(flowIDs, id)
	}

	ph := strings.Repeat("?,", len(flowIDs)-1) + "?"
	var count int
	var maxUpdated sql.NullTime
	err = db.QueryRowContext(ctx, "SELECT COUNT(*), MAX(updated_at) FROM microbus_steps WHERE flow_id IN ("+ph+")", flowIDs...).Scan(&count, &maxUpdated)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	var maxMs int64
	if maxUpdated.Valid {
		maxMs = maxUpdated.Time.UnixMilli()
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%d", status, count, maxMs)))
	return hex.EncodeToString(sum[:]), status, nil
}

func (e *Engine) list(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, string, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	numShards := e.numDBShards()

	joinSQL, whereSQL, baseArgs, restrictShardNum, err := e.queryClauses(ctx, query)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	perShardCursor := map[int]int{}
	if query.Cursor != "" {
		for _, part := range strings.Split(query.Cursor, ",") {
			s, fid, ok := strings.Cut(part, "=")
			if !ok {
				return nil, "", errors.New("malformed cursor", http.StatusBadRequest)
			}
			si, sErr := strconv.Atoi(s)
			fi, fErr := strconv.Atoi(fid)
			if sErr != nil || fErr != nil || si < 1 {
				return nil, "", errors.New("malformed cursor", http.StatusBadRequest)
			}
			perShardCursor[si] = fi
		}
	}

	singleShard := restrictShardNum != 0
	perShardLimit := limit
	if !singleShard && numShards > 0 {
		perShardLimit = (limit + numShards - 1) / numShards
		if perShardLimit < 1 {
			perShardLimit = 1
		}
	}

	type listRow struct {
		summary workflow.FlowSummary
		flowID  int
	}
	perShard := make([][]listRow, numShards+1)

	err = e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if restrictShardNum != 0 && shardIdx != restrictShardNum {
			return nil
		}
		conditions := []string{whereSQL}
		args := append([]any(nil), baseArgs...)
		if cur, ok := perShardCursor[shardIdx]; ok {
			conditions = append(conditions, "f.flow_id<?")
			args = append(args, cur)
		}
		if sc, scArgs := searchClause(db.DriverName(), shardIdx, query.Search); sc != "" {
			conditions = append(conditions, sc)
			args = append(args, scArgs...)
		}
		args = append(args, perShardLimit)
		stmt := "SELECT f.flow_id, f.flow_token, f.thread_id, f.thread_token, f.workflow_name, f.status, s.task_name, f.error, f.cancel_reason, f.created_at, f.started_at, f.updated_at" +
			" FROM microbus_flows f" + joinSQL +
			" WHERE " + strings.Join(conditions, " AND ") +
			" ORDER BY f.flow_id DESC LIMIT_OFFSET(?, 0)"
		rows, err := db.QueryContext(ctx, stmt, args...)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		var shardRows []listRow
		for rows.Next() {
			var lr listRow
			var flowToken, threadToken, flowError, cancelReason string
			var threadID int
			var taskName sql.NullString
			err = rows.Scan(&lr.flowID, &flowToken, &threadID, &threadToken, &lr.summary.WorkflowName, &lr.summary.Status, &taskName, &flowError, &cancelReason, &lr.summary.CreatedAt, &lr.summary.StartedAt, &lr.summary.UpdatedAt)
			if err != nil {
				return errors.Trace(err)
			}
			lr.summary.FlowKey = fmt.Sprintf("%d-%d-%s", shardIdx, lr.flowID, strings.TrimSpace(flowToken))
			lr.summary.ThreadKey = fmt.Sprintf("%d-%d-%s", shardIdx, threadID, strings.TrimSpace(threadToken))
			lr.summary.Status = strings.TrimSpace(lr.summary.Status)
			lr.summary.TaskName = taskName.String
			lr.summary.Error = strings.TrimSpace(flowError)
			lr.summary.CancelReason = strings.TrimSpace(cancelReason)
			shardRows = append(shardRows, lr)
		}
		perShard[shardIdx] = shardRows
		return rows.Err()
	})
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	nextPerShard := map[int]int{}
	for s, fid := range perShardCursor {
		nextPerShard[s] = fid
	}
	var flows []workflow.FlowSummary
	for s := 1; s <= numShards; s++ {
		rows := perShard[s]
		if len(rows) == 0 {
			continue
		}
		nextPerShard[s] = rows[len(rows)-1].flowID
		for _, lr := range rows {
			flows = append(flows, lr.summary)
		}
	}
	anyAdvance := false
	for s, fid := range nextPerShard {
		if cur, had := perShardCursor[s]; !had || cur != fid {
			anyAdvance = true
			break
		}
	}
	var nextCursor string
	if anyAdvance {
		shardOrder := make([]int, 0, len(nextPerShard))
		for s := range nextPerShard {
			shardOrder = append(shardOrder, s)
		}
		sort.Ints(shardOrder)
		var b strings.Builder
		for i, s := range shardOrder {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Itoa(s))
			b.WriteByte('=')
			b.WriteString(strconv.Itoa(nextPerShard[s]))
		}
		nextCursor = b.String()
	}
	return flows, nextCursor, nil
}

func searchClause(driverName string, shardIdx int, search string) (string, []any) {
	if search == "" {
		return "", nil
	}
	pattern := "%" + strings.ToLower(search) + "%"
	var flowKeyExpr string
	switch driverName {
	case "mysql", "mssql":
		flowKeyExpr = fmt.Sprintf("CONCAT('%d-', f.flow_id, '-', TRIM(f.flow_token))", shardIdx)
	default:
		flowKeyExpr = fmt.Sprintf("'%d-' || f.flow_id || '-' || TRIM(f.flow_token)", shardIdx)
	}
	sql := "(LOWER(f.workflow_name) LIKE ? OR LOWER(s.task_name) LIKE ? OR LOWER(f.error) LIKE ? OR LOWER(f.cancel_reason) LIKE ? OR LOWER(" + flowKeyExpr + ") LIKE ?)"
	return sql, []any{pattern, pattern, pattern, pattern, pattern}
}

func (e *Engine) queryClauses(ctx context.Context, query workflow.Query) (string, string, []any, int, error) {
	numShards := e.numDBShards()
	if query.Shard < 0 || query.Shard > numShards {
		return "", "", nil, 0, errors.New("invalid shard", http.StatusBadRequest)
	}
	restrictShardNum := query.Shard

	conditions := []string{"f.surgraph_flow_id=0"}
	var args []any
	if query.Status != "" {
		conditions = append(conditions, "f.status=?")
		args = append(args, query.Status)
	}
	if query.WorkflowName != "" {
		conditions = append(conditions, "f.workflow_name=?")
		args = append(args, query.WorkflowName)
	}
	if query.ThreadKey != "" {
		threadShardNum, threadFlowID, threadFlowToken, parseErr := parseFlowKey(query.ThreadKey)
		if parseErr != nil {
			return "", "", nil, 0, errors.Trace(parseErr)
		}
		db, dErr := e.shard(threadShardNum)
		if dErr != nil {
			return "", "", nil, 0, errors.Trace(dErr)
		}
		var resolvedThreadID int
		err := db.QueryRowContext(ctx, "SELECT thread_id FROM microbus_flows WHERE flow_id=? AND flow_token=?", threadFlowID, threadFlowToken).Scan(&resolvedThreadID)
		if err != nil {
			return "", "", nil, 0, errors.New("flow not found", http.StatusNotFound)
		}
		conditions = append(conditions, "f.thread_id=?")
		args = append(args, resolvedThreadID)
		restrictShardNum = threadShardNum
	}
	if query.TaskName != "" {
		conditions = append(conditions, "s.task_name=?")
		args = append(args, query.TaskName)
	}
	if query.TenantID != 0 {
		conditions = append(conditions, "f.tenant_id=?")
		args = append(args, query.TenantID)
	}
	if query.OlderThan > 0 {
		conditions = append(conditions, "f.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)")
		args = append(args, -int64(query.OlderThan/time.Millisecond))
	}
	if query.NewerThan > 0 {
		conditions = append(conditions, "f.updated_at >= DATE_ADD_MILLIS(NOW_UTC(), ?)")
		args = append(args, -int64(query.NewerThan/time.Millisecond))
	}

	joinSQL := " LEFT JOIN microbus_steps s ON f.step_id = s.step_id"
	whereSQL := strings.Join(conditions, " AND ")
	return joinSQL, whereSQL, args, restrictShardNum, nil
}

func (e *Engine) purge(ctx context.Context, query workflow.Query) (int, error) {
	if query.Status == "" && query.WorkflowName == "" && query.OlderThan == 0 {
		return 0, errors.New("purge requires at least one filter (status, workflowName, or olderThan)", http.StatusBadRequest)
	}
	const purgeCap = 10000
	limit := query.Limit
	if limit <= 0 || limit > purgeCap {
		limit = purgeCap
	}
	numShards := e.numDBShards()

	joinSQL, whereSQL, baseArgs, restrictShardNum, err := e.queryClauses(ctx, query)
	if err != nil {
		return 0, errors.Trace(err)
	}

	singleShard := restrictShardNum != 0
	perShardLimit := limit
	if !singleShard && numShards > 0 {
		perShardLimit = (limit + numShards - 1) / numShards
		if perShardLimit < 1 {
			perShardLimit = 1
		}
	}

	perShardDeleted := make([]int, numShards+1)
	err = e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if restrictShardNum != 0 && shardIdx != restrictShardNum {
			return nil
		}
		where := whereSQL
		args := append([]any(nil), baseArgs...)
		if sc, scArgs := searchClause(db.DriverName(), shardIdx, query.Search); sc != "" {
			where += " AND " + sc
			args = append(args, scArgs...)
		}
		args = append(args, workflow.StatusRunning, perShardLimit)
		selectIDs := "SELECT DISTINCT f.flow_id FROM microbus_flows f" + joinSQL +
			" WHERE " + where + " AND f.status<>? LIMIT_OFFSET(?, 0)"
		rows, err := db.QueryContext(ctx, selectIDs, args...)
		if err != nil {
			return errors.Trace(err)
		}
		var flowIDs []any
		for rows.Next() {
			var fid int
			if err := rows.Scan(&fid); err != nil {
				rows.Close()
				return errors.Trace(err)
			}
			flowIDs = append(flowIDs, fid)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		if len(flowIDs) == 0 {
			return nil
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return errors.Trace(err)
		}
		defer tx.Rollback()

		placeholders := strings.Repeat("?,", len(flowIDs)-1) + "?"
		_, err = tx.ExecContext(ctx,
			"DELETE FROM microbus_steps WHERE flow_id IN ("+placeholders+")",
			flowIDs...,
		)
		if err != nil {
			return errors.Trace(err)
		}
		// Re-guard against the race where a flow transitioned to running between SELECT and DELETE.
		delArgs := append([]any(nil), flowIDs...)
		delArgs = append(delArgs, workflow.StatusRunning)
		res, err := tx.ExecContext(ctx,
			"DELETE FROM microbus_flows WHERE flow_id IN ("+placeholders+") AND status<>?",
			delArgs...,
		)
		if err != nil {
			return errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		perShardDeleted[shardIdx] = int(n)
		return errors.Trace(tx.Commit())
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	total := 0
	for i := 1; i <= numShards; i++ {
		total += perShardDeleted[i]
	}
	return total, nil
}

func (e *Engine) shardInfo(ctx context.Context) ([]ShardSummary, error) {
	numShards := e.numDBShards()
	results := make([]ShardSummary, numShards+1)
	e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		results[shardIdx].Shard = shardIdx
		start := time.Now()
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		results[shardIdx].LatencyMs = int(time.Since(start) / time.Millisecond)
		if err != nil {
			results[shardIdx].Error = err.Error()
			return nil
		}
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM microbus_steps").Scan(&results[shardIdx].Steps)
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM microbus_flows").Scan(&results[shardIdx].Flows)
		return nil
	})
	shards := make([]ShardSummary, 0, numShards)
	for i := 1; i <= numShards; i++ {
		shards = append(shards, results[i])
	}
	return shards, nil
}

func (e *Engine) continueFlow(ctx context.Context, threadKey string, additionalState any, opts *workflow.FlowOptions) (string, error) {
	shardNum, flowID, flowToken, err := parseFlowKey(threadKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}
	opts = e.resolveFlowOptions(opts, nil)

	var threadID int
	var threadToken string
	err = db.QueryRowContext(ctx, "SELECT thread_id, thread_token FROM microbus_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&threadID, &threadToken)
	if err != nil {
		return "", errors.New("flow not found", http.StatusNotFound)
	}
	threadToken = strings.TrimSpace(threadToken)

	var flowStatus, finalStateJSON, graphJSON, workflowName string
	err = db.QueryRowContext(ctx,
		"SELECT status, final_state, graph, workflow_name FROM microbus_flows WHERE thread_id=? ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
		threadID,
	).Scan(&flowStatus, &finalStateJSON, &graphJSON, &workflowName)
	if err != nil {
		return "", errors.New("no flows found in thread", http.StatusNotFound)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	if flowStatus != workflow.StatusCompleted {
		return "", errors.New("latest flow in thread is not completed (status: %s)", flowStatus, http.StatusConflict)
	}

	var finalState map[string]any
	if err = json.Unmarshal([]byte(finalStateJSON), &finalState); err != nil {
		return "", errors.Trace(err)
	}
	var graph workflow.Graph
	if err = json.Unmarshal([]byte(graphJSON), &graph); err != nil {
		return "", errors.Trace(err)
	}

	mergedState, err := workflow.MergeState(finalState, additionalState, graph.Reducers())
	if err != nil {
		return "", errors.Trace(err)
	}

	return e.createWithGraph(ctx, shardNum, workflowName, &graph, mergedState, nil, threadID, threadToken, opts)
}

func (e *Engine) setBreakpoint(ctx context.Context, flowKey string, key string, enabled bool) error {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	var breakpointsJSON string
	err = db.QueryRowContext(ctx, "SELECT breakpoints FROM microbus_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&breakpointsJSON)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	breakpoints := map[string]string{}
	json.Unmarshal([]byte(breakpointsJSON), &breakpoints)
	if enabled {
		breakpoints[key] = "b"
	} else {
		delete(breakpoints, key)
	}
	updatedJSON, _ := json.Marshal(breakpoints)
	_, err = db.ExecContext(ctx, "UPDATE microbus_flows SET breakpoints=?, updated_at=NOW_UTC() WHERE flow_id=? AND flow_token=?", string(updatedJSON), flowID, flowToken)
	return errors.Trace(err)
}
