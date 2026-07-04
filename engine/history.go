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

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

func (e *Engine) history(ctx context.Context, flowKey string) ([]workflow.FlowStep, error) {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// delete_after_ms=0 excludes a flow scheduled for deletion: History is the full step detail a disposable
	// flow is discarding, so it 404s during the grace window (Snapshot/Await still serve the outcome).
	var exists int
	err = db.QueryRowContext(ctx, "SELECT 1 FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND delete_after_ms=0", flowID, flowToken).Scan(&exists)
	if err == sql.ErrNoRows {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Load the whole subgraph tree in a fixed two queries via root_flow_id (single-shard by parent-shard
	// affinity), then assemble the nested history in memory - instead of a per-step surgraph_step_id lookup,
	// an N+1 over every step in the tree. This mirrors the root_flow_id membership walks in completion.go.
	// Starting from a mid-tree flow (a subgraph child, which History accepts) over-fetches the ancestor and
	// sibling subtrees, but the assembly only descends from flowID, so the extra rows are never rendered.
	childByCaller, err := e.loadSubgraphChildren(ctx, db, flowID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	stepsByFlow, err := e.loadTreeSteps(ctx, db, shardNum, flowID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return assembleHistory(flowID, childByCaller, stepsByFlow), nil
}

// subgraphChild is the latest child flow launched by a surgraph caller step, used to attach subhistory
// during in-memory History assembly.
type subgraphChild struct {
	flowID       int
	workflowURL  string
	workflowName string
}

// loadSubgraphChildren returns, for the whole subgraph tree flowID belongs to, a map from each surgraph
// caller step's id to the latest child flow it launched. flow.Retry rewinds a caller in place and re-spawns
// a fresh child each attempt, so several child flows can share one surgraph_step_id; ORDER BY flow_id ASC
// means the last row written for a caller step wins, giving the highest flow_id - matching subgraphHistory's
// former ORDER BY flow_id DESC LIMIT 1 (the child whose value the caller actually used).
func (e *Engine) loadSubgraphChildren(ctx context.Context, db *sequel.DB, flowID int) (map[int]subgraphChild, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, surgraph_step_id, workflow_url, workflow_name FROM dwarf_flows"+
			" WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?) AND surgraph_step_id>0 ORDER BY flow_id",
		flowID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	childByCaller := map[int]subgraphChild{}
	for rows.Next() {
		var childFlowID, callerStepID int
		var url, name string
		err := rows.Scan(&childFlowID, &callerStepID, &url, &name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		childByCaller[callerStepID] = subgraphChild{
			flowID:       childFlowID,
			workflowURL:  strings.TrimSpace(url),
			workflowName: strings.TrimSpace(name),
		}
	}
	return childByCaller, errors.Trace(rows.Err())
}

// loadTreeSteps loads every step of every flow in flowID's subgraph tree (one query, keyed on the
// root_flow_id membership index), grouped by owning flow_id and ordered within each flow.
func (e *Engine) loadTreeSteps(ctx context.Context, db *sequel.DB, shardNum int, flowID int) (map[int][]workflow.FlowStep, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, step_id, step_token, step_depth, task_name, attempt, status, error, created_at, started_at, updated_at, predecessor_id, successor_id, parked FROM dwarf_steps"+
			" WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)) ORDER BY flow_id, step_depth, step_id",
		flowID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	stepsByFlow := map[int][]workflow.FlowStep{}
	for rows.Next() {
		var step workflow.FlowStep
		var stepFlowID, stepID, parked int
		var stepToken, errMsg string
		err := rows.Scan(&stepFlowID, &stepID, &stepToken, &step.StepDepth, &step.TaskName, &step.Attempt, &step.Status, &errMsg, &step.CreatedAt, &step.StartedAt, &step.UpdatedAt, &step.PredecessorID, &step.SuccessorID, &parked)
		if err != nil {
			return nil, errors.Trace(err)
		}
		step.StepID = stepID
		step.Parked = parked != 0
		step.StepKey = fmt.Sprintf("%d-%d-%s", shardNum, stepID, strings.TrimSpace(stepToken))
		step.Status = strings.TrimSpace(step.Status)
		step.Error = strings.TrimSpace(errMsg)
		stepsByFlow[stepFlowID] = append(stepsByFlow[stepFlowID], step)
	}
	return stepsByFlow, errors.Trace(rows.Err())
}

// assembleHistory builds one flow's history from the pre-loaded tree maps, recursively attaching each
// surgraph caller step's latest child subhistory. Purely in memory - no per-step query. Each flow has one
// surgraph parent, so the recursion visits every flow at most once (no cycle guard needed).
func assembleHistory(flowID int, childByCaller map[int]subgraphChild, stepsByFlow map[int][]workflow.FlowStep) []workflow.FlowStep {
	steps := stepsByFlow[flowID]
	for i := range steps {
		child, ok := childByCaller[steps[i].StepID]
		if !ok {
			continue
		}
		sub := assembleHistory(child.flowID, childByCaller, stepsByFlow)
		if len(sub) > 0 {
			steps[i].Subgraph = true
			steps[i].SubWorkflowURL = child.workflowURL
			steps[i].SubWorkflowName = child.workflowName
			steps[i].SubHistory = sub
		}
	}
	return steps
}

func (e *Engine) step(ctx context.Context, stepKey string) (*workflow.FlowStep, error) {
	shardNum, stepID, stepToken, err := keys.ParseStepKey(stepKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var taskName, statusStr, errMsg string
	var stateJSON, changesJSON, interruptJSON string
	var stepDepth, attempt, predID, succID int
	var createdAt, updatedAt time.Time
	err = db.QueryRowContext(ctx,
		"SELECT step_depth, task_name, attempt, state, changes, interrupt_payload, status, error, created_at, updated_at, predecessor_id, successor_id FROM dwarf_steps WHERE step_id=? AND step_token=?",
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
			"SELECT f.surgraph_step_id FROM dwarf_steps s JOIN dwarf_flows f ON s.flow_id = f.flow_id WHERE s.step_id=?",
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
				"SELECT predecessor_id, successor_id FROM dwarf_steps WHERE step_id=?",
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
		"SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=?",
		stepID,
	).Scan(&ownChildFlow)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Trace(err)
	}
	if ownChildFlow > 0 {
		var entry int
		err = db.QueryRowContext(ctx,
			"SELECT step_id FROM dwarf_steps WHERE flow_id=? AND predecessor_id=0 ORDER BY step_id LIMIT_OFFSET(1, 0)",
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
			"SELECT step_id, step_token FROM dwarf_steps WHERE step_id IN ("+placeholders+")",
			ids...,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer nrows.Close()
		for nrows.Next() {
			var nid int
			var ntoken string
			err := nrows.Scan(&nid, &ntoken)
			if err != nil {
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
		if err := nrows.Err(); err != nil {
			return nil, errors.Trace(err)
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
			"SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=?",
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
			"SELECT step_id FROM dwarf_steps WHERE flow_id=? AND predecessor_id=0 ORDER BY step_id LIMIT_OFFSET(1, 0)",
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
			"SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=?",
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
			"SELECT step_id FROM dwarf_steps WHERE flow_id=? AND successor_id=0 AND status='completed' ORDER BY step_id DESC LIMIT_OFFSET(1, 0)",
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
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	var status string
	err = db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&status)
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
	// MAX(updated_at) is an untyped aggregate expression; SQLite returns it as a
	// string (no column affinity) while other dialects return a time value. Scan
	// into any and hash its string form — the fingerprint only needs a stable,
	// change-detecting digest, not a parsed timestamp.
	var maxUpdated any
	err = db.QueryRowContext(ctx, "SELECT COUNT(*), MAX(updated_at) FROM dwarf_steps WHERE flow_id IN ("+ph+")", flowIDs...).Scan(&count, &maxUpdated)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	if b, ok := maxUpdated.([]byte); ok {
		maxUpdated = string(b)
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d|%v", status, count, maxUpdated)))
	return hex.EncodeToString(sum[:]), status, nil
}

func (e *Engine) list(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, string, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	numShards := e.db.NumShards()

	joinSQL, whereSQL, baseArgs, restrictShardNum, err := e.queryClauses(ctx, query, subgraphCondition(query.IncludeSubgraphs))
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

	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if restrictShardNum != 0 && shardIdx != restrictShardNum {
			return nil
		}
		// Exclude flows scheduled for deletion (delete_after_ms>0): they are logically gone, awaiting the reaper.
		conditions := []string{whereSQL, "f.delete_after_ms=0"}
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
		stmt := "SELECT f.flow_id, f.flow_token, f.thread_id, f.thread_token, f.workflow_url, f.workflow_name, f.status, s.task_name, f.error, f.cancel_reason, f.created_at, f.started_at, f.updated_at, f.priority, f.fairness_key, f.surgraph_flow_id, f.trace_parent" +
			" FROM dwarf_flows f" + joinSQL +
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
			var flowToken, threadToken, flowError, cancelReason, traceParent string
			var threadID, surgraphFlowID int
			var taskName sql.NullString
			err = rows.Scan(&lr.flowID, &flowToken, &threadID, &threadToken, &lr.summary.WorkflowURL, &lr.summary.WorkflowName, &lr.summary.Status, &taskName, &flowError, &cancelReason, &lr.summary.CreatedAt, &lr.summary.StartedAt, &lr.summary.UpdatedAt, &lr.summary.Priority, &lr.summary.FairnessKey, &surgraphFlowID, &traceParent)
			if err != nil {
				return errors.Trace(err)
			}
			lr.summary.Subgraph = surgraphFlowID != 0
			lr.summary.TraceID = traceIDFromParent(traceParent)
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

// likeEscape is the LIKE ESCAPE character used by searchClause. It is a printable char that is neither a
// LIKE metacharacter nor special inside a string literal on any of the four dialects (unlike backslash,
// which MySQL and Postgres treat differently in literals), so `ESCAPE '~'` is portable as-is.
const likeEscape = "~"

var likeEscaper = strings.NewReplacer(likeEscape, likeEscape+likeEscape, "%", likeEscape+"%", "_", likeEscape+"_")

func searchClause(driverName string, shardIdx int, search string) (string, []any) {
	if search == "" {
		return "", nil
	}
	// Escape the caller-supplied term's LIKE metacharacters (% and _) and the escape char itself before
	// wrapping in %...%, so a search for "a_b" or "50%" matches literally instead of being steered into an
	// unbounded wildcard full scan. Every LIKE below carries the matching ESCAPE clause. No injection risk
	// either way (the term is a bind parameter); this is purely about scan cost.
	pattern := "%" + likeEscaper.Replace(strings.ToLower(search)) + "%"
	var flowKeyExpr string
	switch driverName {
	case "mysql", "mssql":
		flowKeyExpr = fmt.Sprintf("CONCAT('%d-', f.flow_id, '-', TRIM(f.flow_token))", shardIdx)
	default:
		flowKeyExpr = fmt.Sprintf("'%d-' || f.flow_id || '-' || TRIM(f.flow_token)", shardIdx)
	}
	e := " ESCAPE '" + likeEscape + "'"
	sql := "(LOWER(f.workflow_url) LIKE ?" + e + " OR LOWER(f.workflow_name) LIKE ?" + e + " OR LOWER(s.task_name) LIKE ?" + e + " OR LOWER(f.error) LIKE ?" + e + " OR LOWER(f.cancel_reason) LIKE ?" + e + " OR LOWER(" + flowKeyExpr + ") LIKE ?" + e + ")"
	return sql, []any{pattern, pattern, pattern, pattern, pattern, pattern}
}

// subgraphCondition maps Query.IncludeSubgraphs to the surgraph_flow_id predicate: the default excludes
// subgraph children (roots only), IncludeSubgraphs returns both kinds.
func subgraphCondition(includeSubgraphs bool) string {
	if includeSubgraphs {
		return "1=1" // roots and subgraph children
	}
	return "f.surgraph_flow_id=0" // roots only (default)
}

// queryClauses builds the shared WHERE/JOIN for list and purge. subgraphCond is the surgraph_flow_id
// predicate the caller chose: list honors Query.Subgraph, purge always passes roots-only.
func (e *Engine) queryClauses(ctx context.Context, query workflow.Query, subgraphCond string) (string, string, []any, int, error) {
	numShards := e.db.NumShards()
	if query.Shard < 0 || query.Shard > numShards {
		return "", "", nil, 0, errors.New("invalid shard", http.StatusBadRequest)
	}
	restrictShardNum := query.Shard

	conditions := []string{subgraphCond}
	var args []any
	if query.Status != "" {
		// Inline the status as a literal (like every other status predicate) so SQL Server reads the
		// histogram and index-seeks a rare status instead of a clustered scan driven by the ORDER BY
		// flow_id DESC + LIMIT under a parameter's density-average estimate. Validated against the known
		// set first, so only an engine constant - never caller input - is concatenated (no injection).
		if !workflow.IsValidStatus(query.Status) {
			return "", "", nil, 0, errors.New("invalid status filter: %q", query.Status, http.StatusBadRequest)
		}
		conditions = append(conditions, "f.status='"+query.Status+"'")
	}
	if query.WorkflowURL != "" {
		conditions = append(conditions, "f.workflow_url=?")
		args = append(args, query.WorkflowURL)
	}
	if query.WorkflowName != "" {
		conditions = append(conditions, "f.workflow_name=?")
		args = append(args, query.WorkflowName)
	}
	if query.ThreadKey != "" {
		threadShardNum, threadFlowID, threadFlowToken, parseErr := keys.ParseFlowKey(query.ThreadKey)
		if parseErr != nil {
			return "", "", nil, 0, errors.Trace(parseErr)
		}
		db, dErr := e.db.Shard(threadShardNum)
		if dErr != nil {
			return "", "", nil, 0, errors.Trace(dErr)
		}
		var resolvedThreadID int
		err := db.QueryRowContext(ctx, "SELECT thread_id FROM dwarf_flows WHERE flow_id=? AND flow_token=?", threadFlowID, threadFlowToken).Scan(&resolvedThreadID)
		if err == sql.ErrNoRows {
			return "", "", nil, 0, errors.New("flow not found", http.StatusNotFound)
		}
		if err != nil {
			return "", "", nil, 0, errors.Trace(err)
		}
		conditions = append(conditions, "f.thread_id=?")
		args = append(args, resolvedThreadID)
		restrictShardNum = threadShardNum
	}
	if query.TaskName != "" {
		conditions = append(conditions, "s.task_name=?")
		args = append(args, query.TaskName)
	}
	if query.FairnessKey != "" {
		conditions = append(conditions, "f.fairness_key=?")
		args = append(args, query.FairnessKey)
	}
	if query.Priority != 0 {
		conditions = append(conditions, "f.priority=?")
		args = append(args, query.Priority)
	}
	if query.OlderThan > 0 {
		conditions = append(conditions, "f.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)")
		args = append(args, -int64(query.OlderThan/time.Millisecond))
	}
	if query.NewerThan > 0 {
		conditions = append(conditions, "f.updated_at >= DATE_ADD_MILLIS(NOW_UTC(), ?)")
		args = append(args, -int64(query.NewerThan/time.Millisecond))
	}

	joinSQL := " LEFT JOIN dwarf_steps s ON f.step_id = s.step_id"
	whereSQL := strings.Join(conditions, " AND ")
	return joinSQL, whereSQL, args, restrictShardNum, nil
}

// intCSV renders ids as a comma-separated list for direct embedding in a SQL IN (...) clause. The ids are
// trusted integers scanned from the engine's own query, so there is no injection surface.
func intCSV(ids []int) string {
	var b strings.Builder
	for i, id := range ids {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Itoa(id))
	}
	return b.String()
}

func (e *Engine) purge(ctx context.Context, query workflow.Query) (int, error) {
	if query.Status == "" && query.WorkflowURL == "" && query.WorkflowName == "" && query.OlderThan == 0 {
		return 0, errors.New("purge requires at least one filter (status, workflowURL, workflowName, or olderThan)", http.StatusBadRequest)
	}
	// A subgraph child cannot be purged independently - it is removed as part of its root's subtree. Reject
	// IncludeSubgraphs rather than silently ignoring it, so a caller is not surprised the flag did nothing.
	if query.IncludeSubgraphs {
		return 0, errors.New("purge cannot include subgraphs; a subgraph child is purged with its root", http.StatusBadRequest)
	}
	// Cap the matched-root count per call so the per-shard id list embedded into each DELETE's `IN (...)`
	// predicate stays well under the size where the optimizer struggles. This is not just a slow-plan concern:
	// on SQL Server a large enough literal IN-list can *fail* to plan outright (errors 8623 "query processor
	// ran out of internal resources" / 8632 "expression services limit reached"), a hard error rather than a
	// slow query - which is why there is a hard cap. That failure regime is in the tens of thousands of terms;
	// 4096 keeps an order-of-magnitude of headroom under it while still purging in useful batches. It is a
	// single unbatched statement (ids embedded as integer literals via intCSV, so the per-driver bind-param
	// ceiling - SQL Server 2100 / older SQLite 999 - does not apply; the ceiling here is IN-list planning, not
	// statement size). A caller trimming more loops Purge until it returns 0.
	const purgeCap = 4096
	limit := query.Limit
	if limit <= 0 || limit > purgeCap {
		limit = purgeCap
	}
	numShards := e.db.NumShards()

	// Purge selects root flows only and deletes each matched root's whole subgraph subtree (below).
	joinSQL, whereSQL, baseArgs, restrictShardNum, err := e.queryClauses(ctx, query, "f.surgraph_flow_id=0")
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
	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if restrictShardNum != 0 && shardIdx != restrictShardNum {
			return nil
		}
		where := whereSQL
		args := append([]any(nil), baseArgs...)
		if sc, scArgs := searchClause(db.DriverName(), shardIdx, query.Search); sc != "" {
			where += " AND " + sc
			args = append(args, scArgs...)
		}
		args = append(args, perShardLimit)
		// Purge MARKS, it does not delete: select candidate roots (not running, not already scheduled), then
		// stamp delete_after_ms=1 (due immediately) so the reaper removes each tree on its next pass. Deleting
		// nothing inline is what closes the old strand race - and the stamp UPDATE re-guards status<>running
		// under the row lock, so a Resume racing in after this SELECT either wins (row is running, excluded) or
		// loses (we stamp, and its interrupted CAS then finds cancelled). No lock-first re-selection needed.
		selectIDs := "SELECT DISTINCT f.flow_id FROM dwarf_flows f" + joinSQL +
			" WHERE " + where + " AND f.status<>'" + workflow.StatusRunning + "' AND f.delete_after_ms=0 ORDER BY f.flow_id LIMIT_OFFSET(?, 0)"
		rows, err := db.QueryContext(ctx, selectIDs, args...)
		if err != nil {
			return errors.Trace(err)
		}
		var flowIDs []int
		for rows.Next() {
			var fid int
			err := rows.Scan(&fid)
			if err != nil {
				rows.Close()
				return errors.Trace(err)
			}
			flowIDs = append(flowIDs, fid)
		}
		rows.Close()
		err = rows.Err()
		if err != nil {
			return errors.Trace(err)
		}
		if len(flowIDs) == 0 {
			return nil
		}

		// One set-based UPDATE per shard. The CASE terminalizes any interrupted root (interrupted -> cancelled)
		// in the same write, preserving the Resume gate. ids are trusted integers embedded as literals to dodge
		// the per-driver bind-param ceiling. The count returned is roots MARKED (reaped shortly after).
		ids := intCSV(flowIDs)
		return db.Transact(ctx, func(tx *sequel.Tx) error {
			res, err := tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET delete_after_ms=1, status=CASE WHEN status='"+workflow.StatusInterrupted+"' THEN '"+workflow.StatusCancelled+"' ELSE status END"+
					" WHERE flow_id IN ("+ids+") AND status<>'"+workflow.StatusRunning+"' AND delete_after_ms=0",
			)
			if err != nil {
				return errors.Trace(err)
			}
			n, _ := res.RowsAffected()
			perShardDeleted[shardIdx] = int(n)
			return nil
		})
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
	numShards := e.db.NumShards()
	results := make([]ShardSummary, numShards+1)
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		results[shardIdx].Shard = shardIdx
		start := time.Now()
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		results[shardIdx].LatencyMs = int(time.Since(start) / time.Millisecond)
		if err != nil {
			results[shardIdx].Error = err.Error()
			return nil
		}
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps").Scan(&results[shardIdx].Steps)
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows").Scan(&results[shardIdx].Flows)
		return nil
	})
	shards := make([]ShardSummary, 0, numShards)
	for i := 1; i <= numShards; i++ {
		shards = append(shards, results[i])
	}
	return shards, nil
}

func (e *Engine) continueFlow(ctx context.Context, threadKey string, additionalState any) (string, error) {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(threadKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}

	var threadID int
	var threadToken string
	var surgraphFlowID int
	err = db.QueryRowContext(ctx, "SELECT thread_id, thread_token, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&threadID, &threadToken, &surgraphFlowID)
	if err == sql.ErrNoRows {
		return "", errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	// A subgraph child runs in its own thread (subgraphs never join the parent's continuation chain), so
	// continuing one would spin up a detached top-level flow from the subgraph's final state - not a thread
	// turn. Continue must be addressed by a root flow's key.
	if surgraphFlowID != 0 {
		return "", errors.New("cannot continue a subgraph child; use a root flow key", http.StatusBadRequest)
	}
	threadToken = strings.TrimSpace(threadToken)

	// Create the new turn atomically with a re-check of the thread's latest turn, all under a write-first
	// lock on the thread anchor row (flow_id == threadID). Without the lock, two concurrent Continues on
	// one thread could both read turn N as the latest completed turn and both insert a successor - silently
	// branching a thread that is meant to be linear. The lock serializes them: the winner inserts its new
	// (running) turn; every other concurrent Continue then reads THAT running turn as the latest and is
	// rejected with 409 (a next turn is already in flight). This makes the outcome deterministic - exactly
	// one Continue succeeds per race - rather than timing-dependent. touch is the non-indexed lock-grab
	// column, so the anchor's updated_at stays frozen at its own last status transition; and because
	// interrupt/cancel/resume never lock a thread SIBLING's rows, this anchor lock cannot cycle with them.
	newFlowToken := keys.RandomIdentifier(16)
	newStepToken := keys.RandomIdentifier(16)
	var newFlowID, newStepID int64
	var newWorkflowURL string
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		// Write-first: grab the thread anchor's row lock to serialize concurrent Continues on this thread.
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=?", threadID); err != nil {
			return errors.Trace(err)
		}

		// The new turn inherits the latest completed turn's full policy (scheduling, baggage).
		// Exclude debug forks: a Fork shares the thread_id for List grouping but must never become a
		// production Continue's base (forked_from_step<>0 marks a fork).
		var flowStatus, finalStateJSON, graphJSON, workflowURL, baggageJSON, fairnessKey string
		var priority, timeBudgetMs int
		var fairnessWeight float64
		err := tx.QueryRowContext(ctx,
			// delete_after_ms=0 skips turns scheduled for deletion, so Continue builds on the latest UNDELETED
			// turn even when the newest turn is on its way out (its state was copied here before the reap).
			"SELECT status, final_state, graph, workflow_url, baggage, priority, fairness_key, fairness_weight, time_budget_ms FROM dwarf_flows WHERE thread_id=? AND forked_from_step=0 AND delete_after_ms=0 ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
			threadID,
		).Scan(&flowStatus, &finalStateJSON, &graphJSON, &workflowURL, &baggageJSON, &priority, &fairnessKey, &fairnessWeight, &timeBudgetMs)
		if err == sql.ErrNoRows {
			return errors.New("no flows found in thread", http.StatusNotFound)
		}
		if err != nil {
			return errors.Trace(err)
		}
		if strings.TrimSpace(flowStatus) != workflow.StatusCompleted {
			return errors.New("latest flow in thread is not completed (status: %s)", strings.TrimSpace(flowStatus), http.StatusConflict)
		}

		var finalState map[string]any
		if err := json.Unmarshal([]byte(finalStateJSON), &finalState); err != nil {
			return errors.Trace(err)
		}
		var graph workflow.Graph
		if err := json.Unmarshal([]byte(graphJSON), &graph); err != nil {
			return errors.Trace(err)
		}
		entryPoint := graph.EntryPoint()
		if entryPoint == "" {
			return errors.New("workflow has no entry point", http.StatusBadRequest)
		}
		mergedState, err := workflow.MergeState(finalState, additionalState, graph.Reducers())
		if err != nil {
			return errors.Trace(err)
		}
		mergedStateJSON, err := json.Marshal(mergedState)
		if err != nil {
			return errors.Trace(err)
		}

		// Inherit the thread's baggage; DeleteOnCompletion is forced off (a disposable flow deletes itself,
		// so it could never have been a Continue source). A turn wanting different policy uses Create with
		// FlowOptions.ThreadKey instead.
		var inheritedBaggage map[string]any
		unmarshalJSONMap(baggageJSON, &inheritedBaggage)
		baggageOut, err := json.Marshal(inheritedBaggage)
		if err != nil {
			return errors.Trace(err)
		}
		timeBudget := time.Duration(timeBudgetMs) * time.Millisecond
		if timeBudget <= 0 {
			timeBudget = e.taskTimeBudget()
		}
		newWorkflowURL = workflowURL

		// A Continue turn starts its own trace (fresh detached root span, empty parent) and no surgraph
		// linkage (threadID/threadToken carried, surgraph/root ids left 0 to self-root).
		seed := flowSeed{
			workflowURL:    workflowURL,
			workflowName:   graph.Name(),
			graphJSON:      graphJSON,
			baggageJSON:    string(baggageOut),
			stateJSON:      string(mergedStateJSON),
			traceParent:    e.mintWorkflowSpan(ctx, workflowURL, ""),
			flowToken:      newFlowToken,
			stepToken:      newStepToken,
			entryPoint:     entryPoint,
			entryURL:       dispatchURLOf(&graph, entryPoint),
			timeBudgetMs:   timeBudget.Milliseconds(),
			threadID:       threadID,
			threadToken:    threadToken,
			priority:       priority,
			fairnessKey:    fairnessKey,
			fairnessWeight: fairnessWeight,
		}
		newFlowID, newStepID, err = insertFlowTx(ctx, tx, seed)
		return err
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	flowKey := fmt.Sprintf("%d-%d-%s", shardNum, newFlowID, newFlowToken)
	e.logger.DebugContext(ctx, "Flow continued and started", "workflow", newWorkflowURL)
	e.metricFlowStarted(ctx, newWorkflowURL)
	// Ring the doorbell so a replica with spare capacity claims the entry step immediately.
	e.enqueueStep(ctx, shardNum, int(newStepID))
	return flowKey, nil
}
