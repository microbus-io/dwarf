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
	"database/sql"
	"fmt"
	"maps"
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

func (e *Engine) list(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, string, error) {
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	indices, pos := e.shardOrdinals()

	joinSQL, whereSQL, baseArgs, restrictShardNum, err := e.queryClauses(ctx, query, subgraphCondition(query.IncludeSubgraphs))
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	perShardCursor := map[int]int{}
	if query.Cursor != "" {
		for part := range strings.SplitSeq(query.Cursor, ",") {
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
	if !singleShard && len(indices) > 0 {
		perShardLimit = max((limit+len(indices)-1)/len(indices), 1)
	}

	type listRow struct {
		summary workflow.FlowSummary
		flowID  int
	}
	perShard := make([][]listRow, len(indices))

	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if restrictShardNum != 0 && shardIdx != restrictShardNum {
			return nil
		}
		// A turn on each shard it reads, so this queues for that shard's connections like any other caller
		// instead of taking them from work already under way.
		ctx, doneTurn := e.opTurn(ctx, shardIdx, "list")
		defer doneTurn()
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
		// The thread key is the thread's anchor flow's key, so it takes that row's creation hour. An anchor that was
		// deleted leaves the key unresolvable whatever its hour, as it always was.
		stmt := "SELECT f.flow_id, f.flow_token, f.created_hour, f.thread_id, f.thread_token, COALESCE(t.created_hour, 0), f.workflow_url, f.workflow_name, f.status, s.task_name, f.error, f.cancel_reason, f.created_at, f.started_at, f.updated_at, f.priority, f.fairness_key, f.surgraph_flow_id, f.trace_parent" +
			" FROM dwarf_flows f LEFT JOIN dwarf_flows t ON t.flow_id=f.thread_id" + joinSQL +
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
			var flowHour, threadID, threadHour, surgraphFlowID int
			var taskName sql.NullString
			err = rows.Scan(&lr.flowID, &flowToken, &flowHour, &threadID, &threadToken, &threadHour, &lr.summary.WorkflowURL, &lr.summary.WorkflowName, &lr.summary.Status, &taskName, &flowError, &cancelReason, &lr.summary.CreatedAt, &lr.summary.StartedAt, &lr.summary.UpdatedAt, &lr.summary.Priority, &lr.summary.FairnessKey, &surgraphFlowID, &traceParent)
			if err != nil {
				return errors.Trace(err)
			}
			lr.summary.Subgraph = surgraphFlowID != 0
			lr.summary.TraceID = traceIDFromParent(traceParent)
			lr.summary.FlowKey = keys.NewFlowKey(shardIdx, flowHour, lr.flowID, flowToken)
			lr.summary.ThreadKey = keys.NewFlowKey(shardIdx, threadHour, threadID, threadToken)
			lr.summary.StoppedTaskName = taskName.String
			lr.summary.Error = strings.TrimSpace(flowError)
			// cancel_reason is the shared reason column both Terminate and Cancel write into;
			// splitTerminateCancelReason is the one place that maps it onto the two reason fields, shared
			// with snapshot()'s FlowOutcome.
			lr.summary.TerminateReason, lr.summary.CancelReason = splitTerminateCancelReason(lr.summary.Status, strings.TrimSpace(cancelReason))
			shardRows = append(shardRows, lr)
		}
		perShard[pos[shardIdx]] = shardRows
		return rows.Err()
	})
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	nextPerShard := map[int]int{}
	maps.Copy(nextPerShard, perShardCursor)
	var flows []workflow.FlowSummary
	for _, s := range indices {
		rows := perShard[pos[s]]
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
		flowKeyExpr = fmt.Sprintf("CONCAT('%d-', f.created_hour, '-', f.flow_id, '-', TRIM(f.flow_token))", shardIdx)
	default:
		flowKeyExpr = fmt.Sprintf("'%d-' || f.created_hour || '-' || f.flow_id || '-' || TRIM(f.flow_token)", shardIdx)
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
// predicate the caller chose: list honors Query.IncludeSubgraphs, purge always passes roots-only.
func (e *Engine) queryClauses(ctx context.Context, query workflow.Query, subgraphCond string) (string, string, []any, int, error) {
	if query.Shard != 0 && !e.db.Has(query.Shard) {
		return "", "", nil, 0, errors.New("invalid shard", http.StatusBadRequest)
	}
	restrictShardNum := query.Shard
	// A negative duration would otherwise add no condition, and Purge's guard counts it as a filter: it would
	// purge every flow (a time.Until on a past time yields one).
	if query.OlderThan < 0 || query.NewerThan < 0 {
		return "", "", nil, 0, errors.New("OlderThan and NewerThan cannot be negative", http.StatusBadRequest)
	}

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
		threadShardNum, threadFlowHour, threadFlowID, threadFlowToken, parseErr := keys.ParseFlowKey(query.ThreadKey)
		if parseErr != nil {
			return "", "", nil, 0, errors.Trace(parseErr)
		}
		db, dErr := e.db.Shard(threadShardNum)
		if dErr != nil {
			return "", "", nil, 0, errors.Trace(dErr)
		}
		// Its own turn: this resolve runs BEFORE the per-shard fan-out below takes one, so it would
		// otherwise be the one read of the operation that goes unordered.
		tctx, doneTurn := e.dbTurn(ctx, threadShardNum)
		var resolvedThreadID int
		err := db.QueryRowContext(tctx, "SELECT thread_id FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND created_hour=?", threadFlowID, threadFlowToken, threadFlowHour).Scan(&resolvedThreadID)
		doneTurn()
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
	if query.StoppedTaskName != "" {
		conditions = append(conditions, "s.task_name=?")
		args = append(args, query.StoppedTaskName)
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

	joinSQL := " LEFT JOIN dwarf_steps s ON f.stopped_step_id = s.step_id"
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
