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
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// maxListLimit is the most flows ListLive and ListArchived return in a call. It must stay above the reaper's batch
// (reapBatch roots, about reapBatchRows flows): a batch's trees share one archived_at, and a caller paging ListArchived
// by window cannot page across a tie larger than its Limit (TestList_TheLimitIsAboveAReaperBatch). A batch takes at
// least one tree however large, so with IncludeSubgraphs one tree of more flows than this is a tie no walk pages across.
const maxListLimit = 65536

// maxEndedWindowDays is the most archive days a ListArchived window may reach, after clamping: today and the 30 before
// it, the days the default retention keeps. Each day is one more pair of tables in the shard's statement, and
// PostgreSQL locks every index of a table it plans: about 11 locks a day (the flows table and its 7 indexes counting
// the key, the steps table and its 2), so 31 days are about 340 - some 5% of the default shared lock table (64 per
// connection) for one statement.
const maxEndedWindowDays = 31

// listRow is one flow a list read, with what the merge orders it by.
type listRow struct {
	summary workflow.FlowSummary
	shard   int
	flowID  int
	at      time.Time // archived_at for an archived flow, updated_at for a live one
}

// summaryColumns are what a list reads of a flow, f being its flows table and s its steps table. The names are unique,
// so a union of them can be selected from by name.
const summaryColumns = "f.flow_id, f.flow_token, f.created_hour, f.workflow_url, f.workflow_name, f.status, s.task_name," +
	" f.error, f.cancel_reason, f.created_at, f.started_at, f.updated_at, f.priority, f.fairness_key, f.surgraph_flow_id," +
	" f.trace_parent, f.archived_at"

// scanListRow reads one row of summaryColumns from shardIdx.
func scanListRow(rows *sequel.Rows, shardIdx int, archived bool) (listRow, error) {
	lr := listRow{shard: shardIdx}
	var flowToken, flowError, cancelReason, traceParent string
	var flowHour, surgraphFlowID int
	var taskName sql.NullString
	var archivedAt time.Time
	err := rows.Scan(&lr.flowID, &flowToken, &flowHour, &lr.summary.WorkflowURL, &lr.summary.WorkflowName,
		&lr.summary.Status, &taskName, &flowError, &cancelReason, &lr.summary.CreatedAt, &lr.summary.StartedAt,
		&lr.summary.UpdatedAt, &lr.summary.Priority, &lr.summary.FairnessKey, &surgraphFlowID, &traceParent, &archivedAt)
	if err != nil {
		return lr, errors.Trace(err)
	}
	lr.summary.Subgraph = surgraphFlowID != 0
	lr.summary.TraceID = traceIDFromParent(traceParent)
	lr.summary.FlowKey = keys.NewFlowKey(shardIdx, flowHour, lr.flowID, flowToken)
	lr.summary.StoppedTaskName = taskName.String
	lr.summary.Error = strings.TrimSpace(flowError)
	// cancel_reason is the shared reason column both Terminate and Cancel write into; splitTerminateCancelReason is
	// the one place that maps it onto the two reason fields, shared with snapshot()'s FlowOutcome.
	lr.summary.TerminateReason, lr.summary.CancelReason = splitTerminateCancelReason(lr.summary.Status, strings.TrimSpace(cancelReason))
	lr.at = lr.summary.UpdatedAt
	if archived {
		lr.summary.ArchivedAt = archivedAt
		lr.at = archivedAt
	}
	return lr, nil
}

// listLimit returns the query's Limit, required and at most maxListLimit.
func listLimit(query workflow.Query) (int, error) {
	if query.Limit <= 0 {
		return 0, errors.New("Limit is required: at most how many flows to return", http.StatusBadRequest)
	}
	if query.Limit > maxListLimit {
		return 0, errors.New("Limit %d is over the most a call returns, %d", query.Limit, maxListLimit, http.StatusBadRequest)
	}
	return query.Limit, nil
}

// listWindow is a query's time window by this process's clock: from lower, inclusive, to upper, exclusive. Each of the
// query's bounds narrows it, an absolute one and a relative one alike.
type listWindow struct {
	lower, upper time.Time
}

// listWindowOf returns the query's window as of now, the lower bound before the upper. With bounds required, it needs a
// lower bound (FromTime or NewerThan) and an upper bound (ToTime or OlderThan); otherwise a missing one is zero.
func listWindowOf(query workflow.Query, now time.Time, required bool) (listWindow, error) {
	if query.OlderThan < 0 || query.NewerThan < 0 {
		return listWindow{}, errors.New("OlderThan and NewerThan cannot be negative", http.StatusBadRequest)
	}
	if required && ((query.FromTime.IsZero() && query.NewerThan == 0) || (query.ToTime.IsZero() && query.OlderThan == 0)) {
		return listWindow{}, errors.New("a list needs a lower bound (FromTime or NewerThan) and an upper bound (ToTime or OlderThan)",
			http.StatusBadRequest)
	}
	w := listWindow{lower: query.FromTime, upper: query.ToTime}
	if t := now.Add(-query.NewerThan); query.NewerThan > 0 && (w.lower.IsZero() || t.After(w.lower)) {
		w.lower = t
	}
	if t := now.Add(-query.OlderThan); query.OlderThan > 0 && (w.upper.IsZero() || t.Before(w.upper)) {
		w.upper = t
	}
	if !w.lower.IsZero() && !w.upper.IsZero() && !w.lower.Before(w.upper) {
		return listWindow{}, errors.New("the time window is empty: its lower bound is not before its upper bound", http.StatusBadRequest)
	}
	return w, nil
}

// timeConditions returns the query's time bounds as conditions on column, and their arguments: the absolute ones bound
// as text (sqlTime), the relative ones from the database's clock.
func timeConditions(driverName, column string, query workflow.Query) (conds []string, args []any) {
	if !query.FromTime.IsZero() {
		expr, arg := sqlTime(driverName, query.FromTime)
		conds, args = append(conds, column+">="+expr), append(args, arg)
	}
	if !query.ToTime.IsZero() {
		expr, arg := sqlTime(driverName, query.ToTime)
		conds, args = append(conds, column+"<"+expr), append(args, arg)
	}
	if query.NewerThan > 0 {
		conds, args = append(conds, column+">=DATE_ADD_MILLIS(NOW_UTC(), ?)"), append(args, -query.NewerThan.Milliseconds())
	}
	if query.OlderThan > 0 {
		conds, args = append(conds, column+"<DATE_ADD_MILLIS(NOW_UTC(), ?)"), append(args, -query.OlderThan.Milliseconds())
	}
	return conds, args
}

// listFilters returns a list's WHERE conditions on one shard, and their arguments, but for the time bounds: the
// query's filters, the status condition (statuses, if any, when the query names none), the exclusion of flows marked
// for deletion, and the search.
func (e *Engine) listFilters(driverName string, shardIdx int, query workflow.Query, statuses string) (conds []string, args []any) {
	_, where, args, _ := e.queryClauses(query, subgraphCondition(query.IncludeSubgraphs))
	conds = []string{where, "f.delete_after_ms=0"}
	if query.Status == "" && statuses != "" {
		conds = append(conds, "f.status IN ("+statuses+")")
	}
	if sc, scArgs := searchClause(driverName, shardIdx, query.Search); sc != "" {
		conds, args = append(conds, sc), append(args, scArgs...)
	}
	return conds, args
}

// listLive lists the flows in the live tables, any status - running, interrupted, and ended ones not archived yet -
// newest updated first across shards. Its window is optional: the live tables are one table per shard, however wide
// the window.
func (e *Engine) listLive(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error) {
	limit, err := listLimit(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := listWindowOf(query, time.Now(), false); err != nil {
		return nil, errors.Trace(err)
	}
	if _, _, _, err := e.queryClauses(query, "1=1"); err != nil {
		return nil, errors.Trace(err)
	}
	indices, pos := e.shardOrdinals()
	perShard := make([][]listRow, len(indices))
	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if query.Shard != 0 && shardIdx != query.Shard {
			return nil
		}
		// A turn on each shard it reads, so this queues for that shard's connections like any other caller instead of
		// taking them from work already under way.
		ctx, doneTurn := e.opTurn(ctx, shardIdx, "list_live")
		defer doneTurn()
		conds, args := e.listFilters(db.DriverName(), shardIdx, query, "")
		timeConds, timeArgs := timeConditions(db.DriverName(), "f.updated_at", query)
		conds, args = append(conds, timeConds...), append(append(args, timeArgs...), limit)
		rows, err := db.QueryContext(ctx, "SELECT "+summaryColumns+" FROM dwarf_flows f"+stepsJoin("dwarf_steps")+
			" WHERE "+strings.Join(conds, " AND ")+" ORDER BY f.updated_at DESC, f.flow_id DESC LIMIT_OFFSET(?, 0)", args...)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		for rows.Next() {
			lr, err := scanListRow(rows, shardIdx, false)
			if err != nil {
				return errors.Trace(err)
			}
			perShard[pos[shardIdx]] = append(perShard[pos[shardIdx]], lr)
		}
		return errors.Trace(rows.Err())
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// updated_at comes from each shard's own clock, so flows updated within the skew between two shard servers may
	// swap; the tie-break keeps the order deterministic.
	merged := mergeByTime(slices.Concat(perShard...), false)
	return summariesOf(merged[:min(len(merged), limit)]), nil
}

// listArchived lists the archived flows, at most limit of them across shards, in mergeByTime's order on archived_at:
// newest first, or ascending. Each shard reads every archive day the window reaches in one UNION ALL, each table's
// newest (or oldest) limit under an outer limit; the live tables are never read.
func (e *Engine) listArchived(ctx context.Context, query workflow.Query, limit int, ascending bool) ([]listRow, error) {
	now := time.Now()
	w, err := listWindowOf(query, now, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if query.Status != "" && workflow.IsValidStatus(query.Status) && !workflow.IsTerminalStatus(query.Status) {
		return nil, errors.New("ListArchived lists archived flows, which have all ended; status %q has not", query.Status,
			http.StatusBadRequest)
	}
	if _, _, _, err := e.queryClauses(query, "1=1"); err != nil {
		return nil, errors.Trace(err)
	}
	// The window's archive days, clamped to the retention's first day and to today: nothing is archived before the one
	// or after the other. One more day past today is read when the window reaches it, for a database clock ahead of
	// this process's across midnight.
	set := e.archives.Load()
	if set == nil || e.retentionDays <= 0 {
		return nil, nil
	}
	today := archiver.DayOf(now, set.dayLength)
	fromDay := max(archiver.DayOf(w.lower, set.dayLength), today-e.retentionDays)
	toDay := min(archiver.DayOf(w.upper.Add(-time.Nanosecond), set.dayLength), today)
	if span := toDay - fromDay + 1; span > maxEndedWindowDays {
		return nil, errors.New("the time window reaches %d days of the archive; a call reads at most %d", span,
			maxEndedWindowDays, http.StatusBadRequest)
	}
	if toDay == today && archiver.DayOf(w.upper.Add(-time.Nanosecond), set.dayLength) > today {
		toDay++
	}
	if toDay < fromDay {
		return nil, nil
	}
	order, idOrder := "DESC", "DESC"
	if ascending {
		order, idOrder = "ASC", "ASC"
	}
	indices, pos := e.shardOrdinals()
	perShard := make([][]listRow, len(indices))
	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if query.Shard != 0 && shardIdx != query.Shard {
			return nil
		}
		a := set.byShard[shardIdx]
		if a == nil {
			return nil
		}
		ctx, doneTurn := e.opTurn(ctx, shardIdx, "list_archived")
		defer doneTurn()
		// Every archived flow has ended, so no status condition is added when the query names none: one would only
		// steer the planner to the status index rather than the archive-time one the order wants.
		conds, condArgs := e.listFilters(db.DriverName(), shardIdx, query, "")
		timeConds, timeArgs := timeConditions(db.DriverName(), "f.archived_at", query)
		branchWhere := strings.Join(append(conds, timeConds...), " AND ")
		branchArgs := append(condArgs, timeArgs...)
		read := func() ([]listRow, error) {
			days := a.DaysIn(fromDay, toDay)
			if len(days) == 0 {
				return nil, nil
			}
			branches := make([]string, len(days))
			var args []any
			for i, d := range days {
				// Each branch is a derived table so it can carry its own ORDER BY and limit on every dialect: SQLite
				// allows neither on a compound member, and SQL Server allows an ORDER BY in a derived table only with
				// OFFSET/FETCH, which LIMIT_OFFSET is there.
				branches[i] = "SELECT * FROM (SELECT " + summaryColumns + " FROM " + migrations.FlowsTable(d) + " f" +
					stepsJoin(migrations.StepsTable(d)) + " WHERE " + branchWhere +
					" ORDER BY f.archived_at " + order + ", f.flow_id " + idOrder + " LIMIT_OFFSET(?, 0)) b" + strconv.Itoa(i)
				args = append(append(args, branchArgs...), limit)
			}
			rows, err := db.QueryContext(ctx, "SELECT * FROM ("+strings.Join(branches, " UNION ALL ")+") u ORDER BY archived_at "+
				order+", flow_id "+idOrder+" LIMIT_OFFSET(?, 0)", append(args, limit)...)
			if err != nil {
				return nil, errors.Trace(err)
			}
			defer rows.Close()
			var got []listRow
			for rows.Next() {
				lr, err := scanListRow(rows, shardIdx, true)
				if err != nil {
					return nil, errors.Trace(err)
				}
				got = append(got, lr)
			}
			return got, errors.Trace(rows.Err())
		}
		got, err := read()
		// The archive's record of its days is its last catalog read, so a day a peer has dropped since fails the
		// statement on its missing table: read the catalog and run it again, once.
		if sequel.IsUndefinedTableError(err) {
			if err = a.Refresh(ctx); err == nil {
				got, err = read()
			}
		}
		perShard[pos[shardIdx]] = got
		return errors.Trace(err)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	merged := mergeByTime(slices.Concat(perShard...), ascending)
	return merged[:min(len(merged), limit)], nil
}

// mergeByTime orders a list's rows from every shard by their time (updated_at for the live list, archived_at for the
// archived one): newest first, or ascending. Ties go by shard ascending, then flow id (descending when newest first).
func mergeByTime(rows []listRow, ascending bool) []listRow {
	dir := -1
	if ascending {
		dir = 1
	}
	slices.SortFunc(rows, func(a, b listRow) int {
		return cmp.Or(dir*a.at.Compare(b.at), cmp.Compare(a.shard, b.shard), dir*cmp.Compare(a.flowID, b.flowID))
	})
	return rows
}

// summariesOf returns the summaries of rows, in order.
func summariesOf(rows []listRow) []workflow.FlowSummary {
	flows := make([]workflow.FlowSummary, len(rows))
	for i, lr := range rows {
		flows[i] = lr.summary
	}
	return flows
}

// stepsJoin joins a list's flows table to the step each flow stopped at, in steps.
func stepsJoin(steps string) string {
	return " LEFT JOIN " + steps + " s ON f.stopped_step_id=s.step_id"
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

// queryClauses builds the shared WHERE/JOIN for the lists and purge, but for the time bounds (timeConditions), whose
// column differs. subgraphCond is the surgraph_flow_id predicate the caller chose: the lists honor
// Query.IncludeSubgraphs, purge always passes roots-only.
func (e *Engine) queryClauses(query workflow.Query, subgraphCond string) (string, string, []any, error) {
	if query.Shard != 0 && !e.db.Has(query.Shard) {
		return "", "", nil, errors.New("invalid shard", http.StatusBadRequest)
	}
	// A negative duration would otherwise add no condition, and Purge's guard counts it as a filter: it would
	// purge every flow (a time.Until on a past time yields one).
	if query.OlderThan < 0 || query.NewerThan < 0 {
		return "", "", nil, errors.New("OlderThan and NewerThan cannot be negative", http.StatusBadRequest)
	}

	conditions := []string{subgraphCond}
	var args []any
	if query.Status != "" {
		// Inline the status as a literal (like every other status predicate) so SQL Server reads the
		// histogram and index-seeks a rare status instead of a clustered scan driven by the ORDER BY
		// flow_id DESC + LIMIT under a parameter's density-average estimate. Validated against the known
		// set first, so only an engine constant - never caller input - is concatenated (no injection).
		if !workflow.IsValidStatus(query.Status) {
			return "", "", nil, errors.New("invalid status filter: %q", query.Status, http.StatusBadRequest)
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

	joinSQL := stepsJoin("dwarf_steps")
	whereSQL := strings.Join(conditions, " AND ")
	return joinSQL, whereSQL, args, nil
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
