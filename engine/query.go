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
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// maxListLimit is the most flows a list returns in a call. It must stay above the reaper's batch
// (reapBatch roots, about reapBatchRows flows): a batch's trees share one archived_at, and a caller paging ListClosed
// by window cannot page across a tie larger than its Limit (TestList_TheLimitIsAboveAReaperBatch). A batch takes at
// least one tree however large, so with IncludeSubgraphs one tree of more flows than this is a tie no walk pages across.
const maxListLimit = 65536

// maxArchiveWindowDays is the most archive days a ListClosed window may reach, after clamping: today and the 30 before
// it, the days the default retention keeps. Each day is one more pair of tables in the shard's statement, and
// PostgreSQL locks every index of a table it plans: about 11 locks a day (the flows table and its 7 indexes counting
// the key, the steps table and its 2), so 31 days are about 340 - some 5% of the default shared lock table (64 per
// connection) for one statement.
const maxArchiveWindowDays = 31

// listRow is one flow a list read, with what the merge orders it by.
type listRow struct {
	summary workflow.FlowSummary
	shard   int
	flowID  int
	at      time.Time // archived_at for an archived flow, updated_at for a live one
	day     int       // the archive day the flow was read from; 0 for a live one
}

// summaryColumns are what a list reads of a flow, f being its flows table, s its stopped step, ps its caller step
// and rf its root flow (summaryJoins). The names are unique, so a union of them can be selected from by name.
const summaryColumns = "f.flow_id, f.flow_token, f.created_hour, f.workflow_url, f.workflow_name, f.status, s.task_name," +
	" f.error, f.cancel_reason, f.created_at, f.started_at, f.updated_at, f.priority, f.fairness_key, f.surgraph_flow_id," +
	" f.trace_parent, f.archived_at, f.surgraph_step_id, ps.flow_created_hour AS parent_step_hour," +
	" ps.step_token AS parent_step_token, f.root_flow_id, rf.created_hour AS root_flow_hour, rf.flow_token AS root_flow_token"

// treeUnstamped is archiver.VisibleSQL over summaryJoins' f and rf: the flow's tree is not marked for deletion, and a
// child's root is there. It reads the root's stamp, not the flow's: only a root is stamped, and a stamped tree's
// children are deleted, in chunks, before it. On the archive side a child whose root is not in its day is a copy not
// published.
var treeUnstamped = archiver.VisibleSQL("f", "rf")

// summaryJoins joins what summaryColumns reads beside the flow row: the step it stopped at, a child's caller step and
// a child's root flow. A tree lives in one table set - it is created on one shard and archived in one move - so the
// caller and the root are in the tables the flow is. Each is a primary-key lookup.
//
// The caller and root joins compare with NULLIF, so a root - every row of a list without IncludeSubgraphs - looks
// neither up: a NULL key matches no row and costs no index probe, where a key of 0 (a root has no caller) or of the
// flow's own id (a root is its own root) is probed for each row. Measured on PostgreSQL 16, 100 archived roots: 446
// buffers without the two joins, 1,153 with them keyed plainly.
func summaryJoins(t flowTables) string {
	return " LEFT JOIN " + t.steps + " s ON f.stopped_step_id=s.step_id" +
		" LEFT JOIN " + t.steps + " ps ON ps.step_id=NULLIF(f.surgraph_step_id, 0)" +
		" LEFT JOIN " + t.flows + " rf ON rf.flow_id=NULLIF(f.root_flow_id, f.flow_id)"
}

// scanListRow reads one row of summaryColumns from shardIdx - followed, when withDay, by the archive day its branch
// selected.
func scanListRow(rows *sequel.Rows, shardIdx int, withDay bool) (listRow, error) {
	lr := listRow{shard: shardIdx}
	var flowToken, flowError, cancelReason, traceParent string
	var flowHour, surgraphFlowID, surgraphStepID, rootFlowID int
	var taskName, parentStepToken, rootFlowToken sql.NullString
	var parentStepHour, rootFlowHour sql.NullInt64
	var archivedAt time.Time
	dest := []any{&lr.flowID, &flowToken, &flowHour, &lr.summary.WorkflowURL, &lr.summary.WorkflowName,
		&lr.summary.Status, &taskName, &flowError, &cancelReason, &lr.summary.CreatedAt, &lr.summary.StartedAt,
		&lr.summary.UpdatedAt, &lr.summary.Priority, &lr.summary.FairnessKey, &surgraphFlowID, &traceParent, &archivedAt,
		&surgraphStepID, &parentStepHour, &parentStepToken, &rootFlowID, &rootFlowHour, &rootFlowToken}
	if withDay {
		dest = append(dest, &lr.day)
	}
	err := rows.Scan(dest...)
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
	if surgraphStepID != 0 && parentStepToken.Valid {
		lr.summary.ParentStepKey = keys.NewStepKey(shardIdx, int(parentStepHour.Int64), surgraphFlowID, surgraphStepID,
			parentStepToken.String)
	}
	switch {
	case rootFlowID == lr.flowID:
		lr.summary.RootFlowKey = lr.summary.FlowKey
	case rootFlowToken.Valid:
		lr.summary.RootFlowKey = keys.NewFlowKey(shardIdx, int(rootFlowHour.Int64), rootFlowID, rootFlowToken.String)
	}
	// Visible live rows keep the far-future sentinel; a published live root is hidden by its stamp.
	if archivedAt.Year() < database.MaxSQLTime().Year() {
		lr.summary.ArchivedAt = archivedAt
	}
	lr.at = lr.summary.UpdatedAt
	if withDay {
		lr.at = archivedAt
	}
	return lr, nil
}

// summary reads one flow's summary by its key, from the live tables or the archive, with the lists' columns and
// scanner so the two cannot disagree. A flow marked for deletion is not found, as the lists leave it out.
func (e *Engine) summary(ctx context.Context, flowKey string) (*workflow.FlowSummary, error) {
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sh, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db := sh.DB()
	ctx, doneTurn := e.opTurn(ctx, shardNum, "summary")
	defer doneTurn()
	var lr listRow
	found, err := e.queryLiveOrArchive(ctx, db, shardNum, flowID, flowHour, closeNone, func(t flowTables) (bool, error) {
		rows, err := db.QueryContext(ctx, "SELECT "+summaryColumns+" FROM "+t.flows+" f"+summaryJoins(t)+
			" WHERE f.flow_id=? AND f.flow_token=? AND f.created_hour=? AND "+treeUnstamped,
			flowID, flowToken, flowHour)
		if err != nil {
			return false, errors.Trace(err)
		}
		defer rows.Close()
		if !rows.Next() {
			return false, errors.Trace(rows.Err())
		}
		lr, err = scanListRow(rows, shardNum, false)
		return err == nil, errors.Trace(err)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	return &lr.summary, nil
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

// listWindow is a query's time window: from lower, inclusive, to upper, exclusive. A bound the query leaves out is
// zero.
type listWindow struct {
	lower, upper time.Time
}

// listWindowOf returns the query's window, the lower bound before the upper. With bounds required, it needs both
// FromTime and ToTime.
func listWindowOf(query workflow.Query, required bool) (listWindow, error) {
	if required && (query.FromTime.IsZero() || query.ToTime.IsZero()) {
		return listWindow{}, errors.New("the call needs a window: a lower bound (FromTime) and an upper bound (ToTime)", http.StatusBadRequest)
	}
	w := listWindow{lower: query.FromTime, upper: query.ToTime}
	if !w.lower.IsZero() && !w.upper.IsZero() && !w.lower.Before(w.upper) {
		return listWindow{}, errors.New("the time window is empty: FromTime is not before ToTime", http.StatusBadRequest)
	}
	return w, nil
}

// timeConditions returns the query's time bounds as conditions on column, and their arguments, bound as text
// (database.SQLTime).
func timeConditions(driverName, column string, query workflow.Query) (conds []string, args []any) {
	if !query.FromTime.IsZero() {
		expr, arg := database.SQLTime(driverName, query.FromTime)
		conds, args = append(conds, column+">="+expr), append(args, arg)
	}
	if !query.ToTime.IsZero() {
		expr, arg := database.SQLTime(driverName, query.ToTime)
		conds, args = append(conds, column+"<"+expr), append(args, arg)
	}
	return conds, args
}

// listFilters returns a list's WHERE conditions on one shard, and their arguments, but for the time bounds: the
// query's filters, the exclusion of flows marked for deletion, and the search.
func (e *Engine) listFilters(driverName string, shardIdx int, query workflow.Query) (conds []string, args []any) {
	where, args, _ := e.queryClauses(query, subgraphCondition(query.IncludeSubgraphs))
	conds = []string{where, treeUnstamped}
	if sc, scArgs := searchClause(driverName, shardIdx, query.Search); sc != "" {
		conds, args = append(conds, sc), append(args, scArgs...)
	}
	return conds, args
}

// openStatuses are the statuses of a flow that has not closed, each a branch of ListOpen's statement. created is not
// among them: it is only ever seen inside Fork's uncommitted clone.
var openStatuses = []string{workflow.StatusRunning, workflow.StatusInterrupted}

// listOpen lists the open flows - running or interrupted - in the live tables, newest updated first across shards. Its
// window is optional: the live tables are one table per shard, however wide the window.
//
// Each shard's statement is one branch per open status, each a range of idx_dwarf_flows_status (status, updated_at)
// that stops at the limit, under an outer limit: a single scan over both statuses has no index order, so it would
// read and sort every open flow of the shard on every call. The status is inlined, never bound.
func (e *Engine) listOpen(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error) {
	if query.Status != "" && !slices.Contains(openStatuses, query.Status) && workflow.IsValidStatus(query.Status) {
		return nil, errors.New("ListOpen lists open flows, running or interrupted; status %q is not open", query.Status,
			http.StatusBadRequest)
	}
	return e.listLive(ctx, query, openStatuses, "list_open")
}

// listRecentlyClosed lists the closed flows still in the live tables - not archived yet - newest updated first across
// shards, with listOpen's statement: a closed flow's last write is the one that closed it, so updated_at is when.
func (e *Engine) listRecentlyClosed(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error) {
	if query.Status != "" && workflow.IsValidStatus(query.Status) && !workflow.IsTerminalStatus(query.Status) {
		return nil, errors.New("ListRecentlyClosed lists closed flows; status %q is not closed", query.Status,
			http.StatusBadRequest)
	}
	return e.listLive(ctx, query, database.TerminalStatuses, "list_recently_closed")
}

// listLive lists the flows in the live tables whose status is among statuses - or is query.Status, which must be one -
// newest updated first across shards, with listOpen's statement. op names the operation for its turn's metric. It
// serves ListOpen, ListRecentlyClosed and the tests (accessors.ListLive), which list every status at once.
func (e *Engine) listLive(ctx context.Context, query workflow.Query, statuses []string, op string) ([]workflow.FlowSummary, error) {
	limit, err := listLimit(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err := listWindowOf(query, false); err != nil {
		return nil, errors.Trace(err)
	}
	if _, _, err := e.queryClauses(query, "1=1"); err != nil {
		return nil, errors.Trace(err)
	}
	if query.Status != "" {
		if !slices.Contains(statuses, query.Status) {
			return nil, nil
		}
		statuses = []string{query.Status}
	}
	branchQuery := query
	branchQuery.Status = "" // each branch names its own
	indices, pos := e.shardOrdinals()
	perShard := make([][]listRow, len(indices))
	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		if query.Shard != 0 && shardIdx != query.Shard {
			return nil
		}
		// A turn on each shard it reads, so this queues for that shard's connections like any other caller instead of
		// taking them from work already under way.
		ctx, doneTurn := e.opTurn(ctx, shardIdx, op)
		defer doneTurn()
		conds, condArgs := e.listFilters(db.DriverName(), shardIdx, branchQuery)
		timeConds, timeArgs := timeConditions(db.DriverName(), "f.updated_at", query)
		branchWhere := strings.Join(append(conds, timeConds...), " AND ")
		branchArgs := append(condArgs, timeArgs...)
		branches := make([]string, len(statuses))
		var args []any
		for i, status := range statuses {
			// A derived table, so the branch carries its own ORDER BY and limit on every dialect (see listArchived).
			branches[i] = "SELECT * FROM (SELECT " + summaryColumns + " FROM dwarf_flows f" + summaryJoins(liveTables) +
				" WHERE " + branchWhere + " AND f.status='" + status + "'" +
				" ORDER BY f.updated_at DESC, f.flow_id DESC LIMIT_OFFSET(?, 0)) b" + strconv.Itoa(i)
			args = append(append(args, branchArgs...), limit)
		}
		rows, err := db.QueryContext(ctx, "SELECT * FROM ("+strings.Join(branches, " UNION ALL ")+") u"+
			" ORDER BY updated_at DESC, flow_id DESC LIMIT_OFFSET(?, 0)", append(args, limit)...)
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

// listArchived lists the archived flows - ListClosed, and Purge's selection - at most limit of them across shards, in
// mergeByTime's order on archived_at: newest first, or ascending. op names the operation for its turn's metric. Each shard reads every archive day the window reaches in one UNION ALL, each table's
// newest (or oldest) limit under an outer limit; the live tables are never read.
func (e *Engine) listArchived(ctx context.Context, query workflow.Query, limit int, ascending bool, op string) ([]listRow, error) {
	now := time.Now()
	w, err := listWindowOf(query, true)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if query.Status != "" && workflow.IsValidStatus(query.Status) && !workflow.IsTerminalStatus(query.Status) {
		return nil, errors.New("ListClosed lists closed flows; status %q is not closed", query.Status,
			http.StatusBadRequest)
	}
	if _, _, err := e.queryClauses(query, "1=1"); err != nil {
		return nil, errors.Trace(err)
	}
	// The window's archive days, clamped to the retention's first day and to today: nothing is archived before the one
	// or after the other. One more day past today is read when the window reaches it, for a database clock ahead of
	// this process's across midnight.
	// At a retention of 0 that is today alone: nothing is archived then, but today's tables can still hold what a run
	// with a retention archived before a restart.
	dayLength := e.dayLength()
	if dayLength == 0 {
		return nil, nil
	}
	today := archiver.DayOf(now, dayLength)
	fromDay := max(archiver.DayOf(w.lower, dayLength), today-max(e.retentionDays, 0))
	toDay := min(archiver.DayOf(w.upper.Add(-time.Nanosecond), dayLength), today)
	if span := toDay - fromDay + 1; span > maxArchiveWindowDays {
		return nil, errors.New("the time window reaches %d days of the archive; a call reads at most %d", span,
			maxArchiveWindowDays, http.StatusBadRequest)
	}
	if toDay == today && archiver.DayOf(w.upper.Add(-time.Nanosecond), dayLength) > today {
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
		a := e.archiveOn(shardIdx)
		if a == nil {
			return nil
		}
		ctx, doneTurn := e.opTurn(ctx, shardIdx, op)
		defer doneTurn()
		// Every archived flow is closed, so no status condition is added when the query names none: one would only
		// steer the planner to the status index rather than the archive-time one the order wants.
		conds, condArgs := e.listFilters(db.DriverName(), shardIdx, query)
		if db.DriverName() == "sqlite" && !query.IncludeSubgraphs && query.Status != "" {
			// SQLite takes the (status, archived_at) index whenever the status is compared plainly, and walks every
			// child of that status before the first root: 139 ms for a page of completed roots on a day of one tree of
			// 300,000 children. The unary plus hides the column from the index choice, so it walks the roots index
			// and filters on the status: 0.4 ms for a common status, about 5 ms for a rare one (0.4 ms otherwise),
			// bounded by the day's roots. Only roots-only: a listing with children cannot use the roots index.
			conds[0] = strings.Replace(conds[0], "f.status='", "+f.status='", 1)
		}
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
				// The branch names its own day, so a caller acting on a row (Purge) finds its table without reading the
				// day back from archived_at as the driver parsed it, which on MySQL follows the DSN's loc.
				branches[i] = "SELECT * FROM (SELECT " + summaryColumns + ", " + strconv.Itoa(d) + " AS archive_day FROM " +
					migrations.FlowsTable(d) + " f" +
					summaryJoins(flowTables{flows: migrations.FlowsTable(d), steps: migrations.StepsTable(d)}) +
					" WHERE " + branchWhere +
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

// mergeByTime orders a list's rows from every shard by their time (updated_at for ListOpen, archived_at for
// ListClosed): newest first, or ascending. Ties go by shard ascending, then flow id (descending when newest first).
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

// likeEscape is the LIKE ESCAPE character used by searchClause. It is a printable char that is neither a
// LIKE metacharacter nor special inside a string literal on any of the four dialects (unlike backslash,
// which MySQL and Postgres treat differently in literals), so `ESCAPE '~'` is portable as-is.
const likeEscape = "~"

var likeEscaper = strings.NewReplacer(likeEscape, likeEscape+likeEscape, "%", likeEscape+"%", "_", likeEscape+"_")

// mssqlLikeEscaper also escapes `[`, which opens a character set (`[x]`, a range `[a-c]`, a negated `[^x]`) only in
// SQL Server's LIKE. Unescaped, a search for "[x]" matches every value holding an x, and Purge, which selects its
// victims through this clause, deletes them. A `]` or `^` outside a set is already literal there. The other dialects
// do not get the escape: SQLite defines its escape character only before %, _ and itself.
var mssqlLikeEscaper = strings.NewReplacer(likeEscape, likeEscape+likeEscape, "%", likeEscape+"%", "_", likeEscape+"_", "[", likeEscape+"[")

// likePattern is the %...% pattern that matches search as a literal substring under ESCAPE likeEscape, lowercased.
func likePattern(driverName string, search string) string {
	escaper := likeEscaper
	if driverName == "mssql" {
		escaper = mssqlLikeEscaper
	}
	return "%" + escaper.Replace(strings.ToLower(search)) + "%"
}

func searchClause(driverName string, shardIdx int, search string) (string, []any) {
	if search == "" {
		return "", nil
	}
	// Escape the caller-supplied term's LIKE metacharacters and the escape char itself before wrapping in %...%, so a
	// search for "a_b", "50%" or "[x]" matches literally. Every LIKE below carries the matching ESCAPE clause. The term
	// is a bind parameter, so this is not about injection: an unescaped wildcard widens what the search selects, which
	// for Purge is what it deletes.
	pattern := likePattern(driverName, search)
	var flowKeyExpr string
	switch driverName {
	case "mysql", "mariadb", "mssql":
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

// queryClauses builds the shared WHERE for the lists and purge, but for the time bounds (timeConditions), whose
// column differs. subgraphCond is the surgraph_flow_id predicate the caller chose: the lists honor
// Query.IncludeSubgraphs, purge always passes roots-only. A `s.` condition needs the caller's summaryJoins.
func (e *Engine) queryClauses(query workflow.Query, subgraphCond string) (where string, args []any, err error) {
	if query.Shard != 0 && !e.db.Has(query.Shard) {
		return "", nil, errors.New("invalid shard", http.StatusBadRequest)
	}

	conditions := []string{subgraphCond}
	if query.Status != "" {
		// Inline the status as a literal (like every other status predicate) so SQL Server reads the
		// histogram and index-seeks a rare status instead of a clustered scan driven by the ORDER BY
		// flow_id DESC + LIMIT under a parameter's density-average estimate. Validated against the known
		// set first, so only an engine constant - never caller input - is concatenated (no injection).
		if !workflow.IsValidStatus(query.Status) {
			return "", nil, errors.New("invalid status filter: %q", query.Status, http.StatusBadRequest)
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

	return strings.Join(conditions, " AND "), args, nil
}
