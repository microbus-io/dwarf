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

package janitor

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestJanitor_EveryJobRunsOnAPoolOfOne pins that no job runs a statement on the shard's DB while it holds a connection
// of its own - inside a transaction, or with rows unread: with a pool of one connection, every job archives and
// deletes what is due. Such a statement would wait for the connection its own job holds, and nothing would move.
func TestJanitor_EveryJobRunsOnAPoolOfOne(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	r.settings.stageRows = 8 // so large-tree archiving claims, copies and publishes one, and fast deletion removes it
	closed := seedTrees(t, r.db, 3, callShape)
	closed = append(closed, seedWideTree(t, r.db, 3, 10, 3))
	stamped := seedTrees(t, r.db, 2, callShape)
	for _, root := range stamped {
		stampTree(t, r.db, root)
	}
	r.db.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second*timeoutScale)
	defer cancel()
	r.pass(ctx)
	assert.NoError(ctx.Err(), "no job waited for a second connection")
	for _, id := range append(closed, stamped...) {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows, "root %d removed", id)
	}
}

// TestSize_TheSizingReadStopsAtItsLimit pins the sizing's bounded reads on PostgreSQL: a flow of 10,000 steps at 4%
// density among 250,000, sized up to 1,001 rows, with no statistics and with statistics that price a flow small
// (n_distinct 1,000), each as a custom plan and as a generic plan cached on the connection beforehand. Under
// sizingReads the step read walks the flow index and stops at its limit; left to itself it is a bitmap scan of the
// flow's whole index in three of the four.
func TestSize_TheSizingReadStopsAtItsLimit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	if r.db.DriverName() != "pgx" {
		t.Skip("PostgreSQL's planner")
	}
	const flow, every, target, limit = 7, 25, 10_000, 1001
	for _, s := range []string{
		"ALTER TABLE dwarf_steps SET (autovacuum_enabled = false)",
		"ALTER TABLE dwarf_flows SET (autovacuum_enabled = false)",
		"INSERT INTO dwarf_flows (flow_id, flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id)" +
			" VALUES (7, 'tok', 'u', 'n', '{}', 'h', 'completed', 7)",
	} {
		_, err := r.db.ExecContext(ctx, s)
		assert.Must(assert.NoError(err))
	}
	_, err := r.db.ExecContext(ctx, "INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status,"+
		" time_budget_ms, flow_created_hour, state_bytes) SELECT CASE WHEN g % $1 = 0 THEN $2 ELSE 1000 + g % 997 END, 1,"+
		" 'tok', 'T', 'seed/t', 'completed', 60000, 0, 200 FROM generate_series(1, $3) g ORDER BY g", every, flow, every*target)
	assert.Must(assert.NoError(err))

	stmt, args := treesSizesUpToSQL("pgx", "", []int{flow}, []int{limit})
	n := 0
	numbered := regexp.MustCompile(`\?`).ReplaceAllStringFunc(stmt, func(string) string { n++; return "$" + strconv.Itoa(n) })
	lits := make([]string, len(args))
	for i, a := range args {
		lits[i] = strconv.Itoa(a.(int))
	}
	execute := "EXECUTE sz(" + strings.Join(lits, ",") + ")"

	conn, err := r.db.Conn(ctx)
	assert.Must(assert.NoError(err))
	defer conn.Close()
	explain := func(q sequel.Executor, s string, a ...any) string {
		rows, err := q.QueryContext(ctx, "EXPLAIN (ANALYZE) "+s, a...)
		assert.Must(assert.NoError(err))
		defer rows.Close()
		var b strings.Builder
		for rows.Next() {
			var line string
			assert.Must(assert.NoError(rows.Scan(&line)))
			b.WriteString(line + "\n")
		}
		assert.Must(assert.NoError(rows.Err()))
		return b.String()
	}
	readRows := regexp.MustCompile(`Index Scan using idx_dwarf_steps_flow_id[^\n]*actual time=[0-9.]+\.\.[0-9.]+ rows=([0-9.]+)`)
	bounded := func(what, plan string) {
		assert.Contains(plan, "Index Scan using idx_dwarf_steps_flow_id", "%s: %s", what, plan)
		assert.NotContains(plan, "Bitmap", "%s: %s", what, plan)
		read := readRows.FindStringSubmatch(plan)
		if assert.Len(read, 2, plan) {
			got, _ := strconv.ParseFloat(read[1], 64)
			assert.True(got <= limit, "%s: read %v rows for a limit of %d", what, got, limit)
		}
	}
	for _, regime := range []string{"no statistics", "n_distinct 1000"} {
		if regime == "n_distinct 1000" {
			for _, s := range []string{"ALTER TABLE dwarf_steps ALTER COLUMN flow_id SET (n_distinct = 1000)", "ANALYZE dwarf_steps", "ANALYZE dwarf_flows"} {
				_, err := conn.ExecContext(ctx, s)
				assert.Must(assert.NoError(err))
			}
		}
		var plan string
		assert.NoError(sizingReads(ctx, conn, func(q sequel.Executor) error { plan = explain(q, stmt, args...); return nil }))
		bounded(regime+", custom", plan)

		for _, s := range []string{"PREPARE sz AS " + numbered, "SET plan_cache_mode = force_generic_plan", execute} {
			_, err := conn.ExecContext(ctx, s)
			assert.Must(assert.NoError(err))
		}
		_, err = conn.ExecContext(ctx, "SET plan_cache_mode = auto")
		assert.Must(assert.NoError(err))
		assert.NoError(sizingReads(ctx, conn, func(q sequel.Executor) error { plan = explain(q, execute); return nil }))
		bounded(regime+", cached generic", plan)
		_, err = conn.ExecContext(ctx, "DEALLOCATE sz")
		assert.Must(assert.NoError(err))
	}
}
