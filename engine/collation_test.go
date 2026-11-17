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
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestQuery_FiltersMatchCaseExactly pins that the list filters compare a value exactly on every dialect, live and
// archived: two flows whose workflow URL, workflow name, stopped task and fairness key differ only in case are two,
// and a Purge of one tenant's flows leaves the other's. Under a case-insensitive collation (the default on MySQL,
// MariaDB and SQL Server) each filter returned both, so a tenant-scoped Purge erased a second tenant. Search stays
// case-insensitive, by its own contract.
func TestQuery_FiltersMatchCaseExactly(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	for _, name := range []string{"Case", "case"} {
		task := "Work"
		if name == "case" {
			task = "work"
		}
		g := workflow.NewGraph(name)
		g.SetURL(task, "collation/"+task)
		g.AddTransition(task, workflow.End)
		mux.HandleGraph("collation/"+name, g)
		mux.HandleTask("collation/"+task, func(ctx context.Context, f *workflow.Flow) error { return nil })
	}

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	upper, outcome, err := e.Run(ctx, "collation/Case", nil, &workflow.FlowOptions{FairnessKey: "Acme"})
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	lower, outcome, err := e.Run(ctx, "collation/case", nil, &workflow.FlowOptions{FairnessKey: "acme"})
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	listed := func(q workflow.Query) []string {
		summaries, err := enginetest.ListAll(ctx, e, q)
		assert.NoError(err)
		var keys []string
		for _, s := range summaries {
			keys = append(keys, s.FlowKey)
		}
		return keys
	}
	filtersMatchExactly := func(where string) {
		for _, c := range []struct {
			upper, lower workflow.Query
		}{
			{workflow.Query{WorkflowURL: "collation/Case"}, workflow.Query{WorkflowURL: "collation/case"}},
			{workflow.Query{WorkflowName: "Case"}, workflow.Query{WorkflowName: "case"}},
			{workflow.Query{StoppedTaskName: "Work"}, workflow.Query{StoppedTaskName: "work"}},
			{workflow.Query{FairnessKey: "Acme"}, workflow.Query{FairnessKey: "acme"}},
		} {
			assert.Equal([]string{upper}, listed(c.upper), "%s: %+v", where, c.upper)
			assert.Equal([]string{lower}, listed(c.lower), "%s: %+v", where, c.lower)
		}
		both := listed(workflow.Query{Search: "COLLATION/CASE"})
		assert.Len(both, 2, "%s: Search is case-insensitive", where)
		assert.Contains(both, upper)
		assert.Contains(both, lower)
	}

	filtersMatchExactly("live")
	enginetest.ArchiveNow(ctx, e)
	filtersMatchExactly("archived")

	n, _, err := e.Purge(ctx, enginetest.Recent(workflow.Query{FairnessKey: "acme"}))
	assert.NoError(err)
	assert.Equal(1, n, "a tenant's Purge removed another tenant's flow")
	assert.Equal([]string{upper}, listed(workflow.Query{Search: "collation/"}))
}

// TestCollation_PostgresIdentityColumnsAreC pins that on PostgreSQL every VARCHAR column of the live tables and of an
// archive day is collated "C", and the free-text columns are not. Left at the database's collation, a libc locale on
// most servers, an index on a key or URL is ordered by rules a glibc update changes, and is then out of order until
// reindexed. The free text keeps the database's collation: Search lowercases it, which under "C" changes only ASCII.
func TestCollation_PostgresIdentityColumnsAreC(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(hosts.NewMux())
	assert.Must(assert.NoError(e.Startup(ctx)))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	if db.DriverName() != "pgx" {
		t.Skip("PostgreSQL only: the other dialects' binary collations are pinned by TestQuery_FiltersMatchCaseExactly")
	}
	today, err := e.archiveOn(1).Today(ctx)
	assert.Must(assert.NoError(err))
	tables := []any{"dwarf_flows", "dwarf_steps", migrations.FlowsTable(today), migrations.StepsTable(today)}

	rows, err := db.QueryContext(ctx,
		"SELECT table_name, column_name, data_type, COALESCE(collation_name, '') FROM information_schema.columns"+
			" WHERE table_schema=current_schema() AND table_name IN (?, ?, ?, ?) AND data_type IN ('character varying', 'text')",
		tables...)
	assert.Must(assert.NoError(err))
	defer rows.Close()
	seen := map[string]int{}
	for rows.Next() {
		var table, column, dataType, collation string
		assert.Must(assert.NoError(rows.Scan(&table, &column, &dataType, &collation)))
		seen[table]++
		if dataType == "text" {
			assert.Equal("", collation, "%s.%s is free text and keeps the database's collation", table, column)
		} else {
			assert.Equal("C", collation, "%s.%s", table, column)
		}
	}
	assert.NoError(rows.Err())
	for _, table := range tables {
		assert.True(seen[table.(string)] > 0, "no text columns read from %s", table)
	}
}
