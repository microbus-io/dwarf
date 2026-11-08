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
