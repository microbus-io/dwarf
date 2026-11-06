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
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestPurge_EveryListFilterNarrows pins that Purge accepts any filter the lists do, beside the window and the Limit it
// requires, and rejects a query missing either end of the window or the Limit.
func TestPurge_EveryListFilterNarrows(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Tenant")
	g.SetURL("Work", "pf/work")
	g.AddTransition("Work", workflow.End)
	mux.HandleGraph("pf/g", g)
	mux.HandleTask("pf/work", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	tenantA, outcome, err := e.Run(ctx, "pf/g", nil, &workflow.FlowOptions{FairnessKey: "tenantA"})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	tenantB, outcome, err := e.Run(ctx, "pf/g", nil, &workflow.FlowOptions{FairnessKey: "tenantB"})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	now := time.Now()
	for _, q := range []workflow.Query{
		{}, {Shard: 1}, {Limit: 10},
		{FromTime: now.Add(-time.Hour), Limit: 10},
		{ToTime: now.Add(time.Hour), Limit: 10},
		{FromTime: now.Add(-time.Hour), ToTime: now.Add(time.Hour)},
	} {
		_, _, err := e.Purge(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%+v must be rejected", q)
	}

	// The tenant erasure the docs describe: a FairnessKey alone removes that tenant's flows and no other's, once they
	// are archived.
	enginetest.ArchiveNow(ctx, e)
	n, _, err := e.Purge(ctx, enginetest.Recent(workflow.Query{FairnessKey: "tenantA"}))
	assert.NoError(err)
	assert.Equal(1, n)
	summaries, err := enginetest.ListAll(ctx, e, workflow.Query{WorkflowURL: "pf/g"})
	assert.NoError(err)
	var keys []string
	for _, s := range summaries {
		keys = append(keys, s.FlowKey)
	}
	assert.NotContains(keys, tenantA)
	assert.Contains(keys, tenantB)

	for _, q := range []workflow.Query{
		{StoppedTaskName: "Work"}, {Priority: 99}, {FromTime: time.Now().Add(-time.Hour)}, {Search: "no-such-flow"},
	} {
		_, _, err := e.Purge(ctx, enginetest.Recent(q))
		assert.NoError(err, "%+v", q)
	}
}
