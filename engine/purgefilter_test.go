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
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestPurge_EveryListFilterNarrows pins that Purge accepts any filter List does, on its own, and still rejects a
// query that names nothing to narrow by: Shard and Limit bound a purge but select every flow they reach.
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

	for _, q := range []workflow.Query{{}, {Shard: 1}, {Limit: 10}, {Shard: 1, Limit: 10}} {
		_, err := e.Purge(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%+v must be rejected", q)
	}

	// The tenant erasure the docs describe: a FairnessKey alone marks that tenant's flows and no other's.
	n, err := e.Purge(ctx, workflow.Query{FairnessKey: "tenantA"})
	assert.NoError(err)
	assert.Equal(1, n)
	summaries, _, err := e.List(ctx, workflow.Query{WorkflowURL: "pf/g"})
	assert.NoError(err)
	var keys []string
	for _, s := range summaries {
		keys = append(keys, s.FlowKey)
	}
	assert.NotContains(keys, tenantA)
	assert.Contains(keys, tenantB)

	for _, q := range []workflow.Query{
		{StoppedTaskName: "Work"}, {Priority: 99}, {NewerThan: time.Hour}, {Search: "no-such-flow"},
	} {
		_, err := e.Purge(ctx, q)
		assert.NoError(err, "%+v", q)
	}
}

// TestPurge_NegativeDurationIsRejected pins that a negative OlderThan or NewerThan is a 400 on both List and
// Purge. It adds no condition, so it would pass Purge's filter guard and purge every flow.
func TestPurge_NegativeDurationIsRejected(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Negative")
	g.SetURL("Work", "pn/work")
	g.AddTransition("Work", workflow.End)
	mux.HandleGraph("pn/g", g)
	mux.HandleTask("pn/work", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	flowKey, outcome, err := e.Run(ctx, "pn/g", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	for _, q := range []workflow.Query{
		{OlderThan: -time.Nanosecond}, {NewerThan: -time.Nanosecond}, {OlderThan: time.Hour, NewerThan: -time.Hour},
	} {
		_, err := e.Purge(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "Purge %+v", q)
		_, _, err = e.List(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "List %+v", q)
	}
	summaries, _, err := e.List(ctx, workflow.Query{WorkflowURL: "pn/g"})
	assert.NoError(err)
	if assert.Len(summaries, 1) {
		assert.Equal(flowKey, summaries[0].FlowKey, "nothing was marked")
	}
}
