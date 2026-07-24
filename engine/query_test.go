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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestQuery_WorkflowName exercises the Query.WorkflowName filter: List narrows to flows whose graph
// display name matches, Search matches the display name too, and Purge accepts WorkflowName as a sole
// filter and deletes only the matching flows. Two graphs share neither URL nor display name, so the
// filter must distinguish them by name (not URL).
func TestQuery_WorkflowName(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	alpha := workflow.NewGraph("AlphaFlow")
	alpha.SetEndpoint("A", "q.verify:0/a")
	alpha.AddTransition("A", workflow.END)
	proxy.HandleGraph("q.verify:0/alpha", alpha)
	beta := workflow.NewGraph("BetaFlow")
	beta.SetEndpoint("B", "q.verify:0/b")
	beta.AddTransition("B", workflow.END)
	proxy.HandleGraph("q.verify:0/beta", beta)
	proxy.HandleTask("q.verify:0/a", func(context.Context, *workflow.Flow) error { return nil })
	proxy.HandleTask("q.verify:0/b", func(context.Context, *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(ctx))

	const alphas, betas = 3, 2
	var keys []string
	for range alphas {
		k, err := e.Create(ctx, "q.verify:0/alpha", nil, nil)
		assert.NoError(err)
		keys = append(keys, k)
	}
	for range betas {
		k, err := e.Create(ctx, "q.verify:0/beta", nil, nil)
		assert.NoError(err)
		keys = append(keys, k)
	}
	// Create auto-starts; wait for all to complete so the query/purge below see a stable terminal state
	// (Purge skips running flows).
	for _, k := range keys {
		_, err := e.Await(ctx, k)
		assert.NoError(err)
	}

	// List by WorkflowName returns only the matching graph's flows.
	got, _, err := e.List(ctx, workflow.Query{WorkflowName: "AlphaFlow"})
	assert.NoError(err)
	assert.Equal(alphas, len(got))
	for _, s := range got {
		assert.Equal("AlphaFlow", s.WorkflowName)
		assert.Equal("q.verify:0/alpha", s.WorkflowURL)
	}

	// A non-matching name returns nothing.
	none, _, err := e.List(ctx, workflow.Query{WorkflowName: "Nope"})
	assert.NoError(err)
	assert.Equal(0, len(none))

	// Search matches the display name as a substring.
	searched, _, err := e.List(ctx, workflow.Query{Search: "betaflow"})
	assert.NoError(err)
	assert.Equal(betas, len(searched))

	// Purge accepts WorkflowName as a sole filter and deletes only the matching flows.
	deleted, err := e.Purge(ctx, workflow.Query{WorkflowName: "AlphaFlow"})
	assert.NoError(err)
	assert.Equal(alphas, deleted)
	remaining, _, err := e.List(ctx, workflow.Query{})
	assert.NoError(err)
	assert.Equal(betas, len(remaining))
}

// TestQuery_StatusFilter pins the Query.Status filter: a valid status narrows to matching flows, a valid
// status with no matches returns empty, and an unknown status is rejected 400 (it is inlined as a literal
// after validation, so an invalid value must never reach the SQL string).
func TestQuery_StatusFilter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("StatusFlow")
	g.SetEndpoint("A", "qs.verify:0/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("qs.verify:0/g", g)
	proxy.HandleTask("qs.verify:0/a", func(context.Context, *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(ctx))

	const n = 3
	for range n {
		k, err := e.Create(ctx, "qs.verify:0/g", nil, nil)
		assert.NoError(err)
		_, err = e.Await(ctx, k)
		assert.NoError(err)
	}

	// Valid status with matches.
	completed, _, err := e.List(ctx, workflow.Query{Status: workflow.StatusCompleted})
	assert.NoError(err)
	assert.Equal(n, len(completed))
	for _, s := range completed {
		assert.Equal(workflow.StatusCompleted, s.Status)
	}

	// Valid status with no matches (all flows completed, none failed).
	failed, _, err := e.List(ctx, workflow.Query{Status: workflow.StatusFailed})
	assert.NoError(err)
	assert.Equal(0, len(failed))

	// An unknown status is rejected before it can reach the (literal-inlined) SQL.
	_, _, err = e.List(ctx, workflow.Query{Status: "bogus'; DROP TABLE dwarf_flows;--"})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
}

// TestQuery_SearchEscapesWildcards pins that Search treats a caller-supplied LIKE metacharacter as a
// literal, not a wildcard - so "_" (SQL LIKE "any single char") and "%" ("any run") do not steer the
// query into an unbounded full-table scan that matches every flow. One graph's display name carries a
// literal underscore; the others do not.
func TestQuery_SearchEscapesWildcards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	// Display names: only "Under_Score" contains a LIKE metacharacter. URLs and task names carry none, and
	// flow tokens are hex, so the underscore appears in exactly one searched column across all flows.
	plain := workflow.NewGraph("PlainName")
	plain.SetEndpoint("A", "q.esc:0/a")
	plain.AddTransition("A", workflow.END)
	proxy.HandleGraph("q.esc:0/plain", plain)
	under := workflow.NewGraph("Under_Score")
	under.SetEndpoint("B", "q.esc:0/b")
	under.AddTransition("B", workflow.END)
	proxy.HandleGraph("q.esc:0/under", under)
	proxy.HandleTask("q.esc:0/a", func(context.Context, *workflow.Flow) error { return nil })
	proxy.HandleTask("q.esc:0/b", func(context.Context, *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(ctx))

	var keys []string
	for range 3 {
		k, err := e.Create(ctx, "q.esc:0/plain", nil, nil)
		assert.NoError(err)
		keys = append(keys, k)
	}
	k, err := e.Create(ctx, "q.esc:0/under", nil, nil)
	assert.NoError(err)
	keys = append(keys, k)
	for _, k := range keys {
		_, err := e.Await(ctx, k)
		assert.NoError(err)
	}

	// "_" is a wildcard in an unescaped LIKE (would match all 4 flows); escaped, it matches only the one
	// flow whose display name contains a literal underscore.
	underscore, _, err := e.List(ctx, workflow.Query{Search: "_"})
	assert.NoError(err)
	assert.Equal(1, len(underscore))
	if len(underscore) == 1 {
		assert.Equal("Under_Score", underscore[0].WorkflowName)
	}

	// "%" is a wildcard too (would match all); no display name/URL contains a literal percent, so escaped
	// it matches nothing.
	percent, _, err := e.List(ctx, workflow.Query{Search: "%"})
	assert.NoError(err)
	assert.Equal(0, len(percent))

	// A plain substring search still works - the escaping only neutralizes metacharacters.
	byName, _, err := e.List(ctx, workflow.Query{Search: "under_score"})
	assert.NoError(err)
	assert.Equal(1, len(byName))
}
