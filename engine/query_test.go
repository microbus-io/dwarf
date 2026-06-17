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

	"github.com/microbus-io/dwarf/workflow"
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

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	const alphas, betas = 3, 2
	for range alphas {
		_, err := e.Create(ctx, "q.verify:0/alpha", nil, nil)
		assert.NoError(err)
	}
	for range betas {
		_, err := e.Create(ctx, "q.verify:0/beta", nil, nil)
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
