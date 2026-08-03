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

/*
Search literal-wildcard pin. Query.Search escapes the LIKE metacharacters
(% and _) in the caller's term, so a search matches the term LITERALLY rather than being steered
into an unbounded wildcard scan: "a_b" matches only "a_b" (not "axb"), and "50%" matches only
"50%off". This pins that escaping and guards the workflow/query.go Search godoc wording.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestSearchLiteralflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Three flows whose workflow URLs contain LIKE metacharacters or their literal look-alikes. The graph
	// display name and task URL are neutral (no search-term substrings) so only the workflow_url matches.
	urls := []string{"a_b", "axb", "50%off"}
	for _, u := range urls {
		g := workflow.NewGraph("SearchWF")
		g.SetEndpoint("Only", "searchliteral.verify:428/only")
		g.AddTransition("Only", workflow.END)
		proxy.HandleGraph(u, g)
	}
	proxy.HandleTask("searchliteral.verify:428/only", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	byURL := map[string]string{} // workflow_url -> flowKey
	for _, u := range urls {
		fk, outcome, err := eng.Run(ctx, u, nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		byURL[u] = fk
	}

	// searchURLs returns the set of workflow URLs matched by a Search term.
	searchURLs := func(term string) map[string]bool {
		flows, _, err := eng.List(ctx, workflow.Query{Search: term})
		assert.NoError(err)
		got := map[string]bool{}
		for _, s := range flows {
			got[s.WorkflowURL] = true
		}
		return got
	}

	t.Run("underscore_matches_literally", func(t *testing.T) {
		assert := testarossa.For(t)
		got := searchURLs("a_b")
		// "_" is a single-char LIKE wildcard; escaped, so "a_b" must NOT match "axb".
		assert.True(got["a_b"], "search a_b should match a_b")
		assert.False(got["axb"], "search a_b must not match axb (underscore is literal)")
		assert.Equal(1, len(got))
	})

	t.Run("percent_matches_literally", func(t *testing.T) {
		assert := testarossa.For(t)
		got := searchURLs("50%")
		// "%" is the multi-char LIKE wildcard; escaped, so "50%" matches only the literal "50%off".
		assert.True(got["50%off"], "search 50%% should match 50%%off")
		assert.False(got["a_b"], "search 50%% must not match a_b")
		assert.False(got["axb"], "search 50%% must not match axb")
		assert.Equal(1, len(got))
	})
}
