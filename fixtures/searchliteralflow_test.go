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
Search literal-wildcard pin. Query.Search escapes the LIKE metacharacters in the caller's term, so a
search matches the term LITERALLY rather than as a wildcard: "a_b" matches only "a_b" (not "axb"),
"50%" only "50%off", and "[ab]" only "set[ab]" (not "seta", which SQL Server's character set would
match). Purge selects its victims through the same clause, so it removes only the literal match. This
pins that escaping and guards the workflow/query.go Search godoc wording.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestSearchLiteralflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	// Three flows whose workflow URLs contain LIKE metacharacters or their literal look-alikes. The graph
	// display name and task URL are neutral (no search-term substrings) so only the workflow_url matches.
	urls := []string{"a_b", "axb", "50%off", "set[ab]", "seta", "range[c-e]", "ranged"}
	for _, u := range urls {
		g := workflow.NewGraph("SearchWF")
		g.SetURL("Only", "searchliteral.verify:428/only")
		g.AddTransition("Only", workflow.End)
		mux.HandleGraph(u, g)
	}
	mux.HandleTask("searchliteral.verify:428/only", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

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
		flows, err := enginetest.ListAll(ctx, eng, workflow.Query{Search: term})
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

	t.Run("brackets_match_literally", func(t *testing.T) {
		assert := testarossa.For(t)
		// "[ab]" is a character set and "[c-e]" a range in SQL Server's LIKE; escaped, each matches only itself.
		assert.Equal(map[string]bool{"set[ab]": true}, searchURLs("[ab]"))
		assert.Equal(map[string]bool{"range[c-e]": true}, searchURLs("[c-e]"))
	})

	t.Run("purge_removes_only_the_literal_match", func(t *testing.T) {
		assert := testarossa.For(t)
		enginetest.ArchiveNow(ctx, eng)
		n, _, err := eng.Purge(ctx, enginetest.Recent(workflow.Query{Search: "[ab]"}))
		assert.NoError(err)
		assert.Equal(1, n)
		got := searchURLs("")
		assert.False(got["set[ab]"], "the literal match survived the purge")
		for _, u := range []string{"a_b", "axb", "50%off", "seta", "range[c-e]", "ranged"} {
			assert.True(got[u], "purge of [ab] removed %s", u)
		}
	})
}
