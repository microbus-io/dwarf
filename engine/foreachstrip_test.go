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
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestForEachStrip_ScopedToItsOwnCohort pins that the per-branch forEach bookkeeping (`<as>`, `<as>Index`,
// `<as>Count`) is stripped ONLY for the cohort being closed - never for every forEach name in the graph.
//
// Stripping by graph made the three injected names of every `as` GLOBALLY RESERVED, silently: a graph with
// `forEach ... as "page"` reserved `pageCount` for the whole workflow, so a task writing its own `pageCount`
// anywhere - even a task downstream of the fan-in, outside the cohort entirely - saw it deleted from
// final_state while History still showed the step had written it. A name collision, keyed on a name the author
// was never told was reserved, producing silent data loss.
func TestForEachStrip_ScopedToItsOwnCohort(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngine()
	proxy := NewTestProxy()
	e.SetHost(proxy)
	e.RunInTest(t)

	g := workflow.NewGraph("Collide")
	g.SetEndpoint("Split", "c/split")
	g.SetEndpoint("Page", "c/page")
	g.SetEndpoint("Join", "c/join")
	g.SetFanIn("Join")
	g.SetReducer("done", workflow.ReducerAdd)
	g.AddTransitionForEach("Split", "Page", "pages", "page")
	g.AddTransitionChain("Page", "Join", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("c/wf", g)

	proxy.HandleTask("c/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("c/page", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("done", 1)
		return nil
	})
	// The fan-in task writes fields whose names collide EXACTLY with the bookkeeping the forEach injects
	// (as="page" reserves page / pageIndex / pageCount). It runs at the convergence - outside the cohort - so
	// they are its own output and must survive to final_state.
	proxy.HandleTask("c/join", func(ctx context.Context, f *workflow.Flow) error {
		// The cohort IS behind us here, so the branch's own bookkeeping is correctly gone from the input.
		if f.Has("page") || f.Has("pageIndex") || f.Has("pageCount") {
			return errors.New("the closing cohort's bookkeeping must not survive its own fan-in")
		}
		f.SetInt("pageCount", f.GetInt("done"))
		f.SetString("page", "summary")
		f.SetInt("pageIndex", 99)
		return nil
	})

	_, outcome, err := e.Run(ctx, "c/wf", map[string]any{"pages": []string{"a", "b", "c"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(3.0, outcome.State["pageCount"], "a task's own field must not be eaten by a same-named bookkeeping key")
	assert.Equal("summary", outcome.State["page"])
	assert.Equal(99.0, outcome.State["pageIndex"])
}

// TestForEachStrip_NestedOuterSurvivesInnerFanIn pins the other half of the scoping. A step converging out of an
// INNER cohort is still inside the OUTER branch, so it must still see which outer element that branch is working
// on. Stripping every forEach in the graph at every fan-in deleted the outer cohort's bookkeeping at the inner
// fan-in, blinding the rest of the outer branch.
func TestForEachStrip_NestedOuterSurvivesInnerFanIn(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngine()
	proxy := NewTestProxy()
	e.SetHost(proxy)
	e.RunInTest(t)

	// Split -forEach(pages, as=page)-> Page -forEach(chunks, as=chunk)-> Chunk -> JoinChunk -> JoinPage
	g := workflow.NewGraph("Nested")
	for _, n := range []string{"Split", "Page", "Chunk", "JoinChunk", "JoinPage"} {
		g.SetEndpoint(n, "n/"+n)
	}
	g.SetFanIn("JoinChunk")
	g.SetFanIn("JoinPage")
	g.SetReducer("seen", workflow.ReducerAppend)
	g.AddTransitionForEach("Split", "Page", "pages", "page")
	g.AddTransitionForEach("Page", "Chunk", "chunks", "chunk")
	g.AddTransitionChain("Chunk", "JoinChunk")
	g.AddTransitionChain("JoinChunk", "JoinPage", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("n/wf", g)

	proxy.HandleTask("n/Split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("n/Page", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("chunks", []string{"c1", "c2"})
		return nil
	})
	proxy.HandleTask("n/Chunk", func(ctx context.Context, f *workflow.Flow) error {
		// A branch of the inner cohort sees BOTH levels' bookkeeping.
		if f.GetString("page") == "" || f.GetString("chunk") == "" {
			return errors.New("an inner branch must see both its own element and its outer one")
		}
		return nil
	})
	// The inner fan-in. Its own cohort (chunks) is behind it, but it is STILL inside a page branch.
	proxy.HandleTask("n/JoinChunk", func(ctx context.Context, f *workflow.Flow) error {
		if f.Has("chunk") || f.Has("chunkIndex") || f.Has("chunkCount") {
			return errors.New("the inner cohort's own bookkeeping must not survive its fan-in")
		}
		page := f.GetString("page")
		if page == "" {
			return errors.New("the OUTER cohort's element must survive an inner fan-in - this branch is still inside it")
		}
		if f.GetInt("pageCount") != 2 {
			return errors.New("the outer cohort's ordinal context must survive an inner fan-in")
		}
		f.Set("seen", []string{page})
		return nil
	})
	proxy.HandleTask("n/JoinPage", func(ctx context.Context, f *workflow.Flow) error {
		if f.Has("page") || f.Has("pageIndex") || f.Has("pageCount") {
			return errors.New("the outer cohort's bookkeeping must be gone once ITS fan-in closes")
		}
		f.SetInt("pagesSeen", len(f.GetStrings("seen")))
		return nil
	})

	_, outcome, err := e.Run(ctx, "n/wf", map[string]any{"pages": []string{"p1", "p2"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal(2.0, outcome.State["pagesSeen"])
	// Neither level's bookkeeping escapes to final_state; both were stripped at their OWN fan-in.
	assert.NotContains(outcome.State, "page")
	assert.NotContains(outcome.State, "chunk")
}

// TestForEachStrip_FailedFanOutStillStrips pins the case the strip exists for, which the scoping must not lose.
// A failed cohort never reaches its fan-in, so the terminal-state merge bases on a completed sibling's
// BRANCH-LOCAL snapshot - which carries that branch's private bookkeeping. Left in, whichever branch happens to
// have the lowest step_id donates its element to the flow's final_state, and with 3+ branches which one wins is
// arbitrary. Here the merge base IS inside the cohort, so its lineage names the spawn and the strip fires.
func TestForEachStrip_FailedFanOutStillStrips(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngine()
	proxy := NewTestProxy()
	e.SetHost(proxy)
	e.RunInTest(t)

	g := workflow.NewGraph("FailFan")
	g.SetEndpoint("Split", "ff/split")
	g.SetEndpoint("Page", "ff/page")
	g.SetEndpoint("Join", "ff/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Split", "Page", "pages", "page")
	g.AddTransitionChain("Page", "Join", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("ff/wf", g)

	proxy.HandleTask("ff/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("ff/page", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("page") == "bad" {
			return errors.New("branch failed")
		}
		return nil
	})
	proxy.HandleTask("ff/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	_, outcome, err := e.Run(ctx, "ff/wf", map[string]any{"pages": []string{"ok1", "bad", "ok2"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	// The surviving branches' private bookkeeping must NOT ride forward into the failed flow's final_state -
	// otherwise a fan-out's terminal state would mean different things depending on how it ended.
	assert.NotContains(outcome.State, "page")
	assert.NotContains(outcome.State, "pageIndex")
	assert.NotContains(outcome.State, "pageCount")
	// The source array the branches were spawned from is still there (it is flow state, not bookkeeping).
	assert.Contains(outcome.State, "pages")
}
