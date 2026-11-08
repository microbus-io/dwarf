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

package fixtures

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
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
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := workflow.NewGraph("Collide")
	g.SetURL("Split", "c/split")
	g.SetURL("Page", "c/page")
	g.SetURL("Join", "c/join")
	g.SetFanIn("Join")
	g.SetReducer("done", workflow.ReducerAdd)
	g.AddTransitionForEach("Split", "Page", "pages", "page")
	g.AddTransitionChain("Page", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("c/wf", g)

	mux.HandleTask("c/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("c/page", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("done", 1)
		return nil
	})
	// The fan-in task writes fields whose names collide EXACTLY with the bookkeeping the forEach injects
	// (as="page" reserves page / pageIndex / pageCount). It runs at the convergence - outside the cohort - so
	// they are its own output and must survive to final_state.
	mux.HandleTask("c/join", func(ctx context.Context, f *workflow.Flow) error {
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
	assert.Equal(3.0, stateVal(outcome.State, "pageCount"), "a task's own field must not be eaten by a same-named bookkeeping key")
	assert.Equal("summary", stateVal(outcome.State, "page"))
	assert.Equal(99.0, stateVal(outcome.State, "pageIndex"))
}

// TestForEachStrip_NestedOuterSurvivesInnerFanIn pins the other half of the scoping. A step converging out of an
// INNER cohort is still inside the OUTER branch, so it must still see which outer element that branch is working
// on. Stripping every forEach in the graph at every fan-in deleted the outer cohort's bookkeeping at the inner
// fan-in, blinding the rest of the outer branch.
func TestForEachStrip_NestedOuterSurvivesInnerFanIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// Split -forEach(pages, as=page)-> Page -forEach(chunks, as=chunk)-> Chunk -> JoinChunk -> JoinPage
	g := workflow.NewGraph("Nested")
	for _, n := range []string{"Split", "Page", "Chunk", "JoinChunk", "JoinPage"} {
		g.SetURL(n, "n/"+n)
	}
	g.SetFanIn("JoinChunk")
	g.SetFanIn("JoinPage")
	g.SetReducer("seen", workflow.ReducerAppend)
	g.AddTransitionForEach("Split", "Page", "pages", "page")
	g.AddTransitionForEach("Page", "Chunk", "chunks", "chunk")
	g.AddTransitionChain("Chunk", "JoinChunk")
	g.AddTransitionChain("JoinChunk", "JoinPage", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("n/wf", g)

	mux.HandleTask("n/Split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("n/Page", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("chunks", []string{"c1", "c2"})
		return nil
	})
	mux.HandleTask("n/Chunk", func(ctx context.Context, f *workflow.Flow) error {
		// A branch of the inner cohort sees BOTH levels' bookkeeping.
		if f.GetString("page") == "" || f.GetString("chunk") == "" {
			return errors.New("an inner branch must see both its own element and its outer one")
		}
		return nil
	})
	// The inner fan-in. Its own cohort (chunks) is behind it, but it is STILL inside a page branch.
	mux.HandleTask("n/JoinChunk", func(ctx context.Context, f *workflow.Flow) error {
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
	mux.HandleTask("n/JoinPage", func(ctx context.Context, f *workflow.Flow) error {
		if f.Has("page") || f.Has("pageIndex") || f.Has("pageCount") {
			return errors.New("the outer cohort's bookkeeping must be gone once ITS fan-in closes")
		}
		f.SetInt("pagesSeen", len(f.GetStrings("seen")))
		return nil
	})

	_, outcome, err := e.Run(ctx, "n/wf", map[string]any{"pages": []string{"p1", "p2"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal(2.0, stateVal(outcome.State, "pagesSeen"))
	// Neither level's bookkeeping escapes to final_state; both were stripped at their OWN fan-in.
	assert.False(outcome.State.Has("page"))
	assert.False(outcome.State.Has("chunk"))
}

// TestForEachStrip_FailedFanOutStillStrips pins the case the strip exists for, which the scoping must not lose.
// A failed cohort never reaches its fan-in, so the terminal-state merge bases on a completed sibling's
// BRANCH-LOCAL snapshot - which carries that branch's private bookkeeping. Left in, whichever branch happens to
// have the lowest step_id donates its element to the flow's final_state, and with 3+ branches which one wins is
// arbitrary. Here the merge base IS inside the cohort, so its lineage names the spawn and the strip fires.
func TestForEachStrip_FailedFanOutStillStrips(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := workflow.NewGraph("FailFan")
	g.SetURL("Split", "ff/split")
	g.SetURL("Page", "ff/page")
	g.SetURL("Join", "ff/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Split", "Page", "pages", "page")
	g.AddTransitionChain("Page", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ff/wf", g)

	mux.HandleTask("ff/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("ff/page", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("page") == "bad" {
			return errors.New("branch failed")
		}
		return nil
	})
	mux.HandleTask("ff/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	_, outcome, err := e.Run(ctx, "ff/wf", map[string]any{"pages": []string{"ok1", "bad", "ok2"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	// The surviving branches' private bookkeeping must NOT ride forward into the failed flow's final_state -
	// otherwise a fan-out's terminal state would mean different things depending on how it ended.
	assert.False(outcome.State.Has("page"))
	assert.False(outcome.State.Has("pageIndex"))
	assert.False(outcome.State.Has("pageCount"))
	// The source array the branches were spawned from is still there (it is flow state, not bookkeeping).
	assert.True(outcome.State.Has("pages"))
}

// TestFailedFanOut_KeepsEveryBranchesIntermediateOutput pins that a fan-out which resolved with FAILURES reports
// the same terminal state a successful convergence would have - specifically, that it keeps the intermediate
// output of EVERY surviving branch, not just the first.
//
// The hole this guards: a branch's intermediate output lives in the NEXT step's `state`, not in any tail's
// `changes`. A terminal merge that bases on the FIRST tail's `state` and folds in only the tails' `changes` keeps
// branch 0's Cell output in `forEach -> Cell -> Enrich -> Join` (it rides in via the base) and silently drops
// branches 1..N-1's. The converged path (fanInRow) merges EVERY cohort member's changes, so the failed path must
// run the same merge - spawn base + every completed member's changes, in fan_out_ordinal order - or the two
// paths disagree on what a fan-out's terminal state means.
func TestFailedFanOut_KeepsEveryBranchesIntermediateOutput(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// Two-STEP branches: Cell writes, then Enrich writes. Cell's output survives only in Enrich's `state`.
	g := workflow.NewGraph("MultiStepBranch")
	for _, n := range []string{"Seed", "Cell", "Enrich", "Join"} {
		g.SetURL(n, "ms/"+n)
	}
	g.SetFanIn("Join")
	g.SetReducer("cells", workflow.ReducerAppend)
	g.SetReducer("enriched", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Cell", "items", "item")
	g.AddTransitionChain("Cell", "Enrich", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ms/wf", g)

	mux.HandleTask("ms/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	// The FIRST step of each branch. Its output must reach final_state for every branch, not just branch 0.
	mux.HandleTask("ms/Cell", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("cells", []string{"cell:" + f.GetString("item")})
		return nil
	})
	// The SECOND step. One branch fails here, so the cohort resolves with failures and never reaches the fan-in.
	mux.HandleTask("ms/Enrich", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("item") == "b" {
			return errors.New("branch b failed at Enrich")
		}
		f.Set("enriched", []string{"enriched:" + f.GetString("item")})
		return nil
	})
	mux.HandleTask("ms/Join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	_, outcome, err := e.Run(ctx, "ms/wf", map[string]any{"items": []string{"a", "b", "c"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)

	// Cell ran and COMPLETED in all three branches, so all three contributions survive - including branch b's,
	// whose branch later failed at Enrich (Cell's own step completed; only Enrich failed).
	cells, _ := stateVal(outcome.State, "cells").([]any)
	assert.Equal(3, len(cells), "every branch's FIRST-step output must survive, not just the first branch's")
	assert.Contains(cells, "cell:a")
	assert.Contains(cells, "cell:b")
	assert.Contains(cells, "cell:c")

	// Enrich completed only in branches a and c; b's failed step contributes nothing (an error voids its changes).
	enriched, _ := stateVal(outcome.State, "enriched").([]any)
	assert.Equal(2, len(enriched))
	assert.Contains(enriched, "enriched:a")
	assert.Contains(enriched, "enriched:c")

	// The per-branch bookkeeping still does not ride forward, and the source array still does.
	assert.False(outcome.State.Has("item"))
	assert.True(outcome.State.Has("items"))
}

// TestForEachStrip_FailedNestedFanOutMatchesConvergence pins that a nested fan-out which FAILED reports the
// final_state its fan-ins would have built, at every level. Each cohort that never converged is folded as its fan-in
// would have folded it, stripped of its OWN bookkeeping only, and handed to the next cohort out as that fan-in's
// part would have been.
//
// The shape is chapters -> pages -> lines, and the trunk carries `pageCount` and `lineCount` of its own - names
// the inner cohorts also inject. Two traps:
//   - Stripping every cohort's names from the final state deletes the TRUNK's `pageCount`/`lineCount` on a failure,
//     while a convergence keeps them: the inner names were only ever stripped from the inner folds.
//   - Handing a never-converged cohort's raw contributions to the outer fold keeps a Line's own write to `lineCount`,
//     which a convergence turns into a delete: the inner fan-in strips it, and its part projects the absence.
//
// The successful branches' appends land either way - a failure voids only the failed task's own writes.
func TestForEachStrip_FailedNestedFanOutMatchesConvergence(t *testing.T) {
	t.Parallel()

	run := func(t *testing.T, failAt string, lineWritesOwn bool) *workflow.FlowOutcome {
		ctx := context.Background()
		assert := testarossa.For(t)
		e := engine.NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		mux := hosts.NewMux()
		e.SetHost(mux)
		assert.Must(assert.NoError(e.Startup(t.Context())))

		g := bookGraph(map[string]workflow.Reducer{"collection": workflow.ReducerAppend})
		assert.NoError(g.Validate())
		mux.HandleGraph("book/wf", g)

		nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
		mux.HandleTask("book/Seed", nop)
		mux.HandleTask("book/JoinLines", nop)
		mux.HandleTask("book/JoinPages", nop)
		mux.HandleTask("book/JoinChapters", nop)
		mux.HandleTask("book/Chapter", func(ctx context.Context, f *workflow.Flow) error {
			f.Set("pages", []string{"p1", "p2"})
			return nil
		})
		mux.HandleTask("book/Page", func(ctx context.Context, f *workflow.Flow) error {
			if failAt == "page" && f.GetString("chapter") == "c2" && f.GetString("page") == "p2" {
				return errors.New("page failed")
			}
			f.Set("lines", []string{"l1", "l2"})
			return nil
		})
		mux.HandleTask("book/Line", func(ctx context.Context, f *workflow.Flow) error {
			at := f.GetString("chapter") + "/" + f.GetString("page") + "/" + f.GetString("line")
			f.Set("collection", []string{at})
			if lineWritesOwn {
				f.Set("lineCount", 1)
			}
			if failAt == "line" && at == "c2/p2/l2" {
				return errors.New("line failed")
			}
			return nil
		})

		_, out, err := e.Run(ctx, "book/wf", map[string]any{
			"chapters": []string{"c1", "c2"}, "pageCount": 635, "lineCount": 9000,
		}, nil)
		if !assert.NoError(err) || !assert.NotNil(out) {
			t.FailNow()
		}
		return out
	}

	all := []string{
		"c1/p1/l1", "c1/p1/l2", "c1/p2/l1", "c1/p2/l2",
		"c2/p1/l1", "c2/p1/l2", "c2/p2/l1", "c2/p2/l2",
	}
	for _, tc := range []struct {
		failAt     string
		status     string
		collection []string
	}{
		{"", workflow.StatusCompleted, all},
		{"line", workflow.StatusFailed, all[:7]},
		{"page", workflow.StatusFailed, all[:6]},
	} {
		for _, writesOwn := range []bool{false, true} {
			name := "converged"
			if tc.failAt != "" {
				name = tc.failAt + "_failed"
			}
			if writesOwn {
				name += "_line_writes_lineCount"
			}
			t.Run(name, func(t *testing.T) {
				assert := testarossa.For(t)
				out := run(t, tc.failAt, writesOwn)
				assert.Equal(tc.status, out.Status, out.Error)
				assert.Equal(tc.collection, out.State.GetStrings("collection"), "every completed branch's append lands, in input order")
				assert.Equal(635, out.State.GetInt("pageCount"), "a trunk field sharing an inner `as` name survives")
				if writesOwn {
					assert.False(out.State.Has("lineCount"), "a Line's own write to lineCount projects as a delete, as at its fan-in")
				} else {
					assert.Equal(9000, out.State.GetInt("lineCount"), "a trunk field sharing an inner `as` name survives")
				}
				for _, k := range []string{"chapter", "chapterIndex", "chapterCount", "page", "pageIndex", "line", "lineIndex"} {
					assert.False(out.State.Has(k), "no cohort's bookkeeping reaches final_state: %s", k)
				}
			})
		}
	}
}

// bookGraph is the three-level fan-out the nested final-state tests run: chapters -> pages -> lines, each level
// converging on its own fan-in, with every field in reducers wired to its reducer.
func bookGraph(reducers map[string]workflow.Reducer) *workflow.Graph {
	g := workflow.NewGraph("Book")
	for _, n := range []string{"Seed", "Chapter", "Page", "Line", "JoinLines", "JoinPages", "JoinChapters"} {
		g.SetURL(n, "book/"+n)
	}
	for field, r := range reducers {
		g.SetReducer(field, r)
	}
	g.AddTransitionForEach("Seed", "Chapter", "chapters", "chapter")
	g.AddTransitionForEach("Chapter", "Page", "pages", "page")
	g.AddTransitionForEach("Page", "Line", "lines", "line")
	g.AddTransition("Line", "JoinLines")
	g.SetFanIn("JoinLines")
	g.AddTransition("JoinLines", "JoinPages")
	g.SetFanIn("JoinPages")
	g.AddTransition("JoinPages", "JoinChapters")
	g.SetFanIn("JoinChapters")
	g.AddTransition("JoinChapters", workflow.End)
	return g
}

// TestForEachStrip_FailedNestedFanOutMatchesConvergenceForEveryReducer compares two runs of one nested fan-out:
// in one a Line fails, in the other the same Line completes having written nothing, and the fan-in tasks write
// nothing in either. Their final states must be identical - every reducer kind folded through a simulated inner
// fan-in must give what a real one gives. It also carries a trunk field that shares the middle cohort's `as` name
// (`pageCount`), writes the outer and middle cohorts' bookkeeping names from a Line, deletes a trunk field, and
// carries large fields by ref ("refs": a large trunk field, a large append base,
// and a large field each Chapter writes, which its inner cohorts carry as a ref).
func TestForEachStrip_FailedNestedFanOutMatchesConvergenceForEveryReducer(t *testing.T) {
	t.Parallel()
	big := strings.Repeat("x", 9000)

	run := func(t *testing.T, fail, refs bool) *workflow.FlowOutcome {
		ctx := context.Background()
		assert := testarossa.For(t)
		e := engine.NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		mux := hosts.NewMux()
		e.SetHost(mux)
		assert.Must(assert.NoError(e.Startup(t.Context())))

		g := bookGraph(map[string]workflow.Reducer{
			"app": workflow.ReducerAppend, "uni": workflow.ReducerUnion, "cat": workflow.ReducerConcat,
			"sum": workflow.ReducerAdd, "lo": workflow.ReducerMin, "hi": workflow.ReducerMax,
			"all": workflow.ReducerAnd, "any": workflow.ReducerOr, "obj": workflow.ReducerMerge,
			"bigapp": workflow.ReducerAppend,
		})
		assert.NoError(g.Validate())
		mux.HandleGraph("book/wf", g)
		nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
		for _, n := range []string{"JoinLines", "JoinPages", "JoinChapters"} {
			mux.HandleTask("book/"+n, nop)
		}
		mux.HandleTask("book/Seed", func(ctx context.Context, f *workflow.Flow) error {
			if refs {
				f.Set("bigapp", []string{big})
			}
			f.Set("app", []string{"seed"})
			return nil
		})
		mux.HandleTask("book/Chapter", func(ctx context.Context, f *workflow.Flow) error {
			f.Set("pages", []string{"p1", "p2"})
			f.Set("app", []string{"ch-" + f.GetString("chapter")})
			f.Set("sum", 100)
			if refs {
				f.Set("chbig", big+f.GetString("chapter"))
			}
			return nil
		})
		mux.HandleTask("book/Page", func(ctx context.Context, f *workflow.Flow) error {
			f.Set("lines", []string{"l1", "l2", "l3"})
			f.Set("app", []string{"pg-" + f.GetString("chapter") + f.GetString("page")})
			f.Set("cat", "P")
			f.Set("obj", map[string]any{"pg" + f.GetString("page"): 1})
			return nil
		})
		mux.HandleTask("book/Line", func(ctx context.Context, f *workflow.Flow) error {
			at := f.GetString("chapter") + "/" + f.GetString("page") + "/" + f.GetString("line")
			bad := at == "c2/p2/l2"
			if bad && !fail {
				return nil // the converged twin of the failing branch: it contributes nothing either
			}
			f.Set("app", []string{at})
			if refs {
				f.Set("bigapp", []string{at})
			}
			f.Set("uni", []string{f.GetString("line"), "x"})
			f.Set("cat", at+";")
			f.Set("sum", 1)
			f.Set("lo", len(at))
			f.Set("hi", len(at)+f.GetInt("lineIndex"))
			f.Set("all", f.GetString("line") != "l3")
			f.Set("any", false)
			f.Set("obj", map[string]any{at: f.GetInt("lineIndex")})
			f.Set("plain", at)
			f.Set("pageIndex", 77)
			f.Set("chapter", "written-by-a-line")
			if f.GetString("line") == "l1" {
				f.Del("scratch")
			}
			if bad {
				return errors.New("line failed")
			}
			return nil
		})

		initial := map[string]any{
			"chapters": []string{"c1", "c2"}, "pageCount": 635, "scratch": "keep", "sum": 5, "lo": 1000, "hi": 0,
			"all": true, "any": true, "obj": map[string]any{"trunk": true}, "uni": []string{"x"}, "cat": "T:",
		}
		if refs {
			initial["doc"] = big
		}
		_, out, err := e.Run(ctx, "book/wf", initial, nil)
		if !assert.NoError(err) || !assert.NotNil(out) {
			t.FailNow()
		}
		return out
	}

	for _, refs := range []bool{false, true} {
		name := "inline"
		if refs {
			name = "refs"
		}
		t.Run(name, func(t *testing.T) {
			assert := testarossa.For(t)
			var converged, failed *workflow.FlowOutcome
			t.Run("converged", func(t *testing.T) { converged = run(t, false, refs) })
			t.Run("failed", func(t *testing.T) { failed = run(t, true, refs) })
			if converged == nil || failed == nil {
				t.FailNow()
			}
			assert.Equal(workflow.StatusCompleted, converged.Status, converged.Error)
			assert.Equal(workflow.StatusFailed, failed.Status)
			// Decoded before comparing: stored JSON bytes are not canonical, so only the values are.
			decode := func(s workflow.State) any {
				raw, err := json.Marshal(s)
				assert.NoError(err)
				var v any
				assert.NoError(json.Unmarshal(raw, &v))
				return v
			}
			assert.Equal(decode(converged.State), decode(failed.State), "a failed nested fan-out's final_state is what its fan-ins would have built")
		})
	}
}

// TestForEachStrip_TerminatedNestedFanOutKeepsCompletedBranches pins the other way a nested cohort is left
// unconverged: a Terminate while one of its branches is still running, so the cohort has arrived short of its
// size with no failure. The final state is the same simulated fold - every completed Line's append, the trunk's
// own `pageCount`, and the Lines' own writes to `lineCount` projected as a delete.
func TestForEachStrip_TerminatedNestedFanOutKeepsCompletedBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := bookGraph(map[string]workflow.Reducer{"collection": workflow.ReducerAppend})
	assert.NoError(g.Validate())
	mux.HandleGraph("book/wf", g)
	nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	for _, n := range []string{"Seed", "JoinLines", "JoinPages", "JoinChapters"} {
		mux.HandleTask("book/"+n, nop)
	}
	mux.HandleTask("book/Chapter", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("pages", []string{"p1", "p2"})
		return nil
	})
	mux.HandleTask("book/Page", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("lines", []string{"l1", "l2"})
		return nil
	})
	hold := make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(hold) }) // after the Shutdown defer, so it unwinds first
	mux.HandleTask("book/Line", func(ctx context.Context, f *workflow.Flow) error {
		at := f.GetString("chapter") + "/" + f.GetString("page") + "/" + f.GetString("line")
		if at == "c2/p2/l2" {
			select {
			case <-hold:
			case <-ctx.Done():
			}
			return nil
		}
		f.Set("collection", []string{at})
		f.Set("lineCount", 1)
		return nil
	})

	flowKey, err := e.Create(ctx, "book/wf", map[string]any{"chapters": []string{"c1", "c2"}, "pageCount": 635, "lineCount": 9000}, nil)
	if !assert.NoError(err) {
		return
	}
	// Every Line but the held one has committed its completion.
	shard, _, _, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	assert.True(enginetest.WaitUntil(t, 30*time.Second, func() bool {
		return enginetest.CountRows(t, e, shard,
			"SELECT COUNT(*) FROM dwarf_steps WHERE task_name='Line' AND status='"+workflow.StatusCompleted+"'") == 7
	}), "seven Lines never completed")

	assert.NoError(e.Terminate(ctx, flowKey, "stop"))
	release.Do(func() { close(hold) })
	out, err := e.Snapshot(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusTerminated, out.Status)
	assert.Equal([]string{
		"c1/p1/l1", "c1/p1/l2", "c1/p2/l1", "c1/p2/l2", "c2/p1/l1", "c2/p1/l2", "c2/p2/l1",
	}, out.State.GetStrings("collection"))
	assert.Equal(635, out.State.GetInt("pageCount"))
	assert.False(out.State.Has("lineCount"), "the Lines' own writes to lineCount project as a delete, as at their fan-in")
}

// TestForEachStrip_JSONGraphWithoutAlias pins that a graph decoded from JSON with no forEach `as` strips the default
// alias's branch-private names exactly as a graph built with AddTransitionForEach does, at a converged fan-in and in
// a failed fan-out's final state. The alias defaulted only at dispatch, so the strip, keyed on the stored alias,
// found none, and item, itemIndex and itemCount rode past the cohort.
func TestForEachStrip_JSONGraphWithoutAlias(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	built := workflow.NewGraph("NoAlias")
	built.SetURL("S", "noalias/s")
	built.SetURL("W", "noalias/w")
	built.SetURL("J", "noalias/j")
	built.SetFanIn("J")
	built.AddTransitionForEach("S", "W", "items", "")
	built.AddTransitionChain("W", "J", workflow.End)
	raw, err := json.Marshal(built)
	assert.Must(assert.NoError(err))
	var doc map[string]any
	assert.Must(assert.NoError(json.Unmarshal(raw, &doc)))
	for _, tr := range doc["transitions"].([]any) {
		delete(tr.(map[string]any), "as")
	}
	raw, err = json.Marshal(doc)
	assert.Must(assert.NoError(err))
	assert.False(strings.Contains(string(raw), `"as"`), "the host's JSON carries no alias")
	var g workflow.Graph
	assert.Must(assert.NoError(json.Unmarshal(raw, &g)))
	assert.NoError(g.Validate())
	mux.HandleGraph("noalias/wf", &g)

	mux.HandleTask("noalias/s", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("noalias/w", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("item") == 0 {
			return errors.New("branch failed")
		}
		// The branch writes its private names itself, so they are in its changes and would fold into the fan-in.
		f.SetInt("item", 99)
		f.SetInt("itemIndex", 99)
		f.SetInt("itemCount", 99)
		return nil
	})
	mux.HandleTask("noalias/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	for _, tc := range []struct {
		name   string
		items  []int
		status string
	}{
		{"converged", []int{1, 2}, workflow.StatusCompleted},
		{"failed", []int{1, 0, 2}, workflow.StatusFailed},
	} {
		_, outcome, err := e.Run(ctx, "noalias/wf", map[string]any{"items": tc.items}, nil)
		assert.NoError(err)
		assert.Equal(tc.status, outcome.Status, tc.name)
		for _, name := range []string{"item", "itemIndex", "itemCount"} {
			assert.False(outcome.State.Has(name), "%s: %s escaped the cohort", tc.name, name)
		}
		assert.True(outcome.State.Has("items"), tc.name)
	}
}
