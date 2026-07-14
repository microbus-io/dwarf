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
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestStaterefsflow is the end-to-end contract of state refs: a large carried field is stored ONCE and
// referenced by every step that merely carries it - and NOTHING a workflow author or an API caller can observe
// changes. The ref encoding is internal storage; every task still receives the whole value, and every flow
// boundary (final_state, Continue, Fork, Snapshot, History) is materialized.
//
// The shape is the doc-extraction one that motivated the design: a document is fanned out over its pages and
// each page over its chunks, so the payload would otherwise be re-serialized into ~N*D step rows. The engine's
// old concession to this - deleting the forEach source array from each branch's state - was removed precisely
// because it fixed one field by hand while every other carried field paid the same cost. Refs own it now, and
// they own it without lying to the branch: a branch still sees the array its own element came from.
func TestStaterefsflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	// Big enough to be worth an anchor at any fan-out width, and distinctive enough that a truncated or
	// mis-resolved carry is impossible to mistake for the real thing.
	const docLen = 40000
	pdf := strings.Repeat("P", docLen)

	graph := workflow.NewGraph("DocRefs")
	graph.SetEndpoint("Split", "staterefsflow.verify:429/split")
	graph.SetEndpoint("Page", "staterefsflow.verify:429/page")
	graph.SetEndpoint("Collect", "staterefsflow.verify:429/collect")
	graph.SetFanIn("Collect")
	graph.SetReducer("transcript", workflow.ReducerAppend)
	graph.AddTransitionForEach("Split", "Page", "pages", "page")
	graph.AddTransitionChain("Page", "Collect", workflow.END)
	if err := graph.Validate(); err != nil {
		t.Fatal(err)
	}
	proxy.HandleGraph("staterefsflow.verify:429/doc-refs", graph)

	// The entry task adds nothing to the payload; it exists so the fan-out has a spawn. The document arrives
	// as INITIAL STATE, so its bytes live in the entry step's `state` column - an anchor no task ever wrote,
	// and the case a changes-only resolver would miss entirely.
	proxy.HandleTask("staterefsflow.verify:429/split", func(ctx context.Context, f *workflow.Flow) error {
		if len(f.GetString("pdf")) != docLen {
			return errors.New("the spawn cannot see the initial document")
		}
		return nil
	})
	// Every branch must see the WHOLE document (carried by reference), its own element, and the source array
	// its element came from - the branch's state is not a lie.
	proxy.HandleTask("staterefsflow.verify:429/page", func(ctx context.Context, f *workflow.Flow) error {
		if got := len(f.GetString("pdf")); got != docLen {
			return errors.New("branch sees a %d-byte document, want %d", got, docLen)
		}
		// The branch can still see the ARRAY its own element came from - the engine no longer strips it, and
		// refs de-duplicate it instead of deleting it. The count varies by turn, so assert presence, not size.
		if len(f.GetStrings("pages")) == 0 {
			return errors.New("branch cannot see the forEach source array it was spawned from")
		}
		f.Set("transcript", []string{f.GetString("page") + ":" + f.GetString("pdf")[:1]})
		return nil
	})
	// The fan-in folds the append reducer (so its operands are materialized) while the document is merely
	// carried across the cohort and must NOT be re-copied to get there.
	proxy.HandleTask("staterefsflow.verify:429/collect", func(ctx context.Context, f *workflow.Flow) error {
		if len(f.GetString("pdf")) != docLen {
			return errors.New("the fan-in cannot see the carried document")
		}
		f.SetInt("pdfLen", len(f.GetString("pdf")))
		f.SetInt("transcribed", len(f.GetStrings("transcript")))
		return nil
	})

	initialState := map[string]any{
		"pdf":   pdf,
		"pages": []string{"p1", "p2", "p3", "p4", "p5", "p6"},
	}

	t.Run("carried_document_survives_fanout_and_fanin", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, outcome, err := eng.Run(ctx, "staterefsflow.verify:429/doc-refs", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(float64(docLen), outcome.State["pdfLen"])
		assert.Equal(6.0, outcome.State["transcribed"])
		// FLATTEN at the flow boundary: final_state is a dwarf_flows column that outlives the steps backing it,
		// so it is always materialized. A ref that escaped the flow would dangle.
		assert.Equal(docLen, len(outcome.State["pdf"].(string)))

		// History/Step are API surface, so they report the input each step actually SAW - never a ref.
		steps, err := eng.History(ctx, flowKey)
		assert.NoError(err)
		branches := 0
		for _, s := range steps {
			if s.TaskName != "Page" {
				continue
			}
			branches++
			step, err := eng.Step(ctx, s.StepKey)
			assert.NoError(err)
			doc, ok := step.State["pdf"].(string)
			assert.True(ok, "Step must materialize the branch's carried document, not expose a ref")
			assert.Equal(docLen, len(doc))
		}
		assert.Equal(6, branches)
	})

	t.Run("continue_carries_the_document_into_the_next_turn", func(t *testing.T) {
		assert := testarossa.For(t)

		// The prior turn's final_state seeds the next turn's INITIAL state - so the document crosses a flow
		// boundary (materialized) and is then re-anchored in the new flow's entry step. This is the second of
		// the three anchor locations, reached the way real multi-turn workflows reach it.
		threadKey, outcome, err := eng.Run(ctx, "staterefsflow.verify:429/doc-refs", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		next, err := eng.Continue(ctx, threadKey, map[string]any{"pages": []string{"q1", "q2"}})
		assert.NoError(err)
		turn2, err := eng.Await(ctx, next)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, turn2.Status)
		// Wait - the second turn re-runs with 2 pages and must still see the full document it inherited.
		assert.Equal(float64(docLen), turn2.State["pdfLen"])
		assert.Equal(docLen, len(turn2.State["pdf"].(string)))
	})

	t.Run("fork_clones_a_self_contained_tree", func(t *testing.T) {
		assert := testarossa.For(t)

		// Fork remaps every ref target through its clone id map (the reason refs live in their own column: the
		// payload columns ride a DB-side INSERT...SELECT and never pass through the engine). The forked flow
		// must be self-contained - its steps' anchors are its OWN cloned steps, so it still resolves the
		// document even though the origin is untouched.
		flowKey, outcome, err := eng.Run(ctx, "staterefsflow.verify:429/doc-refs", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		steps, err := eng.History(ctx, flowKey)
		assert.NoError(err)
		var branchKey string
		for _, s := range steps {
			if s.TaskName == "Page" {
				branchKey = s.StepKey
				break
			}
		}
		assert.NotEqual("", branchKey)

		forked, err := eng.Fork(ctx, branchKey, nil)
		assert.NoError(err)
		forkOutcome, err := eng.Await(ctx, forked)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, forkOutcome.Status)
		assert.Equal(float64(docLen), forkOutcome.State["pdfLen"])
		assert.Equal(6.0, forkOutcome.State["transcribed"])

		// The origin is never mutated by a fork.
		origin, err := eng.Snapshot(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, origin.Status)
		assert.Equal(docLen, len(origin.State["pdf"].(string)))
	})

	t.Run("a_task_can_overwrite_and_delete_a_refd_field", func(t *testing.T) {
		assert := testarossa.For(t)

		// The three-case merge rule, end to end. A branch REWRITES the carried document (so the ref is stale
		// and must re-anchor at the writer), and the fan-in DELETES it (a tombstone must kill the ref, not
		// leave the field resurrectable through it).
		g := workflow.NewGraph("DocRefsMutate")
		g.SetEndpoint("Split", "staterefsflow.verify:429/m-split")
		g.SetEndpoint("Page", "staterefsflow.verify:429/m-page")
		g.SetEndpoint("Collect", "staterefsflow.verify:429/m-collect")
		g.SetEndpoint("After", "staterefsflow.verify:429/m-after")
		g.SetFanIn("Collect")
		g.AddTransitionForEach("Split", "Page", "pages", "page")
		g.AddTransitionChain("Page", "Collect")
		g.AddTransitionChain("Collect", "After", workflow.END)
		assert.NoError(g.Validate())
		proxy.HandleGraph("staterefsflow.verify:429/doc-refs-mutate", g)

		replacement := strings.Repeat("R", docLen)
		proxy.HandleTask("staterefsflow.verify:429/m-split", func(ctx context.Context, f *workflow.Flow) error {
			// Overwrite the inherited document: the new bytes are in THIS step's changes, so the successors'
			// refs must re-anchor here rather than keep pointing at the entry step's stale copy.
			f.SetString("pdf", replacement)
			return nil
		})
		proxy.HandleTask("staterefsflow.verify:429/m-page", func(ctx context.Context, f *workflow.Flow) error {
			if got := f.GetString("pdf"); got != replacement {
				return errors.New("branch sees the STALE document, not the rewritten one")
			}
			return nil
		})
		proxy.HandleTask("staterefsflow.verify:429/m-collect", func(ctx context.Context, f *workflow.Flow) error {
			if f.GetString("pdf") != replacement {
				return errors.New("the fan-in sees the stale document")
			}
			f.Delete("pdf") // a tombstone must invalidate the ref, not just the inline copy
			return nil
		})
		proxy.HandleTask("staterefsflow.verify:429/m-after", func(ctx context.Context, f *workflow.Flow) error {
			f.SetBool("pdfPresent", f.Has("pdf"))
			f.SetInt("pdfLen", len(f.GetString("pdf")))
			return nil
		})

		_, outcome, err := eng.Run(ctx, "staterefsflow.verify:429/doc-refs-mutate", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(false, outcome.State["pdfPresent"], "a deleted field must not be resurrectable through its ref")
		assert.Equal(0.0, outcome.State["pdfLen"])
		assert.NotContains(outcome.State, "pdf")
	})
}
