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
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestStaterefs_CarriedRefAcrossFanInWhenSpawnOnlyCarries guards a carried ref crossing a fan-in when the cohort
// SPAWN step does not itself hold the payload bytes - it merely carries the field as a ref pointing at an
// EARLIER anchor. The staterefsflow fixture never hits this: there the fan-out source IS the entry step, so the
// fan-in re-anchors at the spawn, which holds the literal. Put an anchoring step BEFORE the fan-out source and
// the spawn only carries the ref - and the fan-in mint (fanInRow / fireFanInDirect) must re-emit that
// carried ref rather than drop the field. A dropped carry is silent, permanent data loss: the fan-in task and
// every downstream step see the field as empty, and it vanishes from final_state.
func TestStaterefs_CarriedRefAcrossFanInWhenSpawnOnlyCarries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const docLen = 40000
	pdf := strings.Repeat("P", docLen)

	// checkTask fails the flow if it cannot see the whole carried document, so StatusCompleted alone proves
	// every step - the fan-out source, the branches, and the fan-in - resolved the carried ref.
	checkTask := func(where string) func(context.Context, *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			if got := len(f.GetString("pdf")); got != docLen {
				return errors.New("%s sees a %d-byte document, want %d", where, got, docLen)
			}
			return nil
		}
	}

	newEngine := func(t *testing.T) (*engine.Engine, *hosts.Mux) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		eng := unseededEngine(t.Name())
		eng.SetHost(mux)
		assert.NoError(eng.Startup(t.Context()))
		return eng, mux
	}

	t.Run("populated_cohort", func(t *testing.T) {
		assert := testarossa.For(t)
		eng, mux := newEngine(t)
		defer eng.Shutdown(ctx)

		graph := workflow.NewGraph("CarryRef")
		graph.SetURL("Anchor", "carryref.verify:429/anchor")
		graph.SetURL("Split", "carryref.verify:429/split")
		graph.SetURL("Page", "carryref.verify:429/page")
		graph.SetURL("Collect", "carryref.verify:429/collect")
		graph.SetFanIn("Collect")
		graph.AddTransitionChain("Anchor", "Split")
		graph.AddTransitionForEach("Split", "Page", "pages", "page")
		graph.AddTransitionChain("Page", "Collect", workflow.End)
		assert.NoError(graph.Validate())
		mux.HandleGraph("carryref.verify:429/carry", graph)
		mux.HandleTask("carryref.verify:429/anchor", checkTask("Anchor"))
		mux.HandleTask("carryref.verify:429/split", checkTask("Split"))
		mux.HandleTask("carryref.verify:429/page", checkTask("Page"))
		mux.HandleTask("carryref.verify:429/collect", func(ctx context.Context, f *workflow.Flow) error {
			if err := checkTask("Collect")(ctx, f); err != nil {
				return err
			}
			f.SetInt("pdfLen", len(f.GetString("pdf")))
			return nil
		})

		initialState := map[string]any{"pdf": pdf, "pages": []string{"p1", "p2", "p3"}}
		flowKey, outcome, err := eng.Run(ctx, "carryref.verify:429/carry", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(float64(docLen), stateVal(outcome.State, "pdfLen"))
		assert.Equal(docLen, len(stateVal(outcome.State, "pdf").(string)))

		// Continue carries the (materialized) final_state into the next turn, which re-anchors it at the new
		// entry step - the pre-spawn anchor shape again on turn 2.
		next, err := eng.Continue(ctx, flowKey, map[string]any{"pages": []string{"q1", "q2"}})
		assert.NoError(err)
		turn2, err := eng.Await(ctx, next)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, turn2.Status)
		assert.Equal(float64(docLen), stateVal(turn2.State, "pdfLen"))

		// Fork at a branch step re-runs the cohort; the fan-in's carried ref must remap onto the cloned anchor.
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
		assert.Equal(float64(docLen), stateVal(forkOutcome.State, "pdfLen"))
	})

	t.Run("empty_cohort_fires_fan_in_direct", func(t *testing.T) {
		assert := testarossa.For(t)
		eng, mux := newEngine(t)
		defer eng.Shutdown(ctx)

		// An empty forEach routes straight to the fan-in via fireFanInDirect - the other mint site with the same
		// discarded-carried-ref hazard. The carried document must still reach Collect.
		graph := workflow.NewGraph("CarryRefEmpty")
		graph.SetURL("Anchor", "carryrefe.verify:429/anchor")
		graph.SetURL("Split", "carryrefe.verify:429/split")
		graph.SetURL("Page", "carryrefe.verify:429/page")
		graph.SetURL("Collect", "carryrefe.verify:429/collect")
		graph.SetFanIn("Collect")
		graph.AddTransitionChain("Anchor", "Split")
		graph.AddTransitionForEach("Split", "Page", "pages", "page")
		graph.AddTransitionChain("Page", "Collect", workflow.End)
		assert.NoError(graph.Validate())
		mux.HandleGraph("carryrefe.verify:429/carry", graph)
		mux.HandleTask("carryrefe.verify:429/anchor", checkTask("Anchor"))
		mux.HandleTask("carryrefe.verify:429/split", checkTask("Split"))
		mux.HandleTask("carryrefe.verify:429/page", checkTask("Page"))
		mux.HandleTask("carryrefe.verify:429/collect", func(ctx context.Context, f *workflow.Flow) error {
			if err := checkTask("Collect")(ctx, f); err != nil {
				return err
			}
			f.SetInt("pdfLen", len(f.GetString("pdf")))
			return nil
		})

		// Empty pages array: Split fans out over nothing, so the fan-in fires directly.
		initialState := map[string]any{"pdf": pdf, "pages": []string{}}
		_, outcome, err := eng.Run(ctx, "carryrefe.verify:429/carry", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(float64(docLen), stateVal(outcome.State, "pdfLen"))
		assert.Equal(docLen, len(stateVal(outcome.State, "pdf").(string)))
	})

	t.Run("nested_fanout_carries_through_both_fan_ins", func(t *testing.T) {
		assert := testarossa.For(t)
		eng, mux := newEngine(t)
		defer eng.Shutdown(ctx)

		// Seed -forEach-> Cell -forEach-> Chunk -> JoinChunk -> JoinCell. The document is carried from the entry
		// step through BOTH cohorts. At the inner fan-in (JoinChunk) the spawn is Cell, which only carries the
		// ref (bytes are in the entry step); at the outer fan-in (JoinCell) the spawn is Seed. Both must carry.
		graph := workflow.NewGraph("CarryRefNested")
		graph.SetURL("Seed", "carryrefn.verify:429/seed")
		graph.SetURL("Cell", "carryrefn.verify:429/cell")
		graph.SetURL("Chunk", "carryrefn.verify:429/chunk")
		graph.SetURL("JoinChunk", "carryrefn.verify:429/joinchunk")
		graph.SetURL("JoinCell", "carryrefn.verify:429/joincell")
		graph.SetFanIn("JoinChunk")
		graph.SetFanIn("JoinCell")
		graph.AddTransitionForEach("Seed", "Cell", "cells", "cell")
		graph.AddTransitionForEach("Cell", "Chunk", "chunks", "chunk")
		graph.AddTransitionChain("Chunk", "JoinChunk")
		graph.AddTransitionChain("JoinChunk", "JoinCell", workflow.End)
		assert.NoError(graph.Validate())
		mux.HandleGraph("carryrefn.verify:429/carry", graph)
		mux.HandleTask("carryrefn.verify:429/seed", checkTask("Seed"))
		mux.HandleTask("carryrefn.verify:429/cell", checkTask("Cell"))
		mux.HandleTask("carryrefn.verify:429/chunk", checkTask("Chunk"))
		mux.HandleTask("carryrefn.verify:429/joinchunk", checkTask("JoinChunk"))
		mux.HandleTask("carryrefn.verify:429/joincell", func(ctx context.Context, f *workflow.Flow) error {
			if err := checkTask("JoinCell")(ctx, f); err != nil {
				return err
			}
			f.SetInt("pdfLen", len(f.GetString("pdf")))
			return nil
		})

		initialState := map[string]any{
			"pdf":    pdf,
			"cells":  []string{"c1", "c2"},
			"chunks": []string{"k1", "k2"},
		}
		_, outcome, err := eng.Run(ctx, "carryrefn.verify:429/carry", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(float64(docLen), stateVal(outcome.State, "pdfLen"))
		assert.Equal(docLen, len(stateVal(outcome.State, "pdf").(string)))
	})
}
