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
	"os"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// pendingFixture skips a test that pins behavior the engine does not have yet, naming what is missing.
// DWARF_RUN_PENDING=1 runs it anyway, to watch it fail while the behavior is being built.
func pendingFixture(t *testing.T, missing string) {
	t.Helper()
	if os.Getenv("DWARF_RUN_PENDING") == "" {
		t.Skip("pending: " + missing)
	}
}

// TestNestedreducerflow pins that a fan-out nested three levels deep delivers every level's contributions
// to the outermost fan-in, and to the final state of a fan-out that failed at either level:
//
//	Seed =forEach=> Cell =forEach=> Chunk =forEach=> Part -> JoinPart -> AfterPart -> JoinChunk -> AfterChunk -> JoinCell -> END
//
// An inner fan-in is a member of the next cohort out, and what it contributes there is everything its own
// cohort added plus its own task's writes. A step between an inner fan-in and the next level up
// (AfterPart, AfterChunk) contributes like any other member.
//
// Every task writes only its own contribution, so the expected values are the same whether a reduced field
// is folded only at fan-ins or at every hand-off. One field per reducer shape:
//
//   - messages (append) records the fold ORDER: branch order at every level, never completion order;
//   - tags (merge) records which keys arrived, and lastPart which write came last;
//   - total (add) counts contributions per level, one decimal digit per task;
//   - last (replace) is written only by Part, two levels below the outermost fan-in.
func TestNestedreducerflow(t *testing.T) {
	t.Parallel()
	pendingFixture(t, "a nested fan-in does not pass its inner cohort's contributions to the next cohort out")
	assert := testarossa.For(t)
	ctx := context.Background()

	const url = "nestedreducerflow.verify:428/"

	mux := engine.NewHostMux()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("NestedReducer")
	for _, task := range []string{"Seed", "Cell", "Chunk", "Part", "JoinPart", "AfterPart", "JoinChunk", "AfterChunk", "JoinCell"} {
		graph.SetURL(task, url+task)
	}
	graph.SetFanIn("JoinPart")
	graph.SetFanIn("JoinChunk")
	graph.SetFanIn("JoinCell")
	graph.SetReducer("messages", workflow.ReducerAppend)
	graph.SetReducer("tags", workflow.ReducerMerge)
	graph.SetReducer("total", workflow.ReducerAdd)
	graph.AddTransitionForEach("Seed", "Cell", "cells", "cell")
	graph.AddTransitionForEach("Cell", "Chunk", "chunks", "chunk")
	graph.AddTransitionForEach("Chunk", "Part", "parts", "part")
	graph.AddTransitionChain("Part", "JoinPart", "AfterPart", "JoinChunk", "AfterChunk", "JoinCell", workflow.End)
	mux.HandleGraph(url+"g", graph)

	contribute := func(f *workflow.Flow, message string, total int) {
		f.Set("messages", []string{message})
		f.SetInt("total", total)
	}
	mux.HandleTask(url+"Seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("cells", []string{"A", "B"})
		return nil
	})
	mux.HandleTask(url+"Cell", func(ctx context.Context, f *workflow.Flow) error {
		c := f.GetString("cell")
		f.Set("chunks", []string{c + "1", c + "2"})
		f.Set("tags", map[string]any{c: "cell"})
		contribute(f, "cell:"+c, 1)
		return nil
	})
	mux.HandleTask(url+"Chunk", func(ctx context.Context, f *workflow.Flow) error {
		k := f.GetString("chunk")
		f.Set("parts", []string{k + "x", k + "y"})
		contribute(f, "chunk:"+k, 10)
		return nil
	})
	mux.HandleTask(url+"Part", func(ctx context.Context, f *workflow.Flow) error {
		p := f.GetString("part")
		if f.GetString("failAt") == "part:"+p {
			return errors.New("part %s is broken", p)
		}
		f.Set("tags", map[string]any{p: "part", "lastPart": p})
		f.SetString("last", "part:"+p)
		contribute(f, "part:"+p, 100)
		return nil
	})
	mux.HandleTask(url+"JoinPart", func(ctx context.Context, f *workflow.Flow) error {
		contribute(f, "joinpart:"+f.GetString("chunk"), 1000)
		return nil
	})
	mux.HandleTask(url+"AfterPart", func(ctx context.Context, f *workflow.Flow) error {
		k := f.GetString("chunk")
		f.Set("tags", map[string]any{k: "afterpart"})
		contribute(f, "after:"+k, 10000)
		return nil
	})
	mux.HandleTask(url+"JoinChunk", func(ctx context.Context, f *workflow.Flow) error {
		contribute(f, "joinchunk:"+f.GetString("cell"), 100000)
		return nil
	})
	mux.HandleTask(url+"AfterChunk", func(ctx context.Context, f *workflow.Flow) error {
		c := f.GetString("cell")
		if f.GetString("failAt") == "afterchunk:"+c {
			return errors.New("after-chunk %s is broken", c)
		}
		contribute(f, "after:"+c, 1000000)
		return nil
	})
	mux.HandleTask(url+"JoinCell", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	// chunkMessages is everything one Chunk branch contributes when it converges.
	chunkMessages := func(k string) []any {
		return []any{"chunk:" + k, "part:" + k + "x", "part:" + k + "y", "joinpart:" + k, "after:" + k}
	}
	// cellMessages is everything one Cell branch contributes when it converges.
	cellMessages := func(c string) []any {
		out := []any{"cell:" + c}
		out = append(out, chunkMessages(c+"1")...)
		out = append(out, chunkMessages(c+"2")...)
		return append(out, "joinchunk:"+c, "after:"+c)
	}
	cellTags := func(c string, into map[string]any) {
		into[c] = "cell"
		for _, k := range []string{c + "1", c + "2"} {
			into[k+"x"] = "part"
			into[k+"y"] = "part"
			into[k] = "afterpart"
		}
	}
	run := func(failAt string) *workflow.FlowOutcome {
		_, outcome, err := eng.Run(ctx, url+"g", map[string]any{
			"messages": []string{"seed"},
			"tags":     map[string]any{"seed": "init"},
			"failAt":   failAt,
		}, nil)
		if !assert.NoError(err) {
			return nil
		}
		return outcome
	}

	t.Run("converged", func(t *testing.T) {
		assert := testarossa.For(t)
		outcome := run("")
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		messages := append([]any{"seed"}, cellMessages("A")...)
		messages = append(messages, cellMessages("B")...)
		assert.Equal(messages, stateVal(outcome.State, "messages"))
		tags := map[string]any{"seed": "init", "lastPart": "B2y"}
		cellTags("A", tags)
		cellTags("B", tags)
		assert.Equal(tags, stateVal(outcome.State, "tags"))
		assert.Equal(2244842.0, stateVal(outcome.State, "total"))
		assert.Equal("part:B2y", stateVal(outcome.State, "last"))
	})

	t.Run("outer_cohort_failed", func(t *testing.T) {
		assert := testarossa.For(t)
		// Both inner cohorts converge; AfterChunk B, a member of the outermost cohort, fails. JoinCell never
		// runs, and the final state is every completed member's contribution, in branch order.
		outcome := run("afterchunk:B")
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusFailed, outcome.Status)

		messages := append([]any{"seed"}, cellMessages("A")...)
		messages = append(messages, cellMessages("B")...)
		messages = messages[:len(messages)-1] // after:B failed
		assert.Equal(messages, stateVal(outcome.State, "messages"))
		tags := map[string]any{"seed": "init", "lastPart": "B2y"}
		cellTags("A", tags)
		cellTags("B", tags)
		assert.Equal(tags, stateVal(outcome.State, "tags"))
		assert.Equal(1244842.0, stateVal(outcome.State, "total"))
		assert.Equal("part:B2y", stateVal(outcome.State, "last"))
	})

	t.Run("inner_cohort_failed", func(t *testing.T) {
		assert := testarossa.For(t)
		// Part B2y fails, so JoinPart B2 never runs, nor JoinChunk B, nor JoinCell. The failure resolves only
		// once every cohort it passes through has fully arrived, so every completed step below it contributes.
		outcome := run("part:B2y")
		if outcome == nil {
			return
		}
		assert.Equal(workflow.StatusFailed, outcome.Status)

		messages := append([]any{"seed"}, cellMessages("A")...)
		messages = append(messages, "cell:B")
		messages = append(messages, chunkMessages("B1")...)
		messages = append(messages, "chunk:B2", "part:B2x")
		assert.Equal(messages, stateVal(outcome.State, "messages"))
		tags := map[string]any{"seed": "init", "lastPart": "B2x", "B": "cell", "B1x": "part", "B1y": "part", "B1": "afterpart", "B2x": "part"}
		cellTags("A", tags)
		assert.Equal(tags, stateVal(outcome.State, "tags"))
		assert.Equal(1133742.0, stateVal(outcome.State, "total"))
		assert.Equal("part:B2x", stateVal(outcome.State, "last"))
	})
}
