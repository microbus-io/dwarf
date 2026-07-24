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
	"sort"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestGotoFanInFlow pins that a fan-out source may route STRAIGHT to its own fan-in - the legitimate "if the
// array I would fan out on is empty, Goto the fan-in" shape. The source is the spawn, so the fan-in converges
// on the source's own state exactly as an empty cohort would; the shape is handled, not rejected.
//
// Before the fix, the source's Goto edge left it mis-attributed: a TRUNK source failed the not-in-a-cohort
// guard ("routed to fan-in ... but is not part of a fan-out cohort"), and a NESTED source bumped its OUTER
// cohort's arrival counter while inserting no step - silently dropping the branch and letting the outer fan-in
// fire early, yet reporting the flow completed.
func TestGotoFanInFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	graph := workflow.NewGraph("GotoFanIn")
	graph.SetEndpoint("Split", "gotofaninflow.verify:645/split")
	graph.SetEndpoint("Work", "gotofaninflow.verify:645/work")
	graph.SetEndpoint("Join", "gotofaninflow.verify:645/join")
	graph.SetFanIn("Join")
	graph.SetReducer("processed", workflow.ReducerAppend)
	graph.AddTransitionForEach("Split", "Work", "items", "item")
	// The escape hatch: when there is nothing to fan out on, the source steers straight to its fan-in.
	graph.AddTransitionGoto("Split", "Join")
	graph.AddTransitionChain("Work", "Join", workflow.END)
	if err := graph.Validate(); err != nil {
		t.Fatal(err)
	}
	proxy.HandleGraph("gotofaninflow.verify:645/goto-fan-in", graph)

	proxy.HandleTask("gotofaninflow.verify:645/split", func(ctx context.Context, f *workflow.Flow) error {
		if len(f.GetStrings("items")) == 0 {
			f.SetString("route", "direct")
			f.Goto("Join") // no branches to spawn: converge on the fan-in directly
			return nil
		}
		f.SetString("route", "fanout")
		return nil
	})
	proxy.HandleTask("gotofaninflow.verify:645/work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("processed", []string{f.GetString("item")})
		return nil
	})
	proxy.HandleTask("gotofaninflow.verify:645/join", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("joined", true)
		return nil
	})

	t.Run("empty_array_gotos_the_fan_in", func(t *testing.T) {
		assert := testarossa.For(t)
		_, outcome, err := eng.Run(ctx, "gotofaninflow.verify:645/goto-fan-in",
			map[string]any{"items": []string{}}, nil)
		assert.NoError(err)
		// The trunk source Goto's the fan-in; the flow converges and completes. Before the fix this FAILED the
		// not-in-a-cohort guard.
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["joined"], "the fan-in must run")
		// The source's own changes flowed through the direct fan-in.
		assert.Equal("direct", outcome.State["route"])
	})

	t.Run("non_empty_array_fans_out_normally", func(t *testing.T) {
		assert := testarossa.For(t)
		_, outcome, err := eng.Run(ctx, "gotofaninflow.verify:645/goto-fan-in",
			map[string]any{"items": []string{"x", "y", "z"}}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["joined"])
		assert.Equal("fanout", outcome.State["route"])
		got := toStringSlice(outcome.State["processed"])
		sort.Strings(got)
		assert.Equal([]string{"x", "y", "z"}, got, "every branch converged at the fan-in")
	})
}

// TestGotoFanInFlow_NestedStaysInOuterCohort pins the dangerous half: a NESTED fan-out source that Goto's its
// own (inner) fan-in must remain a member of its OUTER cohort, so the outer fan-in still counts it. Before the
// fix this branch was silently dropped and the flow reported completed with a cell missing from the result.
func TestGotoFanInFlow_NestedStaysInOuterCohort(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	graph := workflow.NewGraph("NestedGotoFanIn")
	graph.SetEndpoint("Seed", "gotofaninflow.verify:645/n-seed")
	graph.SetEndpoint("Cell", "gotofaninflow.verify:645/n-cell")
	graph.SetEndpoint("Chunk", "gotofaninflow.verify:645/n-chunk")
	graph.SetEndpoint("JoinChunk", "gotofaninflow.verify:645/n-joinchunk")
	graph.SetEndpoint("JoinCell", "gotofaninflow.verify:645/n-joincell")
	graph.SetFanIn("JoinChunk")
	graph.SetFanIn("JoinCell")
	graph.SetReducer("cellsSeen", workflow.ReducerUnion)
	graph.AddTransitionForEach("Seed", "Cell", "cells", "cell")
	graph.AddTransitionForEach("Cell", "Chunk", "chunks", "chunk")
	// The inner source's escape hatch: a cell with no chunks Goto's its inner fan-in directly.
	graph.AddTransitionGoto("Cell", "JoinChunk")
	graph.AddTransitionChain("Chunk", "JoinChunk")
	graph.AddTransitionChain("JoinChunk", "JoinCell", workflow.END)
	if err := graph.Validate(); err != nil {
		t.Fatal(err)
	}
	proxy.HandleGraph("gotofaninflow.verify:645/nested-goto-fan-in", graph)

	proxy.HandleTask("gotofaninflow.verify:645/n-seed", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// Every cell has no chunks, so every (nested) Cell source Goto's its inner fan-in - each must still arrive
	// at the OUTER fan-in as a member of Seed's cohort.
	proxy.HandleTask("gotofaninflow.verify:645/n-cell", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("chunks", []string{})
		f.Goto("JoinChunk")
		return nil
	})
	proxy.HandleTask("gotofaninflow.verify:645/n-chunk", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// The inner fan-in records which cell converged; the outer fan-in unions them.
	proxy.HandleTask("gotofaninflow.verify:645/n-joinchunk", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("cellsSeen", []string{f.GetString("cell")})
		return nil
	})
	proxy.HandleTask("gotofaninflow.verify:645/n-joincell", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	assert := testarossa.For(t)
	_, outcome, err := eng.Run(ctx, "gotofaninflow.verify:645/nested-goto-fan-in",
		map[string]any{"cells": []string{"a", "b", "c"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	// All three cells must reach the outer fan-in. Before the fix, a Goto'ing cell bumped the outer cohort's
	// arrival counter without inserting its inner fan-in step, so a cell was dropped and the outer fan-in could
	// fire early - the result would be missing one or more cells while the flow still reported completed.
	got := toStringSlice(outcome.State["cellsSeen"])
	sort.Strings(got)
	assert.Equal([]string{"a", "b", "c"}, got, "every nested cell must converge on the outer fan-in")
}

func toStringSlice(v any) []string {
	arr, _ := v.([]any)
	out := make([]string, 0, len(arr))
	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
