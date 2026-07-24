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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFanOutOrdinalFlow pins that an order-sensitive reducer (append/concat/union) folds a MULTI-STEP branch's
// output in input-array order, not in the order the branches happened to complete. fan_out_ordinal identifies
// the branch; the fan-in merge folds by it. A branch's first step carries its spawn ordinal, but a later step
// must inherit it - otherwise every branch's second-or-later step lands in the ordinal-0 bucket and folds by
// completion order, making the reduced result nondeterministic.
//
// The graph is Split -forEach-> Work -> Post -> Join. Only Post writes the reducer field, so the fold order is
// the order of the Post steps. Work sleeps longest for the EARLIEST element, so the branches complete in
// REVERSE input order - the completion order and the input order disagree, and the result must follow input
// order regardless.
func TestFanOutOrdinalFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("FanOutOrdinal")
	graph.SetEndpoint("Split", "fanoutordinalflow.verify:828/split")
	graph.SetEndpoint("Work", "fanoutordinalflow.verify:828/work")
	graph.SetEndpoint("Post", "fanoutordinalflow.verify:828/post")
	graph.SetEndpoint("Join", "fanoutordinalflow.verify:828/join")
	graph.SetFanIn("Join")
	graph.SetReducer("results", workflow.ReducerAppend)
	graph.AddTransitionForEach("Split", "Work", "items", "item")
	graph.AddTransitionChain("Work", "Post", "Join", workflow.END)
	assert.NoError(graph.Validate())
	proxy.HandleGraph("fanoutordinalflow.verify:828/fan-out-ordinal", graph)

	proxy.HandleTask("fanoutordinalflow.verify:828/split", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// The first step of the branch sleeps LONGEST for the earliest element, so branch 0 finishes last: the
	// Post steps are created (and thus ordered by step_id) in reverse input order.
	proxy.HandleTask("fanoutordinalflow.verify:828/work", func(ctx context.Context, f *workflow.Flow) error {
		idx := f.GetInt("itemIndex")
		cnt := f.GetInt("itemCount")
		time.Sleep(time.Duration(cnt-idx) * 50 * time.Millisecond)
		return nil
	})
	// The SECOND step of the branch writes the reducer field. Its fan_out_ordinal must be inherited from Work,
	// or every Post lands in the ordinal-0 bucket and folds by step_id = completion order.
	proxy.HandleTask("fanoutordinalflow.verify:828/post", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("results", []string{f.GetString("item")})
		return nil
	})
	proxy.HandleTask("fanoutordinalflow.verify:828/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	_, outcome, err := eng.Run(ctx, "fanoutordinalflow.verify:828/fan-out-ordinal",
		map[string]any{"items": []string{"a", "b", "c", "d"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	// Deterministic input order, NOT the reverse completion order the sleeps force.
	assert.Equal([]string{"a", "b", "c", "d"}, toStringSlice(outcome.State["results"]),
		"an append reducer must fold a multi-step branch in input-array order, not completion order")
}

// TestFanOutOrdinalFlow_Nested pins the nested layer: an inner fan-in step is a member of its OUTER cohort, so
// it must carry the spawning cell's ordinal, or the OUTER fan-in folds the per-cell results by inner-cohort
// completion order. Each cell runs a one-chunk inner fan-out; the chunk sleeps longest for the earliest cell,
// so the cells converge in REVERSE order. The outer append reducer must still be in cell input order.
func TestFanOutOrdinalFlow_Nested(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("NestedOrdinal")
	graph.SetEndpoint("Seed", "fanoutordinalflow.verify:828/n-seed")
	graph.SetEndpoint("Cell", "fanoutordinalflow.verify:828/n-cell")
	graph.SetEndpoint("Chunk", "fanoutordinalflow.verify:828/n-chunk")
	graph.SetEndpoint("JoinChunk", "fanoutordinalflow.verify:828/n-joinchunk")
	graph.SetEndpoint("JoinCell", "fanoutordinalflow.verify:828/n-joincell")
	graph.SetFanIn("JoinChunk")
	graph.SetFanIn("JoinCell")
	graph.SetReducer("order", workflow.ReducerAppend)
	graph.AddTransitionForEach("Seed", "Cell", "cells", "cell")
	graph.AddTransitionForEach("Cell", "Chunk", "chunks", "chunk")
	graph.AddTransitionChain("Chunk", "JoinChunk")
	graph.AddTransitionChain("JoinChunk", "JoinCell", workflow.END)
	assert.NoError(graph.Validate())
	proxy.HandleGraph("fanoutordinalflow.verify:828/nested-ordinal", graph)

	proxy.HandleTask("fanoutordinalflow.verify:828/n-seed", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	// Each cell fans out over a single chunk.
	proxy.HandleTask("fanoutordinalflow.verify:828/n-cell", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("chunks", []string{f.GetString("cell") + "-0"})
		return nil
	})
	// The chunk sleeps longest for the earliest cell, so the inner cohorts converge in reverse cell order.
	proxy.HandleTask("fanoutordinalflow.verify:828/n-chunk", func(ctx context.Context, f *workflow.Flow) error {
		idx := f.GetInt("cellIndex")
		cnt := f.GetInt("cellCount")
		time.Sleep(time.Duration(cnt-idx) * 50 * time.Millisecond)
		return nil
	})
	// The INNER fan-in records its cell. It is an outer-cohort member; its ordinal must be the cell's, or the
	// outer fan-in folds these by completion (reverse) order.
	proxy.HandleTask("fanoutordinalflow.verify:828/n-joinchunk", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("order", []string{f.GetString("cell")})
		return nil
	})
	proxy.HandleTask("fanoutordinalflow.verify:828/n-joincell", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	_, outcome, err := eng.Run(ctx, "fanoutordinalflow.verify:828/nested-ordinal",
		map[string]any{"cells": []string{"a", "b", "c"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal([]string{"a", "b", "c"}, toStringSlice(outcome.State["order"]),
		"the outer fan-in must fold each cell's inner fan-in in cell input order")
}
