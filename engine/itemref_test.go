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
	"fmt"
	"testing"

	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// itemPages builds a forEach source array of n elements, each about size bytes and naming its own index.
func itemPages(n, size int) []any {
	pages := make([]any, n)
	for i := range pages {
		pages[i] = map[string]any{"n": i, "text": blob(size)}
	}
	return pages
}

// checkItem fails unless the element under key is the one at the branch's own index.
func checkItem(f *workflow.Flow, key string, size int) error {
	var el struct {
		N    int    `json:"n"`
		Text string `json:"text"`
	}
	if _, err := f.Get(key, &el); err != nil {
		return errors.Trace(err)
	}
	if el.N != f.GetInt(key+"Index") || len(el.Text) != size {
		return errors.New("%s: branch %d sees element %d of %d bytes", key, f.GetInt(key+"Index"), el.N, len(el.Text))
	}
	return nil
}

// TestItemRefs_BranchPointsIntoTheArray pins the storage shape of a forEach over large elements: the array is
// stored once, every branch step - not just the first - points at its element inside it, and every task still
// sees its own element whole. Without item refs each element is a second copy of the array's bytes, and an
// element below the linear bar is copied again at every step of its branch.
func TestItemRefs_BranchPointsIntoTheArray(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const width, size = 8, 2000
	g := workflow.NewGraph("ItemRefs")
	for _, n := range []string{"Seed", "Work", "Check", "Join"} {
		g.SetURL(n, "items/"+n)
	}
	g.SetFanIn("Join")
	g.SetReducer("seen", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Check", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("items/wf", g)

	mux.HandleTask("items/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("items/Work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("seen", []int{f.GetInt("pageIndex")})
		return checkItem(f, "page", size)
	})
	mux.HandleTask("items/Check", func(ctx context.Context, f *workflow.Flow) error {
		return checkItem(f, "page", size)
	})
	mux.HandleTask("items/Join", func(ctx context.Context, f *workflow.Flow) error {
		var seen []int
		_, err := f.Get("seen", &seen)
		f.SetInt("count", len(seen))
		return err
	})

	flowKey, outcome, err := e.Run(ctx, "items/wf", map[string]any{"pages": itemPages(width, size)}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Equal(float64(width), stateVal(outcome.State, "count"))
	assert.Len(stateVal(outcome.State, "pages"), width)

	// The entry step holds the array (the flow's initial input); every branch step points into it.
	entry := stepIDsByTask(t, e, "Seed")[0]
	for _, task := range []string{"Work", "Check"} {
		ids := stepIDsByTask(t, e, task)
		assert.Len(ids, width)
		for _, id := range ids {
			st, refs, stored := stepRefsOf(t, e, id)
			i := st.GetInt("pageIndex")
			assert.Equal(staterefs.Ref{Anchor: entry, Field: "pages", Path: []int{i}}, refs["page"], task)
			assert.False(st.Has("page"), "%s stores no copy of its element", task)
			assert.True(stored < size, "%s stores %d bytes of state for a %d-byte element", task, stored, size)
		}
	}

	// Fork at a second branch step: the kept first steps' item refs are remapped onto the cloned anchor, and the
	// re-run's tasks still see their elements.
	steps, err := e.History(ctx, flowKey)
	assert.NoError(err)
	var checkKey string
	for _, s := range steps {
		if s.TaskName == "Check" {
			checkKey = s.StepKey
			break
		}
	}
	forked, err := e.Fork(ctx, checkKey, nil)
	assert.NoError(err)
	forkOutcome, err := e.Await(ctx, forked)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, forkOutcome.Status, forkOutcome.Error)
	assert.Equal(float64(width), stateVal(forkOutcome.State, "count"))
}

// TestItemRefs_NestedFanOut pins the doc -> pages -> chunks shape: an inner branch carries the outer element as
// the outer branch's item ref, and points its own element into the array its spawn computed.
func TestItemRefs_NestedFanOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const pages, pageSize, lines, lineSize = 3, 2000, 8, 400
	g := workflow.NewGraph("NestedItemRefs")
	for _, n := range []string{"Seed", "Cell", "Chunk", "Inner", "Outer"} {
		g.SetURL(n, "nested/"+n)
	}
	g.SetFanIn("Inner")
	g.SetFanIn("Outer")
	g.SetReducer("chunks", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Cell", "pages", "page")
	g.AddTransitionForEach("Cell", "Chunk", "lines", "line")
	g.AddTransitionChain("Chunk", "Inner", "Outer", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("nested/wf", g)

	mux.HandleTask("nested/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("nested/Cell", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("lines", itemPages(lines, lineSize))
		return checkItem(f, "page", pageSize)
	})
	mux.HandleTask("nested/Chunk", func(ctx context.Context, f *workflow.Flow) error {
		if err := checkItem(f, "page", pageSize); err != nil {
			return err
		}
		f.Set("chunks", []string{fmt.Sprintf("%d/%d", f.GetInt("pageIndex"), f.GetInt("lineIndex"))})
		return checkItem(f, "line", lineSize)
	})
	mux.HandleTask("nested/Inner", func(ctx context.Context, f *workflow.Flow) error {
		return checkItem(f, "page", pageSize)
	})
	mux.HandleTask("nested/Outer", func(ctx context.Context, f *workflow.Flow) error { return nil })

	_, outcome, err := e.Run(ctx, "nested/wf", map[string]any{"pages": itemPages(pages, pageSize)}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Len(stateVal(outcome.State, "chunks"), pages*lines)

	entry := stepIDsByTask(t, e, "Seed")[0]
	cells := map[int]bool{}
	for _, id := range stepIDsByTask(t, e, "Cell") {
		cells[id] = true
	}
	chunks := stepIDsByTask(t, e, "Chunk")
	assert.Len(chunks, pages*lines)
	for _, id := range chunks {
		st, refs, _ := stepRefsOf(t, e, id)
		assert.Equal(staterefs.Ref{Anchor: entry, Field: "pages", Path: []int{st.GetInt("pageIndex")}}, refs["page"],
			"the outer element rides into the inner branch as the outer branch's item ref")
		line := refs["line"]
		assert.True(cells[line.Anchor], "the inner element points into the array its Cell computed")
		assert.Equal(staterefs.Ref{Anchor: line.Anchor, Field: "lines", Path: []int{st.GetInt("lineIndex")}}, line)
		assert.False(st.Has("page"))
		assert.False(st.Has("line"))
	}
}

// TestItemRefs_FailedFanOut pins the failed-fan-out terminal merge over item-ref'd branches: it bases on a
// completed sibling's branch-local rows, so it must resolve their item refs - and still strip the element.
func TestItemRefs_FailedFanOut(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const width, size = 4, 2000
	g := workflow.NewGraph("FailedItemRefs")
	for _, n := range []string{"Seed", "Work", "Check", "Join"} {
		g.SetURL(n, "failitems/"+n)
	}
	g.SetFanIn("Join")
	g.SetReducer("seen", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "pages", "page")
	g.AddTransitionChain("Work", "Check", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("failitems/wf", g)

	mux.HandleTask("failitems/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("failitems/Work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("seen", []int{f.GetInt("pageIndex")})
		return checkItem(f, "page", size)
	})
	mux.HandleTask("failitems/Check", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("pageIndex") == 1 {
			return errors.New("branch 1 failed")
		}
		return checkItem(f, "page", size)
	})
	mux.HandleTask("failitems/Join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	_, outcome, err := e.Run(ctx, "failitems/wf", map[string]any{"pages": itemPages(width, size)}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.Len(stateVal(outcome.State, "pages"), width, "the source array resolves into final_state")
	assert.Len(stateVal(outcome.State, "seen"), width, "every branch's first-step output is kept")
	assert.False(outcome.State.Has("page"), "the branch's element is bookkeeping, stripped from final_state")
}

// TestItemRefs_InnerFanInStripsASharedAs pins the inner fan-in's strip when both levels of a nested fan-out
// use the same `as` (the default "item" makes it easy). The inner spawn holds the OUTER element as an item
// ref; the inner convergence strips "item", and that ref must not carry it back past the inner fan-in.
func TestItemRefs_InnerFanInStripsASharedAs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const pages, pageSize, lines, lineSize = 3, 2000, 4, 400
	g := workflow.NewGraph("SharedAs")
	for _, n := range []string{"Seed", "Cell", "Chunk", "Inner", "Outer"} {
		g.SetURL(n, "sharedas/"+n)
	}
	g.SetFanIn("Inner")
	g.SetFanIn("Outer")
	g.AddTransitionForEach("Seed", "Cell", "pages", "")
	g.AddTransitionForEach("Cell", "Chunk", "lines", "")
	g.AddTransitionChain("Chunk", "Inner", "Outer", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("sharedas/wf", g)

	mux.HandleTask("sharedas/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("sharedas/Cell", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("lines", itemPages(lines, lineSize))
		return checkItem(f, "item", pageSize)
	})
	mux.HandleTask("sharedas/Chunk", func(ctx context.Context, f *workflow.Flow) error {
		return checkItem(f, "item", lineSize)
	})
	mux.HandleTask("sharedas/Inner", func(ctx context.Context, f *workflow.Flow) error {
		if f.Has("item") || f.Has("itemIndex") || f.Has("itemCount") {
			return errors.New("the inner cohort's bookkeeping survived its fan-in")
		}
		return nil
	})
	mux.HandleTask("sharedas/Outer", func(ctx context.Context, f *workflow.Flow) error { return nil })

	_, outcome, err := e.Run(ctx, "sharedas/wf", map[string]any{"pages": itemPages(pages, pageSize)}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	for _, id := range stepIDsByTask(t, e, "Inner") {
		_, refs, _ := stepRefsOf(t, e, id)
		assert.NotContains(refs, "item")
	}
}

// TestItemRefs_TwoDimensionalArray pins a nested array fanned out level by level - forEach(data) as row, then
// forEach(row) as cell. Every level points into the one anchor holding the whole array: a row is
// "<anchor> data[x]" and a cell "<anchor> data[x][y]", so no step copies a row or a cell.
func TestItemRefs_TwoDimensionalArray(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const rows, cols, size = 3, 6, 400
	data := make([]any, rows)
	for x := range data {
		row := make([]any, cols)
		for y := range row {
			row[y] = map[string]any{"x": x, "y": y, "text": blob(size)}
		}
		data[x] = row
	}

	g := workflow.NewGraph("TwoD")
	for _, n := range []string{"Seed", "Row", "Cell", "Inner", "Outer"} {
		g.SetURL(n, "twod/"+n)
	}
	g.SetFanIn("Inner")
	g.SetFanIn("Outer")
	g.SetReducer("cells", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Row", "data", "row")
	g.AddTransitionForEach("Row", "Cell", "row", "cell")
	g.AddTransitionChain("Cell", "Inner", "Outer", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("twod/wf", g)

	mux.HandleTask("twod/Seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("twod/Row", func(ctx context.Context, f *workflow.Flow) error {
		var row []any
		if _, err := f.Get("row", &row); err != nil || len(row) != cols {
			return errors.New("row %d has %d cells", f.GetInt("rowIndex"), len(row))
		}
		return nil
	})
	mux.HandleTask("twod/Cell", func(ctx context.Context, f *workflow.Flow) error {
		var cell struct {
			X, Y int
			Text string
		}
		if _, err := f.Get("cell", &cell); err != nil {
			return err
		}
		if cell.X != f.GetInt("rowIndex") || cell.Y != f.GetInt("cellIndex") || len(cell.Text) != size {
			return errors.New("branch %d/%d sees cell %d/%d", f.GetInt("rowIndex"), f.GetInt("cellIndex"), cell.X, cell.Y)
		}
		f.Set("cells", []string{fmt.Sprintf("%d/%d", cell.X, cell.Y)})
		return nil
	})
	mux.HandleTask("twod/Inner", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("twod/Outer", func(ctx context.Context, f *workflow.Flow) error { return nil })

	_, outcome, err := e.Run(ctx, "twod/wf", map[string]any{"data": data}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	assert.Len(stateVal(outcome.State, "cells"), rows*cols)

	entry := stepIDsByTask(t, e, "Seed")[0]
	for _, id := range stepIDsByTask(t, e, "Row") {
		st, refs, _ := stepRefsOf(t, e, id)
		assert.Equal(staterefs.Ref{Anchor: entry, Field: "data", Path: []int{st.GetInt("rowIndex")}}, refs["row"])
		assert.False(st.Has("row"))
	}
	cellSteps := stepIDsByTask(t, e, "Cell")
	assert.Len(cellSteps, rows*cols)
	for _, id := range cellSteps {
		st, refs, stored := stepRefsOf(t, e, id)
		x, y := st.GetInt("rowIndex"), st.GetInt("cellIndex")
		assert.Equal(staterefs.Ref{Anchor: entry, Field: "data", Path: []int{x}}, refs["row"])
		assert.Equal(staterefs.Ref{Anchor: entry, Field: "data", Path: []int{x, y}}, refs["cell"])
		assert.False(st.Has("row"))
		assert.False(st.Has("cell"))
		assert.True(stored < size, "a cell step stores %d bytes of state for a %d-byte cell", stored, size)
	}
}
