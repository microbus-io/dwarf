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
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestCreate_RejectsNegativeOptions pins that Create rejects a negative Priority, FairnessWeight, or
// TimeBudget with a 400 rather than silently coercing it to the engine default, which would hide a caller
// bug. 0 still means "use the default" for all three, so a zero-valued FlowOptions and a nil
// FlowOptions both create fine.
func TestCreate_RejectsNegativeOptions(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "fopt/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("fopt/g", g)
	mux.HandleTask("fopt/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// Negative priority -> 400, no flow created.
	_, err := e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{Priority: -1})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// Negative fairness weight -> 400.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{FairnessWeight: -0.5})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// A non-finite weight passes a `< 0` test, and the planner's draw has no answer for it -> 400; nor does a weight
	// past the bound, whose sum with others can overflow. The bound itself is accepted.
	for _, w := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), maxWeight * 1.5, 1e308} {
		_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{FairnessWeight: w})
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "weight %v", w)
	}

	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{FairnessWeight: maxWeight})
	assert.NoError(err)

	// A fairness key is ASCII letters, digits, '-', '_' and '.', at most 256 of them -> anything else is a 400. SQL
	// groups and matches the key as well as Go, and outside that alphabet a collation can fold two keys into one
	// (a trailing space, an accent) or the planner's JSON encoding can rewrite one (invalid UTF-8).
	for _, key := range []string{"tenant 42", "tenant-42 ", "tenant-\u00e9", "org/team", "a@b", "a:b", "'", "\"", string([]byte{0xff}), strings.Repeat("k", 257)} {
		_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{FairnessKey: key})
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "key %q", key)
	}
	for _, key := range []string{"", "tenant-42", "Org_7.prod", strings.Repeat("k", 256)} {
		_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{FairnessKey: key})
		assert.NoError(err, "key %q", key)
	}

	// Run shares the same create path, so it rejects too.
	_, _, err = e.Run(ctx, "fopt/g", nil, &workflow.FlowOptions{Priority: -7})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// Negative time budget -> 400, not silently coerced to the engine default.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{TimeBudget: -time.Second})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// A POSITIVE budget below 1ms is rejected too, and this is the case worth pinning: the budget is
	// persisted in milliseconds, so 500us truncates to a stored 0 - and a step with a 0 budget is dispatched
	// with a context deadline that has already passed, failing every attempt instantly. Truncation makes a
	// value that looks valid behave exactly like the negative one.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{TimeBudget: 500 * time.Microsecond})
	assert.Error(err)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	// Exactly 1ms is the floor, and is accepted.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{TimeBudget: time.Millisecond})
	assert.NoError(err)

	// Zero values (the "unset -> default" sentinel) and nil options are accepted.
	_, err = e.Create(ctx, "fopt/g", nil, &workflow.FlowOptions{Priority: 0, FairnessWeight: 0, TimeBudget: 0})
	assert.NoError(err)
	_, err = e.Create(ctx, "fopt/g", nil, nil)
	assert.NoError(err)
}

// SetTimeBudget is the engine-wide default, so 0 cannot mean "unset" the way it does on FlowOptions - this
// setter IS the default. A non-positive (or sub-millisecond, hence truncating-to-zero) value would hand
// EVERY new flow a task deadline that has already passed, silently and engine-wide, so it is rejected.
func TestSetTimeBudget_RejectsNonPositive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngine()

	assert.Error(e.SetTimeBudget(0))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetTimeBudget(0)))
	assert.Error(e.SetTimeBudget(-time.Minute))
	assert.Error(e.SetTimeBudget(500*time.Microsecond), "sub-millisecond truncates to a zero budget")

	assert.NoError(e.SetTimeBudget(time.Millisecond))
	assert.NoError(e.SetTimeBudget(30 * time.Second))

	// A rejected value leaves the previous default intact rather than half-applying.
	assert.NoError(e.SetTimeBudget(30 * time.Second))
	assert.Error(e.SetTimeBudget(-1))
	assert.Equal(30*time.Second, e.taskTimeBudget())
}

// TestCreate_RejectsAnEmptyTaskName pins that a graph with a task named "" is refused at Create with a 400,
// before any flow is stored. Accepted, the transition into it was read as "no successor" and the flow
// completed without running the task.
func TestCreate_RejectsAnEmptyTaskName(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("EmptyName")
	g.SetURL("Start", "emptyname/start")
	g.SetURL("", "emptyname/empty")
	g.AddTransition("Start", "")
	g.AddTransition("", workflow.End)
	mux.HandleGraph("emptyname/g", g)
	ran := false
	mux.HandleTask("emptyname/start", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("emptyname/empty", func(ctx context.Context, f *workflow.Flow) error { ran = true; return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, _, err := e.Run(ctx, "emptyname/g", nil, nil)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%v", err)
	assert.False(ran)
	assert.Zero(enginetest.CountRows(t, e, 1, "SELECT COUNT(*) FROM dwarf_flows"))
}

// TestSubgraph_EmptyURLFailsTheStep pins the end-to-end shape of an empty Subgraph URL: a task following the
// documented `if yield || err != nil { return err }` pattern fails its step with the 400, and nothing after it
// runs. Accepted, Subgraph returned yield=true for a request the engine could not see, so the step completed
// with no child and the flow ran on to its next task.
func TestSubgraph_EmptyURLFailsTheStep(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("EmptySubgraph")
	g.SetURL("Call", "emptysub/call")
	g.SetURL("After", "emptysub/after")
	g.AddTransition("Call", "After")
	g.AddTransition("After", workflow.End)
	mux.HandleGraph("emptysub/g", g)
	afterRan := false
	mux.HandleTask("emptysub/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("", nil, nil)
		if yield || err != nil {
			return err
		}
		f.SetBool("childReturned", true)
		return nil
	})
	mux.HandleTask("emptysub/after", func(ctx context.Context, f *workflow.Flow) error { afterRan = true; return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, out, err := e.Run(ctx, "emptysub/g", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.Contains(out.Error, "workflow URL")
	}
	assert.False(afterRan, "the task after the Subgraph call ran")
}

// TestCreate_RejectsAPaddedTaskName pins that a graph with a task name padded by whitespace is refused at Create
// with a 400, before any flow is stored. Accepted, a padded inner fan-in's cohort dropped out of the outer fold
// wherever the engine trimmed the name it read back from the step row, so the flow completed without that
// cohort's contributions.
func TestCreate_RejectsAPaddedTaskName(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("PaddedName")
	for _, task := range []string{"Start", "Inner", "Part", "JoinOuter"} {
		g.SetURL(task, "paddedname/"+task)
	}
	g.SetURL(" JoinInner ", "paddedname/JoinInner")
	g.AddTransitionForEach("Start", "Inner", "outer", "o")
	g.AddTransitionForEach("Inner", "Part", "inner", "i")
	g.AddTransitionChain("Part", " JoinInner ", "JoinOuter", workflow.End)
	g.SetFanIn(" JoinInner ")
	g.SetFanIn("JoinOuter")
	mux.HandleGraph("paddedname/g", g)
	ran := false
	for _, task := range []string{"Start", "Inner", "Part", "JoinInner", "JoinOuter"} {
		mux.HandleTask("paddedname/"+task, func(ctx context.Context, f *workflow.Flow) error { ran = true; return nil })
	}

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, _, err := e.Run(ctx, "paddedname/g", map[string]any{"outer": []int{1}, "inner": []int{1}}, nil)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%v", err)
	assert.False(ran)
	assert.Zero(enginetest.CountRows(t, e, 1, "SELECT COUNT(*) FROM dwarf_flows"))
}

// TestCreate_RejectsAPaddedWorkflowURL pins that Create refuses a workflow URL that begins or ends with whitespace,
// before any flow is stored. The URL is stored on the flow and matched by the lists, and SQL Server and the MySQL
// family compare it ignoring trailing spaces, so "g" and "g " would be one workflow in SQL and two in Go.
func TestCreate_RejectsAPaddedWorkflowURL(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	g := workflow.NewGraph("PaddedURL")
	g.SetURL("Work", "paddedurl/work")
	g.AddTransition("Work", workflow.End)
	for _, url := range []string{" paddedurl/g", "paddedurl/g ", "paddedurl/g\t"} {
		mux.HandleGraph(url, g)
	}
	ran := false
	mux.HandleTask("paddedurl/work", func(ctx context.Context, f *workflow.Flow) error { ran = true; return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	for _, url := range []string{" paddedurl/g", "paddedurl/g ", "paddedurl/g\t"} {
		_, _, err := e.Run(ctx, url, nil, nil)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%q: %v", url, err)
	}
	assert.False(ran)
	assert.Zero(enginetest.CountRows(t, e, 1, "SELECT COUNT(*) FROM dwarf_flows"))
}

// TestSubgraph_PaddedURLFromAWireCarrierFailsTheSpawn pins that the engine refuses a padded subgraph URL itself, not
// only through flow.Subgraph: a carrier decoded from a remote host's reply arms the URL directly. The spawn fails
// as an unloadable child does - no child flow, the 400 delivered to the caller - though a graph is registered under
// the padded URL, so without the refusal the child would be created.
func TestSubgraph_PaddedURLFromAWireCarrierFailsTheSpawn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux := hosts.NewMux()
	child := workflow.NewGraph("Child")
	child.SetURL("Leaf", "paddedsub/leaf")
	child.AddTransition("Leaf", workflow.End)
	mux.HandleGraph("paddedsub/child ", child)
	mux.HandleTask("paddedsub/leaf", func(ctx context.Context, f *workflow.Flow) error { return nil })
	g := workflow.NewGraph("Caller")
	g.SetURL("Call", "paddedsub/call")
	g.AddTransition("Call", workflow.End)
	mux.HandleGraph("paddedsub/g", g)
	mux.HandleTask("paddedsub/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("paddedsub/child", nil, nil)
		if err != nil || !yield {
			return err
		}
		return overWire(f, func(wire map[string]json.RawMessage) { wire["subgraphURL"] = json.RawMessage(`"paddedsub/child "`) })
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, out, err := e.Run(ctx, "paddedsub/g", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.Contains(out.Error, "whitespace")
	}
	assert.Equal(1, enginetest.CountRows(t, e, 1, "SELECT COUNT(*) FROM dwarf_flows"), "a child was created under the padded URL")
}
