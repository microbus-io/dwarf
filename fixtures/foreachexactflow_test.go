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

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestForEachExactFlow pins that a forEach hands each branch its element exactly. The source array was decoded
// through float64, so two ids past 2^53 reached their branches as one rounded id, and a union of what the branches
// saw held one wrong id where there were two right ones. A source that is not an array still fails the step.
func TestForEachExactFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("ForEachExact")
	g.SetURL("Split", "foreachexact/split")
	g.SetURL("Work", "foreachexact/work")
	g.SetURL("Join", "foreachexact/join")
	g.SetFanIn("Join")
	g.SetReducer("seen", workflow.ReducerUnion)
	g.AddTransitionForEach("Split", "Work", "ids", "id")
	g.AddTransitionChain("Work", "Join", workflow.End)
	mux.HandleGraph("foreachexact/g", g)
	nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	mux.HandleTask("foreachexact/split", nop)
	mux.HandleTask("foreachexact/join", nop)
	mux.HandleTask("foreachexact/work", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("seen", []int64{int64(f.GetInt("id"))})
	})

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	t.Run("elements_are_exact", func(t *testing.T) {
		assert := testarossa.For(t)
		ids := []int64{1234567890123456789, 1234567890123456791} // 2 apart, and 2^53 is far below
		flowKey, err := e.Create(ctx, "foreachexact/g", map[string]any{"ids": ids}, nil)
		if !assert.NoError(err) {
			return
		}
		out, err := enginetest.BoundedAwait(t, e, flowKey)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
			return
		}
		var seen []int64
		_, err = out.State.Get("seen", &seen)
		assert.NoError(err)
		assert.Equal(ids, seen, "each branch saw its own id, exactly")
	})

	t.Run("non_array_source_fails", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, err := e.Create(ctx, "foreachexact/g", map[string]any{"ids": "not a list"}, nil)
		if !assert.NoError(err) {
			return
		}
		out, err := enginetest.BoundedAwait(t, e, flowKey)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusFailed, out.Status)
			assert.Contains(out.Error, "is not an array")
		}
	})
}
