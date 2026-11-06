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

// TestReducerDeleteflow pins what a fan-out branch may delete. A reduced field cannot be deleted or overwritten
// inside a branch: the branches are parallel, and a reset in one of them has no defined meaning for the
// others, so the step fails, naming the field. A field with no reducer can, and the deletes and writes fold in
// branch order, as any write to such a field does. A field is reset on the trunk: before the fan-out or after
// the fan-in.
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestReducerDeleteflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	mux.HandleTask("reducerdelete.verify:428/spawn", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("reducerdelete.verify:428/deleter", func(ctx context.Context, f *workflow.Flow) error {
		f.Del(f.GetString("field"))
		return nil
	})
	mux.HandleTask("reducerdelete.verify:428/writer", func(ctx context.Context, f *workflow.Flow) error {
		f.SetStrings("log", []string{"b2"}) // append reducer: write only the delta
		f.SetString("note", "written")
		return nil
	})
	mux.HandleTask("reducerdelete.verify:428/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// buildGraph wires Spawn -> {Deleter, Writer} -> Join(fan-in) -> END, declaring the two fan-out transitions
	// in the requested order so the fan_out_ordinal is deterministic.
	buildGraph := func(name string, deleterFirst bool) *workflow.Graph {
		g := workflow.NewGraph(name)
		g.SetURL("Spawn", "reducerdelete.verify:428/spawn")
		g.SetURL("Deleter", "reducerdelete.verify:428/deleter")
		g.SetURL("Writer", "reducerdelete.verify:428/writer")
		g.SetURL("Join", "reducerdelete.verify:428/join")
		g.SetFanIn("Join")
		g.SetReducer("log", workflow.ReducerAppend)
		if deleterFirst {
			g.AddTransition("Spawn", "Deleter")
			g.AddTransition("Spawn", "Writer")
		} else {
			g.AddTransition("Spawn", "Writer")
			g.AddTransition("Spawn", "Deleter")
		}
		g.AddTransition("Deleter", "Join")
		g.AddTransition("Writer", "Join")
		g.AddTransition("Join", workflow.End)
		assert.NoError(g.Validate())
		return g
	}
	mux.HandleGraph("reducerdelete.verify:428/delete-first", buildGraph("DeleteFirst", true))
	mux.HandleGraph("reducerdelete.verify:428/write-first", buildGraph("WriteFirst", false))

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	t.Run("a_branch_cannot_delete_a_reduced_field", func(t *testing.T) {
		assert := testarossa.For(t)
		for _, url := range []string{"reducerdelete.verify:428/delete-first", "reducerdelete.verify:428/write-first"} {
			_, outcome, err := eng.Run(ctx, url, map[string]any{"log": []string{"seed"}, "field": "log"}, nil)
			if !assert.NoError(err) {
				return
			}
			assert.Equal(workflow.StatusFailed, outcome.Status, url)
			assert.Contains(outcome.Error, `"log"`, url)
		}
	})

	t.Run("a_branch_deletes_a_plain_field_in_branch_order", func(t *testing.T) {
		assert := testarossa.For(t)

		// The delete comes first in branch order, so the write after it wins.
		_, outcome, err := eng.Run(ctx, "reducerdelete.verify:428/delete-first",
			map[string]any{"log": []string{"seed"}, "note": "seed", "field": "note"}, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
			assert.Equal("written", outcome.State.GetString("note"))
			assert.Equal([]string{"seed", "b2"}, outcome.State.GetStrings("log"))
		}

		// The delete comes last, so the field is gone.
		_, outcome, err = eng.Run(ctx, "reducerdelete.verify:428/write-first",
			map[string]any{"log": []string{"seed"}, "note": "seed", "field": "note"}, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
			assert.False(outcome.State.Has("note"))
			assert.Equal([]string{"seed", "b2"}, outcome.State.GetStrings("log"))
		}
	})
}
