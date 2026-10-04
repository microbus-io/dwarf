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
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestSuccessorBatch_WideFanOutKeepsBranchOrder pins the multi-row successor insert across chunk boundaries. A
// width of 150 is inserted as chunks of 64, 64, 16, 4 and 2, and the ids each chunk returns are matched back to
// their rows by token rather than position - so every branch must still carry the ordinal of its own element, and
// an order-sensitive reducer must fold in input-array order.
func TestSuccessorBatch_WideFanOutKeepsBranchOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const width = 150
	g := workflow.NewGraph("Wide")
	g.SetURL("Seed", "wide/seed")
	g.SetURL("Work", "wide/work")
	g.SetURL("Join", "wide/join")
	g.SetFanIn("Join")
	g.SetReducer("seen", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "items", "n")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("wide/wf", g)

	mux.HandleTask("wide/seed", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("wide/work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("seen", []int{f.GetInt("n")})
		return nil
	})
	mux.HandleTask("wide/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	items := make([]int, width)
	for i := range items {
		items[i] = i
	}
	_, outcome, err := e.Run(ctx, "wide/wf", map[string]any{"items": items}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	seen, _ := stateVal(outcome.State, "seen").([]any)
	if assert.Equal(width, len(seen)) {
		for i, v := range seen {
			assert.Equal(float64(i), v, "the append reducer folds in input-array order")
		}
	}

	// Every branch row carries its own element's ordinal, and they all hang off the one spawn.
	db, err := e.db.Shard(1)
	assert.NoError(err)
	seed := stepIDsByTask(t, e, "Seed")[0]
	rows, err := db.QueryContext(ctx,
		"SELECT state, fan_out_ordinal, predecessor_id, lineage_id FROM dwarf_steps WHERE task_name='Work'")
	assert.NoError(err)
	defer rows.Close()
	count := 0
	for rows.Next() {
		var stateJSON []byte
		var ordinal, predecessor, lineage int
		assert.NoError(rows.Scan(&stateJSON, &ordinal, &predecessor, &lineage))
		state, _ := workflow.NewState(stateJSON)
		assert.Equal(state.GetInt("nIndex"), ordinal, "a branch's ordinal is its element's index")
		assert.Equal(state.GetInt("nIndex"), state.GetInt("n"))
		assert.Equal(seed, predecessor)
		assert.Equal(seed, lineage)
		count++
	}
	assert.NoError(rows.Err())
	assert.Equal(width, count)

	// The ids are matched back to their rows by token: the spawn's successor link is the branch of element 0.
	var successorID, firstBranch int
	assert.NoError(db.QueryRowContext(ctx, "SELECT successor_id FROM dwarf_steps WHERE step_id=?", seed).Scan(&successorID))
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT step_id FROM dwarf_steps WHERE task_name='Work' AND fan_out_ordinal=0").Scan(&firstBranch))
	assert.Equal(firstBranch, successorID)
}
