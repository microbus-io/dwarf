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
	"strconv"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
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
	mux := hosts.NewMux()
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

// TestSuccessorBatch_LoneSuccessorLinks pins the single-successor transition, whose own-row write names the new
// step in SQL on PostgreSQL rather than by a bind. Linear flows run concurrently, so inserts from
// different sessions interleave: an id that is not the session's own would link a step to another flow's row.
// Every link must agree with its successor's predecessor_id and stay within the flow, and every flow must point
// at its own last step.
func TestSuccessorBatch_LoneSuccessorLinks(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	const steps, flows = 6, 8
	g := workflow.NewGraph("Chain")
	for i := range steps {
		task := "S" + strconv.Itoa(i)
		g.SetURL(task, "lone/s")
		if i+1 < steps {
			g.AddTransition(task, "S"+strconv.Itoa(i+1))
		} else {
			g.AddTransition(task, workflow.End)
		}
	}
	assert.NoError(g.Validate())
	mux.HandleGraph("lone/wf", g)
	mux.HandleTask("lone/s", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("hops", f.GetInt("hops")+1)
		return nil
	})

	var wg sync.WaitGroup
	for range flows {
		wg.Go(func() {
			_, outcome, err := e.Run(ctx, "lone/wf", nil, nil)
			if assert.NoError(err) {
				assert.Equal(workflow.StatusCompleted, outcome.Status)
				assert.Equal(steps, outcome.State.GetInt("hops"))
			}
		})
	}
	wg.Wait()

	db, err := e.db.Shard(1)
	assert.NoError(err)
	rows, err := db.QueryContext(ctx,
		"SELECT s.step_id, s.flow_id, s.task_name, s.successor_id, n.flow_id, n.predecessor_id, n.task_name"+
			" FROM dwarf_steps s LEFT JOIN dwarf_steps n ON n.step_id=s.successor_id")
	assert.NoError(err)
	defer rows.Close()
	links := 0
	for rows.Next() {
		var stepID, flowID, successorID int
		var taskName string
		var nextFlowID, nextPredecessor *int
		var nextTask *string
		assert.NoError(rows.Scan(&stepID, &flowID, &taskName, &successorID, &nextFlowID, &nextPredecessor, &nextTask))
		if taskName == "S"+strconv.Itoa(steps-1) {
			assert.Zero(successorID, "the last step has no successor")
			continue
		}
		if assert.NotNil(nextFlowID, "step %d links to a step that exists", stepID) {
			assert.Equal(flowID, *nextFlowID, "step %d links within its flow", stepID)
			assert.Equal(stepID, *nextPredecessor, "step %d's successor names it as predecessor", stepID)
			n, _ := strconv.Atoi(taskName[1:])
			assert.Equal("S"+strconv.Itoa(n+1), *nextTask)
		}
		links++
	}
	assert.NoError(rows.Err())
	assert.Equal(flows*(steps-1), links)

	var strays int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_flows f LEFT JOIN dwarf_steps s ON s.step_id=f.stopped_step_id"+
			" WHERE s.flow_id IS NULL OR s.flow_id<>f.flow_id OR s.task_name<>?", "S"+strconv.Itoa(steps-1)).Scan(&strays))
	assert.Zero(strays, "every flow stopped at its own last step")
}
