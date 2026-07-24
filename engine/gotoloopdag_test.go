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
	"sort"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFanIn_GotoLoopBranchKeepsInteriorDAGEdges pins the execution DAG for a fan-out branch that LOOPS through
// its exit task via flow.Goto before reaching the fan-in. The looping node shares its task name with the fan-in's
// exit task, so insertFanInStep's exit scan (task_name IN fan-in-predecessors) must not tag the interior loop
// iterations as exit steps and overwrite their successor_id with the fan-in id - only the iteration that actually
// transitioned to the fan-in (successor_id still 0) is an exit. Branch lengths are asymmetric (one branch loops,
// one does not), so the fan-in also exercises deterministic reducer folding across uneven branches.
func TestFanIn_GotoLoopBranchKeepsInteriorDAGEdges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	eng := NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	graph := workflow.NewGraph("GotoLoopDAG")
	graph.SetEndpoint("Split", "gotoloopdag.verify:900/split")
	graph.SetEndpoint("Work", "gotoloopdag.verify:900/work")
	graph.SetEndpoint("Join", "gotoloopdag.verify:900/join")
	graph.SetFanIn("Join")
	graph.SetReducer("done", workflow.ReducerAppend)
	graph.AddTransitionForEach("Split", "Work", "items", "item")
	graph.AddTransitionGoto("Work", "Work")
	graph.AddTransitionChain("Work", "Join", workflow.END)
	assert.NoError(graph.Validate())
	proxy.HandleGraph("gotoloopdag.verify:900/loop", graph)

	proxy.HandleTask("gotoloopdag.verify:900/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("gotoloopdag.verify:900/work", func(ctx context.Context, f *workflow.Flow) error {
		n := f.GetInt("iter")
		limit := 0
		if f.GetString("item") == "b" {
			limit = 2 // branch "b" loops twice; branch "a" not at all
		}
		if n < limit {
			f.SetInt("iter", n+1)
			f.Goto("Work")
			return nil
		}
		f.Set("done", []string{f.GetString("item")})
		return nil
	})
	proxy.HandleTask("gotoloopdag.verify:900/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	flowKey, outcome, err := eng.Run(ctx, "gotoloopdag.verify:900/loop", map[string]any{"items": []string{"a", "b"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Both branches converge exactly once at the fan-in, in input-array order (append reducer).
	var done []string
	for _, v := range outcome.State["done"].([]any) {
		done = append(done, v.(string))
	}
	sort.Strings(done)
	assert.Equal([]string{"a", "b"}, done)

	shard, _ := parseFlowShard(flowKey)
	db, err := eng.db.Shard(shard)
	assert.NoError(err)
	var joinStepID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE task_name='Join'").Scan(&joinStepID))

	rows, err := db.QueryContext(ctx, "SELECT step_id, successor_id FROM dwarf_steps WHERE task_name='Work' ORDER BY step_id")
	assert.NoError(err)
	defer rows.Close()
	total, pointingAtJoin := 0, 0
	for rows.Next() {
		var id, succ int
		assert.NoError(rows.Scan(&id, &succ))
		total++
		if succ == joinStepID {
			pointingAtJoin++
		}
	}
	// 1 Work (branch a) + 3 Work (branch b: iter 0,1,2) = 4 steps; only the two real exits point at the fan-in.
	assert.Equal(4, total)
	assert.Equal(2, pointingAtJoin, "only the exit Work of each branch points at the fan-in; loop iterations keep their forward edge to the next Work")
}

// parseFlowShard parses the leading shard number from a "{shard}-{id}-{token}" key.
func parseFlowShard(flowKey string) (int, bool) {
	n := 0
	for i := 0; i < len(flowKey); i++ {
		c := flowKey[i]
		if c == '-' {
			return n, i > 0
		}
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return 0, false
}
