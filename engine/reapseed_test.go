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
	"strconv"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
)

// treeShape is a closed tree to seed: a root of rootSteps steps whose last step called a child flow of childSteps
// steps, or no child when childSteps is negative. A child of no steps is a flow the move counts as one row.
type treeShape struct {
	rootSteps, childSteps int
}

var (
	// callShape is a root of one step that called a child of one step: two flows, two steps.
	callShape = treeShape{rootSteps: 1, childSteps: 1}
)

// seedTrees inserts n closed trees of shape on db, one after another, and returns their roots in that order. Every flow
// is completed now by the database clock and every step completed, as a tree the engine ran to its end would be, so
// the engine's own sweeps leave it alone; created_hour takes its column default, the database clock's hour.
func seedTrees(t *testing.T, db *sequel.DB, n int, shape treeShape) []int {
	t.Helper()
	ctx := t.Context()
	const token = "0123456789abcdef"
	flow := func(tx *sequel.Tx, root, surgraphFlow, surgraphStep int) (int, int, error) {
		id, err := tx.InsertReturnID(ctx, "flow_id",
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status,"+
				" surgraph_flow_id, surgraph_step_id, root_flow_id) VALUES (?,?,?,?,?,'"+workflow.StatusCompleted+"',?,?,?)",
			token, "seed/tree", "Seed", []byte("{}"), "", surgraphFlow, surgraphStep, root) // graph bound: binary on SQL Server
		if err != nil {
			return 0, 0, err
		}
		var hour int
		err = tx.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", id).Scan(&hour)
		return int(id), hour, err
	}
	steps := func(tx *sequel.Tx, flowID, hour, count int) (last int, err error) {
		for i := range count {
			id, err := tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
					" flow_created_hour) VALUES (?,?,?,?,?,'"+workflow.StatusCompleted+"',60000,?)",
				flowID, i+1, token, "T"+strconv.Itoa(i+1), "seed/t", hour)
			if err != nil {
				return 0, err
			}
			last = int(id)
		}
		return last, nil
	}
	roots := make([]int, 0, n)
	for range n {
		var root int
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			id, hour, err := flow(tx, 0, 0, 0)
			if err != nil {
				return err
			}
			root = id
			if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET root_flow_id=? WHERE flow_id=?", root, root); err != nil {
				return err
			}
			caller, err := steps(tx, root, hour, shape.rootSteps)
			if err != nil || shape.childSteps < 0 {
				return err
			}
			child, childHour, err := flow(tx, root, root, caller)
			if err != nil {
				return err
			}
			_, err = steps(tx, child, childHour, shape.childSteps)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
	}
	return roots
}

// closeTreesAgo moves the close of every flow of the trees rooted at roots to ago before now.
func closeTreesAgo(t *testing.T, db *sequel.DB, ago time.Duration, roots ...int) {
	t.Helper()
	for _, root := range roots {
		if _, err := db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE root_flow_id=?",
			-ago.Milliseconds(), root); err != nil {
			t.Fatal(err)
		}
	}
}
