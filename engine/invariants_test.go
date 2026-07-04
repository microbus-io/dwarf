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

// assertInvariants is the reusable end-of-test sweep that proves a workload never
// *created* any of the states the background wedge sweep / orphan detector exist to catch. It runs a fixed
// set of structural checks by direct SQL against every shard and must be called only after the workload has
// quiesced (all Awaits returned), because several checks are transiently violated mid-flight (a straggler
// sibling still settling, a fan-in mid-commit). A clean sweep means the recovery machinery had nothing to do.
func assertInvariants(t *testing.T, e *Engine) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	terminal := "('" + workflow.StatusCompleted + "', '" + workflow.StatusFailed + "', '" + workflow.StatusCancelled + "')"
	nonTerminal := "('" + workflow.StatusCreated + "', '" + workflow.StatusPending + "', '" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')"

	for shard := 1; shard <= e.db.NumShards(); shard++ {
		db, err := e.db.Shard(shard)
		if !assert.NoError(err) {
			continue
		}

		check := func(label, query string) {
			var n int
			if !assert.NoError(db.QueryRowContext(ctx, query).Scan(&n)) {
				return
			}
			assert.Equal(0, n, "shard %d: invariant violated: %s (%d offending rows)", shard, label, n)
		}

		// 1. No terminal flow retains a non-terminal step. Delete-marked flows (delete_after_ms>0) are excluded:
		// Delete flips an interrupted flow to cancelled but leaves its interrupted leaf for the reaper to remove
		// with the whole tree (deferred deletion), so a doomed row legitimately carries a non-terminal step
		// until reaped - inert meanwhile (selection/lease-recovery ignore interrupted; Resume 409s the cancelled
		// flow).
		check("terminal flow with a non-terminal step",
			"SELECT COUNT(*) FROM dwarf_steps s JOIN dwarf_flows f ON s.flow_id=f.flow_id"+
				" WHERE f.status IN "+terminal+" AND f.delete_after_ms=0 AND s.status IN "+nonTerminal)

		// 2. No terminal step is left parked.
		check("terminal step with parked<>0",
			"SELECT COUNT(*) FROM dwarf_steps WHERE status IN "+terminal+" AND parked<>0")

		// 3. Every running flow has at least one non-terminal step (else it is the orphan shape).
		check("running flow with zero non-terminal steps",
			"SELECT COUNT(*) FROM dwarf_flows f WHERE f.status='"+workflow.StatusRunning+"'"+
				" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN "+nonTerminal+")")

		// 4. Cohort counters never overshoot: arrivals<=size and failures<=arrivals on every spawn step.
		check("cohort counter overshoot",
			"SELECT COUNT(*) FROM dwarf_steps WHERE cohort_size>0 AND (cohort_arrivals>cohort_size OR cohort_failures>cohort_arrivals)")

		// 5. Every subgraph child's surgraph links resolve: the caller step exists, and the caller flow exists
		// and shares this child's root_flow_id (same tree).
		check("dangling surgraph link",
			"SELECT COUNT(*) FROM dwarf_flows f WHERE f.surgraph_step_id>0 AND ("+
				"NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.step_id=f.surgraph_step_id)"+
				" OR NOT EXISTS (SELECT 1 FROM dwarf_flows p WHERE p.flow_id=f.surgraph_flow_id AND p.root_flow_id=f.root_flow_id))")

		// 6. Every flow's root_flow_id resolves to an existing flow that is its own root.
		check("root_flow_id not resolving to a self-root",
			"SELECT COUNT(*) FROM dwarf_flows f WHERE NOT EXISTS ("+
				"SELECT 1 FROM dwarf_flows r WHERE r.flow_id=f.root_flow_id AND r.root_flow_id=r.flow_id)")

		// 7. No DAG edge (successor_id/predecessor_id) crosses into a different flow (0 = no edge, allowed).
		check("cross-flow DAG edge",
			"SELECT COUNT(*) FROM dwarf_steps s WHERE"+
				" (s.successor_id<>0 AND EXISTS (SELECT 1 FROM dwarf_steps t WHERE t.step_id=s.successor_id AND t.flow_id<>s.flow_id))"+
				" OR (s.predecessor_id<>0 AND EXISTS (SELECT 1 FROM dwarf_steps t WHERE t.step_id=s.predecessor_id AND t.flow_id<>s.flow_id))")
	}
}
