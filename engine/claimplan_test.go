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
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestClaim_NoPartialIndexPlan pins that the Postgres claim can be served by the primary key alone. A partial
// index on pending rows filtered on step_id walks the whole backlog, and after an ANALYZE taken at a small
// backlog the planner picks one, so the claim's eligibility test must not imply any partial index's predicate.
//
// Statistics cannot be made to reproduce that choice on demand, so the test asks a sharper question: with the
// primary key gone and sequential scans priced out, which indexes could the planner use? The engine's claim
// must name none of the partial ones. The control - the same statement with the eligibility test as plain
// conjuncts - must name one, which is what keeps this test able to fail. Everything runs in a transaction that
// is rolled back.
func TestClaim_NoPartialIndexPlan(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("C")
	g.SetURL("A", "claimplan/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("claimplan/g", g)
	mux.HandleTask("claimplan/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(mux))
	assert.NoError(e.SetWorkers(0)) // nothing dispatches: the entry step stays pending
	assert.NoError(e.Startup(t.Context()))
	_, err := e.Create(ctx, "claimplan/g", nil, nil)
	if !assert.NoError(err) {
		return
	}

	var db *sequel.DB
	var stepID int64
	for _, idx := range e.db.Indices() {
		shard, err := e.db.Shard(idx)
		if !assert.NoError(err) {
			return
		}
		if shard.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE status='"+workflow.StatusPending+"'").Scan(&stepID) == nil {
			db = shard
			break
		}
	}
	if !assert.NotNil(db, "the entry step should be pending on some shard") {
		return
	}
	if db.DriverName() != "pgx" {
		t.Skip("the partial-index plan choice is Postgres's: its claim is the only one joined and planned this way")
	}

	partial := []string{"idx_dwarf_steps_due", "idx_dwarf_steps_selection", "idx_dwarf_steps_status"}
	errRollback := errors.New("rollback")
	plan := func(stmt string) string {
		var out strings.Builder
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			if _, err := tx.ExecContext(ctx, "ALTER TABLE dwarf_steps DROP CONSTRAINT dwarf_steps_pkey CASCADE"); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
				return err
			}
			rows, err := tx.QueryContext(ctx, "EXPLAIN (COSTS OFF) "+stmt,
				workflow.StatusRunning, 1000, e.engineID, stepID, parkedNone)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var line string
				if err := rows.Scan(&line); err != nil {
					return err
				}
				out.WriteString(line)
				out.WriteString("\n")
			}
			if err := rows.Err(); err != nil {
				return err
			}
			return errRollback
		})
		if !errors.Is(err, errRollback) {
			assert.NoError(err)
		}
		return out.String()
	}
	uses := func(p string) []string {
		var found []string
		for _, idx := range partial {
			if strings.Contains(p, idx) {
				found = append(found, idx)
			}
		}
		return found
	}

	claim := plan(claimSQLPostgres)
	assert.Equal(0, len(uses(claim)), "the claim must not be servable by a partial index:\n%s", claim)

	plain := "s.status='" + workflow.StatusPending + "' AND s.parked=? AND s.not_before<=NOW_UTC() AND s.lease_expires<=NOW_UTC()"
	control := plan(strings.Replace(claimSQLPostgres, claimEligible("s."), plain, 1))
	assert.True(len(uses(control)) > 0, "control: plain conjuncts should let a partial index serve the claim:\n%s", control)
}
