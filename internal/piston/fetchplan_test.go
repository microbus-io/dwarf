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

package piston

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestFetchQuery_PostgresPlan pins the two properties the Postgres fetch's plan depends on, neither of which
// statistics can be made to break on demand:
//
//   - The planner knows how many pairs there are. A custom plan sizes unnest's output from the bound arrays,
//     where a JSON set-returning function is estimated at 100 rows whatever it holds.
//   - idx_dwarf_steps_due cannot serve the fetch at all. With it the only index left and sequential scans
//     priced out, the fetch must still not use it, because its partial predicate carries a conjunct
//     (dueIndexGuard) the fetch does not state; the control - the same statement with the guard - must use
//     it, which is what keeps this able to fail.
//
// Everything that changes the schema runs in a transaction that is rolled back.
func TestFetchQuery_PostgresPlan(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	if r.db.DriverName() != "pgx" {
		t.Skip("the plan properties pinned here are Postgres's")
	}

	stmt, args, err := fetchQuery("pgx", pairsAt(3, "a", "b", "c"), 4, "", nil)
	if !assert.NoError(err) {
		return
	}
	explain := func(tx *sequel.Tx, q string) string {
		var out strings.Builder
		rows, err := tx.QueryContext(ctx, "EXPLAIN "+q, args...)
		if !assert.NoError(err) {
			return ""
		}
		defer rows.Close()
		for rows.Next() {
			var line string
			assert.NoError(rows.Scan(&line))
			out.WriteString(line + "\n")
		}
		assert.NoError(rows.Err())
		return out.String()
	}
	errRollback := errors.New("rollback")
	var fetchPlan, controlPlan, sized string
	err = r.db.Transact(ctx, func(tx *sequel.Tx) error {
		sized = explain(tx, stmt)
		// Leave the due index as the only one that can serve the predicate, so the question is what it is
		// allowed to seek.
		if _, err := tx.ExecContext(ctx, "DROP INDEX idx_dwarf_steps_selection"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			return err
		}
		fetchPlan = explain(tx, stmt)
		controlPlan = explain(tx, strings.Replace(stmt, dueNow, dueSteps, 1))
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}

	assert.True(regexp.MustCompile(`Function Scan on (unnest )?k .*rows=3 `).MatchString(sized),
		"the pair list must be sized from the bound arrays:\n%s", sized)
	assert.False(strings.Contains(fetchPlan, "idx_dwarf_steps_due"), "the fetch must not use the due index:\n%s", fetchPlan)
	assert.True(strings.Contains(controlPlan, "idx_dwarf_steps_due"), "control: with the guard the due index serves it:\n%s", controlPlan)
}

// TestScanBand_CanSeekTheDueIndex pins the other half of dueIndexGuard: the queries that want
// idx_dwarf_steps_due still reach it. With the selection index dropped and sequential scans priced out, both of
// the band scan's statements - the counts and the tally - must seek the due index on not_before, on every dialect
// whose due index is partial. A query that lost the guard would fall back to a scan here.
func TestScanBand_CanSeekTheDueIndex(t *testing.T) {
	t.Parallel()
	r := newRig(t)
	assertBandScanSeeks(t, r, "counts", bandCountsQuery(r.db.DriverName(), "", 3, 8))
	assertBandScanSeeks(t, r, "tally", bandTallyQuery("", []int{2, 3}, 3))
}

// assertBandScanSeeks is one statement of TestScanBand_CanSeekTheDueIndex.
func assertBandScanSeeks(t *testing.T, r *rig, name, stmt string) {
	assert := testarossa.For(t)
	ctx := context.Background()
	var args []any
	var explain, priceOutScans string
	switch r.db.DriverName() {
	case "pgx":
		explain, priceOutScans = "EXPLAIN ", "SET LOCAL enable_seqscan = off"
	case "sqlite":
		explain = "EXPLAIN QUERY PLAN "
	default:
		t.Skip("the plan shapes pinned here are Postgres's and SQLite's")
	}
	errRollback := errors.New("rollback")
	var plan strings.Builder
	err := r.db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx, "DROP INDEX idx_dwarf_steps_selection"); err != nil {
			return err
		}
		if priceOutScans != "" {
			if _, err := tx.ExecContext(ctx, priceOutScans); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, explain+stmt, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			plan.WriteString(fmt.Sprint(vals...) + "\n")
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}
	seeks := regexp.MustCompile(`idx_dwarf_steps_due[^\n]*\n?[^\n]*not_before`)
	assert.True(seeks.MatchString(plan.String()), "the %s statement must seek the due index on not_before:\n%s", name, plan.String())
}

// TestScanBand_AnEmptyBandIsAProbe pins that the counts statement costs an empty band one probe of the due index,
// however deep another band is. On PostgreSQL the index's key must not lead with status: its partial predicate fixes
// status, so PostgreSQL removes the query's status='pending' from the index conditions, and a leading column with
// no condition cannot be seeked. Versions before 18, which have no skip scan, then walk the whole index for every
// band; 18 skips over the single status value and hides the defect, so this fails only before 18 (measured on 16).
func TestScanBand_AnEmptyBandIsAProbe(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	if r.db.DriverName() != "pgx" {
		t.Skip("buffer counts are PostgreSQL's")
	}
	ctx := context.Background()
	r.insertStep(t, 1, 1, "k", 1)
	for range 14 { // 16,384 due steps at band 1, the depth an empty band's probe must not read
		_, err := r.db.ExecContext(ctx, "INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url,"+
			" status, time_budget_ms, priority, fairness_key, fairness_weight, not_before, lease_expires, created_at)"+
			" SELECT flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, priority, fairness_key,"+
			" fairness_weight, not_before, lease_expires, created_at FROM dwarf_steps")
		assert.NoError(err)
	}
	_, err := r.db.ExecContext(ctx, "ANALYZE dwarf_steps")
	assert.NoError(err)

	errRollback := errors.New("rollback")
	var buffers int
	err = r.db.Transact(ctx, func(tx *sequel.Tx) error {
		// Only the due index is left, and sequential scans are priced out, so the plan reads it.
		if _, err := tx.ExecContext(ctx, "DROP INDEX idx_dwarf_steps_selection"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_seqscan = off"); err != nil {
			return err
		}
		var out string
		// Bands 2 and 3 and the overflow are empty; band 1 stops at its LIMIT of 8.
		stmt := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + bandCountsQuery("pgx", "", 3, 8)
		if err := tx.QueryRowContext(ctx, stmt).Scan(&out); err != nil {
			return err
		}
		var plan []struct {
			Plan struct {
				Hit  int `json:"Shared Hit Blocks"`
				Read int `json:"Shared Read Blocks"`
			} `json:"Plan"`
		}
		if err := json.Unmarshal([]byte(out), &plan); err != nil {
			return err
		}
		buffers = plan[0].Plan.Hit + plan[0].Plan.Read
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}
	// A probe is a few pages per branch; a walk of the index is well over a hundred per empty band.
	assert.True(buffers > 0 && buffers < 60, "the counts statement read %d buffers", buffers)
}

// TestScanBand_PlanningReadsNoIndexEnd pins that planning the band scan's statements reads no index. A guard
// written as a range (step_id>0) lies below every value in the column's histogram, so the planner reads the
// primary key's lowest entry not yet removable to refine the estimate, on every branch of every plan: a few pages
// per branch at the least, and every dead entry before it once those are removable but not yet vacuumed - which
// is what retention leaves. How far the probe walks depends on the cluster-wide vacuum horizon (an open
// transaction in any database stops it at the first dead entry), so the assertions rest only on what holds either
// way: a warm plan without a range reads less than a page per branch, and a range reads at least two per branch
// more. Each statement is planned twice on one connection, so the second plan's buffers are its own, not the
// session's first catalog reads.
func TestScanBand_PlanningReadsNoIndexEnd(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	if r.db.DriverName() != "pgx" {
		t.Skip("planning buffer counts are PostgreSQL's")
	}
	ctx := context.Background()
	_, err := r.db.ExecContext(ctx, "ALTER TABLE dwarf_steps SET (autovacuum_enabled = off)")
	assert.NoError(err)
	r.insertStep(t, 1, 1, "k", 1)
	for range 14 { // 16,384 steps
		_, err := r.db.ExecContext(ctx, "INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url,"+
			" status, time_budget_ms, priority, fairness_key, fairness_weight, not_before, lease_expires, created_at)"+
			" SELECT flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, priority, fairness_key,"+
			" fairness_weight, not_before, lease_expires, created_at FROM dwarf_steps")
		assert.NoError(err)
	}
	_, err = r.db.ExecContext(ctx, "ANALYZE dwarf_steps")
	assert.NoError(err)
	// What retention leaves: the oldest steps gone, their index entries dead until a vacuum.
	_, err = r.db.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE step_id <= (SELECT MAX(step_id) - 1024 FROM dwarf_steps)")
	assert.NoError(err)

	planBuffers := func(stmt string) int {
		var buffers int
		err := r.db.Transact(ctx, func(tx *sequel.Tx) error {
			for range 2 {
				var out string
				if err := tx.QueryRowContext(ctx, "EXPLAIN (BUFFERS, FORMAT JSON) "+stmt).Scan(&out); err != nil {
					return err
				}
				var plan []struct {
					Planning struct {
						Hit  int `json:"Shared Hit Blocks"`
						Read int `json:"Shared Read Blocks"`
					} `json:"Planning"`
				}
				if err := json.Unmarshal([]byte(out), &plan); err != nil {
					return err
				}
				buffers = plan[0].Planning.Hit + plan[0].Planning.Read
			}
			return nil
		})
		assert.NoError(err)
		return buffers
	}
	const countsBranches, tallyBranches = 4, 3 // bands 1-3 and the overflow; bands 1-3
	counts := bandCountsQuery("pgx", "", 3, 8)
	tally := bandTallyQuery("", []int{1, 2, 3}, 3)
	countsBuffers, tallyBuffers := planBuffers(counts), planBuffers(tally)
	assert.True(countsBuffers < countsBranches, "planning the counts statement read %d buffers", countsBuffers)
	assert.True(tallyBuffers < tallyBranches, "planning the tally statement read %d buffers", tallyBuffers)
	// Control: the same statement with the guard written as a range reads the primary key on every branch.
	ranged := planBuffers(strings.ReplaceAll(counts, dueIndexGuard, " AND step_id>0"))
	assert.True(ranged >= countsBuffers+2*countsBranches, "control: a range guard read only %d buffers in planning", ranged)
}

// TestScanBand_SQLServerChoosesTheDueIndex pins that SQL Server's optimizer picks the due index for the counts
// statement while the selection index exists too, as it does in production. Its key must keep status leading on
// SQL Server: without the filter column in the index the optimizer costs the filtered index as needing a lookup,
// prefers the selection index, and walks a band's sleepers. Unlike the plan tests above, nothing is dropped.
func TestScanBand_SQLServerChoosesTheDueIndex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	if r.db.DriverName() != "mssql" {
		t.Skip("the plan pinned here is SQL Server's")
	}
	ctx := context.Background()
	r.insertStep(t, 1, 1, "k", 1)
	r.insertStep(t, 2, 2, "k", 1)
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=DATE_ADD_MILLIS(NOW_UTC(), 3600000) WHERE priority=2")
	assert.NoError(err)
	for range 14 { // 16,384 steps at each of band 1 (due) and band 2 (asleep for an hour)
		_, err := r.db.ExecContext(ctx, "INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url,"+
			" status, time_budget_ms, priority, fairness_key, fairness_weight, not_before, lease_expires, created_at)"+
			" SELECT flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, priority, fairness_key,"+
			" fairness_weight, not_before, lease_expires, created_at FROM dwarf_steps")
		assert.NoError(err)
	}
	_, err = r.db.ExecContext(ctx, "UPDATE STATISTICS dwarf_steps WITH FULLSCAN")
	assert.NoError(err)

	errRollback := errors.New("rollback")
	var plan string
	err = r.db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET SHOWPLAN_XML ON"); err != nil {
			return err
		}
		err := tx.QueryRowContext(ctx, bandCountsQuery("mssql", "", 2, 8)).Scan(&plan)
		if _, offErr := tx.ExecContext(ctx, "SET SHOWPLAN_XML OFF"); offErr != nil && err == nil {
			err = offErr
		}
		if err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}
	// The branches' index reads, in order: band 1, band 2, then the overflow. An empty overflow range is one cheap
	// seek in either index, so only the bands are pinned; band 2 is the one a selection read would walk.
	reads := regexp.MustCompile(`Index="\[(idx_dwarf_steps_\w+)\]"`).FindAllStringSubmatch(plan, -1)
	if assert.True(len(reads) >= 2, "the plan names no index reads:\n%s", plan) {
		assert.Equal("idx_dwarf_steps_due", reads[0][1], "band 1 must read the due index")
		assert.Equal("idx_dwarf_steps_due", reads[1][1], "band 2, all asleep, must read the due index")
	}
}
