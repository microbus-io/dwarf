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
