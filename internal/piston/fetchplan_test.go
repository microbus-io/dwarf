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
//   - The planner knows how many keys there are. A custom plan sizes unnest's output from the bound array,
//     where a JSON set-returning function is estimated at 100 rows whatever it holds.
//   - idx_dwarf_steps_due cannot range-seek the due-ness test. With it the only index left and sequential
//     scans priced out, the fetch must not put not_before in any Index Cond; the control - the same
//     statement with the plain due-ness conjuncts - must, which is what keeps this able to fail.
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

	keys := []string{"a", "b", "c"}
	stmt, args, err := fetchQuery("pgx", 3, keys, 4, "", nil)
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
		controlPlan = explain(tx, strings.Replace(stmt, dueStepsOrdered, dueSteps, 1))
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}

	assert.True(regexp.MustCompile(`Function Scan on unnest k .*rows=3 `).MatchString(sized),
		"the key list must be sized from the bound array:\n%s", sized)
	seeksDue := regexp.MustCompile(`Index Cond: .*not_before`)
	assert.False(seeksDue.MatchString(fetchPlan), "the fetch must not range-seek due-ness:\n%s", fetchPlan)
	assert.True(seeksDue.MatchString(controlPlan), "control: plain conjuncts should let the due index seek:\n%s", controlPlan)
}

// TestFetchSteps_PostgresPlansEveryExecution pins that the Postgres fetch runs as an unnamed statement, so
// Postgres plans it with its actual values on every execution and never caches a generic plan for it. A
// generic plan does not know the key count or the band; after an ANALYZE taken at a small backlog it seeks the
// due index on (parked, priority) and walks every due row at the band for each key (measured on a rig: a
// 509-second fetch). Prepared statements are per session, so the pool is held to one connection and the
// fetch runs past the five executions after which Postgres considers a generic plan. The control runs the
// same statement in pgx's default mode, which must leave a named statement - proving the check can see one.
func TestFetchSteps_PostgresPlansEveryExecution(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	if r.db.DriverName() != "pgx" {
		t.Skip("named statements and generic plans are Postgres's")
	}
	r.db.SetMaxOpenConns(1)
	r.db.SetMaxIdleConns(1)
	r.insertStep(t, 1, 5, "alpha", 1)
	r.insertStep(t, 2, 5, "beta", 1)

	named := func() int {
		var n int
		err := r.db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM pg_prepared_statements WHERE statement LIKE '%CROSS JOIN LATERAL%'"+
				" AND statement NOT LIKE '%pg_prepared_statements%'").Scan(&n)
		assert.NoError(err)
		return n
	}
	for range 7 {
		got, err := r.p.FetchSteps(ctx, 1, 5, []string{"alpha", "beta"}, 2)
		assert.NoError(err)
		assert.Len(got, 2)
	}
	assert.Zero(named(), "the fetch must not leave a named statement, whose plan Postgres may make generic")

	stmt, args, err := fetchQuery("pgx", 5, []string{"alpha", "beta"}, 2, "", nil)
	assert.NoError(err)
	rows, err := r.db.QueryContext(ctx, stmt, args...)
	if assert.NoError(err) {
		rows.Close()
	}
	assert.Equal(1, named(), "control: the default mode prepares a named statement")
}
