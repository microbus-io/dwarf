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
