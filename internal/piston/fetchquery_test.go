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
	"strconv"
	"strings"
	"testing"

	"github.com/microbus-io/testarossa"
)

// fetchDrivers is every driver name fetchQuery answers to, plus one it does not - an unknown name must
// land on a working branch rather than an empty statement, since the alternative is a shard that silently
// supplies nothing.
var fetchDrivers = []string{"pgx", "mysql", "mssql", "sqlite", "unknown"}

// TestFetchQuery_BindCountIsFixed is the regression this file exists for: the chosen keys travel as ONE
// json parameter, so the bind count must not move with how many keys the plan picked.
//
// The bound it protects is a HARD one, not a budget - SQL Server rejects a statement with more than 2,100
// parameters outright, and the key count is bounded only by the candidate cache's capacity (twice the
// worker count), which reaches into the thousands on a large pool. One bind per key therefore fails the
// fetch on a real deployment, and the pipeline's error policy turns that into a shard that keeps claiming
// its band while supplying nothing, every cycle, for as long as the cardinality holds.
func TestFetchQuery_BindCountIsFixed(t *testing.T) {
	t.Parallel()
	for _, driver := range fetchDrivers {
		few := make([]string, 1)
		many := make([]string, 5000)
		for i := range few {
			few[i] = "k" + strconv.Itoa(i)
		}
		for i := range many {
			many[i] = "k" + strconv.Itoa(i)
		}
		_, fewArgs, err := fetchQuery(driver, 3, few, 4, "", nil)
		testarossa.NoError(t, err, driver)
		_, manyArgs, err := fetchQuery(driver, 3, many, 4, "", nil)
		testarossa.NoError(t, err, driver)
		if driver == "mysql" {
			// mysql alone still binds one per key - it cannot use the json key list without tripping a
			// cross-collation comparison, and its 65,535 ceiling is ~20x above any reachable key count.
			// Asserted rather than exempted so the exception stays deliberate.
			testarossa.True(t, len(manyArgs) == len(many)+2,
				"%s: expected one bind per key plus band and cap, got %d", driver, len(manyArgs))
			continue
		}
		testarossa.Equal(t, len(fewArgs), len(manyArgs),
			"%s: bind count must not scale with the key count", driver)
		testarossa.True(t, len(manyArgs) <= 5,
			"%s: %d binds at 5000 keys", driver, len(manyArgs))
	}
}

// TestFetchQuery_PlaceholdersMatchArgs pins the pairing every branch has to get right by hand: one bind per
// '?', in the order the placeholders appear. mssql is the branch this exists for - CROSS APPLY puts TOP's
// cap BEFORE the band, so its argument order differs from every other dialect's and cannot be copied.
//
// Both partition shapes are exercised because partitionPredicate contributes binds of its own: two while
// partitioning strictly, six while stealing.
func TestFetchQuery_PlaceholdersMatchArgs(t *testing.T) {
	t.Parallel()
	parts := []struct {
		name string
		sql  string
		args []any
	}{
		{"none", "", nil},
		{"strict", " AND step_id % ? = ?", []any{3, 1}},
		{
			"stealing",
			" AND (step_id % ? = ? OR (step_id % ? = ? AND not_before <= DATE_ADD_MILLIS(NOW_UTC(), ?))" +
				" OR not_before <= DATE_ADD_MILLIS(NOW_UTC(), ?))",
			[]any{3, 1, 3, 2, -100, -200},
		},
	}
	for _, driver := range fetchDrivers {
		for _, part := range parts {
			stmt, args, err := fetchQuery(driver, 3, []string{"a", "b"}, 4, part.sql, part.args)
			testarossa.NoError(t, err, driver)
			testarossa.Equal(t, strings.Count(stmt, "?"), len(args),
				"%s/%s: %d placeholders against %d args", driver, part.name, strings.Count(stmt, "?"), len(args))
		}
	}
}

// TestFetchQuery_ProjectionIsUniform pins the one thing FetchSteps' scan loop assumes across every branch:
// (fairness_key, step_id), in that order. A branch that projects them the other way round scans a key into
// an int and fails at runtime on that dialect only - which no SQLite-only test run would ever reach.
func TestFetchQuery_ProjectionIsUniform(t *testing.T) {
	t.Parallel()
	for _, driver := range fetchDrivers {
		stmt, _, err := fetchQuery(driver, 3, []string{"a"}, 4, "", nil)
		testarossa.NoError(t, err, driver)
		upper := strings.ToUpper(stmt)
		testarossa.True(t, strings.HasPrefix(upper, "SELECT "), driver)
		head := upper[len("SELECT "):]
		key := strings.Index(head, "FAIRNESS_KEY")
		step := strings.Index(head, "STEP_ID")
		testarossa.True(t, key >= 0 && step >= 0, "%s: projection names both columns", driver)
		testarossa.True(t, key < step, "%s: fairness_key must be projected before step_id", driver)
	}
}
