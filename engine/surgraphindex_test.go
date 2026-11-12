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
	"github.com/microbus-io/testarossa"
)

// TestSurgraphLookups_UseThePartialIndex pins that each lookup of a caller's child flows is served by the partial
// idx_dwarf_flows_surgraph_step on SQLite. A partial index serves only a predicate that implies its own
// (surgraph_step_id > 0), which a bound or correlated id does not, so each lookup carries the literal; without it
// every one scans dwarf_flows - the retry's reap on every flow.Retry, and the wedge sweep once per parked step.
func TestSurgraphLookups_UseThePartialIndex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	if db.DriverName() != "sqlite" {
		t.Skip("the plan shape is SQLite's")
	}

	cases := []struct {
		name string
		sql  string
		args []any
	}{
		{"latest child", latestChildSQL("dwarf_flows"), []any{7}},
		{"caller's children", callerChildrenSQL, []any{7}},
		{"wedged parks", wedgedParksSQL, []any{parkedSubgraph, -1000}},
	}
	for _, c := range cases {
		rows, err := db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+c.sql, c.args...)
		assert.Must(assert.NoError(err))
		var plan []string
		for rows.Next() {
			var id, parent, unused int
			var detail string
			assert.NoError(rows.Scan(&id, &parent, &unused, &detail))
			plan = append(plan, detail)
		}
		assert.NoError(rows.Err())
		rows.Close()
		joined := strings.Join(plan, " | ")
		assert.Contains(joined, "idx_dwarf_flows_surgraph_step", "%s: not served by the partial index: %s", c.name, joined)
		for _, step := range plan {
			assert.False(strings.HasPrefix(step, "SCAN dwarf_flows") || strings.HasPrefix(step, "SCAN f"),
				"%s scans the flows: %s", c.name, joined)
		}
	}
}
