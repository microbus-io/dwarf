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

package janitor

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// flowPlan is the part of PostgreSQL's executed plan needed to distinguish a seek from reading and sorting a tree.
type flowPlan struct {
	Kind     string     `json:"Node Type"`
	Index    string     `json:"Index Name"`
	Rows     float64    `json:"Actual Rows"`
	Filtered float64    `json:"Rows Removed by Filter"`
	Plans    []flowPlan `json:"Plans"`
}

func readFlowPlan(t *testing.T, q sequel.Executor, stmt string, args ...any) flowPlan {
	t.Helper()
	var raw string
	if err := q.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE, FORMAT JSON) "+stmt, args...).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plans []struct{ Plan flowPlan }
	if err := json.Unmarshal([]byte(raw), &plans); err != nil {
		t.Fatal(err)
	}
	if len(plans) != 1 {
		t.Fatalf("unexpected plan: %s", raw)
	}
	return plans[0].Plan
}

func (p flowPlan) walk(fn func(flowPlan)) {
	fn(p)
	for _, child := range p.Plans {
		child.walk(fn)
	}
}

// TestChunk_DescendantReadsWalkTheTreeIndex pins both directions, including deletion from an archive day. With no
// statistics a single-column root index reads and sorts all 10,000 descendants for each 256-row chunk.
func TestChunk_DescendantReadsWalkTheTreeIndex(t *testing.T) {
	t.Parallel()
	r := newRig(t, 30)
	if r.db.DriverName() != "pgx" {
		t.Skip("PostgreSQL's planner")
	}
	assert := testarossa.For(t)
	ctx := t.Context()
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	archived := migrations.FlowsTable(today)
	for _, table := range []string{"dwarf_flows", archived} {
		_, err = r.db.ExecContext(ctx, "ALTER TABLE "+table+" SET (autovacuum_enabled=false)")
		assert.Must(assert.NoError(err))
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO dwarf_flows (flow_id,flow_token,workflow_url,workflow_name,graph,graph_hash,status,root_flow_id,surgraph_flow_id)
        SELECT g,'tok','url','name','{}','hash','completed',CASE WHEN g%25=0 THEN 7 ELSE g END,
        CASE WHEN g%25=0 THEN 7 ELSE 0 END FROM generate_series(1,250000) g`)
	assert.Must(assert.NoError(err))
	_, err = r.db.ExecContext(ctx, "INSERT INTO "+archived+" ("+archivedFlowColumns+", archived_at) SELECT "+archivedFlowColumns+", NOW() FROM dwarf_flows")
	assert.Must(assert.NoError(err))
	for _, analyzed := range []bool{false, true} {
		for _, table := range []string{"dwarf_flows", archived} {
			if analyzed {
				_, err = r.db.ExecContext(ctx, "ANALYZE "+table)
				assert.Must(assert.NoError(err))
			}
			err = r.db.Transact(ctx, func(tx *sequel.Tx) error {
				if err := boundReads(ctx, tx); err != nil {
					return err
				}
				check := func(stmt string, args ...any) {
					plan := readFlowPlan(t, tx, stmt, args...)
					rootIndex := false
					plan.walk(func(p flowPlan) {
						assert.False(strings.Contains(p.Kind, "Sort") || strings.Contains(p.Kind, "Bitmap") || p.Kind == "Seq Scan", "analyzed=%v: %+v", analyzed, plan)
						if p.Index == "idx_"+table+"_root" {
							rootIndex = true
							assert.True(p.Rows+p.Filtered <= treeChunk+1, "a page reads at most its descendants and the root: %+v", plan)
						}
					})
					// With statistics PostgreSQL can prefer an ordered primary-key scan; without them the root
					// index must supply the order, not a sort over the whole tree.
					if !analyzed {
						assert.True(rootIndex, "%+v", plan)
					}
				}
				if table == "dwarf_flows" {
					for _, after := range []int{0, 125000} {
						check(descendantsAfterSQL, 7, 7, after, treeChunk)
					}
				}
				for _, below := range []int{0, 125000} {
					stmt, args := descendantsBelowSQL(strings.TrimPrefix(table, "dwarf_flows"), 7, below)
					check(stmt, args...)
				}
				return nil
			})
			assert.Must(assert.NoError(err))
		}
	}
}

// TestClosed_TheProbeSkipsStagedDescendants pins a root-only ordered scan beside a staged tree's 250,000 flows.
// Its closed descendants remain live until publication and must not be scanned on every lag probe.
func TestClosed_TheProbeSkipsStagedDescendants(t *testing.T) {
	t.Parallel()
	r := newRig(t, 30)
	if r.db.DriverName() != "pgx" {
		t.Skip("PostgreSQL's planner")
	}
	assert := testarossa.For(t)
	ctx := t.Context()
	_, err := r.db.ExecContext(ctx, `INSERT INTO dwarf_flows (flow_id,flow_token,workflow_url,workflow_name,graph,graph_hash,status,root_flow_id,surgraph_flow_id,updated_at,archive_attempt_at)
        SELECT g,'tok','url','name','{}','hash','completed',1,CASE WHEN g=1 THEN 0 ELSE 1 END,
        NOW()-INTERVAL '1 day'+CASE WHEN g=1 THEN INTERVAL '1 second' ELSE INTERVAL '0 seconds' END,
        CASE WHEN g=1 THEN NOW()-INTERVAL '2 hours' ELSE TIMESTAMP '2000-01-01' END FROM generate_series(1,250000) g`)
	assert.Must(assert.NoError(err))
	_, err = r.db.ExecContext(ctx, "ANALYZE dwarf_flows")
	assert.Must(assert.NoError(err))
	for _, s := range []share{{1, 1}, {1, 2}, {2, 2}} {
		where, args := s.sql(0, 0, false)
		stmt, args := closedScanSQL(r.db.DriverName(), time.Minute, where, args, 1)
		plan := readFlowPlan(t, r.db, stmt, args...)
		reads := 0.0
		plan.walk(func(p flowPlan) {
			assert.False(strings.Contains(p.Kind, "Sort") || strings.Contains(p.Kind, "Bitmap") || strings.Contains(p.Kind, "Gather") || p.Kind == "Seq Scan", "%+v", plan)
			if strings.Contains(p.Kind, "Index") {
				assert.Equal("idx_dwarf_flows_closed_roots", p.Index)
				reads += p.Rows + p.Filtered
			}
		})
		assert.True(reads <= 1, "only the root is examined, including a share with no root: %+v", plan)
	}
}
