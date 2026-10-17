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
	"fmt"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestBacklogSQL_UsesTheDueIndex pins that the backlog aggregate reaches idx_dwarf_steps_due, whose partial
// predicate carries a conjunct (step_id>0) besides the status: a query that does not state it cannot use the
// index. With the selection index dropped and sequential scans priced out, the aggregate must read the due
// index, on every dialect whose due index is partial. (It seeks the status and parked prefix; GROUP BY priority
// leaves priority open, so not_before is not part of the seek.)
func TestBacklogSQL_UsesTheDueIndex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	assert.NoError(e.SetWorkers(0))
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	var explain, priceOutScans string
	switch db.DriverName() {
	case "pgx":
		explain, priceOutScans = "EXPLAIN ", "SET LOCAL enable_seqscan = off"
	case "sqlite":
		explain = "EXPLAIN QUERY PLAN "
	default:
		t.Skip("the plan shapes pinned here are Postgres's and SQLite's")
	}
	errRollback := errors.New("rollback")
	var plan strings.Builder
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx, "DROP INDEX idx_dwarf_steps_selection"); err != nil {
			return err
		}
		if priceOutScans != "" {
			if _, err := tx.ExecContext(ctx, priceOutScans); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, explain+backlogSQL, parkedNone)
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
	assert.True(strings.Contains(plan.String(), "idx_dwarf_steps_due"), "the backlog aggregate must read the due index:\n%s", plan.String())
}
