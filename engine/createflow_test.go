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
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestInsertFlowTx_ConcurrentCreates pins flow creation, whose writes after an INSERT name the new row in SQL on
// PostgreSQL rather than by a bind. Flows are created concurrently, so inserts from different sessions interleave:
// an id that is not the session's own would point a flow at another flow's rows. Every new top-level flow must be
// its own thread and root, own exactly one entry step, and record no stopped step while it has not stopped.
func TestInsertFlowTx_ConcurrentCreates(t *testing.T) {
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.NoError(e.SetWorkers(0)) // the entry steps stay as created
	assert.NoError(e.Startup(t.Context()))

	g := workflow.NewGraph("Entry")
	g.SetURL("Entry", "create/entry")
	g.AddTransition("Entry", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("create/wf", g)

	const flows = 16
	var wg sync.WaitGroup
	for range flows {
		wg.Go(func() {
			_, err := e.Create(ctx, "create/wf", nil, nil)
			assert.NoError(err)
		})
	}
	wg.Wait()

	db, err := e.db.Shard(1)
	assert.NoError(err)
	var created, strays int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows").Scan(&created))
	assert.Equal(flows, created)
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_flows f"+
			" WHERE f.thread_id<>f.flow_id OR f.root_flow_id<>f.flow_id OR f.step_id<>0"+
			" OR (SELECT COUNT(*) FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.task_name='Entry')<>1").Scan(&strays))
	assert.Zero(strays, "every flow is its own thread and root, owns its own entry step, and has not stopped")
}
