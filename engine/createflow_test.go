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
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestInsertFlowTx_ConcurrentCreates pins flow creation, whose writes after an INSERT name the new row in SQL on
// PostgreSQL rather than by a bind. Flows are created concurrently, so inserts from different sessions interleave:
// an id that is not the session's own would point a flow at another flow's rows. Every new top-level flow must be
// its own root, own exactly one entry step, and record no stopped step while it has not stopped.
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
			" WHERE f.root_flow_id<>f.flow_id OR f.stopped_step_id<>0"+
			" OR (SELECT COUNT(*) FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.task_name='Entry')<>1").Scan(&strays))
	assert.Zero(strays, "every flow is its own root, owns its own entry step, and has not stopped")
}

// TestInsertFlowTx_SelfColumnsInTheInsert pins how every creation path writes a flow's root id: a new tree's
// root_flow_id is the row's own flow_id, and on PostgreSQL the INSERT writes it itself, so no UPDATE of dwarf_flows
// follows - an UPDATE there changes an indexed column and is never HOT. Elsewhere a fix-up UPDATE writes it, and
// only for a new root: a subgraph flow binds its parent's. It covers Create, a subgraph spawn, Continue and Fork
// (the clone of a root and of a subgraph flow), reading the statements off sequel's debug log, and checks every
// row's root id.
func TestInsertFlowTx_SelfColumnsInTheInsert(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "selfcols/call")
	parent.SetURL("Tail", "selfcols/tail")
	parent.AddTransitionChain("Call", "Tail", workflow.End)
	mux.HandleGraph("selfcols/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Leaf", "selfcols/leaf")
	child.AddTransition("Leaf", workflow.End)
	mux.HandleGraph("selfcols/child", child)
	mux.HandleTask("selfcols/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("selfcols/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("selfcols/tail", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("selfcols/leaf", func(ctx context.Context, f *workflow.Flow) error { return nil })

	log := &queryLog{}
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.SetLogger(slog.New(log)))
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)

	rootKey, outcome, err := e.Run(ctx, "selfcols/parent", nil, nil)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
		return
	}
	turnKey, err := e.Continue(ctx, rootKey, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err = e.Await(ctx, turnKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
		return
	}
	steps, err := e.History(ctx, rootKey)
	if !assert.NoError(err) {
		return
	}
	var tailKey string
	for _, s := range steps {
		if s.TaskName == "Tail" {
			tailKey = s.StepKey
		}
	}
	forkKey, err := e.Fork(ctx, tailKey, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err = e.Await(ctx, forkKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
		return
	}

	// Six flows: the root and its child, the Continue turn and its child, the fork and the clone of the root's
	// child. Only off PostgreSQL does an UPDATE write a root id, and only for the three roots. The counts are lower
	// bounds, since a transaction retry logs its rolled-back attempt's statements too.
	inserts, fixups := log.flowWrites()
	assert.True(inserts >= 6, "%d INSERTs into dwarf_flows logged, want at least 6", inserts)
	if db.DriverName() == "pgx" {
		assert.Zero(fixups, "UPDATEs of a flow's root id")
	} else {
		assert.True(fixups >= 3, "%d UPDATEs of a flow's root id logged, want at least 3", fixups)
	}

	_, _, forkID, forkToken, err := keys.ParseFlowKey(forkKey)
	assert.NoError(err)
	rows, err := db.QueryContext(ctx,
		"SELECT f.flow_id, f.flow_token, f.root_flow_id, f.surgraph_flow_id, COALESCE(p.root_flow_id, 0)"+
			" FROM dwarf_flows f LEFT JOIN dwarf_flows p ON p.flow_id=f.surgraph_flow_id ORDER BY f.flow_id")
	if !assert.NoError(err) {
		return
	}
	defer rows.Close()
	var flows int
	for rows.Next() {
		var flowID, rootFlowID, surgraphFlowID, parentRoot int
		var flowToken string
		assert.NoError(rows.Scan(&flowID, &flowToken, &rootFlowID, &surgraphFlowID, &parentRoot))
		flows++
		if surgraphFlowID == 0 {
			// The root, the Continue turn and the fork: each its own root.
			assert.Equal(flowID, rootFlowID, "flow %d is its own root", flowID)
		} else {
			// A subgraph flow: in its parent's tree.
			assert.Equal(parentRoot, rootFlowID, "subgraph flow %d is in its parent's tree", flowID)
			assert.NotZero(rootFlowID)
		}
		if flowID == forkID {
			assert.Equal(forkToken, flowToken, "the fork's row carries the token of the key Fork returned")
		}
	}
	assert.NoError(rows.Err())
	assert.Equal(6, flows)
}

// queryLog is a slog.Handler that keeps the statements sequel logs at debug level.
type queryLog struct {
	mu      sync.Mutex
	queries []string
}

func (l *queryLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *queryLog) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *queryLog) WithGroup(string) slog.Handler            { return l }

func (l *queryLog) Handle(_ context.Context, r slog.Record) error {
	if r.Message != "sequel query" && r.Message != "sequel batched query" {
		return nil
	}
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "query" {
			l.mu.Lock()
			l.queries = append(l.queries, a.Value.String())
			l.mu.Unlock()
		}
		return true
	})
	return nil
}

// flowWrites counts the logged INSERTs into dwarf_flows, and the UPDATEs of dwarf_flows that set a root id,
// whatever their spelling.
func (l *queryLog) flowWrites() (inserts, fixups int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, q := range l.queries {
		switch {
		case strings.HasPrefix(q, "INSERT INTO dwarf_flows "):
			inserts++
		case strings.HasPrefix(q, "UPDATE dwarf_flows "):
			set, _, _ := strings.Cut(strings.ReplaceAll(q, " ", ""), "WHERE")
			if strings.Contains(set, "root_flow_id=") {
				fixups++
			}
		}
	}
	return inserts, fixups
}
