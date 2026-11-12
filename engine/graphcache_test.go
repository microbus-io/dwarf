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
	"encoding/json"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/graphcache"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestCreate_SeedsGraphCache pins Create's graph-cache seed: a dispatching replica holds the new flow's graph
// before its entry step is claimed - parsed from the stored JSON, never the host's own object - under the flow's
// token as well as its id, and a create whose transaction rolls back leaves nothing cached.
func TestCreate_SeedsGraphCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	registerGate(mux, "seedcache")
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, err := e.Create(ctx, "seedcache/g", nil, nil)
	assert.NoError(err)
	shard, _, flowID, flowToken, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	cg, ok := e.graphs.Flow(graphcache.Key{Shard: shard, FlowID: flowID, Token: flowToken})
	if assert.True(ok, "the creating replica caches the graph it just stored") {
		assert.Equal("seedcache/g", cg.WorkflowURL)
		hostGraph, err := mux.LoadGraph(ctx, "seedcache/g")
		assert.NoError(err)
		assert.True(cg.Graph != hostGraph, "the cached graph is a parse of the stored JSON, not the host's object")
		assert.Equal(hostGraph.EntryPoint(), cg.Graph.EntryPoint())
		assert.NotNil(cg.FanIn)
	}
	// A restore or a lossy failover hands the id to a new flow, with a new token: it does not get this graph.
	_, ok = e.graphs.Flow(graphcache.Key{Shard: shard, FlowID: flowID, Token: "0000000000000000"})
	assert.False(ok, "the same flow id under another token is a different flow")

	// A create that rolls back caches nothing under the id it would have taken.
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var maxID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows").Scan(&maxID))
	graph, err := mux.LoadGraph(ctx, "seedcache/g")
	assert.NoError(err)
	state, _ := workflow.NewState()
	cachedBefore := e.graphs.Flows()
	tctx, doneTurn := e.dbTurn(ctx, shard)
	_, err = e.createWithGraph(tctx, shard, "seedcache/g", graph, state, "", e.resolveFlowOptions(nil), 0, 0, 0, 0,
		func(tx *sequel.Tx) error { return errors.New("rolled back") })
	doneTurn()
	assert.Error(err)
	assert.Equal(cachedBefore, e.graphs.Flows(), "a rolled-back create caches nothing")
}

// TestGraphCache_FlowsShareOneParse pins the shared layer: the flows of one graph point at one parsed entry,
// whether seeded by Create or read back from the column under the flow's graph_hash, while a different graph or
// a different workflow URL never shares one.
func TestGraphCache_FlowsShareOneParse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	registerGate(mux, "sharecache")
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	var entries []*graphcache.Entry
	for range 2 {
		fk, err := e.Create(ctx, "sharecache/g", nil, nil)
		assert.NoError(err)
		shard, _, flowID, flowToken, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		cg, ok := e.graphs.Flow(graphcache.Key{Shard: shard, FlowID: flowID, Token: flowToken})
		assert.True(ok)
		entries = append(entries, cg)

		// The column read path (a cold flow, or completion's flowGraph) interns under the row's graph_hash, so it
		// lands on Create's entry even where the dialect re-spells the stored bytes.
		sh, err := e.db.Shard(shard)
		assert.Must(assert.NoError(err))
		db := sh.DB()
		var graphJSON []byte
		var graphHash string
		assert.NoError(db.QueryRowContext(ctx, "SELECT graph, graph_hash FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&graphJSON, &graphHash))
		assert.Equal(graphcache.HashOf(mustGraphJSON(t, cg.Graph)), graphHash, "graph_hash is the hash of the bytes Create stored")
		read, err := e.internGraph(graphJSON, "sharecache/g", graphHash)
		assert.NoError(err)
		assert.True(read == cg, "a column read finds Create's entry by its hash")
	}
	assert.True(entries[0] == entries[1], "two flows of one graph share one parsed entry")

	other := []byte(`{"name":"other"}`)
	cg, err := e.internGraph(other, "sharecache/g", graphcache.HashOf(other))
	if assert.NoError(err) {
		assert.True(cg != entries[0], "a different graph is a different entry")
	}
	sameJSON := mustGraphJSON(t, entries[0].Graph)
	sameBytesOtherURL, err := e.internGraph(sameJSON, "sharecache/other", graphcache.HashOf(sameJSON))
	if assert.NoError(err) {
		assert.Equal("sharecache/other", sameBytesOtherURL.WorkflowURL, "the workflow URL is part of the key")
	}
}

func mustGraphJSON(t *testing.T, g *workflow.Graph) []byte {
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestGraphCache_DispatchMissInterns pins the dispatch-miss path: flows created by a replica that does not
// dispatch (so nothing seeds a cache) are read back from the column by the replica that runs them, and the
// flows of one graph still share one parse there.
func TestGraphCache_DispatchMissInterns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	registerGate(mux, "misscache")
	creator := NewEngineUnderTest(t.Name())
	defer creator.Shutdown(ctx)
	creator.SetHost(mux)
	assert.NoError(creator.SetWorkers(0))
	assert.Must(assert.NoError(creator.Startup(t.Context())))
	runner := NewEngineUnderTest(t.Name()) // same name, same database
	defer runner.Shutdown(ctx)
	runner.SetHost(mux)
	assert.Must(assert.NoError(runner.Startup(t.Context())))

	var entries []*graphcache.Entry
	for range 2 {
		fk, err := creator.Create(ctx, "misscache/g", nil, nil)
		assert.NoError(err)
		out, err := creator.Await(ctx, fk)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusInterrupted, out.Status, "the runner dispatched the gate")
		}
		shard, _, flowID, flowToken, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		_, seeded := creator.graphs.Flow(graphcache.Key{Shard: shard, FlowID: flowID, Token: flowToken})
		assert.False(seeded, "a replica that does not dispatch the shard seeds nothing")
		cg, ok := runner.graphs.Flow(graphcache.Key{Shard: shard, FlowID: flowID, Token: flowToken})
		if assert.True(ok, "the dispatch miss caches the flow") {
			entries = append(entries, cg)
		}
	}
	if len(entries) == 2 {
		assert.True(entries[0] == entries[1], "two flows of one graph share one parse after a miss")
	}
}

// TestGraphCache_HashHitReadsNoGraph pins what graph_hash is for: a replica that did not create a flow finds
// the flow's parse by the hash its flow row carries, so of several flows of one graph created elsewhere -
// the shape of an HA deployment whose frontends do not dispatch - only the first reads the graph column.
func TestGraphCache_HashHitReadsNoGraph(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	registerGate(mux, "hashcache")
	creator := NewEngineUnderTest(t.Name())
	defer creator.Shutdown(ctx)
	creator.SetHost(mux)
	assert.NoError(creator.SetWorkers(0))
	assert.Must(assert.NoError(creator.Startup(t.Context())))
	runner := NewEngineUnderTest(t.Name()) // same name, same database
	defer runner.Shutdown(ctx)
	runner.SetHost(mux)
	assert.Must(assert.NoError(runner.Startup(t.Context())))
	reads := runner.diags.Seams().Visits(diagnostics.CheckpointGraphRead)

	// One at a time, so no two first dispatches race to parse the graph.
	for range 3 {
		fk, err := creator.Create(ctx, "hashcache/g", nil, nil)
		assert.NoError(err)
		out, err := creator.Await(ctx, fk)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusInterrupted, out.Status, "the runner dispatched the gate")
		}
	}
	assert.Equal(1, runner.diags.Seams().Visits(diagnostics.CheckpointGraphRead)-reads,
		"only the first flow of the graph reads the graph column")
}
