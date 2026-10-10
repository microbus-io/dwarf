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

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestCreate_SeedsGraphCache pins Create's graph-cache seed: a dispatching replica holds the new flow's graph
// before its entry step is claimed - parsed from the stored JSON, never the host's own object - and a create
// whose transaction rolls back leaves nothing cached under the id it would have taken.
func TestCreate_SeedsGraphCache(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := NewHostMux()
	registerGate(mux, "seedcache")
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	fk, err := e.Create(ctx, "seedcache/g", nil, nil)
	assert.NoError(err)
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	cg, ok := e.graphCache.Load(graphCacheKey{shard: shard, flowID: flowID})
	if assert.True(ok, "the creating replica caches the graph it just stored") {
		assert.Equal("seedcache/g", cg.workflowURL)
		hostGraph, err := mux.LoadGraph(ctx, "seedcache/g")
		assert.NoError(err)
		assert.True(cg.graph != hostGraph, "the cached graph is a parse of the stored JSON, not the host's object")
		assert.Equal(hostGraph.EntryPoint(), cg.graph.EntryPoint())
		assert.NotNil(cg.fanIn)
	}

	// A create that rolls back caches nothing under the id it would have taken.
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	var maxID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows").Scan(&maxID))
	graph, err := mux.LoadGraph(ctx, "seedcache/g")
	assert.NoError(err)
	state, _ := workflow.NewState()
	tctx, doneTurn := e.dbTurn(ctx, shard)
	_, err = e.createWithGraph(tctx, shard, "seedcache/g", graph, state, 0, "", "", e.resolveFlowOptions(nil), 0, 0, 0, 0,
		func(tx *sequel.Tx) error { return errors.New("rolled back") })
	doneTurn()
	assert.Error(err)
	_, ok = e.graphCache.Load(graphCacheKey{shard: shard, flowID: maxID + 1})
	assert.False(ok, "a rolled-back create caches nothing")
}

// TestGraphCache_FlowsShareOneParse pins the shared layer: the flows of one graph point at one parsed entry,
// whether seeded by Create or read back from the column, while different bytes or a different workflow URL
// never share one.
func TestGraphCache_FlowsShareOneParse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := NewHostMux()
	registerGate(mux, "sharecache")
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	var entries []*cachedGraph
	for range 2 {
		fk, err := e.Create(ctx, "sharecache/g", nil, nil)
		assert.NoError(err)
		shard, flowID, _, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		cg, ok := e.graphCache.Load(graphCacheKey{shard: shard, flowID: flowID})
		assert.True(ok)
		entries = append(entries, cg)

		// The column read path (a cold flow, or completion's flowGraph) interns the stored bytes the same way.
		db, err := e.db.Shard(shard)
		assert.NoError(err)
		var graphJSON []byte
		assert.NoError(db.QueryRowContext(ctx, "SELECT graph FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&graphJSON))
		read1, err := e.internGraph(graphJSON, "sharecache/g")
		assert.NoError(err)
		read2, err := e.internGraph(graphJSON, "sharecache/g")
		assert.NoError(err)
		assert.True(read1 == read2, "one stored graph is parsed once")
	}
	assert.True(entries[0] == entries[1], "two flows of one graph share one parsed entry")

	cg, err := e.internGraph([]byte(`{"name":"other"}`), "sharecache/g")
	if assert.NoError(err) {
		assert.True(cg != entries[0], "different bytes are a different entry")
	}
	sameBytesOtherURL, err := e.internGraph(mustGraphJSON(t, entries[0].graph), "sharecache/other")
	if assert.NoError(err) {
		assert.Equal("sharecache/other", sameBytesOtherURL.workflowURL, "the workflow URL is part of the key")
	}
}

func mustGraphJSON(t *testing.T, g *workflow.Graph) []byte {
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
