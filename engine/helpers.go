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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"weak"

	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// emptyJSON is the empty JSON object, as a BIND value rather than an inline SQL literal.
//
// Every reset of a payload column (`state`/`changes`/`state_refs`/`interrupt_payload`/`resume_data`/
// `subgraph_result`) must bind this instead of writing `SET col='{}'`. Those columns are `VARBINARY(MAX)` on
// SQL Server (see the migrations), and a T-SQL `'{}'` literal is a *varchar*, so an inline assignment fails
// outright: `Implicit conversion from data type varchar to varbinary(max) is not allowed`. Binding a []byte
// sends the matching type and needs no per-driver branch at all - MySQL `JSON`, Postgres `JSONB`, SQLite
// `TEXT`, and SQL Server `VARBINARY` all accept it. (A *comparison* against a payload column is the one place
// that still needs a per-driver form - see the `interrupt_payload` empty-guard in execution.go.)
var emptyJSON = []byte("{}")

// graphCacheKey scopes the per-flow graph cache by shard, since flow_id is only unique within a shard.
type graphCacheKey struct {
	shard  int
	flowID int
}

// cachedGraph is a parsed graph paired with its derived fan-in map, shared by every flow whose stored graph
// bytes it was parsed from. The fan-in map is a pure function of the graph, not persisted with it, so it is
// computed once per parse and cached alongside - amortizing the O(V+E) analysis across every step.
//
// Every field must be frozen for the flow's life, and nothing may mutate one after the parse: concurrent
// dispatches of many flows read the same entry. The per-flow key relies on a flow_id not being reused while
// its entry lives.
type cachedGraph struct {
	graph       *workflow.Graph
	fanIn       *faninmap.Map
	workflowURL string
}

// cachedFlowGraph returns the flow's entry when this replica holds it. The per-flow layer holds weak
// pointers, so a parse the shared layer has evicted reads as a miss once no dispatch still holds it.
func (e *Engine) cachedFlowGraph(shardNum, flowID int) (*cachedGraph, bool) {
	wp, ok := e.graphCache.Load(graphCacheKey{shard: shardNum, flowID: flowID})
	if !ok {
		return nil, false
	}
	cg := wp.Value()
	return cg, cg != nil
}

// cacheFlowGraph points the flow at a shared entry from internGraph.
func (e *Engine) cacheFlowGraph(shardNum, flowID int, cg *cachedGraph) {
	e.graphCache.Store(graphCacheKey{shard: shardNum, flowID: flowID}, weak.Make(cg))
}

// sharedGraphKey names a parsed graph by the flow's graph_hash, so the flows of one graph share one entry and
// a dispatch finds it without reading the graph column. The workflow URL is in the key because the entry
// carries it. The hash is SHA-256 of the bytes Create stored, never a fast hash: a collision would run a flow
// on another flow's graph.
type sharedGraphKey struct {
	workflowURL string
	graphHash   string
}

// graphHashOf is the graph_hash of graph JSON about to be stored.
func graphHashOf(graphJSON []byte) string {
	sum := sha256.Sum256(graphJSON)
	return hex.EncodeToString(sum[:])
}

// sharedGraph returns the shared entry for a flow's graph_hash, if this replica has parsed that graph.
func (e *Engine) sharedGraph(workflowURL, graphHash string) (*cachedGraph, bool) {
	if graphHash == "" {
		return nil, false
	}
	return e.sharedGraphs.Load(sharedGraphKey{workflowURL: workflowURL, graphHash: graphHash})
}

// internGraph returns the shared cache entry for a flow's stored graph JSON, parsing it only the first time
// the graph is seen. Both the dispatch miss and Create's seed go through it, so a seeded entry is exactly
// what a miss would have built - in particular, a parse of the stored bytes and never a caller's live
// object, which could change under the concurrent dispatches that share the entry. An empty graphHash (a row
// written without one) is taken from the bytes.
func (e *Engine) internGraph(graphJSON []byte, workflowURL, graphHash string) (*cachedGraph, error) {
	if graphHash == "" {
		graphHash = graphHashOf(graphJSON)
	}
	key := sharedGraphKey{workflowURL: workflowURL, graphHash: graphHash}
	if cg, ok := e.sharedGraphs.Load(key); ok {
		return cg, nil
	}
	cg, err := parseCachedGraph(graphJSON, workflowURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.sharedGraphs.Store(key, cg)
	return cg, nil
}

// parseCachedGraph parses a flow's stored graph JSON into a cache entry.
func parseCachedGraph(graphJSON []byte, workflowURL string) (*cachedGraph, error) {
	parsed := &workflow.Graph{}
	if err := json.Unmarshal(graphJSON, parsed); err != nil {
		return nil, errors.Trace(err)
	}
	return &cachedGraph{graph: parsed, fanIn: faninmap.New(parsed), workflowURL: workflowURL}, nil
}
