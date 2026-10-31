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

// Package graphcache holds parsed workflow graphs in two layers, so a dispatch need not read or parse a flow's
// stored graph JSON.
//
// The shared layer owns one Entry per distinct graph, keyed by the flow's workflow URL and graph hash. The
// per-flow layer maps a flow to its graph's Entry, so a flow this replica has seen is found by its key alone. A
// typical dispatch:
//
//	entry, perFlow := c.Flow(key)
//	if !perFlow {
//		entry, ok = c.Shared(workflowURL, graphHash)
//		if !ok {
//			// read the graph column
//			entry, err = c.Intern(graphJSON, workflowURL, graphHash)
//		}
//		c.Point(key, entry)
//	}
//
// An Entry is shared by every flow of its graph and by concurrent dispatches: it must never be mutated. Every
// method is safe for concurrent use.
package graphcache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"
	"weak"

	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/internal/lru"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// flowTTL is how long a per-flow entry lives after it was stored.
const flowTTL = 15 * time.Minute

// Key names a flow in the per-flow layer: its shard, its id within the shard, and its token.
type Key struct {
	Shard  int
	FlowID int
	Token  string
}

// Entry is a parsed graph with its fan-in map and the workflow URL it was stored under. Every field is frozen.
type Entry struct {
	Graph       *workflow.Graph
	FanIn       *faninmap.Map
	WorkflowURL string
}

// sharedKey names a parsed graph in the shared layer.
type sharedKey struct {
	workflowURL string
	graphHash   string
}

// Cache is the two-layer parsed-graph cache.
type Cache struct {
	flows  *lru.Cache[Key, weak.Pointer[Entry]]
	graphs *lru.Cache[sharedKey, *Entry]
}

// New returns a cache whose per-flow layer holds up to flows entries and whose shared layer holds up to graphs
// parses. Memory is bounded by graphs: a per-flow entry is a weak pointer.
func New(flows, graphs int) *Cache {
	return &Cache{
		flows:  lru.New[Key, weak.Pointer[Entry]](flows, flowTTL),
		graphs: lru.New[sharedKey, *Entry](graphs, 0),
	}
}

// Flow returns the flow's entry when this cache holds it. A parse the shared layer has evicted reads as a miss
// once nothing else holds it.
func (c *Cache) Flow(k Key) (*Entry, bool) {
	wp, ok := c.flows.Load(k)
	if !ok {
		return nil, false
	}
	e := wp.Value()
	return e, e != nil
}

// Point maps the flow to an entry returned by Shared or Intern.
func (c *Cache) Point(k Key, e *Entry) {
	c.flows.Store(k, weak.Make(e))
}

// Shared returns the entry for a graph hash, if this cache has parsed that graph. An empty hash is a miss.
func (c *Cache) Shared(workflowURL, graphHash string) (*Entry, bool) {
	if graphHash == "" {
		return nil, false
	}
	return c.graphs.Load(sharedKey{workflowURL: workflowURL, graphHash: graphHash})
}

// Intern returns the shared entry for a flow's stored graph JSON, parsing it only the first time the graph is
// seen. An empty graphHash is computed from the bytes. The JSON must be the stored bytes, never a caller's live
// graph. A graph that cannot be parsed is an error, and is not cached.
func (c *Cache) Intern(graphJSON []byte, workflowURL, graphHash string) (*Entry, error) {
	if graphHash == "" {
		graphHash = HashOf(graphJSON)
	}
	key := sharedKey{workflowURL: workflowURL, graphHash: graphHash}
	if e, ok := c.graphs.Load(key); ok {
		return e, nil
	}
	parsed := &workflow.Graph{}
	if err := json.Unmarshal(graphJSON, parsed); err != nil {
		return nil, errors.New("stored graph of %s: %v", workflowURL, err)
	}
	e := &Entry{Graph: parsed, FanIn: faninmap.New(parsed), WorkflowURL: workflowURL}
	c.graphs.Store(key, e)
	return e, nil
}

// Flows returns how many flows the per-flow layer holds.
func (c *Cache) Flows() int {
	return c.flows.Len()
}

// HashOf is the graph hash of graph JSON about to be stored: hex SHA-256 of the bytes.
func HashOf(graphJSON []byte) string {
	sum := sha256.Sum256(graphJSON)
	return hex.EncodeToString(sum[:])
}
