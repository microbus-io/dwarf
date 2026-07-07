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
	"encoding/json"

	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/workflow"
)

// graphCacheKey scopes the per-flow graph cache by shard, since flow_id is only unique within a shard.
type graphCacheKey struct {
	shard  int
	flowID int
}

// cachedGraph is the per-flow parsed graph paired with its derived fan-in map. The fan-in map is a pure
// function of the graph, not persisted with it, so it is computed once when a flow's graph is first parsed
// and cached alongside - amortizing the O(V+E) analysis across every step of the flow.
type cachedGraph struct {
	graph *workflow.Graph
	fanIn *faninmap.Map
}

// unmarshalJSONMap parses a JSON string into a map. Empty or "{}" input yields a nil map.
func unmarshalJSONMap(jsonStr string, out *map[string]any) {
	if jsonStr == "" || jsonStr == "{}" {
		return
	}
	json.Unmarshal([]byte(jsonStr), out)
}
