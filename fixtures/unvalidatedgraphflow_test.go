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

/*
Create-time graph validation. The engine now
calls graph.Validate() at Create (and at subgraph spawn), as docs/graphs.md promises. This pins the
three consequences:
  - Validate's side effect populates fanOutToFanIn, which the empty-forEach fan-in path (FanInFor)
    reads - so an UNVALIDATED fan-out graph with an empty forEach fires the fan-in and runs downstream
    tasks, instead of silently completing early (data loss).
  - A host returning (nil, nil) from LoadGraph is a clean 404 at Create, not a nil-deref panic.
  - A structurally invalid graph is rejected at Create with a 4xx.
*/
package fixtures

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// nilGraphHost wraps a TestProxy but returns (nil, nil) from LoadGraph - a host that neither finds nor
// errors on a graph. Create must treat that as a 404, never dereference the nil graph.
type nilGraphHost struct{ *engine.TestProxy }

func (nilGraphHost) LoadGraph(ctx context.Context, url string) (*workflow.Graph, error) {
	return nil, nil
}

func TestUnvalidatedGraphflow(t *testing.T) {
	ctx := context.Background()

	// The engine validates an unvalidated graph at Create, populating fanOutToFanIn, so an empty forEach
	// fires the fan-in and runs the downstream task rather than silently completing the flow early.
	t.Run("engine_validates_so_empty_foreach_fires_fanin", func(t *testing.T) {
		assert := testarossa.For(t)
		proxy := engine.NewTestProxy()

		var workRuns, joinRuns, afterRuns atomic.Int64
		g := workflow.NewGraph("Unvalidated")
		g.SetEndpoint("Spawn", "unvalidated.verify:428/spawn")
		g.SetEndpoint("Work", "unvalidated.verify:428/work")
		g.SetEndpoint("Join", "unvalidated.verify:428/join")
		g.SetEndpoint("After", "unvalidated.verify:428/after")
		g.SetFanIn("Join")
		g.AddTransitionForEach("Spawn", "Work", "items", "item")
		g.AddTransition("Work", "Join")
		g.AddTransition("Join", "After")
		g.AddTransition("After", workflow.END)
		// DELIBERATELY do not call g.Validate() here - the engine must validate at Create and populate
		// fanOutToFanIn itself.
		proxy.HandleGraph("unvalidated.verify:428/g", g)
		proxy.HandleTask("unvalidated.verify:428/spawn", func(ctx context.Context, f *workflow.Flow) error { return nil })
		proxy.HandleTask("unvalidated.verify:428/work", func(ctx context.Context, f *workflow.Flow) error { workRuns.Add(1); return nil })
		proxy.HandleTask("unvalidated.verify:428/join", func(ctx context.Context, f *workflow.Flow) error { joinRuns.Add(1); return nil })
		proxy.HandleTask("unvalidated.verify:428/after", func(ctx context.Context, f *workflow.Flow) error { afterRuns.Add(1); return nil })

		eng := engine.NewEngine()
		eng.SetHost(proxy)
		eng.RunInTest(t)

		_, outcome, err := eng.Run(ctx, "unvalidated.verify:428/g", map[string]any{"items": []int{}}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(int64(0), workRuns.Load())  // empty forEach spawns no Work branch
		assert.Equal(int64(1), joinRuns.Load())  // the fan-in fired despite the empty cohort
		assert.Equal(int64(1), afterRuns.Load()) // and the downstream task ran - no silent early completion
	})

	// A host returning (nil, nil) is a 404 at Create, not a panic in EntryPoint().
	t.Run("nil_graph_is_404_not_panic", func(t *testing.T) {
		assert := testarossa.For(t)
		eng := engine.NewEngine()
		eng.SetHost(nilGraphHost{engine.NewTestProxy()})
		eng.RunInTest(t)

		_, err := eng.Create(ctx, "anything", nil, nil)
		if !assert.Error(err) {
			return
		}
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	})

	// A structurally invalid graph (a node unreachable from the entry point) is rejected at Create.
	t.Run("structurally_invalid_graph_rejected_at_create", func(t *testing.T) {
		assert := testarossa.For(t)
		proxy := engine.NewTestProxy()
		g := workflow.NewGraph("Invalid")
		g.SetEndpoint("Entry", "invalid.verify:428/entry")
		g.SetEndpoint("Orphan", "invalid.verify:428/orphan") // has an endpoint but nothing transitions to it
		g.AddTransition("Entry", workflow.END)
		proxy.HandleGraph("invalid.verify:428/g", g)
		proxy.HandleTask("invalid.verify:428/entry", func(ctx context.Context, f *workflow.Flow) error { return nil })
		proxy.HandleTask("invalid.verify:428/orphan", func(ctx context.Context, f *workflow.Flow) error { return nil })

		eng := engine.NewEngine()
		eng.SetHost(proxy)
		eng.RunInTest(t)

		_, err := eng.Create(ctx, "invalid.verify:428/g", nil, nil)
		if !assert.Error(err) {
			return
		}
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	})
}
