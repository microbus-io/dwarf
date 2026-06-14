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

package engine_test

import (
	"context"
	"fmt"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
)

// exampleHost implements engine.Host. A real host loads graphs from a registry/file/database/RPC and
// dispatches tasks over a local call, RPC, or message bus; here an in-memory registry and a local
// function stand in. LoadGraph and ExecuteTask are required; the remaining Host methods (flow-stop
// notification and the cross-replica signals) are left as no-ops.
type exampleHost struct {
	graphs map[string]*workflow.Graph
}

func (h exampleHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) {
	return h.graphs[name], nil
}
func (h exampleHost) ExecuteTask(ctx context.Context, taskName string, f *workflow.Flow) error {
	f.SetString("greeting", "hello "+f.GetString("name"))
	return nil
}
func (exampleHost) FlowStopped(context.Context, string, *workflow.FlowOutcome) {}
func (exampleHost) SignalPeers(context.Context, string, []byte)                {}

// Wire an engine to a host, then create, start, and await a flow.
func Example() {
	ctx := context.Background()

	graphs := map[string]*workflow.Graph{}
	g := workflow.NewGraph("Greet", "greet")
	g.AddTask("Hello", "Hello")
	g.AddTransition("Hello", workflow.END)
	graphs["greet"] = g

	eng := engine.NewEngine().
		WithDSN("postgres://user:pass@localhost:5432/dwarf").
		WithHost(exampleHost{graphs: graphs})

	if err := eng.Startup(ctx); err != nil {
		panic(err)
	}
	defer eng.Shutdown(ctx)

	// Run is Create + Start + Await in one call.
	out, err := eng.Run(ctx, "greet", map[string]any{"name": "ada"}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println(out.State["greeting"])
}

// Create separates flow creation from starting it, and accepts FlowOptions for scheduling and the opaque
// host baggage carried with the flow.
func ExampleEngine_Create() {
	var eng *engine.Engine // obtained from NewEngine().…Startup(ctx)
	ctx := context.Background()

	flowKey, err := eng.Create(ctx, "greet", map[string]any{"name": "ada"},
		&workflow.FlowOptions{
			Priority:    10,                             // lower runs first
			FairnessKey: "tenant-42",                    // fair scheduling bucket
			StartAt:     time.Now().Add(1 * time.Hour),  // delayed start
			Baggage:     map[string]any{"actor": "ada"}, // read with workflow.BaggageFrom(ctx)
		})
	if err != nil {
		panic(err)
	}

	// The flow sits in "created" until Start; Await blocks until it stops.
	if err := eng.Start(ctx, flowKey); err != nil {
		panic(err)
	}
	out, _ := eng.Await(ctx, flowKey)
	fmt.Println(out.Status)
}
