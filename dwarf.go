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

// Package dwarf is a standalone, embeddable workflow-orchestration engine.
//
// Dwarf executes workflow graphs: it dispatches tasks, manages state between steps, and handles
// fan-out/fan-in, retries, sleeps, conditional routing, subgraphs, human-in-the-loop interrupts,
// adaptive backpressure, and circuit breakers. It is library code with no built-in transport: a host
// application wires it to its own task execution, graph storage, and observability through a small set
// of injected dependency interfaces (see the engine package). It depends only on a SQL database (via
// sequel) and a rate limiter (via throttle).
//
// This root package is a thin convenience: NewEngine returns an *engine.Engine. The real API lives in
// two sub-packages:
//
//   - github.com/microbus-io/dwarf/engine - the engine: Startup/Shutdown, the Create/Run/Await
//     operations, configuration, and the dependency interfaces. Import this only in the process that
//     hosts the engine.
//   - github.com/microbus-io/dwarf/workflow - the pure types: Graph, Flow, FlowOptions, FlowOutcome,
//     reducers, and the error-disposition helpers. Import this in code that defines tasks and graphs;
//     it has no heavy dependencies.
//
// A 30-second taste, using the in-process test harness:
//
//	proxy := engine.NewTestProxy()
//	g := workflow.NewGraph("Greet")
//	g.SetEndpoint("Hello", "http://example/hello") // node "Hello" dispatches to this endpoint URL
//	g.AddTransition("Hello", workflow.END)
//	proxy.HandleGraph("http://example/greet", g)
//	proxy.HandleTask("http://example/hello", func(ctx context.Context, f *workflow.Flow) error {
//		f.SetString("greeting", "hello "+f.GetString("name"))
//		return nil
//	})
//
//	eng := dwarf.NewEngine()
//	eng.SetHost(proxy)
//	eng.RunInTest(t) // SQLite in-memory, auto cleanup
//
//	_, out, _ := eng.Run(ctx, "http://example/greet", map[string]any{"name": "ada"}, nil)
//	fmt.Println(out.State["greeting"]) // hello ada
//
// See the docs/ directory in the repository for guides on graphs, tasks, scheduling, observability, and
// deployment.
package dwarf

import "github.com/microbus-io/dwarf/engine"

// NewEngine creates a new workflow engine with default settings.
func NewEngine() *engine.Engine {
	return engine.NewEngine()
}
