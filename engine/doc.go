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

// Package engine is the dwarf workflow-orchestration engine.
//
// The engine executes workflow graphs against a SQL database, scheduling and dispatching one task at a
// time per step, persisting state between steps, and driving fan-out/fan-in, retries, sleeps, subgraphs,
// interrupts, backpressure, and circuit breakers. It owns no transport of its own; a host wires it to
// the outside world through injected dependency interfaces and calls its operations.
//
// # Lifecycle
//
// Build an engine with NewEngine and the Set* methods, register a Host (see SetHost), then Startup (opens
// the database, runs migrations, starts workers). Shutdown drains the workers and closes the database. In
// tests, RunInTest replaces Startup/Shutdown with per-test SQLite databases and t.Cleanup.
//
//	eng := engine.NewEngine()
//	eng.SetDSN("postgres://user:pass@host:5432/dwarf")
//	eng.SetHost(host)
//	if err := eng.Startup(ctx); err != nil { ... }
//	defer eng.Shutdown(ctx)
//
// Each Set* method returns an error. The live ones (SetNumShards, SetMaxOpenConns, SetTimeBudget,
// SetDefaultPriority) take effect immediately on a running engine; the construction-time-only ones (SetDSN,
// SetWorkers, SetHost, SetLogger, SetMeterProvider, SetTracerProvider) return an error if called after
// Startup.
//
// # Host
//
// The engine reaches the outside world through a single injected Host interface (see SetHost):
//
//   - LoadGraph fetches a workflow graph by name (called at Create; the graph JSON is then frozen on the
//     flow), and on subgraph spawn.
//   - ExecuteTask executes one task, given the Flow carrier with its state pre-populated.
//   - FlowStopped is fired when a flow stops, for flows created with FlowOptions.NotifyOnStop; the flow's
//     baggage is on the ctx so the host resolves delivery itself. A host with no notification need does nothing.
//   - SignalPeers ships a cross-replica coordination signal (op + opaque payload bytes) to the other
//     replicas, which hand it back via Engine.DeliverSignal; a single-replica host does nothing.
//
// The flow's opaque baggage (host identity/tenant/context, set in workflow.FlowOptions) rides on the
// dispatch context of every LoadGraph and ExecuteTask call; read it with workflow.BaggageFrom(ctx).
//
// # Operations
//
// Create or CreateTask makes a flow; Start runs it; Await blocks until it stops; Run is
// Create+Start+Await in one call. Snapshot/History/Step/List inspect; Resume/ResumeBreak continue a
// paused flow; Cancel/Restart/RestartFrom/Continue manage lifecycle; Delete/Purge retain. See the
// repository's docs/ directory for guides.
package engine
