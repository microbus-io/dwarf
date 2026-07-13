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
// and interrupts. It owns no transport of its own; a host wires it to the outside world through injected
// dependency interfaces and calls its operations.
//
// # Lifecycle
//
// Build an engine with NewEngine and the Set* methods, register a Host (see SetHost), then Startup (opens
// the database, runs migrations, starts workers). Shutdown drains the workers and closes the database. In
// tests, RunInTest replaces Startup/Shutdown with per-test SQLite databases and t.Cleanup.
//
//	eng := engine.NewEngine()
//	eng.SetShard(ShardSpec{Index: 1, DSN: "postgres://user:pass@host:5432/dwarf"})
//	eng.SetHost(host)
//	err := eng.Startup(ctx)
//	if err != nil { ... }
//	defer eng.Shutdown(ctx)
//
// Each Set* method returns an error. The live ones (SetMaxOpenConns - an expert override,
// SetTimeBudget, SetDefaultPriority) take effect immediately on a running engine; the
// construction-time-only ones (SetShard, SetWorkers, SetHost, SetLogger, SetMeterProvider,
// SetTracerProvider) return an error if called after Startup. Tuning derives from the facts the host
// declares plus what the engine observes: ShardSpec.VirtualCPUs drives each shard's connection budget,
// its placement weight, and - in aggregate - the default worker count; the budget is automatically
// split across the engine replicas sharing the databases, discovered live via peer signals (hello/
// ping/goodbye over SignalPeers - no replica count is ever declared). The SetWorkers/SetMaxOpenConns
// overrides exist for tests, benchmarks, and externally-constrained hosts - and SetWorkers is the
// right tool when tasks run long (the useful worker count grows with task duration, and blocked
// workers are cheap).
//
// # Host
//
// The engine reaches the outside world through a single injected Host interface (see SetHost):
//
//   - LoadGraph fetches a workflow graph by name (called at Create; the graph JSON is then frozen on the
//     flow), and on subgraph spawn.
//   - ExecuteTask executes one task, given the Flow carrier with its state pre-populated.
//   - SignalPeers ships a cross-replica coordination signal (op + opaque payload bytes) to the other
//     replicas, which hand it back via Engine.DeliverSignal; a single-replica host does nothing.
//
// The flow's opaque baggage (host identity/tenant/context, set in workflow.FlowOptions) rides on the
// dispatch context of every LoadGraph and ExecuteTask call; read it with workflow.BaggageFrom(ctx).
//
// # Operations
//
// Create makes a flow and runs it; Await blocks until it stops; Run is Create+Await in one
// call. Snapshot/History/Step/List inspect; Resume continues a paused flow; Cancel/Continue
// manage lifecycle; Fork clones a terminal flow from a chosen step into a new flow for
// non-destructive recovery; Delete/Purge retain. See the repository's docs/ directory for guides.
//
// # Security model
//
// Flow and step keys ("{shard}-{id}-{token}") are unguessable bearer capabilities, not authorization.
// Holding a flow key is by itself sufficient to act on that one flow — Resume, Cancel, Fork, Continue,
// Delete, and every introspection call — with no further check: the sole gate is the key (the numeric id
// plus its random flow_token). The engine performs no authentication, authorization, or rate limiting and
// has no notion of caller identity; its only vantage is the flow reference and the task URL, so ownership
// and tenancy are invisible to it. Authorizing an operation is therefore the host's responsibility:
// before calling the engine, verify the authenticated principal may act on the flow — typically from the
// baggage the host set at Create (see workflow.FlowOptions.Baggage), or the host's own record mapping a
// principal to the keys it was issued.
//
// The token defends only against reference forgery and id enumeration: flow ids are sequential, so without
// the token a caller cannot fabricate a key for a flow it was never handed. That is defense in depth, not
// access control — a leaked, logged, or shared key grants its bearer full write access to that one flow,
// so treat a key like a password. The engine does not emit keys to traces or logs (telemetry carries only
// a token-free "{shard}-{id}" correlation id), and there is deliberately no operation that resolves a
// correlation id back to a key, which would be a capability-minting oracle.
//
// List and Search return keys, tokens included. Exposing them to a principal is equivalent to granting the
// write capability for every flow they return, so a host must gate them by ownership and never surface
// them to less-than-fully-trusted callers. Key exposure is also transitive across an execution tree: Step
// and History navigation resolve and return the keys of neighboring steps, crossing flow boundaries into
// parent and subgraph-child flows, so a single step key reaches the whole tree (each neighbor key both
// discloses that step's state and can seed a Fork). Authorizing introspection by one flow's ownership is
// therefore insufficient when the caller holds any step key in that tree; treat the tree (its root) as the
// authorization unit, or restrict these surfaces to fully-trusted operators. The inbound peer entry point DeliverSignal is unauthenticated
// by the engine; authenticating replica-to-replica transport is the host's responsibility. Operations on
// an unknown or mismatched key return a uniform not-found (no existence oracle), but that is a hardening
// detail, not a substitute for host authorization.
//
// # Resource limits
//
// The engine imposes no size or count limits — not on initial state, baggage, the frozen graph JSON,
// interrupt/resume payloads, forEach fan-out width (one step row per array element), or subgraph nesting
// depth. This is deliberate, the same division of labor as backpressure and time budgets: state size is
// workload-defined (a document-processing workflow may legitimately carry tens of megabytes per flow), and no
// single cap fits both that and a small control flow. Bounding resource use is therefore the host's job — it
// holds the caller identity and tenancy the engine cannot see. A host enforces quotas where it has that
// context: reject an over-large initial state or Baggage before Create, cap forEach input arrays in author
// space, and bound its own retention (Purge deletes at most 4096 roots per call, so a retention job loops).
// For a pass-through host that adds no policy of its own, this obligation flows through to the application
// using that host. (Deep subgraph nesting is bounded storage, not a crash vector: Fork clones the tree
// iteratively, so nesting depth costs no goroutine stack.)
package engine
