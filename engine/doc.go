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
// and interrupts. It owns no transport of its own; a host wires it to the outside world through a single
// injected Host interface, plus optional logging, metrics and tracing providers, and calls its operations.
//
// # Lifecycle
//
// Build an engine with NewEngine and the Set* methods, register a Host (see SetHost), then Startup (opens
// the database, runs migrations, starts workers). Shutdown drains the workers and closes the database. In
// tests, NewEngineUnderTest wires isolated, auto-dropped SQLite databases keyed by a test name, so a test
// just configures the engine, calls Startup, and defers Shutdown (see NewEngineUnderTest).
//
//	eng := engine.NewEngine()
//	eng.SetShard(engine.ShardSpec{Index: 1, DSN: "postgres://user:pass@host:5432/dwarf"})
//	eng.SetHost(host)
//	err := eng.Startup(ctx)
//	if err != nil { ... }
//	defer eng.Shutdown(ctx)
//
// Each Set* method returns an error. The live one (SetTimeBudget) takes effect
// immediately on a running engine; the construction-time-only ones (SetShard, SetWorkers, SetHost,
// SetEngineID, SetStateCacheSize, SetMaxPriority, SetRetention, SetFlowExpiration, SetLogger, SetDebugLogger,
// SetMeterProvider, SetTracerProvider) return an error if called after Startup.
//
// Tuning derives from what the engine observes, plus two optional per-shard levers. Each shard's connection
// budget follows the round-trip time measured at Startup and the server's own connection limit, unless
// ShardSpec.MaxConnections sets it; ShardSpec.Weight sets the shard's share of new flows. The budget is split
// across the few engine replicas that dispatch each database, which the replicas agree on through a shared
// peer registry they heartbeat into, while every other replica holds two connections there - no replica
// count is ever declared, and nothing is sent between replicas to establish it. The worker maximum is derived
// from the crash-recovery lease margin and the measured round-trip time, so it holds for any task duration;
// the worker pool grows into it only on demand, so short-task deployments never pay for the headroom. The
// SetWorkers override exists for tests, benchmarks, and memory-bounded hosts.
//
// # Host
//
// The engine reaches the outside world through a single injected Host interface (see SetHost):
//
//   - LoadGraph fetches a workflow graph by its workflow URL - the opaque key passed to Create or Run, or to
//     a task's Subgraph call. It is called at Create and at each subgraph spawn; the graph is then frozen
//     with the flow.
//   - ExecuteTask executes one task, given the Flow carrier with its state pre-populated.
//
// That is the whole interface; replicas sharing a database coordinate by reading it.
//
// The flow's opaque baggage (host identity/tenant/context, set in workflow.FlowOptions) rides on the
// dispatch context of every LoadGraph and ExecuteTask call; read it with workflow.BaggageFrom(ctx).
//
// # Operations
//
// Create makes a flow and runs it; Await blocks until it stops, and Poll answers within the caller's
// budget; Run is Create+Await in one call. Snapshot/Summary/History/Step/ListOpen/ListRecentlyClosed/ListClosed/Fingerprint inspect; Resume continues
// a paused flow; Cancel requests a graceful stop that tasks can catch, and Terminate a forceful one;
// Continue starts a new flow from a completed one; Fork clones a closed flow from a chosen step into a new flow
// for non-destructive recovery; Delete/Purge retain; ShardInfo summarizes each shard. See
// https://docs.dwarf.run for guides.
//
// # Security model
//
// Flow keys ("{shard}-{hour}-{flowID}-{token}") and step keys ("{shard}-{hour}-{flowID}-{stepID}-{token}") are
// unguessable bearer capabilities, not authorization.
// Holding a flow key is by itself sufficient to act on that one flow — Resume, Terminate, Fork, Continue,
// Delete, and every introspection call — with no further check: the sole gate is the key (its numeric id
// and creation hour, plus its random token). The engine performs no authentication, authorization, or rate
// limiting and has no notion of caller identity; its only vantage is the flow reference and the task URL, so
// ownership and tenancy are invisible to it. Authorizing an operation is therefore the host's responsibility:
// before calling the engine, verify the authenticated principal may act on the flow — typically from the
// baggage the host set at Create (see workflow.FlowOptions.Baggage), or the host's own record mapping a
// principal to the keys it was issued.
//
// The token defends only against reference forgery and id enumeration: flow ids are sequential, so without
// the token a caller cannot fabricate a key for a flow it was never handed. That is defense in depth, not
// access control — a leaked, logged, or shared key grants its bearer full write access to that one flow,
// so treat a key like a password. The engine does not emit keys to traces or logs: telemetry carries only
// a token-free "{shard}-{hour}-{id}" correlation id, the flow key without its token. No operation accepts a
// correlation id. A caller with access to the lists finds its flow by searching for it followed by "-" with
// Query.Shard set, which returns nothing those lists do not already return; a log reader without that access
// gains no key.
//
// ListOpen, ListRecentlyClosed and ListClosed return keys, tokens included — the only key-returning operations. Exposing them to a
// principal is equivalent to granting the
// write capability for every flow they return, so a host must gate them by ownership and never surface
// them to less-than-fully-trusted callers. Key exposure is also transitive across an execution tree: Step
// and History navigation resolve and return the keys of neighboring steps, crossing flow boundaries into
// parent and subgraph-child flows, so a single step key reaches the whole tree (each neighbor key both
// discloses that step's state and can seed a Fork). Authorizing introspection by one flow's ownership is
// therefore insufficient when the caller holds any step key in that tree; treat the tree (its root) as the
// authorization unit, or restrict these surfaces to fully-trusted operators. Operations on an unknown or
// mismatched key return a uniform not-found (no existence oracle), but that is a hardening detail, not a
// substitute for host authorization.
//
// # Resource limits
//
// The one limit is what the database can store: no single JSON value the engine writes may exceed 256 MB
// (256,000,000 bytes). That bounds a task's changes in one step, initial state, baggage, resume data, an
// interrupt payload, and a flow's whole state when it ends (its final state holds every field at once). Mid-flow,
// a large field is stored once and carried by reference, so a running flow's state may exceed the limit;
// https://docs.dwarf.run/operating/data-handling/ says which fields still count toward it.
// Create, Continue, Resume and Fork return a 413 for an oversized value; a step that produces one fails at once,
// and its task is not run again.
//
// Below it, the engine imposes no size or count limits — not on initial state, baggage, the frozen graph JSON,
// interrupt/resume payloads, forEach fan-out width (one step row per array element), or subgraph nesting
// depth. This is deliberate, the same division of labor as backpressure and time budgets: state size is
// workload-defined (a document-processing workflow may legitimately carry tens of megabytes per flow), and no
// single cap fits both that and a small control flow. Bounding resource use is therefore the host's job — it
// holds the caller identity and tenancy the engine cannot see. A host enforces quotas where it has that
// context: reject an over-large initial state or Baggage before Create, cap forEach input arrays in author
// space, and set its retention (SetRetention). For a pass-through host that adds no policy of its own, this
// obligation flows through to the application using that host. Deep subgraph nesting costs storage only.
package engine
