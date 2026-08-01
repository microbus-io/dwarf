# Dwarf documentation

Dwarf is a standalone, embeddable workflow-orchestration engine for Go. These guides cover how to use it
in your own project. For the full API reference, see
[pkg.go.dev/github.com/microbus-io/dwarf](https://pkg.go.dev/github.com/microbus-io/dwarf).

**These docs address two readers, and every page names its own at the top:**

- **Developers** — you are importing dwarf and building workflows with it.
- **Operators** — you are responsible for a deployment that is already running.

A few pages serve both, and say so.

## The one-paragraph summary

You build a `workflow.Graph` of tasks and transitions. You implement a `Host` whose **`LoadGraph`**
returns a graph by name and whose **`ExecuteTask`** runs one task. The engine creates a **flow** (one
execution of a graph), runs each task in turn, persists state to SQL between steps, follows transitions
to decide what runs next, merges parallel branches, and recovers from crashes. You drive it with a handful
of operations — `Create`, `Run`, `Await`, `Resume`, `Cancel`, `Continue`, `Fork` — and observe it through logs,
metrics, and traces.

---

## For developers

If you're new, read in this order:

1. **[Getting started](getting-started.md)** — install, the dependency-injection model, and your first
   running flow with the in-process test harness.
2. **[Concepts](concepts.md)** — the mental model: graph, task, flow, step, thread, reducer, and the flow
   lifecycle. Read this once and the rest will click.
3. **[Building graphs](graphs.md)** — the `workflow.Graph` API: tasks, transitions, conditions, fan-out,
   error handling, and reducers.
4. **[Writing tasks](tasks.md)** — the `workflow.Flow` carrier: reading and writing state, the control
   signals (retry, sleep, goto, interrupt, subgraph), baggage, and handling transient failures.

Then dip into the topic guides as you need them:

- **[Driving flows](flows.md)** — every method on the engine: creating, running, inspecting,
  pausing/resuming, cancelling, forking, continuing a thread, and retention.
- **[Detecting completion](detecting-completion.md)** — the ways to learn a flow's outcome (`Await` vs.
  orchestration) and how to make follow-up delivery reliable.
- **[Fan-out & subgraphs](fan-out-and-subgraphs.md)** — running work in parallel and calling
  sub-workflows.
- **[Testing](testing.md)** — `NewEngineUnderTest` and `TestProxy` patterns.

## For operators

Start at **[Operating dwarf](operations.md)** — what the engine repairs on its own and on what schedule,
what genuinely needs a human, and what belongs on a dashboard. Then:

- **[Production checklist](production-checklist.md)** — the pre-flight list. Everything that is not
  defaulted, or where the default is wrong for production, with a link to the guide behind each.
- **[Runbook](runbook.md)** — symptom-indexed incident response: what to do when an alarm fires, when the
  backlog climbs, when a shard is slow, when the database was down.
- **[Deployment](deployment.md)** — choosing and tuning a database, declaring shards, connection pools,
  workers, drain windows, and running multiple replicas.
- **[Backup and restore](backup-and-restore.md)** — what to back up, and what a restore does to flows
  (they re-run, completed work rewinds, deleted flows come back).
- **[Upgrading](upgrading.md)** — redeploying your app vs. upgrading dwarf itself (which is a maintenance
  window while dwarf is v0.x), plus the graph-evolution contract that outlives any single deploy.
- **[Resharding](resharding.md)** — adding, retiring and moving shards, and what is not supported.
- **[Data handling](data-handling.md)** — what the engine stores, what free-text search reaches, what
  telemetry carries, and how retention and deletion actually work.
- **[Cloud benchmarks](benchmark-cloud.md)** — measured against managed cloud PostgreSQL across a real
  network hop: the sizing formula and the constants behind the engine's fact-derived tuning.

## For both

- **[Concepts](concepts.md)** — the shared vocabulary. Everything else assumes it.
- **[Scheduling & reliability](scheduling-and-reliability.md)** — priority, fairness, retries, and crash
  recovery. Developers set the policy; operators read its effects.
- **[Observability](observability.md)** — structured logs, OpenTelemetry metrics, and distributed tracing.
  Developers inject the providers; operators read the output.
- **[Benchmarks](benchmark.md)** — in-repo throughput/latency benchmarks per SQL dialect.
