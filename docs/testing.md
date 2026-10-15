# Testing

> **For developers** writing tests for their own workflows. It covers the in-process harness dwarf ships so
> your test suite can run real flows with no database and no transport.

Dwarf ships an in-process test harness so you can exercise real workflows — scheduling, fan-out, subgraphs,
interrupts, retries — with no database to set up and no transport to stand up.

## The two pieces

- **`engine.NewEngineUnderTest(name)`** constructs an engine wired for testing: an isolated SQLite in-memory
  database (per name, and per shard for multi-shard tests), auto-dropped, with migrations run at `Startup` —
  no DSN. It takes a plain name rather than a `testing.TB`, so a harness with no `*testing.T` to hand over
  reaches it the same way a Go test does; pass `t.Name()` from a test. You call `Startup(ctx)` yourself, so
  any `Set*` configuration goes in between, and you own teardown: `defer eng.Shutdown(ctx)`. The name is the
  isolation key — give several engines the same name to share one database, or distinct names to isolate
  them. It logs to stderr at Error for CI clues (`DWARF_TEST_LOG_LEVEL` overrides — `info`/`debug` for the
  flow-status play-by-play, `silent`/`off` to quiet it); a benchmark or fuzz target silences it outright with
  `SetLogger(slog.New(slog.DiscardHandler))`, since per-iteration logging would swamp the measurement.
- **`hosts.Mux`** is an in-memory implementation of the `Host` interface. Register graphs and task
  functions on it, then register it with `SetHost(mux)` — its `LoadGraph`/`ExecuteTask` look them up by URL.

```go
func TestCheckout(t *testing.T) {
    ctx := context.Background()
    mux := hosts.NewMux()

    g := workflow.NewGraph("Checkout")
    g.SetURL("Reserve", "inventory.reserve")
    g.SetURL("Charge", "billing.charge")
    g.AddTransition("Reserve", "Charge")
    g.AddTransition("Charge", workflow.End)
    mux.HandleGraph("checkout", g)

    mux.HandleTask("inventory.reserve", func(ctx context.Context, f *workflow.Flow) error {
        f.SetBool("reserved", true)
        return nil
    })
    mux.HandleTask("billing.charge", func(ctx context.Context, f *workflow.Flow) error {
        f.SetString("receipt", "r-123")
        return nil
    })

    eng := dwarf.NewEngineUnderTest(t.Name())
    defer eng.Shutdown(ctx)
    eng.SetHost(mux)
    eng.Startup(ctx)

    _, out, err := eng.Run(ctx, "checkout", map[string]any{"sku": "ABC"}, nil)
    if err != nil {
        t.Fatal(err)
    }
    if out.Status != workflow.StatusCompleted {
        t.Fatalf("status %s", out.Status)
    }
    if receipt := out.State.GetString("receipt"); receipt != "r-123" {
        t.Errorf("receipt %q", receipt)
    }
}
```

`Mux.HandleGraph(workflowURL, graph)` registers a graph under the URL you pass to `Create` or `Run`;
`HandleTask(taskURL, handler)` registers a task under the URL bound with `SetURL`. The handler signature is the same `func(ctx, *workflow.Flow) error`
you write in production.

## Configuring the test engine

Apply any `Set*` settings between `NewEngineUnderTest` and `Startup` — workers, shards, time budget, default
priority:

```go
eng := dwarf.NewEngineUnderTest(t.Name())
defer eng.Shutdown(ctx)
eng.SetHost(mux)
eng.SetWorkers(1) // serialize dispatch to assert ordering
for i := 1; i <= 4; i++ {
	eng.SetShard(engine.ShardSpec{Index: i, DSN: ""}) // each shard gets its own in-memory database
}
eng.Startup(ctx)
```

`SetWorkers(1)` is a common trick for deterministically asserting dispatch order (e.g. fairness or
priority tests).

## Testing the harder paths

The harness drives every engine feature. A few patterns:

**Interrupts / human-in-the-loop.** A task calls `Interrupt`; assert the flow parks, then `Resume`:

```go
flowKey, _ := eng.Create(ctx, "approval", state, nil)
out, _ := eng.Await(ctx, flowKey)
if out.Status != workflow.StatusInterrupted {
	t.Fatalf("status %s", out.Status)
}
// out.InterruptPayload holds what the task asked for

eng.Resume(ctx, flowKey, map[string]any{"approved": true})
out, _ = eng.Await(ctx, flowKey)
if out.Status != workflow.StatusCompleted {
	t.Fatalf("status %s", out.Status)
}
```

**Transient failures.** A task that returns `nil` after arming `flow.Retry` re-runs after backoff; assert
the eventual outcome once the task stops failing:

```go
mux.HandleTask("flaky", func(ctx context.Context, f *workflow.Flow) error {
    if firstFewCalls() {
        if f.Retry(time.Millisecond, 2.0, 10*time.Millisecond, time.Minute) {
            return nil
        }
    }
    return nil
})
```

Because `hosts.Mux` returns synchronously, it produces a far tighter, more adversarial timing environment
than a real network — which makes it excellent at surfacing concurrency bugs in retries, fan-in, and
crash recovery.

**Inspecting execution.** `eng.History(ctx, flowKey)` returns the full step record (including nested
subgraph history); `workflow.NewFlowRenderer(steps).Render()` turns it into a Mermaid diagram of the execution DAG
for eyeballing what ran in what order.

## Multi-replica tests

To test cross-replica behavior, stand up two engines against the same test database. That is all a fleet
is — they coordinate through the database, so there is no relay to wire between them:

```go
mux1, mux2 := hosts.NewMux(), hosts.NewMux()
// register the same graphs/tasks on both...
eng1 := engine.NewEngineUnderTest(t.Name()) // one name → one shared isolated database
defer eng1.Shutdown(ctx)
eng1.SetHost(mux1)
eng2 := engine.NewEngineUnderTest(t.Name())
defer eng2.Shutdown(ctx)
eng2.SetHost(mux2)
eng1.Startup(ctx)
eng2.Startup(ctx)
```

Both engines share one isolated database because they were given the same name — no explicit DSN needed. Note
that sharing a database is exactly what makes them count each other as peers, so two engines meant to be
*separate* deployments need distinct names. This is how the engine's own cross-replica `Await` and step-recovery tests
are written — see the `fixtures` package in the repository for worked examples.

## Where examples live

The repository's fixture suite contains around a hundred end-to-end workflow tests built exactly this way — from
`basicflow` up through `subgraphflow`, `dynamicfanoutflow`, and `fairnessflow`. They're the canonical,
runnable reference for every feature.
