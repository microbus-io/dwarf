# Testing

Dwarf ships an in-process test harness so you can exercise real workflows — scheduling, fan-out, subgraphs,
interrupts, retries — with no database to set up and no transport to stand up.

## The two pieces

- **`engine.NewEngineUnderTest(t)`** constructs an engine wired for testing: an isolated SQLite in-memory
  database (per test, and per shard for multi-shard tests) keyed by `t.Name()`, with migrations run at
  `Startup` and cleanup registered via `t.Cleanup` — no DSN, no teardown code. You still call `Startup(ctx)`
  yourself (it registers the auto-shutdown), so any `Set*` configuration goes in between. It accepts any
  `testing.TB`, so it also serves benchmarks (`*testing.B`) and fuzz targets (`*testing.F`) — those default
  to a silent logger, while a `*testing.T` logs to stderr at Error for CI clues (`DWARF_TEST_LOG_LEVEL`
  overrides — `info`/`debug` for the flow-status play-by-play, `silent`/`off` to quiet it).
  `Engine.SetTestName(name)` overrides the `t.Name()` key — give several engines the same name to share one
  database, or distinct names to isolate them.
- **`engine.TestProxy`** is an in-memory implementation of the `Host` interface. Register graphs and task
  functions on it, then register it with `SetHost(proxy)` — its `LoadGraph`/`ExecuteTask` back the
  required methods, and its peer methods are no-ops.

```go
func TestCheckout(t *testing.T) {
    ctx := context.Background()
    proxy := engine.NewTestProxy()

    g := workflow.NewGraph("Checkout")
    g.SetEndpoint("Reserve", "inventory.reserve")
    g.SetEndpoint("Charge", "billing.charge")
    g.AddTransition("Reserve", "Charge")
    g.AddTransition("Charge", workflow.END)
    proxy.HandleGraph("checkout", g)

    proxy.HandleTask("inventory.reserve", func(ctx context.Context, f *workflow.Flow) error {
        f.SetBool("reserved", true)
        return nil
    })
    proxy.HandleTask("billing.charge", func(ctx context.Context, f *workflow.Flow) error {
        f.SetString("receipt", "r-123")
        return nil
    })

    eng := dwarf.NewEngineUnderTest(t)
    eng.SetHost(proxy)
    eng.Startup(ctx)

    _, out, err := eng.Run(ctx, "checkout", map[string]any{"sku": "ABC"}, nil)
    testarossa.NoError(t, err)
    testarossa.Equal(t, workflow.StatusCompleted, out.Status)
    testarossa.Equal(t, "r-123", out.State.GetString("receipt"))
}
```

`TestProxy.HandleGraph(name, graph)` registers a graph; `HandleTask(url, handler)` registers a task by its
URL (the address bound with `SetEndpoint`). The handler signature is the same `func(ctx, *workflow.Flow) error`
you write in production.

## Configuring the test engine

Apply any `Set*` settings between `NewEngineUnderTest` and `Startup` — workers, shards, time budget, default
priority:

```go
eng := dwarf.NewEngineUnderTest(t)
eng.SetHost(proxy)
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
testarossa.Equal(t, workflow.StatusInterrupted, out.Status)
// out.InterruptPayload holds what the task asked for

eng.Resume(ctx, flowKey, map[string]any{"approved": true})
out, _ = eng.Await(ctx, flowKey)
testarossa.Equal(t, workflow.StatusCompleted, out.Status)
```

**Transient failures.** A task that returns `nil` after arming `flow.Retry` re-runs after backoff; assert
the eventual outcome once the task stops failing:

```go
proxy.HandleTask("flaky", func(ctx context.Context, f *workflow.Flow) error {
    if firstFewCalls() {
        if f.Retry(time.Millisecond, 2.0, 10*time.Millisecond, time.Minute) {
            return nil
        }
    }
    return nil
})
```

Because `TestProxy` returns synchronously, it produces a far tighter, more adversarial timing environment
than a real network — which makes it excellent at surfacing concurrency bugs in retries, fan-in, and
crash recovery.

**Inspecting execution.** `eng.History(ctx, flowKey)` returns the full step record (including nested
subgraph history); `eng.HistoryMermaid(ctx, flowKey, w)` renders the execution DAG as a Mermaid diagram for
eyeballing what ran in what order.

## Multi-replica tests

To test cross-replica behavior, stand up two engines against the same test database. That is all a fleet
is — they coordinate through the database, so there is no relay to wire between them:

```go
proxy1, proxy2 := engine.NewTestProxy(), engine.NewTestProxy()
// register the same graphs/tasks on both...
eng1 := engine.NewEngineUnderTest(t) // both keyed by t.Name() → one shared isolated database
eng1.SetHost(proxy1)
eng2 := engine.NewEngineUnderTest(t)
eng2.SetHost(proxy2)
eng1.Startup(ctx)
eng2.Startup(ctx)
```

Both engines share one isolated database because they key by the same `t.Name()` — no explicit DSN needed
(use `SetTestName` if you want a specific shared key, or distinct keys to isolate them). Note that sharing a
database is exactly what makes them count each other as peers, so two engines meant to be *separate*
deployments need distinct keys. This is how the engine's own cross-replica `Await` and step-recovery tests
are written — see the `fixtures` package in the repository for worked examples.

## Where examples live

The repository's `fixtures` package contains ~60 end-to-end workflow tests built exactly this way — from
`basicflow` up through `subgraphflow`, `dynamicfanoutflow`, and `fairnessflow`. They're the canonical,
runnable reference for every feature.
