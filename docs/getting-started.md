# Getting started

## Install

```bash
go get github.com/microbus-io/dwarf
```

Dwarf requires Go 1.26+ and a SQL database for production use (PostgreSQL, MySQL/MariaDB, or SQL Server).
For tests and local experiments it uses SQLite in-memory automatically — no database to set up.

## The three things you provide

The engine handles scheduling, state, durability, and recovery. You provide three things:

1. **A graph** — the shape of the workflow, built with `workflow.NewGraph`.
2. **A `Host` implementing `LoadGraph`** — returns a graph by name. The engine calls it once when a flow is
   created, then freezes the graph onto the flow.
3. **The same `Host` implementing `ExecuteTask`** — runs one task. It receives a `*workflow.Flow` with the
   step's input state already populated; it does the work and writes outputs back onto the flow.

That's the whole contract. The engine never learns *how* a task is reached — whether your host's
`ExecuteTask` calls a local function, makes an RPC, or publishes to a bus is entirely up to you.

## Your first flow (test harness)

The fastest way to see dwarf run is the in-process test harness. `engine.TestProxy` implements the `Host`
interface against in-memory registries, and `Engine.RunInTest(t)` spins up an
isolated SQLite database with automatic cleanup.

```go
package example

import (
	"context"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf"
	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestGreeting(t *testing.T) {
	ctx := context.Background()
	proxy := engine.NewTestProxy()

	// 1. Define a two-task graph: Greet -> Shout -> END.
	g := workflow.NewGraph("Greet", "greet")
	g.AddTask("Greet", "greet")
	g.AddTask("Shout", "shout")
	g.AddTransition("Greet", "Shout")
	g.AddTransition("Shout", workflow.END)
	proxy.HandleGraph("greet", g)

	// 2. Register the tasks.
	proxy.HandleTask("greet", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("message", "hello "+f.GetString("name"))
		return nil
	})
	proxy.HandleTask("shout", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("message", strings.ToUpper(f.GetString("message")))
		return nil
	})

	// 3. Wire and start the engine.
	eng := dwarf.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	// 4. Run a flow to completion.
	out, err := eng.Run(ctx, "greet", map[string]any{"name": "ada"}, nil)
	testarossa.NoError(t, err)
	testarossa.Equal(t, workflow.StatusCompleted, out.Status)
	testarossa.Equal(t, "HELLO ADA", out.State["message"])
}
```

`Run` is `Create` + `Start` + `Await` in one call. The `nil` last argument is `*workflow.FlowOptions`
(scheduling and baggage) — see [Engine operations](operations.md).

## Wiring a real engine

In production you replace `TestProxy` with your own `Host`, point the engine at a real
database with `WithDSN`, and manage its lifecycle explicitly. A standalone host need only implement the
two required methods — the optional peer/notify methods can be no-ops:

```go
type myHost struct {
	graphs map[string]*workflow.Graph // your graph source
}

func (h *myHost) LoadGraph(ctx context.Context, name string) (*workflow.Graph, error) {
	g, ok := h.graphs[name]
	if !ok {
		return nil, fmt.Errorf("unknown workflow %q", name)
	}
	return g, nil
}

func (h *myHost) ExecuteTask(ctx context.Context, taskName string, f *workflow.Flow) error {
	return dispatch(ctx, taskName, f) // your local table / RPC / bus
}

// Optional methods (no-ops for a single-replica host with no stop-notification need):
func (h *myHost) FlowStopped(context.Context, string, *workflow.FlowOutcome) {}
func (h *myHost) SignalPeers(context.Context, string, []byte)                {}

eng := dwarf.NewEngine().
	WithDSN("postgres://user:pass@db:5432/dwarf").
	WithHost(&myHost{graphs: loadGraphRegistry()})

if err := eng.Startup(ctx); err != nil {
	log.Fatal(err)
}
defer eng.Shutdown(ctx)
```

- `Startup` opens the database connections, runs schema migrations, and starts the worker pool.
- `Shutdown` drains the workers and closes the connections cleanly.
- The database must already exist; the engine migrates the schema but does not `CREATE DATABASE`.

Not registering a host (`WithHost`) makes `Startup` return an error.

## Where to go next

- The [Concepts](concepts.md) guide explains flows, steps, threads, and reducers — the vocabulary the rest
  of the docs assume.
- [Building graphs](graphs.md) and [Writing tasks](tasks.md) are the two day-to-day authoring guides.
