<a href="https://www.dwarf.run"><img src="dwarf-banner.jpg" width=600 alt="Dwarf"></a>
<p></p>

[![License Apache 2](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Go](https://img.shields.io/badge/Go-1.27.3+-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![Reference](https://pkg.go.dev/badge/github.com/microbus-io/dwarf)](https://pkg.go.dev/github.com/microbus-io/dwarf)
[![Test](https://github.com/microbus-io/dwarf/actions/workflows/test.yaml/badge.svg?branch=main&event=push)](https://github.com/microbus-io/dwarf/actions/workflows/test.yaml)
[![Vulnerabilities](https://github.com/microbus-io/dwarf/actions/workflows/vulncheck.yaml/badge.svg?branch=main&event=push)](https://github.com/microbus-io/dwarf/actions/workflows/vulncheck.yaml)
[![Discord](https://img.shields.io/badge/Discord-Join-5865F2?logo=discord&logoColor=white)](https://discord.gg/FAJHnGkNqJ)

**Dwarf is an open source durable execution engine, written in pure Go and backed by Postgres: an alternative
to Temporal and LangGraph that runs as a library, not a platform.** You describe a workflow as a graph of tasks,
and Dwarf runs it, checkpointing every step to your SQL database. Your tasks can be written in any language: the
engine reaches them through a small host you write.

See **[www.dwarf.run](https://www.dwarf.run)** for the overview, and how Dwarf
[compares with Temporal and LangGraph](https://www.dwarf.run/compare/).

## Documentation

Full documentation lives at **[docs.dwarf.run](https://docs.dwarf.run)**, including:

- [Getting Started](https://docs.dwarf.run/getting-started/) — install Dwarf, write the two-method host, and run your first flow.
- [Concepts](https://docs.dwarf.run/concepts/) — graph, task, flow, step, reducer, and the flow lifecycle.
- [How Dwarf Works](https://docs.dwarf.run/how-dwarf-works/) — what runs where, a step's life, crash recovery and at-least-once execution.
- [Building Workflows](https://docs.dwarf.run/building-workflows/) — graphs, tasks, driving flows, fan-out, scheduling, testing, and tasks in other languages.
- [Operating Dwarf](https://docs.dwarf.run/operating/) — deployment, observability, upgrading, resharding, backup and the runbook.
- [Performance](https://docs.dwarf.run/performance/) — cloud benchmarks against managed PostgreSQL, and how to size a deployment.
- [Reference](https://docs.dwarf.run/reference/) — glossary, FAQ, and a concept map for people coming from Temporal or LangGraph.
- [Release Notes](https://docs.dwarf.run/about/release-notes/) — per-release changes.

## Quick Start

You'll need [Go](https://go.dev/) 1.27.3+. Add Dwarf to your module:

```sh
go get github.com/microbus-io/dwarf
```

A one-task workflow, run on the in-process test harness (SQLite in memory, no database to set up):

```go
// Define the workflow:
// a one-task graph whose "Hello" task runs at a URL, then ends.
g := workflow.NewGraph("Greet")
g.SetURL("Hello", "http://example/hello") // task "Hello" dispatches to this URL
g.AddTransition("Hello", workflow.End)

// Wire URLs to what serves them. The engine knows graphs and tasks only by URL: the graph's URL
// resolves to the graph, and each task's URL to the handler that runs it. hosts.Mux is an in-process
// router; a production host maps the same URLs onto its own registry or transport.
mux := hosts.NewMux()
mux.HandleGraph("http://example/greet", g)
mux.HandleTask("http://example/hello", func(ctx context.Context, f *workflow.Flow) error {
    f.SetString("greeting", "hello "+f.GetString("name"))
    return nil
})

// Start an engine that reaches graphs and tasks through the mux.
eng := dwarf.NewEngineUnderTest(t.Name()) // SQLite in-memory, auto-dropped
defer eng.Shutdown(ctx)
eng.SetHost(mux) // hosts.Mux implements the Host interface
eng.Startup(ctx)

// Run a flow of the graph by its URL, with initial state, and wait for its outcome.
_, out, _ := eng.Run(ctx, "http://example/greet", map[string]any{"name": "ada"}, nil) // Run returns (flowKey, outcome, err)
fmt.Println(out.State.GetString("greeting")) // hello ada
```

In production, point the engine at PostgreSQL with `SetShard` and give it your own host. See
[Getting Started](https://docs.dwarf.run/getting-started/) for the full walkthrough.

## Community

| | |
|---|---|
| Website | [www.dwarf.run](https://www.dwarf.run) |
| Docs | [docs.dwarf.run](https://docs.dwarf.run) |
| GitHub | [github.com/microbus-io/dwarf](https://github.com/microbus-io/dwarf) |
| Discord | [discord.gg/FAJHnGkNqJ](https://discord.gg/FAJHnGkNqJ) |
| Email | info@microbus.io |

## License

Apache 2.0. See [LICENSE](./LICENSE).
