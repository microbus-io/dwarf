# Dwarf `fixtures` — test harness and test mode

> Load when: writing or changing fixtures / engine tests.
> Coupled with: root `CLAUDE.md`.

### SQLite Testing Support

**`engine.NewEngineUnderTest(testName string)`** is the sole test-mode entry point. It hashes the name into
`testHashedID` and installs a logger default; then the test configures the engine (`SetHost`, `SetShard`, …)
and calls `Startup` itself.

- **It takes a NAME, not a `testing.TB`, and nothing may reintroduce one.** A host driving the engine under
  its own harness has no `*testing.T` to hand over, and a name is all the constructor needs from one - so
  test mode is reachable by anything that can spell a string, not only by a Go test. The consequences are
  the caller's: a test passes `t.Name()`, and **teardown is `defer eng.Shutdown(ctx)`** at the call site.
  The engine registers no `t.Cleanup` anywhere.
- **The name IS the isolation key, and it is the ONLY one** - there is no separate setter to override it
  with. A test needing several engines in **separate** isolated databases (independent deployments, not one
  shared fleet) names each one (`newSolo` in `engine/poolsizing_test.go` uses `t.Name()+"#"+key`); a
  benchmark reused across warmup/measure passes names each pass (`b.Name()+"-"+seq`); a multi-replica
  fixture gives every peer the SAME `t.Name()`, which is how they land in one database and become a fleet.
- **`defer eng.Shutdown(ctx)` goes IMMEDIATELY after the constructor, and the ordering is load-bearing.**
  Defers unwind LIFO, so registering it first makes it unwind LAST - after every later `defer` that releases a
  held task. A test holding a worker on a channel therefore closes that channel in a defer declared **below**
  the Shutdown one, or the drain waits forever on a worker that can never return (this is why several
  fixtures carry an explicit note at their `defer close(release)`). A `t.Cleanup` release is worse still: it
  runs after ALL defers, including the shutdown.
- **Logger default: stderr at Error** (the CI alarms: wedge/poll/refill faults; stderr not `t.Log`, since a
  `go test` timeout panic drops buffered `t.Log` but not stderr). `DWARF_TESTING_LOG_LEVEL` overrides the level
  (`info`/`debug` for the flow-status play-by-play; `silent`/`off` forces discard). The constructor sees only
  a name, so it cannot tell a benchmark from a test: **a benchmark or fuzz target silences the engine** with
  `SetLogger(slog.New(slog.DiscardHandler))` (`fixtures/benchmark_test.go`) - per-iteration logging otherwise
  dominates the measurement / floods the fuzz output. `SetLogger` before `Startup` takes over entirely.
- **The janitors run their jobs only when a test drives them** - `accessors.ArchiveNow`, or an engine test's
  `reapDueFlows` - never in the background, so a fixture that holds a closed or stamped tree in the live tables
  (a Delete, then a Fork expecting 409) is not raced by them.
- **`DWARF_TESTING_STAGE_ROWS=<n>` runs the suite with staged archival on every tree over `n` rows** (and
  `DWARF_TESTING_STAGE_BYTES` a payload size beside it): `NewEngineUnderTest` reads them into `reapStageRows` and
  `reapStageBytes` (`internal/janitor/CLAUDE.md`), so a test that sets its own threshold overrides them. `1` stages
  every tree of more than one row. Unset, the production threshold (256k rows) stands, which no fixture's tree
  reaches; a value that is not a whole number of at least 0 panics the constructor.

The `testHashedID` switches the open path into test mode: `resolveShardDSN` (`internal/database`)
resolves the base DSN in three tiers - an explicitly-set DSN wins; else `SEQUEL_TESTING_DSN` (the same variable
sequel reads, so one knob redirects the whole suite at a real server); else the SQLite in-memory default
**`file:dwarf_%d?mode=memory&cache=shared`** - substitutes `%d` with the shard index, then routes through
`sequel.CreateTestingDatabase` (from `openShard`), which keys an isolated, auto-dropped database on
`(driver, baseDSN, testID)`.

**Per-shard isolation comes from the DSN's `%d`, not from the testID.** The default DSN carries `%d`, so shard *i*
opens base `file:dwarf_i?…` - a distinct base per shard, so `CreateTestingDatabase`'s key already differs per shard
and a multi-shard engine never collapses onto one in-memory DB. **Do not also fold the shard index into the
testID**: with a `%d`-bearing default it is redundant. The consequence is a deliberately-honest sharp edge: a multi-shard run against a `SEQUEL_TESTING_DSN`
that **lacks `%d`** collapses its shards onto one database - caught loudly at `Open`, which rejects two shards
resolving to the same DSN rather than silently folding them.

The testID is hashed to a bounded 16 hex chars (SQL identifier limits: Postgres 63 / MySQL 64) before it reaches
`CreateTestingDatabase`, so an arbitrarily long Go subtest name still yields a valid database name. The hash is
salted with a per-PROCESS nonce (`database.TestID`), so concurrent `go test` runs of one package never derive the
same database name and drop it out from under each other. Within one process it is deterministic, so two engines
in the **same** test (both keyed by `t.Name()`) resolve to the *same* isolated databases - which is how an in-test
multi-replica fixture gives its peer engines shared state.

### Running the suite against a real server

`SEQUEL_TESTING_DSN` redirects the whole suite at a real database (the `%d` is required - it is what gives each
shard its own isolated test database; see the DSN resolution above). All four dialects pass:

```sh
SEQUEL_TESTING_DSN='postgres://root:secret1234@127.0.0.1:5432/dwarftest_%d?sslmode=disable' go test ./engine/ ./fixtures/
SEQUEL_TESTING_DSN='root:secret1234@tcp(127.0.0.1:3306)/dwarftest_%d'                        go test ./engine/ ./fixtures/
SEQUEL_TESTING_DSN='sqlserver://sa:PASS@127.0.0.1:1433?database=dwarftest_%d&encrypt=disable' go test ./engine/ ./fixtures/
```

**SQL Server needs `&encrypt=disable` against a container's self-signed certificate.** The stock image's cert has a
*negative serial number*, which Go's `crypto/x509` refuses to parse, so every connection dies with
`TLS Handshake failed: x509: negative serial number` before a single query runs. It looks like a dwarf failure (the
tests fail at teardown, dropping the test database) and is not one.

Worth doing before shipping anything dialect-sensitive: SQLite serializes writes and has the weakest planner, so it
hides exactly the bugs the others catch - MySQL's `RowsAffected` counting *changed* rather than *matched* rows, its
gap locks under `REPEATABLE READ`, SQL Server's filtered-index/parameterization rules and its `OUTPUT INSERTED`
claim path, and Postgres's real MVCC lock ordering. Expect the run to be slower there: `fixtures` takes ~3-9x
SQLite's time on Postgres and MySQL and ~25x on SQL Server (below).

#### Per-test engine + parallel execution (connection-budget control)

**The suite runs `t.Parallel()`.** Each fixture stands up its own engine (`engine.NewEngineUnderTest(t.Name())`
+ `defer eng.Shutdown(ctx)` + `SetHost(mux)` + `Startup`, a fresh `mux := hosts.NewMux()` per test)
against its own isolated database (keyed by that `t.Name()`) - so nothing is shared across tests and the
config that mattered (the timing knobs, the peer registry key) is per-engine. There is **no** shared/`TestMain`-built
engine. The suite parallelizes cleanly because the isolation is per-test, not because the tests were written to
cooperate.

**Connection load is bounded by two mechanisms, both engaged only in test mode** (`Config.TestID != ""`), so
`N parallel engines × pool × shards` can never overrun a shared server's `max_connections`:

- **A per-shard pool CAP.** In test mode each shard's pool is clamped to `defaultTestConnCap` (**4**, `engine.go`)
  instead of the derived budget (8 on SQLite; elsewhere 250 ramping to 500 with RTT, clamped to half the server's `max_connections`). The engine still *derives* that budget and sizes
  its worker ceiling / refill floor from it (those read the engine's notion, not the physical pool), but the
  physical pool is clamped at open and on every push through the shard set (`ShardSet.SetShardPool`, in
  `internal/database/database.go`). **Push pool sizes through the shard set, never onto a
  shard's own `*sequel.DB`**: a direct push bypasses the cap, and with a derived budget of up to 500 per shard per
  engine (half the server's `max_connections` on a small one) that exhausts a test server's connections. Four connections is plenty for a fixture; the cap only
  bounds fan-out concurrency, which SQLite serializes anyway.
- **A per-driver global connection BUDGET** (`internal/database/testbudget.go`). Each `ShardSet.Open` reserves
  `cap × shards` from a process-wide weighted semaphore keyed on the driver (`db.DriverName()`), and releases at
  `Close`. The reservation is one atomic acquire per engine (never per connection or per shard - a per-connection
  limiter deadlocks the moment one engine's multi-shard `OnEach` needs several connections while the count is
  exhausted by peers each holding one). Budgets: **pgx 80** (Postgres default 100), mysql 120, **mssql 120**,
  **sqlite effectively unbounded** (no server cap). So on the default in-memory SQLite suite the budget never
  blocks and every test runs concurrently on its own database; against a real `SEQUEL_TESTING_DSN` server the
  budget throttles concurrency to stay under the cap (~80/4 ≈ 20 live engines on Postgres), `Acquire` blocking the
  21st engine until one finishes rather than letting it open a connection the server would reject.

  **SQL Server's suite runtime is NOT concurrency-bound, so do not reach for this budget to fix it.**
  `fixtures` costs **~25x SQLite against SQL Server (160-280s vs 8s)** where Postgres and MySQL cost ~3-9x,
  and the suite lands in that same band whether the mssql budget is 120 or effectively unbounded (**231s at
  120, against 4000**). The cost is therefore per test rather than between tests, and the candidate is that
  every test CREATEs and DROPs its own database, which SQL Server does far more expensively than the other
  engines. 120 is there for consistency with the other servers, which is worth having on its own; the runtime
  is open.

**Two DISTINCT opt-outs, for two distinct reasons - do not conflate them:**

- **A test that asserts the real DERIVED pool sizes sets `e.testConnCap = 0` before Startup** (uncaps its pools).
  Only the pool-sizing, dispatcher and peer-fleet tests need this (`engine/poolsizing_test.go`,
  `dispatchers_test.go`, `peer*_test.go`) - they read physical `db.DB.Stats().MaxOpenConnections` and a cap
  would make them read 4 instead of the 48 they declare. They still run
  parallel; they just use real pools (fine - the budget covers uncapped engines too, and on SQLite there is no cap
  to exceed).
- **A test that asserts an upper-bound REACTION LATENCY omits `t.Parallel()`** so it runs in the serial phase with
  no CPU competition. CPU oversubscription from co-running parallel tests inflates measured latency past the bound.
  These are `crossshardpriorityflow` (urgent burst < 3s), `awaitshutdownflow` / `tworeplicaflow` (prompt wake < 2s),
  and `taskdeadlineflow` (fail at ~budget < 3s safety net). The rule: an **outcome** assertion tolerates parallelism
  (it just waits longer); an upper-bound **timing** assertion does not. A lower-bound timing assertion (`sleepflow`:
  elapsed `>=` the sleep) is parallel-safe, since oversubscription can only make it longer.

  **Dropping `t.Parallel()` buys CPU, not database latency - so a reaction-latency window must still exclude the
  engine's own round trips.** Measure from the last moment before the reaction, never across an engine operation
  that talks to the database: a "prompt wake < 2s" measured across `Shutdown` is really measuring
  drain-plus-release, and the drain alone reaches **3.09s** against a loaded SQL Server. `awaitshutdownflow`
  therefore times from where `Shutdown` RETURNS, which pins the same property - the waiter is not still blocked
  once the board is closed - with nothing else folded in.

**Under `-race`, the engine's shared wait helpers stretch their "don't hang" ceilings.** `-race` slows execution
~10x, and with the whole suite parallel that compounds with CPU oversubscription, so a recovery that finishes in
seconds serially can exceed a 15s ceiling. The `enginetest` wait helpers (`BoundedRun`, `BoundedAwait`,
`AwaitFlowStatus`, `AwaitVisits`, `WaitUntil`) multiply their timeouts by `testTimeoutScale` - a build-tagged constant that is **5 under `-race`, 1 otherwise**
(`internal/enginetest/timeoutscale_*.go`).
The ceilings guard against a genuine hang, not a timing contract, so stretching them under `-race` keeps every test
parallel without masking a real wedge (a wedged flow never completes and still trips even the stretched ceiling).
A test's own ceiling - a `context.WithTimeout`, a `time.After` hang ceiling in a `select`, or the
`deadline := time.Now().Add(...)` a polling loop gives up at - multiplies by `enginetest.TimeoutScale()` the same
way. Left unscaled on purpose: a negative wait ("nothing happens within
500ms"), a bounded optional wait whose expiry is an outcome, and a safety net inside a task body.

**Give a `-race` run of `fixtures` an explicit `-timeout`, e.g. `-timeout 20m`.** It runs for minutes on SQLite,
and `go test`'s 10-minute default would kill a healthy but slow run instead of letting it fail cleanly.

**A fixture holding thousands of due steps pins a 1s refill period under `-race`** (`enginetest.Race()` +
`accessors.SetRefillInterval`, as `TestWideflow` does). The race detector slows the pure-Go SQLite far more
than Go code, so the band scan and fetch - O(due rows) each - outrun the derived ~67ms period and run back to
back on the lock and core the workers need. Scope it to the fixture, never to every test-mode engine: the
period is what `crossshardpriorityflow`'s reaction margin, the steal grace and `TestRefillInterval_*` pin.

**A wall-clock bound may be a "did it hang" ceiling; it may never be the MECHANISM.** Against a real server
under the parallel suite, every latency a test would bound - create, queue, claim, dispatch, an interrupt write
that deadlocks and is retried - is unbounded, so a bound short enough to be the thing a test *relies on*
eventually loses. It fails as a wrong-phase or wrong-count assertion, never as an honest timeout, which is what
makes it expensive to diagnose. Two shapes, each measured failing against SQL Server:

- **A short ctx handed to `Run` bounds create AND await.** A deadline chosen to expire the *await* expires the
  *create* first on a slow server, and the test then measures a create failure - empty flowKey, no flow, every
  later assertion cascading - instead of the semantics it is about. End the ctx from **inside the task**: the
  task cannot run until the flow exists and is running, so create is unbounded and the await ends at a point
  the engine reached rather than one a timer guessed at (`runtimeoutcancelflow`, where 100ms did this).
- **Polling for engine progress on a deadline.** Rendezvous on a checkpoint instead. `CheckpointFlowStopped`
  fires POST-COMMIT and is scoped by `(flowKey, status)`, and `handleInterrupt` fires it once per interrupting
  step for every flow in the surgraph chain - so counting its `Visits` on the ROOT key counts propagations
  durably (`interruptselectionflow`'s `awaitPropagatedInterrupts`; a 5s poll saw ZERO of two on a run whose
  every other assertion passed).

`flow.Retry`'s `giveUpAfter` is not one of these: it bounds the summed backoff `Retry` asks for, computed from
the call's own arguments, never the clock, so dispatch latency cannot move the give-up. Assert the exact give-up
attempt (`retryhorizonflow` gives up at attempt 6).

**No background recovery pass sees a shape a test forges, unless the test lets one run.** The recovery repairs are
janitor jobs, and test mode's janitors run only in a drain (`janitorManual`), so a test that forges a shape a repair
detects - a backdated orphan, a lapsed lease - and runs that repair itself is the only pass that sees it. A drain
(`reapDueFlows` runs each recovery job once an interval) or a test with `janitorManual` off brings one back, and then
the backdating the test does to *trigger* its own call satisfies the job's age guard too, so both count the forged
shape: measured, a background orphan repair landed **27ms** before the test's own, on a run that happened to pass.
The park detectors are less exposed - they run against fresh, un-backdated rows that their 5m guard excludes, while
the tests drive their own call with `minAge=0`.

`awaitPropagatedInterrupts` and `enginetest.AwaitFlowStatus` share one idiom: **arm the waiter FIRST, then read `Visits`.** An
event that landed before the call is caught by the count, a later one by the channel, and one landing between
the two lines by the channel, since the waiter is already registered. Reversing the two lines reintroduces
exactly the race the split-arm `Waiter` exists to remove. `Waiter` is one-shot, so waiting for N occurrences
loops over (arm, check, block) - a rendezvous per occurrence, not a spin on an observation.

Each fixture owns its own engine *and* its own `hosts.Mux`, so there is no cross-test sharing to coordinate. `hosts.Mux`
still guards its handler maps with a `RWMutex` because the engine's own worker goroutines dispatch concurrently while
the test registers handlers; fixture task/graph URLs are namespaced per file (`<fixture>.verify:428/<task>`) by
convention. A fixture asserting over the **full** flow set (the lists/`Purge`/`ShardInfo`) gets a clean database for free
- its engine sees only its own flows.

A fixture needing a **non-default topology** (multiple `SetShard`s, `SetTimeBudget`, a specific `SetWorkers` count) or
**host singletons** (a custom host wrapping `hosts.Mux`) configures
them on its own engine/mux between `NewEngineUnderTest` and `Startup` - the same per-test ownership, just with
non-default knobs.

#### The environment variables the tests and the bench read

Every knob a test or the bench reads from the environment is named `DWARF_TESTING_*`, beside sequel's
`SEQUEL_TESTING_DSN`, and **the engine's production code reads none.** The bench sets the archive overrides on its
engines through `internal/accessors` (`SetArchiveDay`, `SetArchiveGrace`, `SetStageRows`, `SetStageBytes`), since a
bench binary's test seams are inert. A new knob takes the prefix and a row here.

| Variable | Read by | What it does |
|---|---|---|
| `SEQUEL_TESTING_DSN` | the test-mode open (`internal/database`) | runs the suite against a real server (above) |
| `DWARF_TESTING_LOG_LEVEL` | `NewEngineUnderTest` | the test engines' log level on stderr: `error` by default, `info`/`debug`, `silent`/`off` |
| `DWARF_TESTING_STAGE_ROWS`, `DWARF_TESTING_STAGE_BYTES` | `NewEngineUnderTest` | stage every tree over that size (above) |
| `DWARF_TESTING_HISTORY`, `DWARF_TESTING_HISTORY_SEED`, `DWARF_TESTING_HISTORY_SEEDS`, `DWARF_TESTING_HISTORY_MINIMIZE` | `engine/histories_test.go` | replay one history, run one seed or seeds 1..n, minimize a failing history (the file's opening comment) |
| `DWARF_TESTING_LONGSOAK`, `DWARF_TESTING_SOAK_SEED` | `engine/chaossoak_test.go` | lengthen the chaos soak's window; replay its seed |
| `DWARF_TESTING_MEASURE_K` | `engine/roundtrips_test.go` | run the round-trips-per-step measurement (opt-in, slow) |
| `DWARF_TESTING_COHORT_WIDTH` | `engine/cohortwidth_test.go` | run the wide-cohort measurement at that width |
| `DWARF_TESTING_KILL_POINT`, `DWARF_TESTING_KILL_DSN`, `DWARF_TESTING_KILL_LOG` | `engine/processkill_test.go` | set by the test for the child process it kills, never by hand |
| `DWARF_TESTING_DROP_DSN`, `DWARF_TESTING_DROP_KEYS` | `engine/dispatcherdrop_test.go` | set by the test for the child process it kills, never by hand |
| `DWARF_TESTING_ARCHIVE_DAY`, `DWARF_TESTING_ARCHIVE_GRACE`, `DWARF_TESTING_STAGE_ROWS`, `DWARF_TESTING_STAGE_BYTES` | the bench (`archiveOverrides`, `bench/overrides.go`) | the rig's archive day, grace and staging thresholds (`bench/CLAUDE.md`) |
| `DWARF_TESTING_RIG_DSN` | `bench/riginvariants_test.go` | the database a rig run left behind, for the invariant checks (skipped without it) |

#### What a sleep is standing in for, and what to use instead

Every duration in a fixture is standing in for something the engine could say directly. Sorting them by
what they stand in for is what decides whether one is a bug waiting to happen or perfectly fine:

- **Simulated work** - a `time.Sleep` inside a task body, so a step takes measurable time. Legitimate and
  not a target: the delay IS the workload.
- **A "did it hang" ceiling** - the bound on a wait that should never be reached. Legitimate, and it must
  scale under `-race` (`enginetest.TimeoutScale`) rather than be tightened.
- **Waiting for engine progress** - "let the cache converge", "let the Await register", "let the holder take
  the worker". These are the bugs. A duration that comes up short here does not fail; it quietly changes
  what the test exercised, and every assertion still passes.
- **Closing the window of a NEGATIVE assertion** - "give the wrong thing a moment to happen, then confirm
  it did not". Also a bug, and the sneakiest: the sleep is the only thing making the denial mean anything,
  and it is weakest exactly when the machine is busiest.

For the last two, name the event instead:

- **`enginetest.AwaitVisits(t, seams, n, timeout, checkpoint)`** - wait for N further arrivals at a
  checkpoint (a targeted name comes from `diagnostics.Join`). seamster's `Waiter` is one-shot
  and `Visits` is monotonic, so N occurrences means re-arming per occurrence, arming BEFORE reading the
  count each time. Call it from the test goroutine: `t.Fatalf`
  inside a task handler kills the engine goroutine that would have driven the checkpoint, so the suite
  wedges instead of reporting. `enginetest.AwaitVisitCount` is the same wait against an absolute count, for a test that
  must read the base BEFORE triggering the events - because they can land before the wait begins.
- **`enginetest.AwaitShardCycles(t, e, extra)`** - wait for each open shard to SCAN and then PUSH. TWO of
  each, not one: a cycle already in flight when the work committed may have started before it existed. **It
  waits on both of the piston's loops, in that order, and counting pushes alone is not enough** - the two
  run on independent cadences, so a push says only "the partition matches the plan", and the plan is built
  from whatever tally the planner already held, which may predate the work entirely. The scans are what put
  the work into the plan; the pushes are what put the plan into the partition. This is also how a
  **negative** assertion closes its
  window - the thing being denied is almost always a DISPATCH, and a cycle is the dispatcher looking for
  exactly the pending candidate that would carry it. It gets longer on a busy machine, where a duration gets
  weaker.
- **`CheckpointAwaitParked`** - an `Await` is on the latch board and about to block. Any test about a
  BLOCKED caller being woken needs this; the board is polled, so a late-registering Await is answered by its
  own pre-park read and the wake path under test never runs.
- **A task that holds, rather than a task that is slow.** To keep a worker occupied while the test sets up,
  block the task on a channel and have it report when it has the worker - never a fixed delay long enough to
  cover the setup. Release with a `defer` and a `sync.Once`, declared **after** `defer eng.Shutdown(ctx)` so
  it unwinds first - NOT a `t.Cleanup`, and not a defer above the shutdown one. Both of those free the worker
  only once the drain is already waiting on it, and the suite deadlocks.
