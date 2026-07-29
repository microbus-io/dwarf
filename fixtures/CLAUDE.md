# Dwarf `fixtures` — test harness and test mode

> Load when: writing or changing fixtures / engine tests.
> Coupled with: root `CLAUDE.md`.

### SQLite Testing Support

**`engine.NewEngineUnderTest(t testing.TB)`** is the sole test-mode entry point. It stashes `t` on the engine,
hashes `t.Name()` into `testHashedID` (via `SetTestName`, which owns that hashing), and installs a logger
default; then the test configures the engine (`SetHost`, `SetShard`, …) and calls `Startup` itself, which
registers the `t.Cleanup` shutdown — so no explicit `defer Shutdown` is needed. It takes any `testing.TB`, so
the same constructor serves `*testing.B` and `*testing.F`.

- **Overriding the key: `SetTestName(name)`.** The default key is `t.Name()`. A test needing several engines in
  **separate** isolated databases (independent deployments, not one shared fleet) gives each a distinct key
  (`startSolo` uses `t.Name()+"#"+key`); a benchmark reused across warmup/measure passes gives each pass a fresh
  key (`b.Name()+"-"+seq`). `SetTestName` is construction-time only and rejects an engine not built with
  `NewEngineUnderTest` (`e.t == nil`). There is no `*testing.T`-free hook anymore — a host under an external
  harness that needs shared isolation drives `NewEngineUnderTest` with its own `testing.TB`.
- **Logger default.** A `*testing.T` logs to **stderr at Error** (the CI alarms: wedge/poll/refill faults;
  stderr not `t.Log`, since a `go test` timeout panic drops buffered `t.Log` but not stderr). A
  `*testing.B`/`*testing.F` defaults to **silent** (per-iteration logging would dominate the measurement /
  flood the fuzz output). `DWARF_TEST_LOG_LEVEL` overrides the level (`info`/`debug` for the flow-status
  play-by-play; `silent`/`off` forces discard); any explicit level un-silences a benchmark/fuzz. `SetLogger`
  before `Startup` takes over entirely.

The `testHashedID` switches the open path into test mode: `openDatabaseShard`
resolves the base DSN in three tiers - an explicitly-set DSN wins; else `SEQUEL_TESTING_DSN` (the same variable
sequel reads, so one knob redirects the whole suite at a real server); else the SQLite in-memory default
**`file:dwarf_%d?mode=memory&cache=shared`** - substitutes `%d` with the shard index, then routes through
`sequel.CreateTestingDatabase`, which keys an isolated, auto-dropped database on `(driver, baseDSN, testID)`.

**Per-shard isolation comes from the DSN's `%d`, not from the testID.** The default DSN carries `%d`, so shard *i*
opens base `file:dwarf_i?…` - a distinct base per shard, so `CreateTestingDatabase`'s key already differs per shard
and a multi-shard engine never collapses onto one in-memory DB. (An earlier design used a no-`%d` default and folded
the shard index into the testID to manufacture that difference; with a `%d`-bearing default that fold is redundant
and was removed.) The consequence is a deliberately-honest sharp edge: a multi-shard run against a `SEQUEL_TESTING_DSN`
that **lacks `%d`** collapses its shards onto one database - caught loudly at `Open`, which rejects two shards
resolving to the same DSN rather than silently folding them.

The testID is hashed to a bounded 16 hex chars (SQL identifier limits: Postgres 63 / MySQL 64) before it reaches
`CreateTestingDatabase`, so an arbitrarily long Go subtest name still yields a valid database name. The hash is
deterministic, so two engines in the **same** test (both keyed by `t.Name()`) resolve to the *same* isolated
databases - which is how an in-test multi-replica fixture gives its peer engines shared state.

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
tests fail in `t.Cleanup`, dropping the test database) and is not one.

Worth doing before shipping anything dialect-sensitive: SQLite serializes writes and has the weakest planner, so it
hides exactly the bugs the others catch - MySQL's `RowsAffected` counting *changed* rather than *matched* rows, its
gap locks under `REPEATABLE READ`, SQL Server's filtered-index/parameterization rules and its `OUTPUT INSERTED`
claim path, and Postgres's real MVCC lock ordering. Expect the run to be slower there: SQL Server takes ~3x SQLite.

#### Per-test engine + parallel execution (connection-budget control)

**The suite runs `t.Parallel()`.** Each fixture stands up its own engine (`engine.NewEngineUnderTest(t)` +
`SetHost(proxy)` + `Startup`, a fresh `proxy := engine.NewTestProxy()` per test) against its own isolated database
(`t.Name()`), and `Startup` registers a `t.Cleanup` shutdown - so nothing is shared across tests and the
config that mattered (the timing knobs, the peer registry key) is per-engine. There is **no** shared/`TestMain`-built
engine. The suite parallelizes cleanly because the isolation is per-test, not because the tests were written to
cooperate.

**Connection load is bounded by two mechanisms, both engaged only in test mode** (`Config.TestID != ""`), so
`N parallel engines × pool × shards` can never overrun a shared server's `max_connections`:

- **A per-shard pool CAP.** In test mode each shard's pool is clamped to `defaultTestConnCap` (**4**, `engine.go`)
  instead of the derived `connsPerVCPU (6) × defaultVirtualCPUs (2) = 12`. The engine still *derives* 12 and sizes
  its worker ceiling / refill floor from that (those read the engine's notion, not the physical pool), but the
  physical pool is clamped at open and on every `SetMaxOpenConns` push (`internal/database/database.go`). Four
  connections is plenty for a fixture; the cap only bounds fan-out concurrency, which SQLite serializes anyway.
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
  Only the pool-sizing tests need this (`TestPoolSizing_ObservedReplicasLive`,
  `_ConcurrentRecomputeDoesNotClobberOverride`, `_LiveOverride` in `engine/poolsizing_test.go`) - they read
  physical `db.DB.Stats().MaxOpenConnections` and a cap would make them read 4 instead of 48. They still run
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
seconds serially can exceed a 15s ceiling. `enginetest.BoundedRun` and `enginetest.AwaitFlowStatus` multiply their
timeouts by `testTimeoutScale` - a build-tagged constant that is **5 under `-race`, 1 otherwise**
(`internal/enginetest/timeoutscale_*.go`).
The ceilings guard against a genuine hang, not a timing contract, so stretching them under `-race` keeps every test
parallel without masking a real wedge (a wedged flow never completes and still trips even the stretched ceiling).
This applies to the engine package's helpers; a fixture with its own hardcoded ceiling that flakes under `-race`
either routes through those helpers or gets the same treatment.

**A wall-clock bound may be a "did it hang" ceiling; it may never be the MECHANISM.** Against a real server
under the parallel suite, every latency a test would bound - create, queue, claim, dispatch, an interrupt write
that deadlocks and is retried - is unbounded, so a bound short enough to be the thing a test *relies on*
eventually loses. It fails as a wrong-phase or wrong-count assertion, never as an honest timeout, which is what
makes it expensive to diagnose. Three shapes, each measured failing against SQL Server:

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
- **A wall-clock FEATURE under test competes with dispatch latency for the same budget.** `flow.Retry`'s
  `giveUpAfter` is anchored at the step's CREATION, so a 250ms horizon with a 20ms initial delay left 230ms of
  headroom and gave up at attempt 0 - failing "the horizon is not first-try". Size such a horizon to dominate
  dispatch latency, not to make the test fast (`retryhorizonflow`: 2s, ~6 attempts unloaded).

**A test that FORGES a shape a background sweep also detects must first wait for that sweep.** `recoveryLoop`
sweeps ON ENTRY (Startup), concurrently with the test body, and it is four scans on one goroutine sharing the
engine's pool - so on a loaded server it lands seconds in. The orphan detectors are the exposed ones, because
the backdating a test does to *trigger* its own detector call satisfies the background pass's age guard too, and
both then count the forged shape. `awaitStartupRecoverySweep` (engine `checkpointhelpers_test.go`) rendezvouses
on `CheckpointRecoverySweepDone`. Measured: the sweep's own `detectOrphanedFlows` landed **27ms** before the
test's on a run that happened to pass. The park detectors are not exposed - they run against fresh, un-backdated
rows that their 5m guard excludes, while the tests drive their own call with `minAge=0`.

Both helpers, and `enginetest.AwaitFlowStatus`, share one idiom: **arm the waiter FIRST, then read `Visits`.** An
event that landed before the call is caught by the count, a later one by the channel, and one landing between
the two lines by the channel, since the waiter is already registered. Reversing the two lines reintroduces
exactly the race the split-arm `Waiter` exists to remove. `Waiter` is one-shot, so waiting for N occurrences
loops over (arm, check, block) - a rendezvous per occurrence, not a spin on an observation.

Each fixture owns its own engine *and* its own `TestProxy`, so there is no cross-test sharing to coordinate. `TestProxy`
still guards its handler maps with a `RWMutex` because the engine's own worker goroutines dispatch concurrently while
the test registers handlers; fixture task/graph URLs are namespaced per file (`<fixture>.verify:428/<task>`) by
convention. A fixture asserting over the **full** flow set (`List`/`Purge`/`ShardInfo`) gets a clean database for free
- its engine sees only its own flows.

A fixture needing a **non-default topology** (multiple `SetShard`s, `SetTimeBudget`, a specific `SetWorkers` count) or
**host singletons** (a custom host wrapping `TestProxy`) configures
them on its own engine/proxy between `NewEngineUnderTest` and `Startup` - the same per-test ownership, just with
non-default knobs.

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
  checkpoint (a targeted name comes from the package-local `seamsJoin`). seamster's `Waiter` is one-shot
  and `Visits` is monotonic, so N occurrences means re-arming per occurrence, arming BEFORE reading the
  count each time. Call it from the test goroutine: `t.Fatalf`
  inside a task handler kills the engine goroutine that would have driven the checkpoint, so the suite
  wedges instead of reporting.
- **`enginetest.AwaitShardCycles(t, e, shards, extra)`** - wait for each shard's partition to be reconciled
  against the plan. TWO cycles, not one: a cycle already in flight when the work committed may have scanned
  before it existed. This is also how a **negative** assertion closes its window - the thing being denied is
  almost always a DISPATCH, and a cycle is the dispatcher looking for exactly the pending candidate that
  would carry it. It gets longer on a busy machine, where a duration gets weaker.
- **`CheckpointAwaitParked`** - an `Await` is on the latch board and about to block. Any test about a
  BLOCKED caller being woken needs this; the board is polled, so a late-registering Await is answered by its
  own pre-park read and the wake path under test never runs.
- **A task that holds, rather than a task that is slow.** To keep a worker occupied while the test sets up,
  block the task on a channel and have it report when it has the worker - never a fixed delay long enough to
  cover the setup. Release with a `defer` and a `sync.Once`, NOT a `t.Cleanup`: cleanups unwind after
  deferred funcs, so a cleanup frees the worker only after Startup's own Shutdown cleanup is already waiting
  on it, and the suite deadlocks.
