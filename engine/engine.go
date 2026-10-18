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

package engine

import (
	"context"
	"maps"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"log/slog"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/claimstracker"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/latch"
	"github.com/microbus-io/dwarf/internal/lru"
	"github.com/microbus-io/dwarf/internal/peers"
	"github.com/microbus-io/dwarf/internal/piston"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/dwarf/internal/workers"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ShardSummary is the health of a single database shard: whether it answers, and how fast.
type ShardSummary struct {
	// Index is the shard's index, as declared in its ShardSpec.
	Index int `json:"index,omitzero"`
	// Error is the failure of a trivial query to the shard, empty when the shard answered.
	Error string `json:"error,omitzero"`
	// Latency is the round trip of a trivial query to the shard.
	Latency time.Duration `json:"latency,omitzero"`
}

const (
	parkedNone     = 0
	parkedSubgraph = 1
)

// The bounds of the two parsed-graph cache layers. Memory is bounded by graphCacheGraphs parses, since the
// per-flow layer holds only weak pointers (~150 bytes an entry).
const (
	graphCacheFlows  = 1 << 16
	graphCacheGraphs = 4096
)

// terminalStatusesSQL is every terminal flow/step status pre-joined into a quoted literal ready to inline into a
// WHERE clause, e.g. "status IN (" + terminalStatusesSQL + ")" or "status NOT IN (" + terminalStatusesSQL + ")".
// Never bind this as a parameter - a bound status defeats the SQL Server / SQLite filtered index. In Go, test a
// status with workflow.IsTerminalStatus; TestTerminalStatusesSQL_MatchesIsTerminalStatus keeps the two in step.
var terminalStatusesSQL = "'" + strings.Join([]string{
	workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
}, "', '") + "'"

// Engine is the standalone workflow orchestration engine.
type Engine struct {
	// Dependencies (set before Startup)
	host          Host
	logger        *slog.Logger
	meterProvider metric.MeterProvider
	metrics       *engineMetrics
	// meter is the resolved instrumentation scope, held so every module this engine assembles (the
	// pistons) records into one scope rather than deriving its own from the provider.
	meter          metric.Meter
	tracerProvider trace.TracerProvider
	tracer         trace.Tracer

	// Shard registry (construction-time only): shard index -> spec. Sparse - indices must be unique and
	// >= 1 but need not be contiguous. Guarded by shardsLock; empty means one default shard at Startup.
	shardsLock sync.Mutex
	shardSpecs map[int]ShardSpec

	// Configuration (atomically updated, safe to change after Startup)
	workers                atomic.Int32
	workersSet             atomic.Bool // SetWorkers was called: skip the derived default
	timeBudgetMs           atomic.Int64
	defaultPriority        atomic.Int32
	refillIntervalOverride atomic.Int64 // bench override (nanoseconds): pins every piston's cycle period; <=0 = derive

	// engineID is a random positive identifier minted per engine instance (fresh on every restart). It is
	// this replica's identity in the shared dwarf_peers registry - the primary key its Sonars own a row
	// under - and it is stamped on every flow/step INSERT (creator) and overwritten by the claim CAS
	// (claimer) as forensic provenance, deliberately unindexed.
	engineID int64

	// stateCacheBytes is the SetStateCacheSize override of the budget of the replica's one resolve cache,
	// which every shard's Linker reads through; 0 derives it from the process's memory limit (stateCacheBudget).
	stateCacheBytes int64

	// flowsStartedCount / flowsEndedCount are cheap in-memory lifecycle counts, incremented
	// alongside the dwarf_flows_started / dwarf_flows_ended OTEL counters but WITHOUT needing a
	// configured meter (the default provider is a no-op). They let an embedder read in-flight work
	// (started - ended) with an atomic load - no OTEL reader, no query. Used by the open-loop load
	// generator to bound outstanding flows; generally useful for backpressure.
	flowsStartedCount atomic.Int64
	flowsEndedCount   atomic.Int64

	// cohortLocks serializes a fan-out cohort's arrivals THROUGH THIS PEER, so its siblings queue on a Go
	// lock holding no database connection instead of on the spawn step's row lock each holding one. The
	// arrival bump takes that row's write lock and holds it to COMMIT (processStep's transition tx, and the
	// failure-side twin in failStep), so without this every sibling of a wide cohort ties up a connection in
	// lock-wait - measured at fan-out width 64 as ~85% of the pool trapped and throughput inverting past
	// width 16. Taking the stripe before the tx opens (the spawn id is known from the claim) drops the pool's
	// exposure to one in-flight arrival per peer, R across the fleet; the row stays the cross-peer source of
	// truth, so the count and the fan-in trigger are unchanged. A fixed striped array, never resized - see
	// cohortLockStripe. Only the DIRECT (bottom) cohort level is taken here; propagateCohortFailure's ancestor
	// bumps walk UP and stay DB-serialized, which is why one leaf-level stripe per worker cannot form a cycle.
	// Each stripe is a FIFO lock whose waiters keep their leases alive while queued - see cohortStripe.
	cohortLocks [cohortLockStripes]cohortStripe

	// claims holds the steps this replica has a claim CAS in flight on - the intra-peer half of candidate
	// de-duplication, whose cross-peer half is the step_id partition (see partitionPredicate).
	//
	// It exists because the selection predicate filters COMMITTED state: a claim that has been issued but
	// not committed still reads `pending` with a free lease, so the refiller's next pass legitimately
	// re-selects it and a second worker pays a full round trip to lose the CAS. No SQL filter can see an
	// uncommitted write; only this process knows. Measured at R=1 (no peers at all): 7.3% of claims wasted
	// when scanning flat-out, and 0% once passes are spaced past the in-flight window - so the whole
	// baseline is this one effect.
	//
	// Same move as cohortLocks: keep the database row as the source of truth and use a Go structure to
	// avoid paying a round trip to rediscover what this process already knows. Strictly ADVISORY - the CAS
	// remains the only thing that grants a step, so a missed entry costs one wasted round trip and a stale
	// one costs a skipped candidate the next pass re-selects; neither is a correctness question, which is
	// what keeps this consistent with the cache's hints-not-ownership rule. The two-generation window holds
	// a reservation 1-2s (over the max scan floor between refill passes, so a step is never re-selected
	// while its own claim is still in flight) and expires it with no per-entry sweep - see the package doc.
	claims *claimstracker.Tracker

	// Peer discovery: one Sonar per shard owns this replica's row in that shard's dwarf_peers registry and
	// publishes what reading it implies - how many replicas hold connections to the shard (its pool
	// divisor) and which residue class of step_id this replica selects there (its work divisor). Built in
	// Startup before anything is sized from them, read-only thereafter; sonarCancel stops their loops. See
	// peers.go and internal/peers.
	sonars      map[int]*peers.Sonar
	sonarCancel context.CancelFunc
	sonarPool   sync.WaitGroup
	// reconcileStop ends the loop that keeps the derived pool sizes in step with what the Sonars observe.
	reconcileStop   chan struct{}
	reconcileWorker sync.WaitGroup
	// lastAppliedSlots is the per-shard role (slotsOn) the pools were last derived with, to skip no-op
	// recomputes. Written and read only under poolsLock, so a plain map needs no synchronization of its
	// own. Per shard because the roles are: a fleet change on one shard must not re-push every other
	// shard's unchanged sizes, and an unchanged one must not mask a change elsewhere.
	lastAppliedSlots map[int]int
	// roles is this replica's standing on each shard: whether it dispatches there - the doorbell's gate and
	// pickShard's preference - and how many steps its workers are processing there. Startup builds a fresh
	// map per run and PUBLISHES it through the pointer: a host still serving through a Shutdown/Startup
	// restart reads it from Create, and a map assigned in place under that read is a fatal throw. Each
	// published map is read-only; its dispatch flags are flipped by applyRole under poolsLock, and its
	// in-flight counts are moved lock-free by the workers. Read through dispatchFlag and inFlightOn.
	roles atomic.Pointer[map[int]*shardRole]
	// heldDivisors is, for each shard this replica stopped dispatching while its workers still had steps in
	// flight there, the divisor its held pool was last sized with: the pool and turnstile keep a share until
	// those steps finish. Owned by poolsLock.
	heldDivisors map[int]int
	// fleetOverLimit is whether checkFleetFits last found the fleet over a shard's server connection limit.
	// Owned by poolsLock.
	fleetOverLimit map[int]bool
	// poolsLock serializes every APPLICATION of a pool size (recomputePools). Deduping the recompute on
	// lastAppliedSlots is not enough: two registry readings microseconds apart during a rolling deploy each
	// derive a different split, and nothing orders their pushes, so the wider-share sizes can land AFTER the
	// narrower ones and leave every replica over-connecting the shard - sticky until the next fleet change.
	// Holding this across read-of-roles + push makes the last writer win with the value it actually read.
	// Lock order: poolsLock -> shardsLock (the Sonars' readings are lock-free loads).
	poolsLock sync.Mutex

	// Database
	db database.ShardSet
	// testConnCap caps each shard's pool (and reserves that much global connection budget) in test mode,
	// so parallel per-test engines against one server stay under its max_connections. Seeded from
	// defaultTestConnCap; 0 disables it. Only ever consulted when testHashedID != "".
	testConnCap int
	// testHashedID is the hashed test database id when the engine is in test mode (NewEngineUnderTest,
	// keyed by the test name it was given), empty in production. It becomes Config.TestID at Open, wrapping
	// each shard in an isolated test database. Written once at construction, read only after
	// started.Load()==true, so the atomic started flag is the happens-before barrier.
	testHashedID string

	// Candidate cache and worker pool. `workers` is the pool's MAXIMUM (the lease-margin ceiling, or an
	// explicit SetWorkers); the pool is grow-on-demand: initRuntime spawns `workersResident` (2 x conns,
	// floored at 64), and a worker that takes a candidate and leaves nobody idle spawns one more, up to the
	// max. The candidate cache and its refill scan are sized from `workersDispatch` (8 x conns), never from
	// the max: a worker blocked in a long ExecuteTask holds no connection, so the ceiling can be large
	// without the cache paying for it.
	cache candidates.Cache
	// crew runs processStep on the candidates it pops. The engine supplies both sizing numbers.
	crew *workers.Crew
	// turnstiles orders access to each shard's connections by band and then by how long the job asking has
	// been at the database, so that work already under way finishes ahead of work arriving after it. Sized at
	// turnstilePassesPerConn turns per connection, so every path that changes a pool must re-size it - the
	// same rule the worker ceiling, the cache and the refill interval already obey.
	turnstiles *turnstile.Set
	// drainStop is closed at the top of drainRuntime, before the crew is waited on.
	drainStop chan struct{}
	// workersDispatch is the candidate cache's sizing input, and workersResident the eagerly spawned worker
	// count the crew never shrinks below; both resolved at Startup.
	workersDispatch int
	workersResident int
	// dbPhaseWorkers counts how many workers are inside a metered DATABASE PHASE right now, indexed
	// 0=enter 1=exit. See enterDBPhase for why it is not derivable from anything that already existed.
	// There is deliberately no high-water companion: a lifetime peak is a constant within hours of
	// startup, while max_over_time on this gauge gives a WINDOWED peak, which is the useful form.
	dbPhaseWorkers [2]atomic.Int64

	// shardRTTMs is each shard's round-trip time, probed once at Startup and HELD, so the worker ceiling
	// can be re-derived whenever a shard's pool changes - the ceiling is a function of the pool. A failed
	// probe is held as defaultRTTMs.
	shardRTTMs map[int]float64
	// shardSizing is each shard's connection budgets for the run, computed at Startup from its spec, its
	// probed RTT and its server's max_connections. Published under shardsLock with shardRTTMs.
	shardSizing map[int]shardSizing

	// inFlightStateBytes / inFlightStateSteps are the state payload this replica is holding across host
	// calls RIGHT NOW, and how many carriers hold it - the two halves of dwarf_state_in_flight_bytes and
	// _steps. Maintained by processStep around the ExecuteTask call only (see holdState).
	//
	// They measure the JSON the carrier was built FROM, not the decoded maps that actually occupy the
	// heap - the decoded size cannot be had cheaply (re-marshalling costs a full encode per dispatch and
	// still reports wire size; walking the structure is expensive and approximate). That is the right
	// proxy precisely BECAUSE it is invariant to how state is represented in memory: read against the Go
	// heap it yields the decode expansion factor, which is the quantity every held-state decision turns on
	// and which no instrument currently reports.
	inFlightStateBytes atomic.Int64
	inFlightStateSteps atomic.Int64

	// Candidate supply: one piston per shard, each cycling its own database on its own clock with no
	// barrier against its peers. They share the planner (each shard's tally retained, so every shard plans
	// against the merged picture without waiting for the slowest) and the candidate cache. The maps are built in initRuntime before started flips true
	// and are read-only thereafter.
	//
	// pistonCancel stops them: piston.Run ends on ctx alone, and the lifetime ctx is deliberately not
	// cancelled until every other goroutine has drained, so the pistons need a cancellable child of it.
	planner      *planner.Planner
	pistons      map[int]*piston.Piston
	pistonCancel context.CancelFunc
	pistonPool   sync.WaitGroup
	// pistonsReady is set once every piston is in the map. The metrics callback is registered before the
	// pistons are built, so it reads the map only after this.
	pistonsReady atomic.Bool

	// Per-band backlog gauges: the shard's leader refreshes them in the background (backlogLoop) and the
	// metrics callback reads the cache. backlogBands remembers every band reported per shard, so one that
	// empties reads zero rather than vanishing. backlogWanted is set by the first collection; the loop
	// queries nothing before it.
	backlogMu      sync.Mutex
	backlogByShard map[int]shardBacklog
	backlogBands   map[int]map[int]bool
	backlogWanted  atomic.Bool
	backlogCancel  context.CancelFunc
	backlogWorker  sync.WaitGroup

	// Recovery goroutine: runs the defense-in-depth parked-step wedge sweep on its own slow cadence,
	// off the hot poll path.
	recoveryStop   chan struct{}
	recoveryWorker sync.WaitGroup

	// Reaper goroutine: deletes flows whose delete_after_ms window has elapsed, on its own ~1min ticker.
	reaperStop   chan struct{}
	reaperWorker sync.WaitGroup

	// Await latch: the board callers park on, and the detector goroutine that asks the shards which of
	// those flows have settled. A stop this replica commits reaches the board instantly through
	// signalStop; the detector is what makes a PEER's stop observable.
	latches     *latch.Board
	latchStop   chan struct{}
	latchWorker sync.WaitGroup

	// Parsed-graph caches, in two layers. graphCache maps a flow to its entry so a dispatch reads no graph
	// column; sharedGraphs owns the entries, keyed by the flow's graph_hash, so every flow of one graph points at
	// one parse. The graph JSON is frozen at flow creation, so neither ever goes stale.
	graphCache   *lru.Cache[graphCacheKey, weak.Pointer[cachedGraph]]
	sharedGraphs *lru.Cache[sharedGraphKey, *cachedGraph]
	// State-ref linkers, one per shard: they decide which state fields a successor step stores by reference
	// rather than by value, and resolve those references back. Per shard because a step id is only unique
	// within one. They share stateCache, one budget of resolved bytes for the whole replica, so it goes to
	// the shards this replica dispatches rather than being split among shards it may never run.
	linkers    map[int]*staterefs.Linker
	stateCache *staterefs.Cache

	// Lifecycle
	started        atomic.Bool
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc

	// Recovery/await/reap timing. Effectively constants in production (defaulted in NewEngine, then
	// read-only); a white-box test shortens one directly, before Startup for the two read once at Startup
	// (reapInterval, recoveryInterval), so an otherwise minutes-long path is observable.
	leaseMargin         time.Duration // added to a step's budget when sizing the crash-recovery lease (30s)
	latchSweepInterval  time.Duration // Await latch detector cadence - the bound on cross-replica wake (50ms)
	awaitDefaultBudget  time.Duration // how long Await blocks when the caller's ctx carries no deadline (15m)
	recoveryInterval    time.Duration // recovery sweep tick; the park detectors run every heavyRepairEvery-th; read once at Startup (1m)
	parkWedgeThreshold  time.Duration // min age before a parked step is treated as wedged (5m)
	orphanFlowThreshold time.Duration // quiet time before a stranded flow with no step to replay is reported orphaned (5m)
	deletionGrace       time.Duration // DeleteOnCompletion linger window before reap (1m)
	reapInterval        time.Duration // reaper goroutine tick; read once at Startup (1m)
	leaseRecoveryBatch  int           // ids per lease-recovery reset, embedded as a literal IN list (1024)
	maxPayloadBytes     int           // largest JSON value written into one payload column (maxPayloadBytes)
	// persistBackoff is the outcome-write retry schedule, short and exponential because the errors it
	// exists for resolve in SECONDS. A minutes-long backoff would be slow in both directions - slow to
	// recover from a blip that already cleared, and slow to report a permanent failure we could have named
	// after the second attempt. A per-engine field (not a package var) so a test shortens it on its own
	// engine without racing a parallel peer.
	persistBackoff []time.Duration
	// persistLeaseExtension is how far ahead persist renews a step's lease before each retry (60s): the first half
	// covers the retry's sleep and write (persistRetryWindow), the second leaves failOnPersistError room to fail the
	// step. Without it a task that consumed most of its time budget has only `leaseMargin` left, so the backoff would
	// put us PAST the lease: a peer re-claims the step, re-executes the task, and our late write is fenced and
	// discarded - precisely the re-execution persist exists to prevent. A crash mid-ladder strands the step for up to
	// this long - or for what remained of its original lease, if that was longer, since a renewal never shortens a
	// lease - before lease recovery can take it.
	persistLeaseExtension time.Duration

	// Test-only instrumentation seams (see engineundertest.go), delegated to a seamster.Seamster constructed enabled
	// under testing.Testing() in NewEngine. Every consult is a lock-free bool read in production. The
	// per-flow flow-row-write counter is a counting checkpoint on this same seams (Visits); it holds no
	// engine-side state.
	seams *seamster.Seamster
}

// defaultTestConnCap seeds every engine's test-mode per-shard connection cap (see the testConnCap field).
// A test that must observe the real derived pool sizes (the pool-sizing tests) sets e.testConnCap = 0 on
// its own engine before Startup. Inert outside test mode (only consulted when testHashedID != "").
const defaultTestConnCap = 4

// NewEngine creates a new workflow engine.
func NewEngine() *Engine {
	e := &Engine{
		logger:      slog.New(slog.DiscardHandler),
		claims:      claimstracker.New(),
		testConnCap: defaultTestConnCap,
	}
	e.workers.Store(64)
	e.timeBudgetMs.Store(int64(2 * time.Minute / time.Millisecond))
	e.defaultPriority.Store(100)
	e.SetEngineID(int64(rand.Uint64() >> 1)) // positive, 63 bits of entropy
	// Empty, not seeded: "nothing derived yet" per shard. Startup reads each shard's count, sizes its pool
	// directly, and records it here; the reconcile loop then dedupes against that.
	e.lastAppliedSlots = map[int]int{}
	e.fleetOverLimit = map[int]bool{}
	e.heldDivisors = map[int]int{}
	e.leaseMargin = 30 * time.Second
	// The detector's cost scales with concurrent AWAITERS, not with step throughput - one small indexed
	// IN-lookup per shard holding one, and nothing at all when nobody is waiting - so the cadence is picked
	// from what a synchronous caller wants rather than from what the engine can afford. 50ms puts a
	// cross-replica Run at +25ms on average, which is under the noise of the work it is waiting for.
	e.latchSweepInterval = 50 * time.Millisecond
	// Applied only when the caller names no deadline of its own. Long by design: a parked caller costs no
	// query (it rides the detector's existing per-shard lookup), so the only thing a short budget would buy
	// is cutting short a wait somebody legitimately asked for.
	e.awaitDefaultBudget = 15 * time.Minute
	e.persistBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	e.persistLeaseExtension = 60 * time.Second
	e.recoveryInterval = time.Minute
	e.parkWedgeThreshold = 5 * time.Minute
	e.orphanFlowThreshold = 5 * time.Minute
	e.deletionGrace = 1 * time.Minute
	e.reapInterval = 1 * time.Minute
	e.leaseRecoveryBatch = 1024
	e.maxPayloadBytes = maxPayloadBytes
	e.seams = seamster.New(testing.Testing())
	return e
}

// --- Configuration setters ---
//
// Setters split into Live (callable any time) and construction-time-only (rejected after Startup).

// errSetAfterStartup is the error the construction-time-only setters return on a running engine.
func errSetAfterStartup(what string) error {
	return errors.New("%s cannot be changed after Startup", what, http.StatusConflict)
}

// ShardSpec declares one database shard: the facts about it the operator can readily provide, from
// which the engine derives its tuning (connection budget, placement weight).
type ShardSpec struct {
	// Index identifies the shard. Indices must be >= 1 and unique, but need not be contiguous (shards
	// 1 and 99 are fine). The index is encoded into every flow key created on the shard and drives
	// routing, so the index-to-DSN mapping must be identical across all replicas and stable across
	// restarts.
	Index int
	// DSN is the connection string of the shard's database (dialect auto-detected), used EXACTLY as
	// given - the engine never formats or rewrites it, so a percent-encoded credential (a password
	// "p@ss" written "p%40ss") survives intact. Each shard is declared with its own complete DSN. An empty
	// DSN is only valid in test mode, where the DSN is a template (a "%d" is replaced with the shard index,
	// which is what gives each shard its own isolated test database).
	DSN string
	// MaxConnections is the shard's connection budget: the connections the replicas that dispatch the
	// shard (up to two of them) share. Every other replica holds a reader pool of its own there - one
	// connection, two at most - outside the budget, and each dispatcher keeps at least two, so the shard's
	// total can run a little past it. Left at 0, the engine
	// derives it: 250, rising to 500 as the measured round-trip time to the database grows from 0.25 ms to
	// 1 ms, and never more than half the server's own max_connections (on SQLite, a fixed 8). Set it to give
	// a large database more (a 64-vCPU server gains throughput up to ~500 even when close), or a shared one
	// less. Shards that are databases on ONE server each take their own budget from that server's limit, so
	// set it explicitly when shards share a server. Declare the SAME value for a shard on every replica, as
	// with the shard set itself.
	MaxConnections int
	// Weight is the shard's share of new-flow placement, relative to the other shards. Left at 0, it is 1.
	//
	// While every non-cordoned shard's weight is 1, a replica places a new flow on one of the shards it
	// dispatches (over all non-cordoned shards only when it dispatches none), so flows spread by where the
	// Create calls arrive: a single replica creating every flow fills only its own shards. Any weight other
	// than 1 on any non-cordoned shard switches every replica to weighted placement over all non-cordoned
	// shards, in proportion to their weights; a flow placed on a shard its creating replica does not
	// dispatch waits for that shard's dispatcher to pick it up. Give a larger database a larger weight in a
	// mixed fleet.
	Weight float64
	// Cordoned excludes the shard from new-flow placement. Everything already resident proceeds
	// normally: existing flows keep executing, and subgraph children, thread continuations (Continue),
	// and forks - all shard-pinned - are still created on it. Use for retiring or overloaded shards.
	Cordoned bool
}

// SetShard registers a database shard. Call once per shard; see ShardSpec for field semantics. When no
// shard is registered, Startup opens a single default shard 1 (in test mode, an isolated in-memory
// database).
//
// Construction-time only: shards are opened and migrated at Startup and the set is immutable for the
// engine's life, so a call on a running engine is rejected. Changing the shard set requires a coordinated
// restart of every replica (a maintenance window): a flow created on a shard unknown to a peer replica is
// unroutable (404) there.
func (e *Engine) SetShard(spec ShardSpec) error {
	if e.started.Load() {
		return errSetAfterStartup("shards")
	}
	if spec.Index < 1 {
		return errors.New("shard index must be at least 1", http.StatusBadRequest)
	}
	if spec.MaxConnections < 0 {
		return errors.New("max connections must be >= 0", http.StatusBadRequest)
	}
	if spec.Weight < 0 || math.IsNaN(spec.Weight) || math.IsInf(spec.Weight, 0) {
		return errors.New("weight must be a finite number >= 0", http.StatusBadRequest)
	}
	e.shardsLock.Lock()
	defer e.shardsLock.Unlock()
	if _, ok := e.shardSpecs[spec.Index]; ok {
		return errors.New("shard %d is already set", spec.Index, http.StatusConflict)
	}
	if e.shardSpecs == nil {
		e.shardSpecs = map[int]ShardSpec{}
	}
	e.shardSpecs[spec.Index] = spec
	return nil
}

// SetWorkers is an expert override that pins the maximum number of worker goroutines, replacing the
// derived default. The default is the lease-margin ceiling: the largest pool that keeps a synchronized
// completion storm (every in-flight task released at once by a recovering downstream) draining inside
// the crash-recovery lease margin, derived at Startup from each shard's connection budget and its
// measured round-trip time. It contains no assumption about task duration, so it is correct for any
// workload; the pool grows into it only on demand, so a short-task deployment never pays for the
// headroom.
//
// Operators normally never call this. Reasons to: memory (the ceiling can be tens of thousands of
// workers, and each in-flight step also holds its state map - a bound the engine cannot see); a
// deliberately smaller global concurrency cap; deterministic tests (SetWorkers(1) serializes dispatch);
// and benchmark sweeps. Setting it ABOVE the ceiling is allowed - an operator may consciously trade the
// risk of duplicate task execution in a storm for long-task throughput - and is logged as a warning at
// Startup. SetWorkers(0) is a valid shape: a replica that creates, awaits, and serves reads but never
// executes tasks. It holds two connections to each shard and is never chosen as any shard's dispatcher. Construction-time only: the pool bound is fixed at Startup, so a call on a running
// engine is rejected.
func (e *Engine) SetWorkers(n int) error {
	if e.started.Load() {
		return errSetAfterStartup("workers")
	}
	if n < 0 {
		return errors.New("workers must be >= 0", http.StatusBadRequest)
	}
	if n > math.MaxInt32 {
		return errors.New("workers must fit in an int32", http.StatusBadRequest)
	}
	e.workers.Store(int32(n))
	e.workersSet.Store(true)
	return nil
}

// SetEngineID pins this replica's identity, overriding the random identifier minted per instance. The
// identity is what a replica registers in the shared peer registry, from which the replicas agree on which
// of them dispatch each shard. The default is random and fresh on every restart, which is correct for the
// common case (including several engines in one process, which must count as distinct replicas) but leaves a
// stale registry entry behind when a replica crashes: the entry lingers until it ages out, transiently
// over-counting replicas and, if the crashed replica dispatched a shard, holding its place there. Pinning a value that is STABLE
// across a replica's restarts (for example, derived from the deployment's own per-instance identity) lets a
// restarted replica reuse its entry instead, so a crash-restart never inflates the count.
//
// The id must be positive (0 is reserved) and UNIQUE among replicas concurrently sharing the same databases:
// two live replicas with the same id register as one, under-counting replicas and over-sizing pools. Leave it
// unset (random) unless a stable, unique value is genuinely available - a wrong stable id is worse than the
// random default. Construction-time only.
func (e *Engine) SetEngineID(id int64) error {
	if e.started.Load() {
		return errSetAfterStartup("engine id")
	}
	if id <= 0 {
		return errors.New("engine id must be positive", http.StatusBadRequest)
	}
	e.engineID = id
	return nil
}

// SetStateCacheSize sets how much memory, in bytes, the engine may use to keep large state fields it has
// written or read, so the steps that read them next take them from memory rather than from the database. One
// cache serves all of the engine's shards; a field larger than the whole size is not kept. Each field counts
// as at least 4 KB, at most 65,536 are kept, and a field unread for 5 minutes expires, its memory released by
// a later store or lookup.
//
// The default is a tenth of the memory this process may use: GOMEMLIMIT when it is set; otherwise, on
// Linux, the tightest of the process's cgroup memory limits and physical memory. When none of these can be
// read, the default is 1 GiB. Several engines in one process each take their own share, so set the size
// explicitly there. Go's garbage collector lets the heap grow to about twice its live size before collecting,
// so a full cache can occupy up to twice its size unless GOMEMLIMIT is set - setting GOMEMLIMIT is the
// recommended way to bound the whole process.
//
// A smaller size costs database reads, not correctness. Construction-time only.
func (e *Engine) SetStateCacheSize(bytes int64) error {
	if e.started.Load() {
		return errSetAfterStartup("state cache size")
	}
	if bytes <= 0 {
		return errors.New("state cache size must be positive", http.StatusBadRequest)
	}
	e.stateCacheBytes = bytes
	return nil
}

// SetTimeBudget sets the default duration for a single task execution, used by any flow that does not
// override it via FlowOptions.TimeBudget. Live: read fresh on each Create (existing flows keep the budget
// frozen at their own Create). It must be positive.
//
// The lower bound is not cosmetic. The budget is the ExecuteTask call's context deadline and part of the
// size of the step's crash-recovery lease - so a non-positive default hands every new flow a deadline
// that has already passed, failing every task instantly, engine-wide and silently. Unlike Priority and
// FairnessWeight, 0 cannot mean "unset" here: this setter IS the default. The floor is a millisecond
// rather than a nanosecond because the budget is persisted in MILLISECONDS: a positive duration below
// 1ms truncates to zero and produces exactly the expired-deadline case.
func (e *Engine) SetTimeBudget(d time.Duration) error {
	if d < time.Millisecond {
		return errors.New("time budget must be at least 1ms (it is stored in milliseconds)", http.StatusBadRequest)
	}
	e.timeBudgetMs.Store(int64(d / time.Millisecond))
	return nil
}

// SetDefaultPriority sets the default priority for new flows: an integer >= 1, lower runs first. Live:
// read fresh on each Create, so it takes effect on a running engine immediately.
//
// A value below 1, or one too large to store as a 32-bit integer, is rejected with an error: a flow given a
// non-positive priority would never be scheduled. FlowOptions.Priority is validated separately at Create,
// where 0 means "unset, take this default".
func (e *Engine) SetDefaultPriority(p int) error {
	// The pistons select a band only through predicates that assume it is positive, so a non-positive
	// priority leaves the step pending forever, invisible to selection. An int beyond int32 would wrap
	// negative in the column and hang the same way.
	if p < 1 {
		return errors.New("default priority must be >= 1", http.StatusBadRequest)
	}
	if p > math.MaxInt32 {
		return errors.New("default priority must fit in an int32", http.StatusBadRequest)
	}
	e.defaultPriority.Store(int32(p))
	return nil
}

// setRefillInterval is the benchmark override that PINS every piston's cycle period to d, replacing the
// value the engine derives from the cache capacity and the pool (deriveRefillInterval); d <= 0 restores derivation. Reached
// from the bench through internal/engineseams, never by a host.
//
// The override exists for scan-rate sweeps: the period is measured, not tuned, so finding its optimum needs
// to hold it at a series of fixed values - INCLUDING values below the 20ms minimum gap, since the
// unlimited-scanning arm is one of the measured reference points (it costs 8% candidate churn and a 77% worse
// p99 while buying no throughput, and that number has to stay reproducible). So a pinned interval also lowers
// the pipeline's MinGap to match when it is the tighter of the two; restoring derivation restores the default
// gap. Without that the fuse would silently clamp every sub-20ms arm of a sweep to 20ms.
//
// Live, under poolsLock: recomputePools reads the override and then pushes cadences, so a store landing
// between the two would be overwritten by derived cadences and stay lost until the next role change.
func (e *Engine) setRefillInterval(d time.Duration) error {
	e.poolsLock.Lock()
	defer e.poolsLock.Unlock()
	e.refillIntervalOverride.Store(int64(d))
	if e.started.Load() {
		e.recomputeRefillIntervals()
	}
	return nil
}

// FlowsStarted returns the count of flows this engine has started (Create/Continue/Fork/subgraph),
// a cheap atomic read that needs no configured meter. FlowsEnded is its completion counterpart;
// started - ended approximates in-flight work for backpressure.
func (e *Engine) FlowsStarted() int64 { return e.flowsStartedCount.Load() }

// FlowsEnded returns the count of flows this engine has completed/failed/terminated/cancelled. See FlowsStarted.
func (e *Engine) FlowsEnded() int64 { return e.flowsEndedCount.Load() }

// SetHost registers the host the engine reaches the outside world through: it loads graphs and executes
// tasks. Host has exactly two methods, LoadGraph and ExecuteTask, both required; the engine sends nothing
// to its peers and has no other host callback. Construction-time only.
func (e *Engine) SetHost(h Host) error {
	if e.started.Load() {
		return errSetAfterStartup("host")
	}
	e.host = h
	return nil
}

// SetMeterProvider sets the OpenTelemetry MeterProvider the engine builds its dwarf_* instruments from.
// Defaults to the global otel.GetMeterProvider() (the no-op provider unless the host configures the OTEL
// SDK). The engine creates instruments under the "github.com/microbus-io/dwarf" scope; the provider's
// Resource carries the host service's identity. Construction-time only - the engine resolves the meter
// once at Startup.
func (e *Engine) SetMeterProvider(mp metric.MeterProvider) error {
	if e.started.Load() {
		return errSetAfterStartup("meter provider")
	}
	e.meterProvider = mp
	return nil
}

// SetTracerProvider sets the OpenTelemetry TracerProvider the engine builds its spans from. Defaults to
// the global otel.GetTracerProvider(). The engine emits one span per flow and one span per task, nested to
// mirror the call structure, under the "github.com/microbus-io/dwarf" scope, and its database layer one per
// query; the provider's Resource carries the host's identity. The host injects only the provider - it writes
// no span or context code. Construction-time only - the engine resolves the tracer once at Startup.
//
// Install the OpenTelemetry SDK before Startup. If no SDK is installed by then, the engine traces through a
// no-op provider rather than OpenTelemetry's global placeholder, which allocates a span per call even when
// nothing records; a provider installed after Startup is never reached.
func (e *Engine) SetTracerProvider(tp trace.TracerProvider) error {
	if e.started.Load() {
		return errSetAfterStartup("tracer provider")
	}
	e.tracerProvider = tp
	return nil
}

// SetLogger sets the structured logger. The engine logs through the *Context variants
// (DebugContext/InfoContext/WarnContext/ErrorContext) so a handler that reads the context - e.g. the
// otelslog bridge - can correlate each record with the active step span. A host routes logs to OTEL by
// passing a logger whose handler bridges there. Defaults to a discard logger: until a logger is injected
// the engine (and its sequel DB layer) stay silent rather than writing to the application-owned
// slog.Default(). A nil logger resets to that silent default. Construction-time only - the engine resolves
// the logger once at Startup.
func (e *Engine) SetLogger(l *slog.Logger) error {
	if e.started.Load() {
		return errSetAfterStartup("logger")
	}
	if l == nil {
		l = slog.New(slog.DiscardHandler)
	}
	e.logger = l
	return nil
}

// SetDebugLogger is a convenience that wires a human-readable text logger to stderr at debug level -
// shorthand for SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level:
// slog.LevelDebug}))). It is meant for development and test runs where you want to see the engine's (and
// its sequel DB layer's) internal logging without standing up an OTEL pipeline. Output goes to stderr, not
// stdout, so it never mixes with a program's data stream - the standard convention for diagnostic logs.
// Because it routes through SetLogger, it counts as an explicitly-set logger, so it also reaches sequel via
// the engine's existing SetLogger wiring (sequel's migration logs appear too). Construction-time only.
func (e *Engine) SetDebugLogger() error {
	return e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

// --- Lifecycle ---

// Startup initializes the engine: opens database connections, runs migrations,
// and starts worker goroutines.
func (e *Engine) Startup(ctx context.Context) error {
	if e.started.Load() {
		return errors.New("engine is already started", http.StatusConflict)
	}
	if e.host == nil {
		return errors.New("host is required", http.StatusBadRequest)
	}
	// Per-shard pool sizes: each shard's budget derives from its own spec, distance and server limit
	// (shardBudget), split across the replicas that DISPATCH that shard, while every other replica holds a small reader pool there. Which
	// replicas dispatch comes from the shared dwarf_peers registry, which needs open connections to read -
	// so the shards first open at a small BOOTSTRAP pool (enough to register + probe + read the fleet,
	// which is all any pre-dispatch work needs), then, once the roles are known, every pool is resized to
	// its derived share BEFORE a single worker dispatches. There is no async grace window: R is
	// known before the replica takes on work, so a cold-starting fleet's rows settle in the registry and
	// one read yields the converged count - no partial-count over-connect. Lazy fill means the bootstrap
	// ceiling barely materializes (a handful of connections), so the whole fleet's startup stays well
	// under any server's connection limit.
	e.shardsLock.Lock()
	shards := make(map[int]database.ShardConfig, len(e.shardSpecs))
	for idx, spec := range e.shardSpecs {
		shards[idx] = database.ShardConfig{DSN: spec.DSN, MaxIdleConns: startupBootstrapConns, MaxOpenConns: startupBootstrapConns}
	}
	e.shardsLock.Unlock()
	if len(shards) == 0 {
		// No shard registered: the single default shard, opened at the same bootstrap size.
		shards[1] = database.ShardConfig{MaxIdleConns: startupBootstrapConns, MaxOpenConns: startupBootstrapConns}
	}

	// TestConnCap engages only in test mode (TestID != ""): it caps each shard's pool and reserves a
	// per-driver global connection budget so many parallel per-test engines against one server stay under
	// its max_connections. Passed only when in test mode so a production Open never carries it.
	testConnCap := 0
	if e.testHashedID != "" {
		testConnCap = e.testConnCap
	}
	// Resolved before the shards open, so the migrations trace through the same provider as everything after.
	tp := e.initTracer()
	err := e.db.Open(ctx, database.Config{
		Shards:         shards,
		TestID:         e.testHashedID,
		TestConnCap:    testConnCap,
		Logger:         e.logger,
		TracerProvider: tp,
		MeterProvider:  e.meterProvider,
	})
	if err != nil {
		return errors.Trace(err)
	}

	// Measure the RTT to each shard (a few SELECT 1s on connections just opened) and hold it: the worker
	// ceiling is a function of the shard's POOL, which changes when the fleet changes, so the ceiling is
	// re-derived on that event (recomputeWorkerCeiling) rather than frozen here. The map is published under
	// shardsLock because its reader is the reconcile loop, and a Shutdown/Startup restart reassigns it - an
	// unsynchronized map read/write is a fatal throw, not a recoverable panic.
	//
	// The budget takes the RAW probe (an unprobed shard takes the near budget), while the worker ceiling
	// takes the same-zone fallback - each the safe direction for what it sizes.
	rtts := make(map[int]float64, len(shards))
	sizing := make(map[int]shardSizing, len(shards))
	e.shardsLock.Lock()
	declared := maps.Clone(e.shardSpecs)
	e.shardsLock.Unlock()
	for idx := range shards {
		db, dbErr := e.db.Shard(idx)
		if dbErr != nil {
			continue
		}
		rttMs := probeRTT(ctx, db)
		serverMax := readMaxConnections(ctx, db)
		if serverMax == 0 && db.DriverName() != "sqlite" {
			// Fails open to the unclamped budget, which on a server configured small over-connects it.
			e.logger.WarnContext(ctx, "Reading the server's connection limit failed; the shard's connection budget is not clamped to it",
				"shard", idx)
		}
		sizing[idx] = sizeShard(declared[idx], rttMs, serverMax, db.DriverName()) // zero-value spec = the default shard's sizing
		if rttMs <= 0 {
			rttMs = defaultRTTMs // probe failed: fall back to the measured same-zone constant
		}
		rtts[idx] = rttMs
		e.logger.DebugContext(ctx, "Shard sized", "shard", idx, "rttMs", rttMs, "maxConnections", serverMax,
			"budget", sizing[idx].budget)
	}
	e.shardsLock.Lock()
	e.shardRTTMs = rtts
	e.shardSizing = sizing
	e.shardsLock.Unlock()
	// Forget the last run's verdicts, so a fleet still over the limit after a restart is warned about again
	// rather than taken as already reported.
	e.poolsLock.Lock()
	e.fleetOverLimit = map[int]bool{}
	e.heldDivisors = map[int]int{}
	e.poolsLock.Unlock()
	// One role per open shard, rebuilt per run and filled in by the sizing loop below, before anything that
	// could ring a doorbell is running.
	roles := make(map[int]*shardRole, len(shards))
	for idx := range shards {
		roles[idx] = &shardRole{}
	}
	e.roles.Store(&roles)

	// Announce this replica on every shard, wait for peers to notice, and read each shard's fleet back -
	// then size every pool from the derived budget split by THAT shard's count. Those are the real sizes,
	// taken before any worker dispatches, so the replica takes on work with pools already correct for the
	// fleet it is in: no async grace window and no partial-count over-connect.
	//
	// The announce-then-wait ordering is what keeps the join from exceeding a shard's connection budget even
	// momentarily - peers hold pools sized for the fleet WITHOUT this replica until they read again, so it
	// shrinks them first and grows into the space afterward. It costs one wait for the whole fleet, since the
	// shards join in parallel.
	e.buildSonars()
	e.joinFleet(ctx)
	// A fresh turnstile set per run, like the cache and the planner: counts left over from a previous
	// Startup would bound this one against pools it no longer has.
	e.turnstiles = turnstile.NewSet()
	totalConns := 0
	for _, idx := range e.db.Indices() {
		if _, dbErr := e.db.Shard(idx); dbErr != nil {
			continue
		}
		// This replica's role on the shard, from the ranking Join just read. A shard it does not dispatch
		// gets the reader pool and a piston that initRuntime starts idle.
		slots := e.slotsOn(idx)
		e.dispatchFlag(idx).Store(slots > 0)
		idle, open := shardPool(sizing[idx].budget, slots)
		// Through the shard set, which applies the test-mode cap; a push onto the shard's own handle would not.
		e.db.SetShardPool(idx, idle, open)
		if e.seams.Enabled() { // Enabled gates the assembled name and the boxed value in production
			e.seams.Variable(engineseams.Join(engineseams.VariablePoolIdle, strconv.Itoa(idx)), idle)
		}
		// Sized here rather than in initRuntime because it is derived from the pool this loop just set,
		// and the workers initRuntime starts must never see an unsized shard - which, this failing open,
		// would dispatch entirely unordered rather than stalling.
		e.turnstiles.Resize(idx, turnstilePassesPerConn*open)
		e.lastAppliedSlots[idx] = slots
		if slots > 0 {
			// Only a dispatched shard's pool feeds the workers; a reader pool runs no step.
			totalConns += open
		}
	}
	// The candidate cache (and every refill scan) is sized from the aggregate connection budget: dispatch is
	// database-bound, so 8 x conns keeps the pool supplied for short tasks. The resident set is a smaller
	// floor the crew grows from on demand. Both are floored at 64 for the zero-config case.
	e.workersDispatch = max(64, workersPerConnBudget*totalConns)
	e.workersResident = max(64, residentWorkersPerConn*totalConns)
	e.recomputeWorkerCeiling(ctx)

	if err := e.initRuntime(); err != nil {
		// initRuntime fails only before it spawns anything, so what remains to undo is Startup's own work: the
		// registry rows joinFleet wrote, which peers would otherwise count for the fresh window, and the open
		// shards. Shutdown cannot do it - the engine never reported itself started.
		e.leaveFleet(context.Background())
		e.db.Close()
		return errors.Trace(err)
	}
	return nil
}

// Shutdown stops all worker goroutines and closes database connections. Idempotent: a call on an engine
// that is not running (never started, or already shut down) is a no-op, so it is safe to defer and to call
// more than once.
//
// Shutdown does not consult ctx: it waits for the steps in flight to finish (each task is bounded by its
// time budget), and a deadline or cancellation on ctx does not make it return sooner.
func (e *Engine) Shutdown(ctx context.Context) error {
	if !e.started.CompareAndSwap(true, false) {
		return nil
	}
	e.drainRuntime()
	e.db.Close()
	return nil
}

// initRuntime starts all goroutines and initializes runtime state. It returns an error only for a
// failure that would leave the engine half-supplied - see the piston build below.
func (e *Engine) initRuntime() error {
	e.lifetimeCtx, e.lifetimeCancel = context.WithCancel(context.Background())
	// How many workers to spawn eagerly. An explicit SetWorkers is a request for exactly that many, so it
	// is honored in full: the operator's number is the pool, not a ceiling the pool might never reach
	// (no-op tasks never park, so growth would never take it there). The DERIVED maximum is the
	// lease-margin ceiling - tens of thousands - and is emphatically not spawned up front; there the
	// resident set is the connection-derived floor and growth fills the rest on demand.
	maxWorkers := int(e.workers.Load())
	resident := maxWorkers
	if !e.workersSet.Load() {
		resident = min(e.workersResident, maxWorkers)
	}
	// The cache (and every refill scan, which reads up to its capacity) is sized from the DISPATCH count,
	// never from the worker maximum: a worker parked in a long ExecuteTask holds no connection and
	// dispatches nothing, so letting the ceiling size the cache would scan a backlog orders of magnitude
	// larger than the engine can ever claim. workersDispatch is always resolved here - Startup computes it
	// (max(64, ...), so never zero) before it calls initRuntime, which is its only caller - so there is no
	// unresolved-dispatch case to fall back from. It is capped by the worker maximum, not the resident set:
	// a pinned SetWorkers spawns its maximum in full, and the derived resident set is only a floor.
	e.cache.Init(min(maxWorkers, e.workersDispatch))
	e.drainStop = make(chan struct{})
	// One planner for the whole replica, one piston per shard. Reset rather than reused across a
	// Shutdown/Startup cycle: a tally left from the previous run would claim a band nobody is serving.
	e.planner = planner.New()
	e.pistonsReady.Store(false)
	e.pistons = make(map[int]*piston.Piston)
	e.backlogMu.Lock()
	e.backlogByShard = map[int]shardBacklog{}
	e.backlogBands = map[int]map[int]bool{}
	e.backlogMu.Unlock()
	e.backlogWanted.Store(false)
	e.linkers = make(map[int]*staterefs.Linker)
	e.recoveryStop = make(chan struct{})
	e.reaperStop = make(chan struct{})
	// The shared layer has no TTL: a graph's bytes never change, and expiring a hot graph would turn every
	// one of its flows into a miss at once.
	e.graphCache = lru.New[graphCacheKey, weak.Pointer[cachedGraph]](graphCacheFlows, 15*time.Minute)
	e.sharedGraphs = lru.New[sharedGraphKey, *cachedGraph](graphCacheGraphs, 0)
	// A fresh board per run: the previous one was closed at drain, and a closed board turns every Await
	// away. resolveStoppedFlows is non-nil, so New cannot fail.
	e.latches, _ = latch.New(e.resolveStoppedFlows)
	// The park rendezvous, wired only under test so a production board holds no hook and Latch pays one
	// atomic load. Fired unscoped and scoped by flow key, for the same reason signalStop fires both: a
	// scoped fire does not wake an unscoped waiter, and a test may be waiting for THIS caller to park while
	// a subgraph child's or a peer's Await parks alongside it.
	if e.seams.Enabled() {
		e.latches.SetOnPark(func(flowKey string) {
			e.seams.Checkpoint(e.lifetimeCtx, engineseams.CheckpointAwaitParked)
			e.seams.Checkpoint(e.lifetimeCtx, engineseams.Join(engineseams.CheckpointAwaitParked, flowKey))
		})
	}
	e.latchStop = make(chan struct{})
	e.started.Store(true)

	// The state cache exists before the gauge callback that reads it is registered.
	cacheBudget, cacheSource := e.stateCacheBudget()
	e.stateCache = staterefs.NewCache(cacheBudget)
	e.logger.InfoContext(e.lifetimeCtx, "State cache sized", "bytes", cacheBudget, "source", cacheSource)

	// Create the dwarf_* instruments and register the observable-gauge callback before workers start
	// emitting. Falls back to the global (no-op) provider when none was injected.
	err := e.initMetrics()
	if err != nil {
		e.logger.ErrorContext(e.lifetimeCtx, "Initializing metrics", "error", err)
	}

	// Build the pistons now that the cache is sized: each takes its own shard's handle, the shared planner
	// and the shared cache. A piston starts idle on a shard this replica does not dispatch, and on every
	// shard of an await-only replica (SetWorkers(0)): it claims no work, and its idling reports as much,
	// which is what keeps the replica out of that shard's candidate partition.
	idle := int(e.workers.Load()) == 0
	if e.seams.IsFault(engineseams.FaultInitRuntime) {
		e.unwindRuntime()
		return errors.New("injected fault: " + engineseams.FaultInitRuntime)
	}
	for _, idx := range e.db.Indices() {
		// A shard without a piston has no supply cycle, so nothing on it is ever dispatched while the replica
		// goes on advertising itself as alive there - a half-supplied engine reporting success. Startup
		// returns an error; use it. Both failures are near-impossible today (the shard set is already open,
		// the arguments are non-nil), which is exactly why continuing quietly is the wrong default: the only
		// way to reach here is a bug, and a loud one is cheaper than a degraded fleet.
		db, dbErr := e.db.Shard(idx)
		if dbErr != nil {
			e.unwindRuntime()
			return errors.New("resolving shard %d for its piston: %w", idx, dbErr)
		}
		// One state-ref Linker per shard, built from the same handle: it needs the shard's driver name (the
		// only dialect-dependent term in the size policy) and it caches resolved payload bytes under step
		// ids, which are unique only within a shard.
		e.linkers[idx] = staterefs.New(db.DriverName(), idx, e.stateCache)
		p, perr := piston.New(idx, db, e.planner, &e.cache)
		if perr != nil {
			e.unwindRuntime()
			return errors.New("building piston for shard %d: %w", idx, perr)
		}
		p.SetLogger(e.logger)
		p.SetSeams(e.seams)
		// The two halves of this shard's peer accounting, wired to each other through the engine rather than
		// through either package knowing the other: the piston selects within the residue class its shard's
		// Sonar publishes, and that Sonar stamps this replica as SERVING the shard only when the piston says
		// it is turning. Both are pure reads of live state, so neither captures anything that could go stale.
		p.SetPartitionFunc(func() (int, int, bool) { return e.partitionOn(idx) })
		// A cycle's two queries run in the one band above the common one, which the piston shares only with
		// this shard's Sonar (below). Both are bounded by construction - the piston cycles on a derived period
		// and takes two turns per cycle, the Sonar beats on a fixed cadence - so neither can flood the band. The
		// piston needs to be there because a cycle that queued behind the dispatch it feeds would starve the
		// workers it fills for.
		p.SetContextFunc(func(ctx context.Context) context.Context {
			return e.turnstiles.ContextWithPriority(ctx, idx, priorityRefill)
		})
		if s := e.sonarFor(idx); s != nil {
			s.SetEvidence(p.Liveness)
			// The registry runs in the refiller's band, not outside the turnstile. Exempting it would rank
			// it ABOVE every band - a bypasser takes a connection without queueing for one - and would put
			// it outside the population the turn count bounds. Band 0 keeps both properties and costs it
			// nothing: strict priority means it waits only for the NEXT turn to free rather than for the
			// queue to drain, and it shares the band with one piston whose two loops take one turn each per
			// cycle, against a turn count of turnstilePassesPerConn x the connections.
			s.SetTurnFunc(func(ctx context.Context) (context.Context, func()) {
				return e.dbTurnAt(ctx, idx, priorityRefill)
			})
		}
		if merr := p.SetMeter(e.meter); merr != nil {
			e.logger.ErrorContext(e.lifetimeCtx, "Building piston instruments", "shard", idx, "error", merr)
		}
		p.SetIdle(idle || !e.dispatchesOn(idx))
		e.pistons[idx] = p
	}
	e.pistonsReady.Store(true)

	// Derive each piston's cycle period now that the cache is sized and the roles are known. recomputePools
	// re-derives it on every later role change.
	e.recomputeRefillIntervals()

	// The worker pool. Its callback is the claim skip plus processStep - NOT the host's ExecuteTask, which
	// runs deep inside processStep and would drag the whole execution core into the pool if it were the
	// boundary. The skip is checked HERE rather than inside processStep so it does not pay for that setup:
	// a sibling worker in this replica reserved the step within the last ~second, its claim CAS may still
	// be in flight, and the piston re-selected it because an uncommitted claim still reads `pending`, so
	// issuing our own claim would cost a round trip to be told we lost.
	crew, perr := workers.New(&e.cache, e.turnstiles.Gate(priorityWorker), func(ctx context.Context, shard, stepID int, release func()) error {
		if !e.claims.TryClaim(shard, stepID) {
			e.metricStepClaimPreempted(ctx)
			// Release explicitly rather than leaving it to the crew's backstop: a preempted candidate does
			// no database work at all, so holding its turn until the callback returns would bound admission
			// on a step nobody is running.
			release()
			return nil
		}
		// Counted from before the claim to after the transition, so a demotion landing anywhere in between
		// sees the step and keeps the pool its completion will write through.
		if r := e.roleOn(shard); r != nil {
			r.inFlight.Add(1)
			defer r.inFlight.Add(-1)
		}
		return e.processStep(ctx, shard, stepID, release)
	})
	if perr != nil {
		e.unwindRuntime()
		return errors.Trace(perr)
	}
	e.crew = crew
	// The crew measures how long entry blocked; the instrument is named here, because instrument names are
	// a public surface and belong with the owner rather than with a package that gates anything.
	e.crew.SetGateWaitObserver(func(shard int, waited time.Duration) {
		e.metrics.gateWait.Record(e.lifetimeCtx, waited.Seconds(), e.metrics.shardAttrOpts(shard).rec...)
	})
	e.crew.SetLogger(e.logger)
	e.crew.SetMax(maxWorkers)
	// Start the resident set; the rest of the ceiling is spawned on demand whenever a worker's take of a
	// candidate leaves nobody idle - the long-task case, where the resident set alone would cap throughput
	// because nobody is left to dispatch.
	//
	// NOT cancelable, deliberately: no worker can observe the lifetime ctx's cancellation (it lands after
	// Drain), and a cancelable parent makes every query of every worker register on its one mutex.
	e.crew.Start(context.WithoutCancel(e.lifetimeCtx), resident)
	// The pistons run on a CHILD of the lifetime ctx, because they must stop before it does: the lifetime
	// ctx is deliberately left live until every other goroutine has drained (so in-flight database work
	// always commits), while piston.Run ends on ctx alone and has no second signal - both of its queries
	// are read-only, so abandoning one mid-flight strands nothing.
	pistonCtx, pistonCancel := context.WithCancel(e.lifetimeCtx)
	e.pistonCancel = pistonCancel
	for _, p := range e.pistons {
		e.pistonPool.Go(func() {
			p.Run(pistonCtx)
		})
	}
	e.recoveryWorker.Go(func() {
		e.recoveryLoop(e.lifetimeCtx)
	})
	e.reaperWorker.Go(func() {
		e.reaperLoop(e.lifetimeCtx)
	})
	e.latchWorker.Go(func() {
		e.latchLoop(e.lifetimeCtx)
	})
	// On a child of the lifetime ctx, so the drain can abandon an aggregate in flight rather than wait out its
	// timeout: the query is a pure read.
	backlogCtx, backlogCancel := context.WithCancel(e.lifetimeCtx)
	e.backlogCancel = backlogCancel
	e.backlogWorker.Go(func() {
		e.backlogLoop(backlogCtx)
	})
	// Peer discovery. This replica has already joined every shard's registry and the pools are sized for
	// what it read there (Startup); from here the Sonars keep their rows fresh and re-read on their own
	// cadence, while the reconcile loop keeps what the engine DERIVES in step with what they observe.
	//
	// The Sonars run on their OWN child of the lifetime ctx rather than the pistons', because they must
	// outlive them: a replica has to stay registered while its workers are still executing steps, since
	// every peer sized its pools for a fleet that includes us.
	sonarCtx, sonarCancel := context.WithCancel(e.lifetimeCtx)
	e.sonarCancel = sonarCancel
	for _, s := range e.sonars {
		e.sonarPool.Go(func() {
			s.Run(sonarCtx)
		})
	}
	e.reconcileStop = make(chan struct{})
	e.reconcileWorker.Go(func() {
		e.runReconcileLoop()
	})
	return nil
}

// unwindRuntime undoes the part of initRuntime that runs before any goroutine is spawned, so a failed
// Startup leaves an engine that reports itself stopped rather than started-but-inert. Only the metrics
// callback needs undoing - it is registered with the OTEL reader and would otherwise query a database
// nobody is driving.
func (e *Engine) unwindRuntime() {
	e.closeMetrics()
	e.started.Store(false)
}

// drainRuntime stops all goroutines in order. The caller (Shutdown) has already flipped e.started to false
// via CompareAndSwap, which also serves as the single-shutdown guard.
func (e *Engine) drainRuntime() {
	// Tell the workers a shutdown started, before anything waits on them. A worker sleeping out a persistence
	// backoff wakes here, hands its step back (fenced), and exits, rather than making the drain wait out a
	// window nobody is watching.
	if e.drainStop != nil {
		close(e.drainStop)
	}
	// Stop deriving pool sizes from the fleet. Pure in-memory work, so it can stop first and there is
	// nothing in flight to wait for beyond the tick itself.
	if e.reconcileStop != nil {
		close(e.reconcileStop)
	}
	e.reconcileWorker.Wait()
	// Idle every piston. Nothing it pushes can reach a worker from here on (the cache closes below), and an idle
	// piston's Liveness stops the beat stamping dispatched_at, so peers drop this replica from the work divisor
	// within the dispatch window and select its residue class themselves, instead of leaving those steps to the
	// steal grace for as long as an in-flight task holds the drain open. The reconcile loop that re-derives the
	// mode has stopped, so it holds.
	for _, p := range e.pistons {
		p.SetIdle(true)
	}
	// Give up every dispatcher slot if the drain turns out to be LONG. A draining replica takes on no new
	// work, but its row stays fresh until it leaves the fleet at the very end, and a fresh row keeps its rank
	// - so a drain waiting on a long task would leave each shard it dispatched one dispatcher short for the
	// whole wait, and a small shard whose dispatchers all drain at once would run nothing until the slowest
	// task finished. Withdrawn, it stays counted for the connections it still holds while its peers re-rank
	// without it.
	//
	// NOT UNCONDITIONALLY, and not on a count of tasks in flight. Withdrawing costs an overlap: the peers grow
	// into the freed share while this replica still holds it, until the drain ends and its pool closes. A
	// drain that ends at once pays that for nothing - leaveFleet hands the slot on just as fast - and in a
	// rolling restart the overlap stacks with the replacement's bootstrap pool (measured on Postgres with a
	// four-replica rolling restart over a 48-connection budget: 71 connections against a 67 bound). A count read at this
	// instant misses a worker that has claimed a step and not yet entered the task. So the withdrawal waits
	// one registry read cadence, and fires only if the drain is still going by then.
	drained := make(chan struct{})
	var withdrawal sync.WaitGroup
	withdrawal.Go(func() {
		t := time.NewTimer(e.peerCadence())
		defer t.Stop()
		select {
		case <-drained:
		case <-t.C:
			e.withdrawFromFleet(e.lifetimeCtx)
		}
	})
	// Unregister the observable-gauge callback first, then stop the backlog refresh that feeds it - nothing
	// reads that cache any more, and its aggregate must not run against a closing database.
	e.closeMetrics()
	if e.backlogCancel != nil {
		e.backlogCancel()
	}
	e.backlogWorker.Wait()
	// Closing the cache releases workers parked waiting for work, and closing the turnstiles releases
	// those waiting for a turn; Drain then closes the crew to new goroutines and waits. All three halves
	// are the crew's contract - see internal/workers. BOTH closes are required: a worker waiting for a
	// turn is not blocked on the cache, so closing only the cache would leave the drain waiting forever.
	e.cache.Close()
	if e.turnstiles != nil {
		e.turnstiles.Close()
	}
	e.crew.Drain()
	close(drained)
	withdrawal.Wait() // before anything below can close what a late withdrawal writes through
	if e.recoveryStop != nil {
		close(e.recoveryStop)
	}
	e.recoveryWorker.Wait()
	if e.reaperStop != nil {
		close(e.reaperStop)
	}
	e.reaperWorker.Wait()
	// The detector stops before the board closes, since Close does not interrupt a sweep in flight.
	if e.latchStop != nil {
		close(e.latchStop)
	}
	e.latchWorker.Wait()
	// Then the pistons: their cycles are pure reads, so cancelling mid-flight strands nothing.
	if e.pistonCancel != nil {
		e.pistonCancel()
	}
	e.pistonPool.Wait()
	// The Sonars LAST, because this replica must stay registered while its workers are still executing
	// steps - every one of them was draining against pools each peer sized for a fleet that still included
	// us. Only once their loops have returned is the last possible beat behind us, which is what makes the
	// delete final: a beat only ever UPDATEs, so nothing can resurrect a row deleted after its loop stopped.
	// Peers then regrow their shares immediately rather than waiting out the freshness window. Best-effort
	// on a still-open shard set, and on a context of its own since the lifetime one is about to be cancelled.
	if e.sonarCancel != nil {
		e.sonarCancel()
	}
	e.sonarPool.Wait()
	e.leaveFleet(context.Background())
	// Wake every blocked Await so it returns a shutdown error rather than waiting out its own context on a
	// board nothing will ever release again - every goroutine that could have is drained above. A caller
	// already holding a stop status keeps it and returns that outcome instead.
	if e.latches != nil {
		e.latches.Close()
	}
	if e.lifetimeCancel != nil {
		e.lifetimeCancel()
	}
}

// shardOrdinals returns the open shard indices in ascending order plus a map from shard index to its
// ordinal position. Per-shard scratch under OnEach is sized len(indices) and written at pos[shard], so
// concurrent shard goroutines write distinct elements (race-free without a lock) even though the sparse
// shard indices cannot themselves index a slice.
func (e *Engine) shardOrdinals() (indices []int, pos map[int]int) {
	indices = e.db.Indices()
	pos = make(map[int]int, len(indices))
	for i, idx := range indices {
		pos[idx] = i
	}
	return indices, pos
}

// taskTimeBudget returns the current time budget for task dispatch.
func (e *Engine) taskTimeBudget() time.Duration {
	return time.Duration(e.timeBudgetMs.Load()) * time.Millisecond
}

// resolveFlowOptions applies defaults to caller-supplied options. TimeBudget falls back to the engine
// default and is frozen onto the returned options.
func (e *Engine) resolveFlowOptions(opts *workflow.FlowOptions) *workflow.FlowOptions {
	resolved := &workflow.FlowOptions{
		Priority:       int(e.defaultPriority.Load()),
		FairnessWeight: 1,
		TimeBudget:     e.taskTimeBudget(),
	}
	if opts != nil {
		if opts.Priority > 0 {
			resolved.Priority = opts.Priority
		}
		if opts.FairnessWeight > 0 {
			resolved.FairnessWeight = opts.FairnessWeight
		}
		resolved.FairnessKey = opts.FairnessKey
		resolved.Baggage = opts.Baggage
		resolved.DeleteOnCompletion = opts.DeleteOnCompletion
		resolved.ThreadKey = opts.ThreadKey
		if opts.TimeBudget > 0 {
			resolved.TimeBudget = opts.TimeBudget
		}
	}
	return resolved
}

// --- Public API ---

// ensureStarted rejects an operation on an engine that is not live - never started, or already shut down.
// It is not merely a nicety: without it the failure is a LIE rather than an error. Every key-addressed
// operation routes through ShardSet.Shard, which returns "flow not found" (404) when no shard is open, so a
// stopped engine tells the caller its flow does not exist - and a caller may act on that (stop retrying,
// recreate the work). The cross-shard operations are worse: List/Purge/ShardInfo fan out over an EMPTY index
// set and return SUCCESS with an empty result - "you have no flows". Both are indistinguishable from the
// truth. 503 says the one true thing: the engine is not available, come back later.
//
// The shutdown half is not API misuse: a host still serving while it tears the engine down, or a request in
// flight when Shutdown lands, is an ordinary race. (pickShard keeps its own no-open-shards guard for exactly
// that race - `started` can flip after this check - where indexing an empty index slice would panic the
// host's process.)
func (e *Engine) ensureStarted() error {
	if !e.started.Load() {
		return errors.New("engine is not started", http.StatusServiceUnavailable)
	}
	return nil
}

// Create creates a new flow for a workflow and starts it, returning the running flow's key. opts carries
// the flow's policy (scheduling, DeleteOnCompletion, Baggage, ThreadKey); nil uses defaults.
// For a flow that must wait for an external trigger, have the entry task call flow.Interrupt and resume it
// with Resume (which, unlike a separate start, also delivers a payload).
func (e *Engine) Create(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	if err := e.ensureStarted(); err != nil {
		return "", errors.Trace(err)
	}
	return e.create(ctx, workflowURL, initialState, opts)
}

// Snapshot returns the current state and status of a flow.
func (e *Engine) Snapshot(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	if err := e.ensureStarted(); err != nil {
		return nil, errors.Trace(err)
	}
	return e.snapshot(ctx, flowKey)
}

// Fingerprint returns a fingerprint and status for change detection.
func (e *Engine) Fingerprint(ctx context.Context, flowKey string) (fingerprint string, status string, err error) {
	if err := e.ensureStarted(); err != nil {
		return "", "", errors.Trace(err)
	}
	return e.fingerprint(ctx, flowKey)
}

// Resume continues a flow paused by flow.Interrupt.
func (e *Engine) Resume(ctx context.Context, flowKey string, resumeData any) error {
	if err := e.ensureStarted(); err != nil {
		return errors.Trace(err)
	}
	return e.resume(ctx, flowKey, resumeData)
}

// Terminate forcefully, unconditionally stops a flow. In-flight work is abandoned immediately - it is not
// awaited, and no handler runs. The flow reads "terminated" as soon as this returns. Terminating a flow that
// has already ended is a no-op that returns nil and leaves its outcome as it was.
func (e *Engine) Terminate(ctx context.Context, flowKey string, reason string) error {
	if err := e.ensureStarted(); err != nil {
		return errors.Trace(err)
	}
	return e.terminate(ctx, flowKey, reason)
}

// Cancel requests graceful cancellation of a flow, addressed by its root flow key. Every step in progress
// when it is called - pending, running, parked on a subgraph, or interrupted - is covered, as is a step that
// finished just before and is handing over to its next one: a step not yet started is skipped, and a step
// already running finishes before the cancellation takes effect - a subgraph it arms still runs. A covered step receives the cancellation as
// an error on its task's onError transition, where workflow.IsCancelled reports true and the task's own
// changes are preserved. An error the task itself returns wins over the cancellation: it takes the ordinary
// error path instead. If a covered step has no onError transition, the flow ends cancelled when every
// unrecovered loss in it was a cancellation, and failed if a real error is mixed in. A subgraph caller runs
// once more when its child returns, so it sees the child's result - a cancelled child's error is one
// workflow.IsCancelled recognizes - before its own cancellation is delivered. An interrupted step is cancelled
// when it is next resumed, and its resume data is not acted on. Cancel returns once the request is recorded,
// not when the flow stops; use Await for that. Terminate is the unconditional, immediate alternative.
//
// A flow is cancelled once. Calling Cancel again on a flow already cancelled records nothing new: the first
// reason stands, and a flow whose handler caught the first cancellation and carried on is left alone. It does
// finish delivering the first cancellation to the steps it covered, so a Cancel that returned an error, or
// whose response was lost, is safe to retry. To stop a flow that recovered from its cancellation, Terminate it.
func (e *Engine) Cancel(ctx context.Context, flowKey string, reason string) error {
	if err := e.ensureStarted(); err != nil {
		return errors.Trace(err)
	}
	return e.cancel(ctx, flowKey, reason)
}

// Fork clones a terminal flow's prefix up to the given step into a new, self-contained running flow and
// re-executes from that step with optional stateOverrides applied to it. The original flow is never
// modified. The fork inherits the original's scheduling and baggage (it does not take FlowOptions).
// Returns the new flow's key.
func (e *Engine) Fork(ctx context.Context, stepKey string, stateOverrides any) (string, error) {
	if err := e.ensureStarted(); err != nil {
		return "", errors.Trace(err)
	}
	return e.forkFlow(ctx, stepKey, stateOverrides)
}

// History returns the step-by-step execution history of a flow.
func (e *Engine) History(ctx context.Context, flowKey string) ([]workflow.FlowStep, error) {
	if err := e.ensureStarted(); err != nil {
		return nil, errors.Trace(err)
	}
	return e.history(ctx, flowKey)
}

// Step returns details of a single step.
func (e *Engine) Step(ctx context.Context, stepKey string) (*workflow.FlowStep, error) {
	if err := e.ensureStarted(); err != nil {
		return nil, errors.Trace(err)
	}
	return e.step(ctx, stepKey)
}

// List queries flows by status, workflow name, or thread key, newest first, with cursor pagination
// (Query.Limit, default 100; the returned cursor fetches the next page). Query.Limit is a per-shard cap
// divided across shards, not a hard ceiling on the total: a multi-shard page can hold up to
// shards*ceil(Limit/shards) summaries (see Query.Limit). Pass Query.Shard, or truncate the page, for a
// strict count.
//
// "Newest first" is per shard, not global. On a MULTI-SHARD fleet each shard contributes its own newest
// flows and the results are grouped by shard, so the concatenation is not in one descending time order -
// shard 2's newest flow follows shard 1's oldest returned one. There is no cross-shard order to give: the
// flow ids are per-shard sequences (a shard with fewer flows has lower ids, so they do not compare), and
// creation times would compare different database servers' clocks. A single-shard engine - the default - is
// globally newest-first. A caller that needs one ordered view across shards sorts the page itself, choosing
// what to trust; a UI that must not show interleaving artifacts can page one shard at a time with
// Query.Shard.
func (e *Engine) List(ctx context.Context, query workflow.Query) (summaries []workflow.FlowSummary, nextCursor string, err error) {
	if err := e.ensureStarted(); err != nil {
		return nil, "", errors.Trace(err)
	}
	return e.list(ctx, query)
}

// Delete marks a flow (and its subgraph subtree) for deletion; a background reaper removes it shortly
// after. The flow is excluded from List/History immediately, though Snapshot/Await/Run still report its
// outcome until the reaper removes it. An interrupted flow is terminated first, subgraphs included, exactly
// as Terminate would, with the reason "deleted". Returns 404 for an unknown key, 400 for a subgraph-child key (address the root
// instead), and 409 for a flow that is still running. Calling it again on an already-marked flow is
// idempotent success.
func (e *Engine) Delete(ctx context.Context, flowKey string) error {
	if err := e.ensureStarted(); err != nil {
		return errors.Trace(err)
	}
	return e.deleteFlow(ctx, flowKey)
}

// Purge marks flows matching a query (and their subgraph subtrees) for deletion; a background reaper removes
// them shortly after. Marked flows are excluded from List/History immediately. Interrupted flows are
// terminated first, as Delete does, with the reason "purged"; running flows are skipped. Returns the count of roots marked. A call
// marks at most min(Query.Limit, 4096) roots, divided per shard exactly as in List (up to ceil(n/shards) per
// shard), so a multi-shard call can mark slightly more; iterate until it returns 0 to mark everything. The
// query takes any filter List does; one that sets none (only Shard or Limit, or nothing) is a 400.
func (e *Engine) Purge(ctx context.Context, query workflow.Query) (int, error) {
	if err := e.ensureStarted(); err != nil {
		return 0, errors.Trace(err)
	}
	return e.purge(ctx, query)
}

// ShardInfo reports, for every shard, whether it answers a trivial query and how fast. It returns a row per
// shard even when some are down, with the failure recorded on that shard's row, so it is the call to make when a
// shard is suspect. It reads no table, so it costs the same however large the shards grow.
func (e *Engine) ShardInfo(ctx context.Context) ([]ShardSummary, error) {
	if err := e.ensureStarted(); err != nil {
		return nil, errors.Trace(err)
	}
	return e.shardInfo(ctx)
}

// Await blocks until a flow stops, then returns its outcome. Running out of time is an error; the flow is
// unaffected and keeps running, and the caller still holds the key to Await/Snapshot/Terminate it later.
//
// Pass a ctx with a deadline. It is the only bound on the wait - a flow can run for as long as its work
// takes, and there is no notification the engine could time out on instead. A ctx without one is honored
// for a long fixed budget and then times out, which is a guard against blocking forever, not a wait to
// design around. A caller whose own deadline is shorter than the flow wants Poll, not Await.
//
// A wait that runs out - the ctx's deadline or that fallback budget - returns a 408 error that satisfies
// errors.Is(err, context.DeadlineExceeded).
func (e *Engine) Await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	if err := e.ensureStarted(); err != nil {
		return nil, errors.Trace(err)
	}
	return e.awaitStopped(ctx, flowKey)
}

// Poll returns a flow's current outcome, blocking up to the ctx deadline for it to stop. Unlike Await, a ctx
// timeout is not an error: it returns the current non-terminal outcome, whose Stopped() reports false, so a
// caller bridging an open-ended flow to a bounded request (e.g. an HTTP poll) can answer within its budget and
// re-poll. A real failure still returns an error.
//
// The ctx deadline IS the budget, so pass one; without it Poll blocks on the same long fallback budget as
// Await before answering, which is rarely what a polling caller wants.
func (e *Engine) Poll(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	if err := e.ensureStarted(); err != nil {
		return nil, errors.Trace(err)
	}
	return e.await(ctx, flowKey)
}

// Run creates, starts, and awaits a flow in one call, returning the new flow's key alongside its outcome
// (the key is the flow's identity, not part of the outcome). opts carries scheduling and the opaque host
// Baggage; nil opts uses defaults.
//
// Error semantics differ by phase. A create failure returns flowKey "" and a nil outcome - no flow
// exists. An await failure - most commonly the caller's ctx expiring before the flow stops - leaves the
// flow running (it is durable and not bound to this call) and returns its flowKey with a nil outcome and
// the error, so the caller keeps a handle to Await/Snapshot/Terminate it later. Run never terminates the flow
// on the caller's behalf; a caller that wants the flow torn down on timeout calls Terminate explicitly. As
// with Await, a wait that runs out returns a 408 error that satisfies errors.Is(err,
// context.DeadlineExceeded).
func (e *Engine) Run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, outcome *workflow.FlowOutcome, err error) {
	if err := e.ensureStarted(); err != nil {
		return "", nil, errors.Trace(err)
	}
	return e.run(ctx, workflowURL, initialState, opts)
}

// Continue creates a new flow from the latest completed flow in a thread, inheriting that flow's policy
// (scheduling, baggage) - it does not take FlowOptions. For a turn with different policy,
// use Create with FlowOptions.ThreadKey.
func (e *Engine) Continue(ctx context.Context, threadKey string, additionalState any) (string, error) {
	if err := e.ensureStarted(); err != nil {
		return "", errors.Trace(err)
	}
	return e.continueFlow(ctx, threadKey, additionalState)
}
