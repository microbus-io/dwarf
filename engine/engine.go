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
	"encoding/json"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"log/slog"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/claimstracker"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/graphcache"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/piston"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/dwarf/internal/staterefs"
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
	// Latency is the round trip of a trivial query to the shard. In JSON it is "latencyMillis", an integer number of
	// milliseconds rounded to the nearest one.
	Latency time.Duration `json:"-"`
}

// shardSummaryJSON is ShardSummary's JSON form: its fields but Latency, which goes as whole milliseconds.
type shardSummaryJSON struct {
	*shardSummaryFields
	LatencyMillis *int64 `json:"latencyMillis,omitempty"`
}

// shardSummaryFields is ShardSummary without its methods, so the JSON form can embed it without recursing.
type shardSummaryFields ShardSummary

// MarshalJSON encodes the summary, Latency as "latencyMillis".
func (s ShardSummary) MarshalJSON() ([]byte, error) {
	w := shardSummaryJSON{shardSummaryFields: (*shardSummaryFields)(&s)}
	if ms := s.Latency.Round(time.Millisecond).Milliseconds(); ms != 0 {
		w.LatencyMillis = &ms
	}
	b, err := json.Marshal(w)
	return b, errors.Trace(err)
}

// UnmarshalJSON decodes the summary, Latency from "latencyMillis". An absent field leaves the value as it was.
func (s *ShardSummary) UnmarshalJSON(data []byte) error {
	w := shardSummaryJSON{shardSummaryFields: (*shardSummaryFields)(s)}
	if err := json.Unmarshal(data, &w); err != nil {
		return errors.Trace(err)
	}
	if w.LatencyMillis != nil {
		s.Latency = time.Duration(*w.LatencyMillis) * time.Millisecond
	}
	return nil
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

// Engine is the standalone workflow orchestration engine.
type Engine struct {
	// Dependencies (set before Startup)
	host Host
	// meterProvider and tracerProvider are the configured providers, nil for the global ones; Startup resolves
	// them into diags.
	meterProvider  metric.MeterProvider
	tracerProvider trace.TracerProvider
	metrics        *engineMetrics
	// diags is what this engine and every component it builds report through: the logger, the seams, and the
	// providers, meter and tracer Startup resolves - the meter held so every component records into one
	// instrumentation scope rather than deriving its own from the provider.
	diags *diagnostics.Diags

	// Shard registry (construction-time only): shard index -> spec. Sparse - indices must be unique and
	// >= 1 but need not be contiguous. Guarded by shardsLock; empty means one default shard at Startup.
	shardsLock sync.Mutex
	shardSpecs map[int]ShardSpec

	// Configuration (atomically updated, safe to change after Startup)
	workers                atomic.Int32
	workersSet             atomic.Bool // SetWorkers was called: skip the derived default
	timeBudgetMs           atomic.Int64
	maxPriority            int           // construction-time only, see SetMaxPriority
	retentionDays          int           // construction-time only, see SetRetention
	flowExpiration         time.Duration // construction-time only, see SetFlowExpiration; 0 is off
	refillIntervalOverride atomic.Int64  // bench override (nanoseconds): pins every piston's cycle period; <=0 = derive

	// engineID is a random positive identifier minted per engine instance (fresh on every restart). It is
	// this replica's identity in the shared dwarf_peers registry - the primary key its Sonars own a row
	// under - and it is stamped on every flow/step INSERT (creator) and overwritten by the claim CAS
	// (claimer) as forensic provenance, deliberately unindexed.
	engineID int64

	// stateCacheBytes is the SetStateCacheSize override of the budget of the replica's one resolve cache,
	// which every shard's Linker reads through; 0 derives it from the process's memory limit (stateCacheBudget).
	stateCacheBytes int64

	// flowsStartedCount / flowsClosedCount count this replica's flow starts and closes beside the
	// dwarf_flows_started / dwarf_flows_closed counters, readable with no meter configured. They are per
	// replica - a flow started on one is closed by whichever dispatches its last step - so only a sum over
	// every engine in a process means anything: the load generator sums FlowsClosed (accessors) over its
	// in-process engines to bound outstanding flows, and a test with one engine compares the two.
	flowsStartedCount atomic.Int64
	flowsClosedCount  atomic.Int64

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
	// fanInHoldAt is how many of this replica's workers may wait at one cohort's fan-in before the cache holds the
	// cohort's lineage (enterFanIn): half the cache's capacity, re-set wherever the cache is sized. Zero holds
	// nothing.
	fanInHoldAt atomic.Int64

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

	// actors is the run's actors by shard (actors.go): Startup builds and publishes a fresh map per run, so a host
	// still serving through a Shutdown/Startup restart reads one run's map or the other's - a map assigned in place
	// under that read is a fatal throw. Each published map is read-only. runDayLength is the run's archive day, in
	// nanoseconds, stored with it.
	actors       atomic.Pointer[map[int]*shardActors]
	runDayLength atomic.Int64

	// Peer discovery: each shard's Sonar (shardActors.sonar) owns this replica's row in that shard's dwarf_peers
	// registry and publishes what reading it implies - how many replicas hold connections to the shard (its pool
	// divisor) and which residue class of step_id this replica selects there (its work divisor). sonarCancel stops
	// their loops. See peers.go and internal/peers.
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

	// Candidate supply: one piston per shard (shardActors.piston), each cycling its own database on its own clock
	// with no barrier against its peers. They share the planner (each shard's tally retained, so every shard plans
	// against the merged picture without waiting for the slowest) and the candidate cache. The planner is built
	// fresh per run with the actors.
	//
	// pistonCancel stops them: piston.Run ends on ctx alone, and the lifetime ctx is deliberately not
	// cancelled until every other goroutine has drained, so the pistons need a cancellable child of it.
	planner      *planner.Planner
	pistonCancel context.CancelFunc
	pistonPool   sync.WaitGroup

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

	// Janitors: one per shard for the run (shardActors.janitor), active where this replica dispatches the shard -
	// applyRole resumes and pauses them. janitorJobsBuilt, under janitorMu, is the settings their jobs were built
	// with. janitorCancel ends their run. janitorIdle replaces every job's idle wait for a test; janitorManual starts
	// no job goroutine, for a test that runs the jobs through reapDueFlows.
	janitorMu        sync.Mutex
	janitorJobsBuilt janitorSettings
	janitorCancel    context.CancelFunc
	janitorIdle      time.Duration
	janitorManual    bool

	// Archive goroutine: refreshes the catalog of each shard's archive days (shardActors.archive) where this replica
	// does not dispatch; the janitor's archive retention keeps the days elsewhere. archiveDayOverride, when set,
	// replaces the day's length for a test or a benchmark (setArchiveDay). noReapQuiet, set in test mode, lets the janitor's archiving jobs
	// archive into their day however late they run (their quiet margin off), so a test's round near midnight, or on a
	// day of seconds, does what it does at noon. archiveCancel stops the loop between its statements.
	archiveDayOverride time.Duration
	noReapQuiet        bool
	archiveCancel      context.CancelFunc
	archiveWorker      sync.WaitGroup

	// Await latch: one board per shard for callers to park on (shardActors.latch), each with a detector goroutine
	// that asks its shard which of those flows have settled. A stop this replica commits reaches its board instantly
	// through signalStop; the detector is what makes a PEER's stop observable. latchCancel stops the detectors,
	// abandoning a sweep in flight.
	latchCancel context.CancelFunc
	latchWorker sync.WaitGroup

	// Parsed graphs, so a dispatch reads no graph column. The graph JSON is frozen at flow creation, so an entry
	// never goes stale.
	graphs *graphcache.Cache
	// State-ref linkers, one per shard (shardActors.linker): they decide which state fields a successor step stores
	// by reference rather than by value, and resolve those references back. Per shard because a step id is only
	// unique within one. They share stateCache, one budget of resolved bytes for the whole replica, so it goes to
	// the shards this replica dispatches rather than being split among shards it may never run.
	stateCache *staterefs.Cache

	// Lifecycle
	started        atomic.Bool
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc
	// lifecycleMu is held by Startup and by Shutdown for the whole of each, so neither runs while the other is under
	// way. started cannot say so: it is false throughout a Shutdown, and true inside a Startup that can still fail.
	lifecycleMu sync.Mutex
	// The calls to exported operations in flight (admitIngress), each by the cancel of the context it runs on, so
	// Shutdown can end them and wait for them (closeIngress). ingressDrained's lock is ingressMu; it wakes Shutdown as
	// the last call returns. Owned by ingressMu, which also orders the check of started with the count-in.
	ingressMu      sync.Mutex
	ingress        map[uint64]context.CancelCauseFunc
	ingressNext    uint64
	ingressDrained sync.Cond

	// Recovery/await/reap timing. Effectively constants in production (defaulted in NewEngine, then
	// read-only); a white-box test shortens one directly, before Startup for the one read once at Startup
	// (recoveryInterval), so an otherwise minutes-long path is observable.
	leaseMargin         time.Duration // added to a step's budget when sizing the crash-recovery lease (30s)
	latchSweepInterval  time.Duration // Await latch detector cadence - the bound on cross-replica wake (50ms)
	awaitDefaultBudget  time.Duration // how long Await blocks when the caller's ctx carries no deadline (15m)
	recoveryInterval    time.Duration // lease recovery's and orphan repair's interval; the wedge sweep's is wedgeSweepEvery of them; read once at Startup (1m)
	parkWedgeThreshold  time.Duration // min age before a parked step is treated as wedged (5m)
	orphanFlowThreshold time.Duration // quiet time before a stranded flow with no step to replay is reported orphaned (5m)
	deletionGrace       time.Duration // DeleteOnCompletion linger window before reap (1m)
	archiveGrace        time.Duration // how long a closed root stays in the live tables before it is archived (1m)
	sweepOverdue        time.Duration // how long past due a root waits before a sweep takes it (30s)
	reapBatch           int           // due roots one janitor batch considers (500)
	reapBatchRows       int           // about how many rows one janitor batch moves, at least one tree (4000)
	reapBatchBytes      int           // about how many payload bytes one janitor batch moves, at least one tree (16 MiB)
	reapStageRows       int           // the tree size above which archival is staged (256k); 0, never
	reapStageBytes      int           // the tree payload above which archival is staged (128 MiB); 0, no byte threshold
	// purgeChunk is how many roots one Purge statement marks, at most database.MaxListIDs, the most one bound list
	// holds; a field so a test can drive several chunks.
	purgeChunk int
	// The expiration pass (expiration.go), which the janitor's expiration job runs: how long one pass may keep reading
	// and starting terminations (30s), and how many rows one page of its walk reads (256).
	expireBudget       time.Duration
	expirePage         int
	leaseRecoveryBatch int // ids per lease-recovery reset, at most database.MaxListIDs (2000)
	maxPayloadBytes    int // largest JSON value written into one payload column (maxPayloadBytes)
	// persistBackoff is the outcome-write retry schedule (1s doubling to 32s, a minute in all), exponential
	// because the errors it exists for - a dropped connection, a failover - clear in seconds to tens of seconds. A
	// longer ladder would be slow to report a permanent failure. A per-engine field (not a package var) so a test
	// shortens it on its own engine without racing a parallel peer.
	persistBackoff []time.Duration
	// persistLeaseExtension is how far past its backoff sleep persist renews a step's lease before each retry (60s):
	// the first half covers the retry's write, the second leaves failOnPersistError room to fail the step. A write that
	// takes longer than that outlives the lease, and lease recovery runs the task again. Without it a task that
	// consumed most of its time budget has only `leaseMargin` left, so the backoff would put us PAST the lease: a peer
	// re-claims the step, re-executes the task, and our late write is fenced and discarded - precisely the re-execution
	// persist exists to prevent. A crash mid-ladder strands the step for up to this long plus the backoff - or for what
	// remained of its original lease, if that was longer, since a renewal never shortens a lease - before lease
	// recovery can take it.
	persistLeaseExtension time.Duration
}

// defaultTestConnCap seeds every engine's test-mode per-shard connection cap (see the testConnCap field).
// A test that must observe the real derived pool sizes (the pool-sizing tests) sets e.testConnCap = 0 on
// its own engine before Startup. Inert outside test mode (only consulted when testHashedID != "").
const defaultTestConnCap = 4

// NewEngine creates a new workflow engine.
func NewEngine() *Engine {
	e := &Engine{
		claims:      claimstracker.New(),
		testConnCap: defaultTestConnCap,
	}
	e.workers.Store(64)
	e.timeBudgetMs.Store(int64(2 * time.Minute / time.Millisecond))
	e.maxPriority = piston.DefaultMaxPriority
	e.SetEngineID(int64(rand.Uint64() >> 1)) // positive, 63 bits of entropy
	// Empty, not seeded: "nothing derived yet" per shard. Startup reads each shard's count, sizes its pool
	// directly, and records it here; the reconcile loop then dedupes against that.
	e.lastAppliedSlots = map[int]int{}
	e.fleetOverLimit = map[int]bool{}
	e.heldDivisors = map[int]int{}
	e.ingress = map[uint64]context.CancelCauseFunc{}
	e.ingressDrained.L = &e.ingressMu
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
	e.persistBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second}
	e.persistLeaseExtension = 60 * time.Second
	e.recoveryInterval = time.Minute
	e.parkWedgeThreshold = 5 * time.Minute
	e.orphanFlowThreshold = 5 * time.Minute
	e.deletionGrace = 1 * time.Minute
	e.archiveGrace = 1 * time.Minute
	e.sweepOverdue = janitor.DefaultSweepOverdue
	// A batch under about 0.25 s on a 32 vCPU PostgreSQL under a saturated bench, where a transaction moved about
	// 13 us a small row and 7.8 ms a MB of payload: 4,000 rows and 16 MiB together take ~0.18 s. The roots are one
	// bound list of a length database.IDList pads to (500), and a large tree's chunk deletes or copies its step ids in
	// two lists of database.MaxListIDs. A tree past the staging threshold is copied a batch at a time; one below it
	// moves whole, alone in its batch if it must - up to ~3.4 s at 256k small rows, ~1 s at 128 MB, neither of which
	// moved the bench's commit latency.
	e.reapBatch = 500
	e.reapBatchRows = 4000
	e.reapBatchBytes = 16 << 20
	e.reapStageRows = 256 << 10
	e.reapStageBytes = 128 << 20
	e.purgeChunk = database.MaxListIDs
	e.expireBudget = 30 * time.Second
	e.expirePage = 256
	e.retentionDays = defaultRetentionDays
	e.leaseRecoveryBatch = database.MaxListIDs
	e.maxPayloadBytes = maxPayloadBytes
	// Test-only instrumentation seams (see engineundertest.go), enabled under testing.Testing(). Every consult is a
	// lock-free bool read in production. The per-flow flow-row-write counter is a counting checkpoint on these same
	// seams (Visits); it holds no engine-side state.
	e.diags = diagnostics.New()
	e.diags.SetSeams(seamster.New(testing.Testing()))
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
	// SetShard rejects a weight above 1,048,576 (2^20); a fraction expresses a smaller share.
	//
	// While every non-cordoned shard's weight is 1, a replica places a new flow on one of the shards it
	// dispatches (over all non-cordoned shards only when it dispatches none), so flows spread by where the
	// Create calls arrive: a single replica creating every flow fills only its own shards. Any weight other
	// than 1 on any non-cordoned shard switches every replica to weighted placement over all non-cordoned
	// shards, in proportion to their weights; a flow placed on a shard its creating replica does not
	// dispatch waits for that shard's dispatcher to pick it up. Give a larger database a larger weight in a
	// mixed fleet.
	//
	// A shard the replica cannot currently read - no answer from it for about half a second - is left out of
	// placement, whatever its weight, until it answers again; when no shard can be read, placement goes ahead
	// over all of them. dwarf_placement_skipped counts the flows placed elsewhere.
	Weight float64
	// Cordoned excludes the shard from new-flow placement. Everything already resident proceeds
	// normally: existing flows keep executing, and subgraph children, continuations (Continue),
	// and forks - all shard-pinned - are still created on it. Use for retiring or overloaded shards.
	Cordoned bool
}

// maxWeight bounds a shard's placement weight. It feeds a weighted pick over a running sum, and finite weights can
// still sum to infinity (two at 1e308), which sends every placement draw to whichever shard comes last in map
// order. A share needs a ratio, not a magnitude, so the bound costs nothing; a fraction expresses a smaller share.
const maxWeight = 1 << 20

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
	if !(spec.Weight >= 0 && spec.Weight <= maxWeight) { // NaN fails both comparisons
		return errors.New("weight must be between 0 and %d", maxWeight, http.StatusBadRequest)
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
// as its size plus a few hundred bytes, at most 1,048,576 are kept, and a field unread for 5 minutes expires, its
// memory released by a later store or lookup.
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
// FairnessWeight, 0 cannot mean "unset" here: this setter IS the default. It must be at least a millisecond:
// a shorter positive duration counts as zero, a deadline already passed.
//
// The budget is at most just under 24.8 days: it is stored as a 32-bit count of milliseconds. A step whose replica
// crashes is not run again until its budget has passed, so a task that waits for days should Sleep or Interrupt
// rather than hold one long call.
func (e *Engine) SetTimeBudget(d time.Duration) error {
	if d < time.Millisecond {
		return errors.New("time budget must be at least 1ms (it is stored in milliseconds)", http.StatusBadRequest)
	}
	if d > maxTimeBudget {
		return errors.New("time budget must be at most %v", maxTimeBudget, http.StatusBadRequest)
	}
	e.timeBudgetMs.Store(int64(d / time.Millisecond))
	return nil
}

// maxTimeBudget is the longest time budget a flow can carry. time_budget_ms is a 32-bit column, and the claim
// adds the lease margin to it in SQL (time_budget_ms + ?), which Postgres computes as a 32-bit integer: a budget
// within the margin of 2^31-1 ms is stored and then fails every claim with "integer out of range", the step
// retried forever. A minute of headroom covers the 30s margin.
const maxTimeBudget = time.Duration(math.MaxInt32)*time.Millisecond - time.Minute

// maxPriorityCeiling is the largest maximum SetMaxPriority accepts. Every band up to the maximum costs the
// selection scan an index probe on every cycle, whether or not anything uses it.
const maxPriorityCeiling = 100

// SetMaxPriority sets how many priority levels flows may use: priorities run from 1 (served first) to max, at
// most 100. The default is 5. A flow that sets no priority gets the midpoint of the range - 3 at the default.
//
// Use the same value on every replica sharing a database. Work that carries a priority beyond the range - created
// before it existed, or by a replica with a larger one - is served after every level in it. Construction-time
// only.
func (e *Engine) SetMaxPriority(max int) error {
	if e.started.Load() {
		return errSetAfterStartup("max priority")
	}
	if max < 1 || max > maxPriorityCeiling {
		return errors.New("max priority must be between 1 and %d", maxPriorityCeiling, http.StatusBadRequest)
	}
	e.maxPriority = max
	return nil
}

// defaultPriority is the priority a flow with none gets: the midpoint of the range.
func (e *Engine) defaultPriority() int {
	return (1 + e.maxPriority) / 2
}

// setRefillInterval is the benchmark override that PINS every piston's cycle period to d, replacing the
// value the engine derives from the cache capacity and the pool (deriveRefillInterval); d <= 0 restores derivation. Reached
// from the bench through internal/accessors, never by a host.
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

// setClaimsTrackerEnabled turns this replica's in-flight claim tracker on or off, live. For a test that puts a step
// back to pending within its last claim's window - a backdated lease, a lost reply that skipped the re-offer - and
// would otherwise wait out the window before claiming it again. Off changes no outcome: the tracker only spares
// claim round trips, and the claim CAS decides.
func (e *Engine) setClaimsTrackerEnabled(enabled bool) {
	e.claims.SetEnabled(enabled)
}

// SetHost registers the host the engine reaches the outside world through: it loads graphs and executes
// tasks. Host has exactly two methods, LoadGraph and ExecuteTask, both required. Construction-time only.
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
	e.diags.SetLogger(l)
	return nil
}

// SetDebugLogger is a convenience that wires a human-readable text logger to stderr at debug level -
// shorthand for SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level:
// slog.LevelDebug}))). It is meant for development and test runs where you want to see the engine's internal
// logging without standing up an OTEL pipeline. Output goes to stderr, not stdout, so it never mixes with a
// program's data stream - the standard convention for diagnostic logs. Because it routes through SetLogger, the
// database layer's migration logs appear too.
// Construction-time only.
func (e *Engine) SetDebugLogger() error {
	return e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

// --- Lifecycle ---

// Startup opens the connections to every shard, applies the schema migrations each shard is missing, and starts the
// engine's workers and background loops; every other operation returns 503 until it has returned nil. On PostgreSQL it
// fails unless it connects as the role that owns the dwarf tables. Calling it on a started engine is a 409.
//
// Startup and Shutdown do not overlap: a call to either while the other, or another Startup, is under way waits for
// that one to finish first.
func (e *Engine) Startup(ctx context.Context) error {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
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
	dayLength := e.archiveDayLength()
	var err error
	// Resolved before the shards open, so the migrations trace through the same provider as everything after.
	e.initProviders()
	err = e.db.Open(ctx, database.Config{
		Shards:      shards,
		TestID:      e.testHashedID,
		TestConnCap: testConnCap,
		// Turns order access to each shard's connections by band and then by how long the job asking has been
		// at the database, so that work already under way finishes ahead of work arriving after it.
		TurnsPerConn: turnstilePassesPerConn,
		Diags:        e.diags,
	})
	if err != nil {
		return errors.Trace(err)
	}
	// Every shard's actors, built and published before anything reads them; none runs yet.
	err = e.buildActors(ctx, dayLength)
	if err != nil {
		e.db.Close()
		return errors.Trace(err)
	}
	// The archive tables of today and the days ahead exist before this replica serves anything.
	err = e.startArchives(ctx)
	if err != nil {
		e.stopJanitors()
		e.db.Close()
		return errors.Trace(err)
	}

	// Each shard's RTT and server connection limit were measured as it opened, and the sizing derives from them
	// (shardFacts). Report what they came to.
	sizing, rtts := e.shardFacts()
	for _, idx := range e.db.Indices() {
		sh, dbErr := e.db.Shard(idx)
		if dbErr != nil {
			continue
		}
		if sh.MaxConnections() == 0 && sh.DB().DriverName() != "sqlite" {
			// Fails open to the unclamped budget, which on a server configured small over-connects it.
			e.diags.Logger().WarnContext(ctx, "Reading the server's connection limit failed; the shard's connection budget is not clamped to it",
				"shard", idx)
		}
		e.diags.Logger().DebugContext(ctx, "Shard sized", "shard", idx, "rttMs", rtts[idx], "maxConnections", sh.MaxConnections(),
			"budget", sizing[idx].budget)
	}
	// Forget the last run's verdicts, so a fleet still over the limit after a restart is warned about again
	// rather than taken as already reported.
	e.poolsLock.Lock()
	e.fleetOverLimit = map[int]bool{}
	e.heldDivisors = map[int]int{}
	e.poolsLock.Unlock()
	// Announce this replica on every shard, wait for peers to notice, and read each shard's fleet back -
	// then size every pool from the derived budget split by THAT shard's count. Those are the real sizes,
	// taken before any worker dispatches, so the replica takes on work with pools already correct for the
	// fleet it is in: no async grace window and no partial-count over-connect.
	//
	// The announce-then-wait ordering is what keeps the join from exceeding a shard's connection budget even
	// momentarily - peers hold pools sized for the fleet WITHOUT this replica until they read again, so it
	// shrinks them first and grows into the space afterward. It costs one wait for the whole fleet, since the
	// shards join in parallel. Each shard's role is filled in by the sizing loop below, before anything that could
	// ring a doorbell is running.
	e.joinFleet(ctx)
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
		// Through the shard set, which applies the test-mode cap - a push onto the shard's own handle would
		// not - and sizes the shard's turns with the pool. Here rather than later because the workers
		// initRuntime starts must never see an unsized shard, whose turns would let them through unordered.
		e.db.SetShardPool(idx, idle, open)
		if e.diags.Seams().Enabled() { // Enabled gates the assembled name and the boxed value in production
			e.diags.Seams().Variable(diagnostics.Join(diagnostics.VariablePoolIdle, strconv.Itoa(idx)), idle)
		}
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
		// registry rows joinFleet wrote, which peers would otherwise count for the fresh window, the janitors'
		// gauges, and the open shards. Shutdown cannot do it - the engine never reported itself started.
		e.leaveFleet(context.Background())
		e.stopJanitors()
		e.db.Close()
		return errors.Trace(err)
	}
	return nil
}

// Shutdown stops all worker goroutines and closes database connections. Idempotent: a call on an engine
// that is not running (never started, or already shut down) is a no-op, so it is safe to defer and to call
// more than once.
//
// Shutdown and Startup do not overlap: a Shutdown called while Startup is under way waits for it to finish, then stops
// the engine it started, and one called while another Shutdown is under way returns once the engine has stopped.
//
// Calls to the engine's operations still in progress when Shutdown is called - an Await, a Run, a Create waiting on
// the host's LoadGraph - are cancelled and return a 503, and Shutdown waits for each to return before it stops
// anything else; a host's LoadGraph that ignores its ctx delays it until that call returns. A call cancelled while
// writing may still have taken effect, as one whose caller gives up may: a Create can have made its flow. An Await
// whose flow stops as Shutdown lands can return the 503 rather than the outcome. Do not call Shutdown from inside a
// call the engine made to the host: it waits for that call to return.
//
// Shutdown does not consult ctx: it waits for the steps in flight to finish (each task is bounded by its
// time budget), and a deadline or cancellation on ctx does not make it return sooner. Background maintenance
// in progress - crash recovery, removing deleted flows, dropping expired archive days - is abandoned rather than
// waited for, and is completed by another replica or the next start. Making an archive day's tables is the
// exception: once begun it runs to its end, so Shutdown can wait for it.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.lifecycleMu.Lock()
	defer e.lifecycleMu.Unlock()
	if !e.closeIngress() {
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
	e.fanInHoldAt.Store(int64(min(maxWorkers, e.workersDispatch)))
	e.drainStop = make(chan struct{})
	e.backlogMu.Lock()
	e.backlogByShard = map[int]shardBacklog{}
	e.backlogBands = map[int]map[int]bool{}
	e.backlogMu.Unlock()
	e.backlogWanted.Store(false)
	// The background loops' contexts exist before the engine reports itself started, as their stop signals must:
	// a drain that found one missing would not stop its loop.
	var archiveCtx, latchCtx context.Context
	archiveCtx, e.archiveCancel = context.WithCancel(e.lifetimeCtx)
	latchCtx, e.latchCancel = context.WithCancel(e.lifetimeCtx)
	var janitorCtx context.Context
	janitorCtx, e.janitorCancel = context.WithCancel(e.lifetimeCtx)
	e.graphs = graphcache.New(graphCacheFlows, graphCacheGraphs)

	// Create the dwarf_* instruments and register the observable-gauge callback before the engine reports itself
	// started: an operation records into them once admitIngress admits it, and the started flag is what orders
	// that read after this write. Falls back to the global (no-op) provider when none was injected.
	err := e.initMetrics()
	if err != nil {
		e.diags.Logger().ErrorContext(e.lifetimeCtx, "Initializing metrics", "error", err)
	}
	e.started.Store(true)

	e.diags.Seams().Checkpoint(e.lifetimeCtx, diagnostics.CheckpointRuntimeStarted)
	if e.diags.Seams().IsFault(diagnostics.FaultInitRuntime) {
		e.unwindRuntime()
		return errors.New("injected fault: " + diagnostics.FaultInitRuntime)
	}
	// A piston idles on a shard this replica does not dispatch, and on every shard of an await-only replica
	// (SetWorkers(0)): it claims no work, and its idling reports as much, which is what keeps the replica out of
	// that shard's candidate partition.
	idle := e.zeroWorkers()
	for idx, a := range e.allActors() {
		a.piston.SetIdle(idle || !e.dispatchesOn(idx))
	}

	// Derive each piston's cycle period now that the cache is sized and the roles are known. recomputePools
	// re-derives it on every later role change.
	e.recomputeRefillIntervals()

	// The worker pool. Its callback is the claim skip plus processStep - NOT the host's ExecuteTask, which
	// runs deep inside processStep and would drag the whole execution core into the pool if it were the
	// boundary. The skip is checked HERE rather than inside processStep so it does not pay for that setup:
	// a sibling worker in this replica reserved the step within the last few seconds, its claim CAS may still
	// be in flight, and the piston re-selected it because an uncommitted claim still reads `pending`, so
	// issuing our own claim would cost a round trip to be told we lost.
	crew, perr := workers.New(&e.cache, e.db.Gate(), func(ctx context.Context, shard, stepID int, release func()) error {
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
	e.crew.SetLogger(e.diags.Logger())
	e.crew.SetMax(maxWorkers)
	// Start the resident set; the rest of the ceiling is spawned on demand whenever a worker's take of a
	// candidate leaves nobody idle - the long-task case, where the resident set alone would cap throughput
	// because nobody is left to dispatch.
	//
	// NOT cancelable, deliberately: no worker can observe the lifetime ctx's cancellation (it lands after
	// Drain), and a cancelable parent makes every query of every worker register on its one mutex.
	e.crew.Start(context.WithoutCancel(e.lifetimeCtx), resident)
	// The pistons run on a CHILD of the lifetime ctx, because they must stop before it does: the lifetime
	// ctx is deliberately left live until every other goroutine has drained, while piston.Run ends on ctx
	// alone and has no second signal - both of its queries are read-only, so abandoning one mid-flight
	// strands nothing.
	pistonCtx, pistonCancel := context.WithCancel(e.lifetimeCtx)
	e.pistonCancel = pistonCancel
	for _, a := range e.allActors() {
		e.pistonPool.Go(func() {
			a.piston.Run(pistonCtx)
		})
	}
	// Before Startup returns, so a shard this replica dispatches has its janitor active - and its gauges their
	// readings - from the start.
	e.startJanitors(janitorCtx)
	// The archive loop and the latch detector each run on their own child of the lifetime ctx, so the drain stops
	// waiting for a statement in flight rather than waiting it out: on a bloated table one statement runs for minutes,
	// and the stop must stay bounded by the steps in flight. Abandoning one strands nothing: every write they make is
	// a single statement or a single transaction, which lands whole or not at all.
	e.archiveWorker.Go(func() {
		e.archiveLoop(archiveCtx)
	})
	for shard, a := range e.allActors() {
		e.latchWorker.Go(func() {
			e.latchLoop(latchCtx, shard, a.latch)
		})
	}
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
	for _, a := range e.allActors() {
		if a.sonar == nil {
			continue
		}
		e.sonarPool.Go(func() {
			a.sonar.Run(sonarCtx)
		})
	}
	e.reconcileStop = make(chan struct{})
	e.reconcileWorker.Go(func() {
		e.runReconcileLoop()
	})
	return nil
}

// unwindRuntime undoes the part of initRuntime that runs before any goroutine is spawned, so a failed
// Startup leaves an engine that reports itself stopped rather than started-but-inert. The engine reported itself
// started, so a call may have been admitted; it is ended first, as Shutdown ends one, before Startup closes the
// shards. Then the metrics callback - it is registered with the OTEL reader and would otherwise query a database
// nobody is driving.
func (e *Engine) unwindRuntime() {
	e.closeIngress()
	e.closeMetrics()
}

// drainRuntime stops all goroutines in order. The caller (Shutdown) has already ended every admitted call and
// flipped e.started to false (closeIngress), which also serves as the single-shutdown guard.
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
	for _, a := range e.allActors() {
		a.piston.SetIdle(true)
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
	// The janitors stop before the workers drain, and are joined. A job's statement holds its connection - a
	// transaction, until it ends - with no deadline, so a worker that needs a connection to finish its step could
	// otherwise wait on it for as long as the statement took, with the drain waiting on the worker.
	if e.janitorCancel != nil {
		e.janitorCancel()
	}
	e.stopJanitors()
	// Closing the cache releases workers parked waiting for work, and closing the turnstiles releases
	// those waiting for a turn; Drain then closes the crew to new goroutines and waits. All three halves
	// are the crew's contract - see internal/workers. BOTH closes are required: a worker waiting for a
	// turn is not blocked on the cache, so closing only the cache would leave the drain waiting forever.
	e.cache.Close()
	e.db.CloseTurns()
	e.crew.Drain()
	close(drained)
	withdrawal.Wait() // before anything below can close what a late withdrawal writes through
	// Then the background loops, each by cancelling its ctx, so the drain stops waiting for a statement in flight:
	// the stop stays bounded by the steps in flight, not by the slowest statement.
	if e.archiveCancel != nil {
		e.archiveCancel()
	}
	e.archiveWorker.Wait()
	// The detector stops before the board closes, since Close does not interrupt a sweep in flight.
	if e.latchCancel != nil {
		e.latchCancel()
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
	// Close the latch boards, which nothing releases any more - every goroutine that could have is drained above. No
	// Await is parked on one: closeIngress ended every admitted call before the drain began. A board refuses a park
	// once closed.
	for _, a := range e.allActors() {
		a.latch.Close()
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
		Priority:       e.defaultPriority(),
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
		if opts.TimeBudget > 0 {
			resolved.TimeBudget = opts.TimeBudget
		}
	}
	return resolved
}

// --- Public API ---

// Create creates a new flow for a workflow and starts it, returning the running flow's key. opts carries
// the flow's policy (scheduling, DeleteOnCompletion, Baggage); nil uses defaults.
// For a flow that must wait for an external trigger, have the entry task call flow.Interrupt and resume it
// with Resume (which, unlike a separate start, also delivers a payload).
//
// An error from the host's LoadGraph is returned with its own status code, so a 503 (the graph's source
// unreachable) tells the caller to try again; nothing is created. A workflowURL that is not valid
// (workflow.IsValidURL) is a 400.
func (e *Engine) Create(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	ctx, release, err := e.admitIngress(ctx, "create")
	if err != nil {
		return "", errors.Trace(err)
	}
	defer release(&err)
	return e.create(ctx, workflowURL, initialState, opts)
}

// Snapshot returns a flow's outcome as it stands: its status, and for a closed flow its final state and its error or
// reason. For an interrupted flow, State is the state at the interrupt and InterruptPayload what flow.Interrupt was
// passed - of the interrupt the next Resume resolves, when several are pending. A running flow has an empty State:
// the state of work in progress is not assembled. Any flow key works, a subgraph child's included, and so does an
// archived flow's, or one marked for deletion until it is removed. Returns 404 for an unknown key, including a
// deleted flow, and 400 for a malformed one.
func (e *Engine) Snapshot(ctx context.Context, flowKey string) (outcome *workflow.FlowOutcome, err error) {
	ctx, release, err := e.admitIngress(ctx, "snapshot")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	return e.snapshot(ctx, flowKey)
}

// Fingerprint returns a short digest of a flow's progress and its status, for a caller polling for change: the
// fingerprint differs whenever the flow's status does, or a step of the flow or of any subgraph beneath it is created
// or changes, and is the same while nothing does, so a caller can poll it and read Snapshot or History only when it
// moves. Errors as Snapshot.
func (e *Engine) Fingerprint(ctx context.Context, flowKey string) (fingerprint string, status string, err error) {
	ctx, release, err := e.admitIngress(ctx, "fingerprint")
	if err != nil {
		return "", "", errors.Trace(err)
	}
	defer release(&err)
	return e.fingerprint(ctx, flowKey)
}

// Resume continues a flow paused by flow.Interrupt, addressed by its root flow key, whichever subgraph the interrupt
// is in. resumeData goes to the interrupted task, as what its flow.Interrupt call returns when the task runs again;
// it is not written to the flow's state, so a task that needs it there sets it. Each call resolves one interrupt -
// the one Snapshot reports - and the flow runs again once none is left. Returns 404 for an unknown key, 400 for a
// malformed key or a subgraph child's, or resume data that is not JSON-marshalable or holds text the database
// cannot store, 413 for resume data over the size limit, and 409 for a flow that is not interrupted, including one a Terminate stopped first.
func (e *Engine) Resume(ctx context.Context, flowKey string, resumeData any) (err error) {
	ctx, release, err := e.admitIngress(ctx, "resume")
	if err != nil {
		return errors.Trace(err)
	}
	defer release(&err)
	return e.resume(ctx, flowKey, resumeData)
}

// Terminate forcefully, unconditionally stops a flow. In-flight work is abandoned immediately - it is not
// awaited, and no handler runs. The flow reads "terminated" as soon as this returns. Terminating a flow that
// has already closed is a no-op that returns nil and leaves its outcome as it was, archived or not; an unknown or
// deleted flow is a 404. The reason is a human-readable note of at most 4,096 bytes of UTF-8 text with no NUL; any other reason is refused with a 400.
func (e *Engine) Terminate(ctx context.Context, flowKey string, reason string) (err error) {
	ctx, release, err := e.admitIngress(ctx, "terminate")
	if err != nil {
		return errors.Trace(err)
	}
	defer release(&err)
	return e.terminate(ctx, flowKey, reason)
}

// Cancel requests graceful cancellation of a flow, addressed by its root flow key. Every step in progress
// when it is called - pending, running, parked on a subgraph, or interrupted - is covered, as is a step that
// finished just before and is handing over to its next one: a step not yet started is skipped, and a step
// already running finishes before the cancellation takes effect. A subgraph a covered step arms is born
// cancelled: its first task does not run, and the child delivers the cancellation as any covered step does - to
// that task's onError transition, which can carry the child on, or as the child's cancelled outcome, which reaches
// the caller as its subgraph's error. A subgraph armed while Cancel is still recording the request, before it
// returns, can still start as an ordinary child, which Terminate stops. A covered step receives the cancellation as
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
//
// The reason is a human-readable note of at most 4,096 bytes of UTF-8 text with no NUL; any other reason is
// refused with a 400.
func (e *Engine) Cancel(ctx context.Context, flowKey string, reason string) (err error) {
	ctx, release, err := e.admitIngress(ctx, "cancel")
	if err != nil {
		return errors.Trace(err)
	}
	defer release(&err)
	return e.cancel(ctx, flowKey, reason)
}

// Fork clones a closed flow's prefix up to the given step into a new, self-contained running flow and
// re-executes from that step with optional stateOverrides applied to it. The original flow is never
// modified. The fork inherits the original's scheduling and baggage (it does not take FlowOptions).
// Returns the new flow's key.
func (e *Engine) Fork(ctx context.Context, stepKey string, stateOverrides any) (flowKey string, err error) {
	ctx, release, err := e.admitIngress(ctx, "fork")
	if err != nil {
		return "", errors.Trace(err)
	}
	defer release(&err)
	return e.forkFlow(ctx, stepKey, stateOverrides)
}

// History returns a flow's steps, ordered by depth, each with its key, task, status, error, timing and links to
// the steps before and after it. A step that ran a subgraph carries the child's key and steps in SubgraphKey and
// SubHistory. The steps carry no State or Changes: Step returns one step's. Any flow key works, a subgraph child's
// included, and so does an archived flow's. Returns 404 for an unknown key, including a flow deleted or marked for
// deletion, and 400 for a malformed one.
func (e *Engine) History(ctx context.Context, flowKey string) (steps []workflow.FlowStep, err error) {
	ctx, release, err := e.admitIngress(ctx, "history")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	return e.history(ctx, flowKey)
}

// Summary returns one flow's summary by its key, as the lists return it: open or closed, archived or not.
// FlowSummary.Subgraph tells a subgraph child from a root, RootFlowKey names the root, which the operations that change
// a flow take, and ParentStepKey the step that called the child. Returns 404 for an unknown key, including a flow
// deleted or marked for deletion, and 400 for a malformed one.
func (e *Engine) Summary(ctx context.Context, flowKey string) (summary *workflow.FlowSummary, err error) {
	ctx, release, err := e.admitIngress(ctx, "summary")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	return e.summary(ctx, flowKey)
}

// Step returns one step by its key, as History lists it but with its State, the input it ran on, and its Changes,
// what it wrote. Step keys come from History, from FlowSummary.ParentStepKey, and from a task's own flow.StepKey.
// Returns 404 for an unknown key, for a step of a flow marked for deletion once its deletion is due (at once after
// Delete, after the grace for DeleteOnCompletion), and 400 for a malformed one.
func (e *Engine) Step(ctx context.Context, stepKey string) (step *workflow.FlowStep, err error) {
	ctx, release, err := e.admitIngress(ctx, "step")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	return e.step(ctx, stepKey)
}

// ListOpen returns the open flows - running or interrupted - matching query's filters, and its window on when each
// last changed status (FlowSummary.UpdatedAt) if it sets one: at most Query.Limit of them, the most recently updated
// first, ties broken by shard and then newest flow. Query.Limit is required; the window's bounds are not. A
// Query.Status that is not open is a 400.
//
// It has no pagination: an open flow changes under the caller, so a caller wanting other flows narrows the filters or
// the window. The order across shards compares the times recorded by each shard's own database, so flows updated
// within the clock skew between two shard servers may be listed out of order. A flow that closes leaves this list at
// once for ListRecentlyClosed, and moves on to ListClosed when it is archived, about a minute later, longer while
// archiving lags or fails. A flow deleted without archiving - DeleteOnCompletion, a retention of 0 - is never in ListClosed.
func (e *Engine) ListOpen(ctx context.Context, query workflow.Query) (summaries []workflow.FlowSummary, err error) {
	ctx, release, err := e.admitIngress(ctx, "list_open")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	return e.listOpen(ctx, query)
}

// ListClosed returns the closed flows - completed, failed, terminated or cancelled - matching query's filters within
// its window on when each was archived (FlowSummary.ArchivedAt): at most Query.Limit of them, the most recently
// archived first, ties broken by shard and then newest flow. A flow is archived, and so listed here, about a minute
// after it closes, longer while archiving lags or fails; until then it is in ListRecentlyClosed. Query.Limit and
// both of the window's bounds are required, and a Query.Status that is not closed is a 400.
//
// A window is cut to what the archive can hold: nothing before the retention's first day (SetRetention), nothing
// archived after now; a window outside both returns no flows. What is left may reach at most 31 days of the archive -
// today and the 30 before it, as much as the default retention keeps - or the call is a 400; split a longer range into
// windows. Each day the window reaches is read on every shard, so a search over many days wants a low Limit. Times
// compare to the millisecond. At a retention of 0 nothing
// is archived, so nothing new is listed.
//
// It has no pagination and no cursor. An archived flow never changes, so a caller can walk a time range in windows,
// each ending just after the oldest ArchivedAt it holds, dropping the flows already held: flows archived together
// share one ArchivedAt. A caller polling forward from its last check up to now sees each flow once, provided its newest
// bound stays behind now by more than archiving a tree takes: flows archived together are stamped with their batch's
// start, so they can appear after a later stamp has. That is seconds, but a tree too large for one batch is copied
// over many and listed once the copy is done, stamped with the time it began - minutes, or hours for the very largest.
// Archive times come from each shard's database and the window is cut by this process's clock, so flows within the
// skew between them may be listed out of order, or, at a window's edge, missed.
func (e *Engine) ListClosed(ctx context.Context, query workflow.Query) (summaries []workflow.FlowSummary, err error) {
	ctx, release, err := e.admitIngress(ctx, "list_closed")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	limit, err := listLimit(query)
	if err != nil {
		return nil, errors.Trace(err)
	}
	rows, err := e.listArchived(ctx, query, limit, false, "list_closed")
	if err != nil {
		return nil, errors.Trace(err)
	}
	return summariesOf(rows), nil
}

// ListRecentlyClosed returns the closed flows - completed, failed, terminated or cancelled - not archived yet, matching
// query's filters and its window on when each closed (FlowSummary.UpdatedAt) if it sets one: at most Query.Limit of
// them, the most recently closed first, ties broken by shard and then newest flow. Query.Limit is required; the
// window's bounds are not. A Query.Status that is not closed is a 400.
//
// A flow is listed here from when it closes until it is archived, about a minute later, longer while archiving lags or
// fails, and then in ListClosed. To see every closed flow, call ListRecentlyClosed and then ListClosed, and drop duplicates by
// FlowKey: a flow archived between the two calls is in both, while in the other order it would be in neither. A flow
// marked for deletion is not listed. At a retention of 0 nothing is archived: a closed flow is listed here until it is
// deleted, about a minute after it closes. Like ListOpen it has no pagination, and flows closed within the clock skew
// between two shard servers may be listed out of order.
func (e *Engine) ListRecentlyClosed(ctx context.Context, query workflow.Query) (summaries []workflow.FlowSummary, err error) {
	ctx, release, err := e.admitIngress(ctx, "list_recently_closed")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	return e.listRecentlyClosed(ctx, query)
}

// Delete removes a closed flow and its subgraph subtree. An archived flow is gone from every read when Delete returns:
// it is marked for deletion, and its rows are deleted in the background, as Purge's are. One not archived yet - the
// minute after it closes - is marked for deletion too, and removed in the background shortly after,
// without archiving it: History, Summary and Step return 404 for it immediately, though Snapshot, Fingerprint and Await
// still report its outcome until it is removed, and calling Delete on it again is success. The same holds for
// a flow archived so recently that its copy in the live tables is still being removed. Returns 404 for an unknown key,
// including a flow already deleted from the archive; 400 for a subgraph-child key (address the root instead); and 409
// for an open flow: Terminate it first. Deleting many flows this way costs the database what Purge does: see Purge.
func (e *Engine) Delete(ctx context.Context, flowKey string) (err error) {
	ctx, release, err := e.admitIngress(ctx, "delete")
	if err != nil {
		return errors.Trace(err)
	}
	defer release(&err)
	return e.deleteFlow(ctx, flowKey)
}

// Purge removes the closed flows ListClosed would list for a query, and their subgraph subtrees, oldest archived
// first: at most Query.Limit roots in a call. A removed root is gone from the archive's reads and from every list when
// the call returns; Snapshot, Fingerprint and Await of one archived so recently that its copy in the live tables is
// still being removed keep reporting its outcome until that copy is gone. Each root is marked for deletion, and its
// tree's rows are deleted in the background, so its storage is released later, or when its day drops.
// dwarf_archive_deletions_pending counts marked trees the background deletion found and could not delete yet - held by
// another transaction, or failing. A flow awaiting archival is not in ListClosed and Purge leaves it - ListRecentlyClosed lists it, and Delete schedules its removal by key. Open
// flows are never touched, and a Query.Status that is not closed is a 400, as is IncludeSubgraphs (a subgraph child is
// removed with its root).
//
// Query.Limit (at most 65,536) and both bounds of the window are required; the window is ListClosed's, on when a flow
// was archived, and may reach at most 31 days of the archive, or the call is a 400: purge a longer range a month at a
// time. Returns how many roots it removed, and whether it is done: call it again until done is true. done is true when
// the call found fewer matching roots than its limit and removed every one; it describes what the call found, and
// covers only what is archived: a flow closed shortly before the call, still on its way to the archive, is not purged until a call
// after it is archived. For an erasure that must also take those, confirm they have reached the archive and call
// again, or Delete them by key. Archival failures can keep closed flows live beyond the usual minute.
//
// Deleting rows writes in proportion to them and leaves dead rows for the database to vacuum; Purge is for removing
// specific records, and bulk removal is the retention's (SetRetention) and PurgeBefore's, which drop whole days. That
// vacuum competes with the work in flight, so run a large purge in off-peak hours.
func (e *Engine) Purge(ctx context.Context, query workflow.Query) (removed int, done bool, err error) {
	ctx, release, err := e.admitIngress(ctx, "purge")
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	defer release(&err)
	return e.purge(ctx, query)
}

// ShardInfo reports, for every shard, whether it answers a trivial query and how fast. It returns a row per
// shard even when some are down, with the failure recorded on that shard's row, so it is the call to make when a
// shard is suspect. It reads no table, so it costs the same however large the shards grow.
func (e *Engine) ShardInfo(ctx context.Context) (shards []ShardSummary, err error) {
	ctx, release, err := e.admitIngress(ctx, "shard_info")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	shards, err = e.shardInfo(ctx)
	// A probe's failure is recorded on its row, not returned, so a probe Shutdown cancelled would read as a shard down.
	if shutErr := shutdownErr(ctx); shutErr != nil {
		return nil, shutErr
	}
	return shards, err
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
func (e *Engine) Await(ctx context.Context, flowKey string) (outcome *workflow.FlowOutcome, err error) {
	ctx, release, err := e.admitIngress(ctx, "await")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	return e.awaitStopped(ctx, flowKey)
}

// Poll returns a flow's current outcome, blocking up to the ctx deadline for it to stop. Unlike Await, a ctx
// timeout is not an error: it returns the current outcome, which has not stopped (Stopped() reports false), so a
// caller bridging a long-running flow to a bounded request (e.g. an HTTP poll) can answer within its budget and
// re-poll. A real failure still returns an error.
//
// The ctx deadline IS the budget, so pass one; without it Poll blocks on the same long fallback budget as
// Await before answering, which is rarely what a polling caller wants.
func (e *Engine) Poll(ctx context.Context, flowKey string) (outcome *workflow.FlowOutcome, err error) {
	ctx, release, err := e.admitIngress(ctx, "poll")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer release(&err)
	outcome, err = e.await(ctx, flowKey)
	if err == nil && !outcome.Stopped() {
		// Not stopped means the caller's budget ran out; a wait Shutdown ended is a 503.
		if shutErr := shutdownErr(ctx); shutErr != nil {
			return nil, shutErr
		}
	}
	return outcome, err
}

// Run creates, starts, and awaits a flow in one call, returning the new flow's key alongside its outcome
// (the key is the flow's identity, not part of the outcome). opts carries scheduling and the opaque host
// Baggage; nil opts uses defaults.
//
// Error semantics differ by phase. A create failure returns flowKey "" and a nil outcome - no flow
// exists. An error from the host's LoadGraph is a create failure, returned with its own status code. An
// await failure - most commonly the caller's ctx expiring before the flow stops - leaves the flow running
// (it is durable and not bound to this call) and returns its flowKey with a nil outcome and the error, so
// the caller keeps a handle to Await/Snapshot/Terminate it later. Run never terminates the flow
// on the caller's behalf; a caller that wants the flow torn down on timeout calls Terminate explicitly. As
// with Await, a wait that runs out returns a 408 error that satisfies errors.Is(err,
// context.DeadlineExceeded).
func (e *Engine) Run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, outcome *workflow.FlowOutcome, err error) {
	ctx, release, err := e.admitIngress(ctx, "run")
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	defer release(&err)
	return e.run(ctx, workflowURL, initialState, opts)
}

// Continue creates and runs a new flow from the completed root flow flowKey names: the new flow starts from that
// flow's final state with additionalState folded in through the graph's reducers, runs the same graph, and
// inherits its policy (scheduling, baggage) - it does not take FlowOptions. For a next flow with different
// policy, use Create with the final state from Snapshot.
//
// Continue builds on the flow it is given, so continuing one flow twice makes two successors; a caller that
// chains flows continues the key each Continue returns. Like Create, it is not idempotent: a Continue retried
// after a lost response makes a second flow. It returns 404 for an unknown key, 400 for a subgraph child's
// key or additionalState holding an invalid field name (workflow.IsValidFieldName) or text the database cannot
// store, and 409 for a flow that is not completed or is scheduled for deletion.
func (e *Engine) Continue(ctx context.Context, flowKey string, additionalState any) (nextKey string, err error) {
	ctx, release, err := e.admitIngress(ctx, "continue")
	if err != nil {
		return "", errors.Trace(err)
	}
	defer release(&err)
	return e.continueFlow(ctx, flowKey, additionalState)
}
