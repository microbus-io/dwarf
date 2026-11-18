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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
)

// The connection-budget and capacity constants below were measured on Cloud SQL PostgreSQL 16 (4-64 vCPU,
// 1 TB SSD, saturated, 16-minute windows). They are engine knowledge; the operator's one lever is
// ShardSpec.MaxConnections.
const (

	// nearBudget and farBudget are a shard's fleet-wide connection budget at nearRTTMs and at farRTTMs;
	// shardBudget ramps linearly between them and holds them outside. Do not size the pool per vCPU: measured
	// saturated, near (0.25 ms), 250 connections beat a 3-6-per-vCPU pool on every tier (+42% at 4 vCPU, +7%
	// at 8, +15% at 16, +10% at 32 and 64) and ran stable at 4 vCPU, where 125 and 250 performed alike.
	// Throughput keeps rising past it, slowly - group commit packs more commits into each WAL flush (22 -> 70
	// per flush from 59 to 384 connections on 16 vCPU) - so the constant sits on the shoulder of the curve,
	// not at a knee: 384 measured +5% over 192 there. At 1 ms, 400-500 recovered what distance cost 200-250
	// (16 vCPU 8,373 vs 8,006; 64 vCPU 24,254 vs 19,567), which is what the far end of the ramp encodes.
	//
	// A constant rather than a fraction of the server's max_connections, because that limit is not set
	// sensibly everywhere: MySQL defaults to 151, SQL Server reports 32,767, a pooler reports whatever it
	// fronts. The limit enters only as a clamp (see shardBudget).
	nearBudget = 250
	farBudget  = 500
	nearRTTMs  = 0.25
	farRTTMs   = 1.0

	// maxDispatchers caps how many replicas dispatch one shard. Two is the availability floor (one dying
	// leaves the other running while its slot passes) and enough engine CPU: a saturated 32 vCPU shard takes
	// ~6 engine cores, ~3 per dispatcher.
	//
	// Do not raise it for engine CPU. Every dispatcher runs its own piston, and its band scan and fetch walk
	// the shard's whole band - the residue predicate that gives each its slice cannot seek - so the shard's
	// refill cost grows with the dispatcher count. Measured locally (one shard, budget 40, saturated): the
	// database spent 727 ms per 1,000 steps at one dispatcher and 957 at four, every per-step statement flat
	// and the whole increase in the band scan and the fetch, each called ~3.5x as often.
	maxDispatchers = 2

	// sqliteBudget is the budget on SQLite, in place of the ramp. SQLite runs one writer at a time, so
	// connections past a handful only queue on its write lock and spin in busy_timeout, and it reports no
	// connection limit to clamp against.
	sqliteBudget = 8

	// workersPerConnBudget sizes the dispatch count - the candidate cache (twice it) and with it every
	// refill scan - from the aggregate connection budget: dispatch is database-bound, and useful
	// dispatching workers = conns x T/db, with T/db measured ~3 for no-op tasks. 8 is deliberately
	// generous for short tasks. A worker blocked in a long ExecuteTask holds no connection, so the cache
	// must never be sized from the worker maximum (see workerCeiling), which serves long tasks only.
	workersPerConnBudget = 8

	// residentWorkersPerConn sizes the RESIDENT (eagerly spawned) worker set, which is also the floor the crew
	// never shrinks below. It is a floor, not a target: the crew grows one worker per take that leaves nobody
	// idle, so demand fills in what the floor leaves out within microseconds per spawn.
	//
	// Do not size the floor from workersPerConnBudget. At the 250-connection budget that put 2,000 resident
	// workers on every shard, and one replica dispatching 4 shards ran 8,000 of them: measured on 4 x 32 vCPU
	// shards, ~28% of its CPU went to GC (stack scanning 22%, mark-assist 18.5%) at 0.54 ms of engine CPU per
	// step, with the databases holding ~43 backends per shard `idle in transaction` waiting on it - against
	// 0.31 ms per step for the same shards driven from four replicas of ~2,000 workers each. A parked goroutine
	// still costs a stack scan every GC cycle, and the cycles come at the allocation rate, so the cost is the
	// product of the floor and throughput.
	residentWorkersPerConn = 2

	// turnstilePassesPerConn sizes each shard's turnstile: the number of turns that may be out at once,
	// as a multiple of that shard's connection pool.
	//
	// ONE TURN PER CONNECTION IS CATASTROPHIC, AND A QUEUE AT THE POOL IS WHY. Measured at 600 flows/s on a
	// local Postgres: 281 steps/s against the 1,687 of a gate sized above the pool on the same rig, a 6x collapse.
	// With exactly as many turns as connections there are exactly as many candidates for a connection, so
	// every gap between a turn-holder finishing and the next waiter being woken, scheduled and asking the pool
	// is IDLE CONNECTION TIME - on the critical path of every round trip a step makes. Any multiple above one
	// keeps a queue at the pool, and the queue is what keeps connections busy.
	//
	// So the turnstile does not remove the wait for a connection and does not empty the pool's queue - a
	// caller holding a turn still competes for one, and still waits when they are all busy. What it changes
	// is WHO gets to compete and in what ORDER: the population at the pool is bounded by the turn count, and
	// admission into it is by band and then by age.
	//
	// THE TURN COUNT IS ALSO THE CREW'S SIZE UNDER SATURATION. A worker waiting for a connection holds a
	// candidate and so counts busy, and the crew grows on every take that leaves nobody idle - so a saturated
	// crew climbs from its resident floor to the turn count, whatever the floor is (measured: 40 connections,
	// a floor of 80, a crew of 325 at 8x), so the multiple is kept small. Measured locally on one shard at 40
	// connections, saturated, 2x / 4x / 8x ran at the same rate in each of the two throughput states every run
	// passed through, with crews of 87 / 167 / 325; and at 8x, one replica dispatching 4 shards of 250
	// connections ran 8,000 workers and spent ~28% of its CPU in GC (see residentWorkersPerConn). 10x and 12x
	// were measured within noise of 8x on a smaller pool; nothing above 1x has been shown to cost throughput.
	turnstilePassesPerConn = 2

	// completionServerMs is the server-side half of a completion: row work plus the group-committed
	// WAL fsync, measured ~3ms (cross-checked by a whole-step fit db = 12.1 x RTT + 4.4ms, taken on PostgreSQL
	// before the transition was pipelined - the 4.4ms intercept is the part that carries over).
	completionServerMs = 3.0

	// defaultRTTMs is the same-zone round-trip time (measured 0.28-0.34ms on GCP private IP), used by the worker
	// ceiling only when the Startup probe fails. Deliberately the OPTIMISTIC value: a failed probe should not
	// silently inflate the worker ceiling, and a small RTT yields a small txTime... which yields a LARGER
	// ceiling. So the fallback is paired with the safety factor below; a persistently unprobeable database is
	// a louder problem than a mis-sized pool. The connection budget does not use it: an unprobed shard takes
	// the near budget.
	defaultRTTMs = 0.3

	// readerIdleConns / readerOpenConns size a replica's pool to a shard it does not dispatch. It still beats
	// that shard's registry every second and polls it for parked Awaits, so one connection stays warm; the
	// second lets a Create's transaction proceed alongside a poll.
	readerIdleConns = 1
	readerOpenConns = 2

	// fleetLimitPercent is how much of a server's connection limit the fleet may hold before checkFleetFits
	// warns. The limit is shared with every other client of that database.
	fleetLimitPercent = 80

	// workerSafetyFactor discounts the theoretical worker ceiling. The clean model assumes the whole
	// connection pool drains completions; in a real storm, claims compete with the drain (~2x), tx time
	// varies under contention, a mature database is ~20% slower, and in-flight steps are not evenly
	// spread across shards. 1/4 is the margin between "the arithmetic says X" and "we would stake the
	// lease protocol on X".
	workerSafetyFactor = 0.25
)

/*
completionRoundTrips is the number of database round trips in a linear step's post-task phase on a driver: the
standalone completion write, then the transition transaction. Multiplied by the measured RTT it gives the network
half of the completion cost.

It is counted, not fitted, and it differs by driver:
  - PostgreSQL 4: the completion write (RETURNING), BEGIN, the transition's one pipelined batch, COMMIT.
  - SQLite 6: the completion write (RETURNING), BEGIN, lock grab, successor INSERT, fenced own-row write, COMMIT.
  - SQL Server 7: as SQLite, plus the SET XACT_ABORT ON that sequel issues at the start of every transaction.
  - MySQL 7: as SQLite, but the completion write is an UPDATE and a SELECT, for want of RETURNING. Each statement
    counts once, as with interpolateParams=true in the DSN; without it the driver prepares every parameterized
    statement in a round trip of its own.

Recount whenever a statement moves into or out of the post-task phase. TestRoundTrips_LinearStep measures the
whole step, which is this count plus the claim and the flow read: one round trip on PostgreSQL, whose claim
returns the flow row (on a graph-cache hit; a miss reads the graph separately), and two elsewhere.
*/
func completionRoundTrips(driver string) int {
	switch driver {
	case "pgx":
		return 4
	case "mysql", "mariadb", "mssql":
		return 7
	}
	return 6
}

// workerCeiling is the largest worker count that keeps a synchronized completion storm inside the
// crash-recovery lease margin, derived per shard and taken at its worst.
//
// The scenario it bounds: every in-flight task blocks on one downstream (an LLM provider outage) and
// is released at once, so N finished tasks contend for the shard's M connections to write their
// completion transactions. They drain at ~M/txTime, and a completion that out-waits its remaining
// margin is fenced after a peer re-claims the step - correct, but the task RE-EXECUTES, duplicating
// the most expensive work at the worst possible moment. Solving `N x txTime / M <= margin` for N:
//
//	N_max = M x margin / txTime x safety      per shard; the fleet takes the minimum
//	txTime = completionRoundTrips(driver) x RTT + completionServerMs
//
// Every input is engine-visible: M is the derived pool, the margin is the engine's own constant, and
// RTT is measured when the shard opens (database.Shard.RTT). Nothing here needs the task duration T, which is exactly why
// this - and not `M x T/db` - is the number the engine can derive for itself.
func workerCeiling(open int, rttMs float64, driver string) int {
	txTimeMs := float64(completionRoundTrips(driver))*rttMs + completionServerMs
	if txTimeMs <= 0 {
		txTimeMs = completionServerMs
	}
	marginMs := float64(30 * time.Second / time.Millisecond)
	return max(1, int(float64(open)*marginMs/txTimeMs*workerSafetyFactor))
}

// shardSizing is one shard's connection budgets for a run. Every input - the spec, the RTT and the server's
// max_connections measured when the shard opened - is fixed for the run.
type shardSizing struct {
	// budget is this replica's view of the shard's fleet-wide budget: shardBudget at the RTT it probed.
	budget int
	// serverMax is the server's max_connections, 0 when unknown.
	serverMax int
}

// sizeShard computes one shard's sizing from its spec, the RTT measured when it opened (0 = unprobed), the
// server's max_connections (0 = unknown) and the shard's driver.
func sizeShard(spec ShardSpec, rttMs float64, serverMax int, driver string) shardSizing {
	return shardSizing{
		budget:    shardBudget(spec, rttMs, serverMax, driver),
		serverMax: serverMax,
	}
}

// shardBudget is a shard's fleet-wide connection budget: the connections every replica dispatching it may
// hold there together. ShardSpec.MaxConnections, when set, is the budget exactly - the operator's lever, with
// no ramp and no clamp. Otherwise nearBudget, ramping linearly to farBudget between nearRTTMs and farRTTMs,
// clamped to half the server's max_connections - except on SQLite, which takes sqliteBudget.
//
// The clamp is at HALF, not the whole limit, because the limit is shared: a 4-vCPU Cloud SQL instance
// (max_connections 500) at 1 ms would otherwise hand the engine every connection, leaving none for the
// application, admin sessions or the server's reserved slots. Half also adapts the budget to a server
// configured small - a stock self-hosted Postgres (100) gets 50 - where the operator raises the limit and
// the engine follows. An unprobed shard (rttMs <= 0) takes the near budget: a failed probe must not
// inflate a pool.
//
// This is a lookup on facts fixed at Startup, not a controller: it reads nothing that moves while the
// engine runs. Do not grow it into one.
func shardBudget(spec ShardSpec, rttMs float64, serverMax int, driver string) int {
	if spec.MaxConnections > 0 {
		return spec.MaxConnections
	}
	if driver == "sqlite" {
		return sqliteBudget
	}
	budget := float64(nearBudget)
	if rttMs > nearRTTMs {
		f := min(1, (rttMs-nearRTTMs)/(farRTTMs-nearRTTMs))
		budget += f * (farBudget - nearBudget)
	}
	n := int(math.Round(budget))
	if serverMax > 0 {
		n = min(n, serverMax/2)
	}
	return max(2, n)
}

// shardPool returns the idle/open pool sizes for one shard. Idle equals open: database/sql closes a returned
// connection whenever the idle set is full, so an idle cap below the ceiling makes demand that swings between
// the two close and reopen backends (a server process plus authentication each) for as long as the load
// lasts. Shedding a pool that load has left is ConnMaxIdleTime's job, not the idle cap's. The shard's budget
// is split across its DISPATCHERS - slots of them, this replica among them. slots == 0 means this replica
// does not dispatch the shard and holds the small fixed pool its reads and heartbeat need.
//
// The split rounds to the nearest connection rather than flooring: each dispatcher gains under half a
// connection, so the shard exceeds its budget by at most slots/2.
func shardPool(budget, slots int) (idle, open int) {
	if slots <= 0 {
		return readerIdleConns, readerOpenConns
	}
	open = max(2, int(math.Round(float64(budget)/float64(slots))))
	return open, open
}

// dispatcherSlots is how many replicas dispatch one shard: maxDispatchers, never more than the replicas there
// are. It must agree across the fleet - it is the width of every shard's cut, and two replicas that disagreed
// would both claim, or both leave, one slot - and it does, because it depends on
// nothing but the candidate count every replica reads from the same registry. Keep it that way: an input a
// replica derives for itself (its probed RTT, its read of the server's limit) would split the fleet.
func dispatcherSlots(replicas int) int {
	return min(maxDispatchers, max(1, replicas))
}

// inWindow reports whether the replica at rank in the fleet's shared order dispatches the shard at place in
// the sorted shard indices: the shard at place p takes the slots positions starting at p*slots, wrapping
// around the candidates. The shards tile the order whatever their indices, so every replica carries the floor
// or the ceiling of shards*slots/candidates roles - a balance an independent draw per shard does not give (at
// 8 replicas and 4 shards it leaves about a third of the replicas with no role and others with three). The
// first shard's window is the top of the order, so a single shard is dispatched by the first slots replicas.
// Pure, and the same on every replica that holds the same shard set. A rank outside the candidates is never in
// the window.
//
// Anchored on the place among the indices rather than on the index, because the steady state is what runs:
// sparse indices (1, 3, 4) would make index-anchored windows collide and leave replicas idle. The cost is
// transient: while a shard is added or retired BELOW others, replicas briefly hold different shard sets and
// disagree on the windows of the shards above it - between none and twice the dispatchers there - until the
// roll completes. A shard added above every other moves nothing.
func inWindow(rank, candidates, place, slots int) bool {
	if rank < 0 || rank >= candidates {
		return false
	}
	start := place * slots % candidates
	return (rank-start+candidates)%candidates < slots
}

// slotsOn is this replica's role on one shard: how many dispatcher-sized shares its budget divides into
// when this replica dispatches it - the dispatchers plus every replica draining a share from outside the
// cut - or 0 when it does not. It is the pool divisor, the piston's on/off switch and the doorbell's gate
// in one number.
//
// The slots are derived from the replicas that MAY dispatch, not from every registered one: an await-only
// replica holds connections but takes no rank, and counting it would divide the budget among more
// dispatchers than exist - one worker beside an await-only frontend would run on half its shard's budget.
//
// Every doubt resolves toward dispatching, because the two errors are not symmetric: a replica wrongly
// dispatching over-connects the shard by one share, while a shard nobody dispatches runs nothing at all.
// A shard with no Sonar dispatches solo.
func (e *Engine) slotsOn(shard int) int {
	return e.readRole(shard).slots
}

// roleReading is what one reading of this replica's standing on a shard implies: the slots slotsOn returns, and
// how many pistons cycle the shard across the fleet - its dispatchers. Draining replicas hold a share but idle
// their pistons, so they are not scanners.
type roleReading struct {
	slots, scanners int
	// held is what a replica holding a pool for in-flight steps on a shard it no longer dispatches divides the
	// budget by: the same count its peers divide by, which counts it among the draining.
	held int
}

// readRole reads this replica's standing on one shard ONCE and derives its roleReading from it. A shard with no
// Sonar is dispatched and scanned by this replica alone.
func (e *Engine) readRole(shard int) roleReading {
	s := e.sonarFor(shard)
	if s == nil {
		if e.zeroWorkers() {
			return roleReading{held: 1}
		}
		return roleReading{slots: 1, scanners: 1, held: 1}
	}
	st := s.Standing()
	scanners := dispatcherSlots(st.Candidates)
	held := scanners + st.Draining + 1
	if e.zeroWorkers() {
		return roleReading{held: held}
	}
	if !st.InCut {
		return roleReading{scanners: scanners, held: held}
	}
	return roleReading{slots: scanners + st.Draining, scanners: scanners, held: held}
}

// rolesByShard is every open shard's roleReading, read once so that everything derived from the roles in one
// pass is derived from the same reading.
func (e *Engine) rolesByShard() map[int]roleReading {
	roles := make(map[int]roleReading, len(e.db.Indices()))
	for _, idx := range e.db.Indices() {
		roles[idx] = e.readRole(idx)
	}
	return roles
}

// slotsOf projects the slots out of a set of role readings.
func slotsOf(roles map[int]roleReading) map[int]int {
	slots := make(map[int]int, len(roles))
	for idx, r := range roles {
		slots[idx] = r.slots
	}
	return slots
}

// zeroWorkers reports a replica configured with SetWorkers(0): it creates, awaits and reads, and dispatches
// no shard.
func (e *Engine) zeroWorkers() bool {
	return e.workers.Load() == 0
}

// dispatchesOn reports whether this replica dispatches one shard - whether a step on it may be offered to
// this replica's own cache. True for a shard with no recorded role, which is the solo default.
func (e *Engine) dispatchesOn(shard int) bool {
	b := e.dispatchFlag(shard)
	return b == nil || b.Load()
}

// shardRole is this replica's standing on one shard in one run.
type shardRole struct {
	// dispatching is whether this replica claims steps on the shard.
	dispatching atomic.Bool
	// inFlight counts the steps this replica's workers are processing on the shard, claim to transition.
	inFlight atomic.Int64
}

// roleOn is one shard's role in the current run, or nil before the first Startup and for a shard that run did not
// open. Its dispatch flag is flipped by applyRole under poolsLock, and its in-flight count is moved lock-free by the
// workers.
func (e *Engine) roleOn(shard int) *shardRole {
	if a := e.actorsOn(shard); a != nil {
		return &a.role
	}
	return nil
}

// dispatchFlag is one shard's role flag in the current run's map, or nil before the first Startup and
// for a shard that run did not open.
func (e *Engine) dispatchFlag(shard int) *atomic.Bool {
	if r := e.roleOn(shard); r != nil {
		return &r.dispatching
	}
	return nil
}

// inFlightOn is how many steps this replica's workers are processing on one shard.
func (e *Engine) inFlightOn(shard int) int64 {
	if r := e.roleOn(shard); r != nil {
		return r.inFlight.Load()
	}
	return 0
}

// checkFleetFits warns when the connections the fleet can hold on one shard approach the server's limit:
// the budget, which the dispatchers and any replicas draining a share divide between them, plus up to
// readerOpenConns for every other replica.
// Advisory and edge-triggered - it never resizes anything, since shrinking dispatchers to make room for
// idle readers is the wrong trade, and a fleet that size wants an external pooler. It is evaluated on
// every reconcile tick, because it is the replica count that moves it and a non-dispatcher joining
// changes no pool. Called under poolsLock, which owns fleetOverLimit.
func (e *Engine) checkFleetFits(shard int, sz shardSizing) {
	maxConns := sz.serverMax
	if maxConns <= 0 {
		return
	}
	replicas := e.replicasOn(shard)
	sharing := 1
	if s := e.sonarFor(shard); s != nil {
		st := s.Standing()
		sharing = dispatcherSlots(st.Candidates) + st.Draining
	}
	held := sz.budget + readerOpenConns*max(0, replicas-sharing)
	over := held > maxConns*fleetLimitPercent/100
	if over == e.fleetOverLimit[shard] {
		return
	}
	e.fleetOverLimit[shard] = over
	if over {
		e.diags.Logger().Warn("Fleet may exceed the database's connection limit; consider an external pooler",
			"shard", shard, "replicas", replicas, "connections", held, "maxConnections", maxConns)
		return
	}
	e.diags.Logger().Info("Fleet back within the database's connection limit",
		"shard", shard, "replicas", replicas, "connections", held, "maxConnections", maxConns)
}

// startupBootstrapConns is the tiny per-shard pool the shards open with BEFORE the replica count is
// known. Reading R from the dwarf_peers registry needs open connections, so the shards open at this
// size (enough to register the peer row, probe the RTT, and read the count - all any pre-dispatch work
// needs), then Startup resizes every pool to its derived R-divided share before a single worker runs.
//
// It is deliberately small: a cold-starting fleet's only connections during this window are these
// bootstrap ones, so even N replicas starting together stay far under any server's limit. Lazy fill
// means the ceiling barely materializes anyway (a handful of connections), and the value only needs to
// clear the parallel register + read + probe, which are a couple of statements per shard.
const startupBootstrapConns = 4

// recomputePools re-derives every shard's role and connection pool from that shard's OWN registry reading
// and pushes the sizes to the open shards (sequel's pool setters are hot/atomic), then re-derives the worker
// ceiling, which is a function of those pools. Called by the reconcile loop on the Sonars' cadence.
// No-ops when the engine is not running and when no shard's role has moved since the last application.
// (Startup itself sizes the pools directly from what it read, not through here, and records the roles.)
//
// The divisor is PER SHARD because the budget is: it belongs to the shard's database, so the replicas that
// matter are the ones dispatching from THAT database. One shard's fleet changing must not re-push another's
// unchanged sizes, and one shard's role staying put must not mask a change elsewhere.
func (e *Engine) recomputePools() {
	// poolsLock is held across the whole read-then-push, not just the dedupe: lastAppliedSlots keeps a no-op
	// recompute from touching the pools, but it does not ORDER two live ones, so without this a push derived
	// from an older reading could land after a newer one (over-connecting the fleet). The poolsLock ->
	// shardsLock order below cannot cycle (the roles are lock-free reads of the Sonars' published state).
	e.poolsLock.Lock()
	defer e.poolsLock.Unlock()
	if !e.started.Load() {
		return
	}
	sizing, rtts := e.shardFacts()
	// Read every shard's role first and compare as a whole: a push is all-or-nothing, so a single shard
	// moving is enough to re-derive, and nothing moving is the cheap common case.
	observed := make(map[int]int, len(e.lastAppliedSlots))
	roles := make(map[int]roleReading, len(e.lastAppliedSlots))
	changed := false
	for _, idx := range e.db.Indices() {
		e.checkFleetFits(idx, sizing[idx])
		role := e.readRole(idx)
		roles[idx] = role
		slots := role.slots
		observed[idx] = slots
		if prev, ok := e.lastAppliedSlots[idx]; !ok || prev != slots {
			changed = true
		}
	}
	// A held pool is pushed again when its steps have finished or the fleet's draining count has moved, with
	// no role of this replica's having moved.
	heldMoved := false
	for idx, divisor := range e.heldDivisors {
		if e.inFlightOn(idx) == 0 || roles[idx].held != divisor {
			heldMoved = true
			break
		}
	}
	if !changed && !heldMoved && len(observed) == len(e.lastAppliedSlots) {
		return
	}
	e.lastAppliedSlots = observed
	// The window poolsLock closes: the roles have been read, the sizes are not yet pushed. A test holds one
	// recompute here on a stale reading while a fresher one tries to run (see TestPoolSizing_-
	// ConcurrentRecomputeAppliesLatestR). The fault picks which recompute reaches the checkpoint, so a Break
	// there holds that one alone and the racing recompute is free to run into the lock.
	if e.diags.Seams().IsFault(diagnostics.FaultSlowPoolPush) {
		e.diags.Seams().Checkpoint(e.lifetimeCtx, diagnostics.CheckpointPoolPushHeld)
	}
	postSplitConns := 0
	for _, idx := range e.db.Indices() {
		if _, err := e.db.Shard(idx); err != nil {
			continue
		}
		slots := observed[idx]
		// A replica leaving a shard stops dispatching BEFORE its pool shrinks, and one joining grows its pool
		// BEFORE it dispatches, so no worker is ever handed a shard through a pool sized for reading.
		if slots == 0 {
			e.applyRole(idx, false)
		}
		// Demoted with steps still in flight: hold a share sized like the peers' divisor until they finish,
		// so their completions do not queue on the reader pool.
		held := slots == 0 && e.inFlightOn(idx) > 0
		divisor := slots
		if held {
			divisor = roles[idx].held
			if _, ok := e.heldDivisors[idx]; !ok {
				e.diags.Logger().Info("Shard pool held for in-flight steps", "shard", idx)
			}
			e.heldDivisors[idx] = divisor
		} else if _, ok := e.heldDivisors[idx]; ok {
			delete(e.heldDivisors, idx)
			e.diags.Logger().Info("Shard pool hold released", "shard", idx)
		}
		idle, open := shardPool(sizing[idx].budget, divisor)
		// Through the shard set, which applies the test-mode cap - a push onto the shard's own handle would
		// not - and resizes the shard's turns with the pool, for the same reason the cache and the worker
		// ceiling follow it: they order access to the connections this replica actually holds.
		e.db.SetShardPool(idx, idle, open)
		if e.diags.Seams().Enabled() { // Enabled gates the assembled name and the boxed value in production
			e.diags.Seams().Variable(diagnostics.Join(diagnostics.VariablePoolIdle, strconv.Itoa(idx)), idle)
		}
		if slots > 0 {
			postSplitConns += open
		}
		// After the push: the statement must never lower a peer's divisor while this replica still holds a
		// share from outside the cut.
		if sonar := e.sonarFor(idx); sonar != nil {
			sonar.SetDraining(held)
		}
		if slots > 0 {
			e.applyRole(idx, true)
		}
	}
	// The candidate cache follows the pool split, for the same reason the worker ceiling does: it is sized from
	// what this replica can actually CLAIM, and only a dispatched shard's pool claims anything.
	//
	// Startup derives the dispatch count from the roles it read at Join, and a fleet change moves them. Left
	// alone, a replica whose pools shrank keeps a cache sized for connections it no longer holds - and the
	// refiller scans up to the cache's capacity per fairness key and wholesale-replaces it, so it is handed far
	// more candidates than it can ever claim. Stale hints whose claim CAS loses to a peer, and wasted
	// round-trips, exactly when the fleet is busiest. This is the same "never size the cache from more than
	// the replica can claim" rule the worker ceiling is kept away from, arrived at through a different door.
	//
	// The RESIDENT worker count is deliberately NOT resized, because the crew shrinks ITSELF: a worker that
	// spent too little of its own recent wall clock holding a candidate retires on a coin flip, so the surplus
	// this smaller budget creates finds itself idle and goes, down to the floor Start was given. Pushing a new
	// resident count here would be a second, coarser actuator on a quantity that already self-corrects - and
	// one that would have to serialize against a shrink in flight. The ceiling and the cache below do not
	// self-correct, which is why they are re-derived and this is not.
	//
	// The ceiling comes FIRST, as in Startup: the cache is capped by it, and a replica that dispatched nothing
	// holds the 64 fallback, so a promotion that resized the cache first would take a reader-sized cache - for
	// good, since the next recompute sees no role change and returns early. Reuses the sizing/rtts already
	// locked-and-cloned above rather than paying for another lock/clone pass over the same maps.
	e.recomputeWorkerCeilingWith(e.lifetimeCtx, sizing, rtts, observed)
	dispatch := max(64, workersPerConnBudget*postSplitConns)
	e.cache.Resize(min(dispatch, int(e.workers.Load())))
	e.fanInHoldAt.Store(int64(min(dispatch, int(e.workers.Load()))))
	// The refill scan floor is measured against the cache's capacity, so it follows the cache.
	e.recomputeRefillIntervalsWith(sizing, roles)
	e.diags.Logger().Info("Derived pools recomputed", "slots", observed, "dispatch", dispatch)
}

// applyRole starts or stops this replica dispatching one shard: the doorbell's gate, the piston's idle mode and the
// janitor's activity move together. Called under poolsLock, in the order recomputePools gives it.
func (e *Engine) applyRole(shard int, dispatches bool) {
	if b := e.dispatchFlag(shard); b != nil && b.Swap(dispatches) != dispatches {
		e.diags.Logger().Info("Shard dispatch role changed", "shard", shard, "dispatching", dispatches)
	}
	a := e.actorsOn(shard)
	if a == nil {
		return
	}
	a.piston.SetIdle(!dispatches || e.zeroWorkers())
	// The janitor works only on a shard this replica dispatches.
	if dispatches {
		a.janitor.Resume()
	} else {
		a.janitor.Pause()
	}
}

// recomputeWorkerCeiling re-derives the worker maximum from each shard's CURRENT pool and its probed
// RTT, taking the worst shard's number. It must follow the pool: the ceiling encodes how fast a
// synchronized completion storm can drain (M / txTime), so a pool that shrank when peers appeared
// leaves a stale, too-high ceiling whose storm math no longer holds. An explicit SetWorkers is honored
// as-is - an operator may consciously trade storm-re-execution risk for long-task throughput - but is
// reported when it exceeds the ceiling.
//
// Shrinking only bounds FUTURE growth: workers already spawned keep running (they are cheap, and killing
// a worker mid-step is not a thing the pool does). The ceiling is a bound on how far the pool may grow,
// not a live target.
//
// Reads the shard facts itself; recomputePools has already read them by the time it gets here, so it calls
// recomputeWorkerCeilingWith directly with its own.
func (e *Engine) recomputeWorkerCeiling(ctx context.Context) {
	sizing, rtts := e.shardFacts()
	e.recomputeWorkerCeilingWith(ctx, sizing, rtts, slotsOf(e.rolesByShard()))
}

// shardDriver is the driver name of a shard's connection, or empty when the shard is not open.
func (e *Engine) shardDriver(idx int) string {
	sh, err := e.db.Shard(idx)
	if err != nil {
		return ""
	}
	db := sh.DB()
	return db.DriverName()
}

// recomputeWorkerCeilingWith is recomputeWorkerCeiling's core, taking the shard sizing, RTTs and roles as
// already-read snapshots rather than re-acquiring shardsLock or re-reading the roles to fetch them.
func (e *Engine) recomputeWorkerCeilingWith(ctx context.Context, sizing map[int]shardSizing, rtts map[int]float64, slotsBy map[int]int) {
	ceiling := math.MaxInt
	for idx, rttMs := range rtts {
		// Each dispatched shard's own pool, since each is divided by its own dispatchers - and the worst
		// shard's number wins, because a storm drains through whichever pool is tightest. A shard this replica
		// only reads runs no step, so its small pool drains no storm and must not bound the workers.
		slots := slotsBy[idx]
		if slots == 0 {
			continue
		}
		_, open := shardPool(sizing[idx].budget, slots)
		ceiling = min(ceiling, workerCeiling(open, rttMs, e.shardDriver(idx)))
	}
	if ceiling == math.MaxInt {
		ceiling = 64 // no shard was probed or dispatched (an unopened, test-mode or reader-only engine)
	}
	if !e.workersSet.Load() {
		// Derived default: the ceiling. It is independent of the task duration T (which the engine
		// cannot know), so it is correct for any workload - and the pool only grows into it on demand,
		// so a short-task deployment never pays for the headroom.
		e.workers.Store(int32(ceiling))
		// The crew's ceiling must follow: it is read on every spawn decision, so a stale one would keep
		// letting the crew grow past what the current connection budget can drain inside the lease margin.
		if e.crew != nil {
			e.crew.SetMax(ceiling)
		}
		return
	}
	if n := int(e.workers.Load()); n > ceiling {
		e.diags.Logger().WarnContext(ctx, "Worker count exceeds the lease-margin ceiling: a synchronized completion storm may re-execute tasks",
			"workers", n, "ceiling", ceiling)
	}
}

// placementWeight is a shard's new-flow placement weight: ShardSpec.Weight, or 1 when unset.
func placementWeight(spec ShardSpec) float64 {
	if spec.Weight > 0 {
		return spec.Weight
	}
	return 1
}

// pickShard selects the shard for a new top-level flow: a weighted-random pick over the non-cordoned
// shards, in proportion to placementWeight. Placement is the engine's only load-balancing moment (flows
// are shard-pinned for life), so a heterogeneous fleet must be weighted by the operator - an unweighted pick
// saturates the smallest shard first while larger ones idle. The weight is declared, not inferred from the
// server's size: capacity is not proportional to vCPUs (16 -> 32 vCPU measured x2.46, 32 -> 64 x1.2).
//
// The pick is among the shards this replica DISPATCHES when there are any, because the doorbell rings only
// for those: a flow placed elsewhere waits for a dispatcher's scan before its first step runs. A replica
// that dispatches nothing picks among every non-cordoned shard. Any declared weight other than 1 drops the
// preference and places over every non-cordoned shard: when each replica dispatches a subset of the shards,
// preferring them would send each shard its own dispatchers' creates whatever its Weight.
//
// A shard this replica cannot read (blindOn) is left out, as a cordoned one is, until its next good read, and each
// placement that left one out is counted against it. When every candidate is blind the pick ignores blindness, so a
// fault on this replica's own side still finds somewhere to place.
func (e *Engine) pickShard(ctx context.Context) (int, error) {
	e.shardsLock.Lock()
	specs := make([]ShardSpec, 0, len(e.shardSpecs))
	for _, spec := range e.shardSpecs {
		specs = append(specs, spec)
	}
	e.shardsLock.Unlock()
	if len(specs) == 0 {
		// No shard was registered: Startup opened the single default shard.
		//
		// An EMPTY index set is not a shardless engine - Startup always opens at least the default shard, so
		// this only happens off a live engine: before Startup, or AFTER Shutdown (ShardSet.Close nils the
		// indices). No exported operation gets here then - Shutdown ends every call it admitted before it closes
		// the shards - but indexing the empty slice panicked the host's process, so a caller that does is
		// owed an error rather than resting on that order.
		indices := e.db.Indices()
		if len(indices) == 0 {
			return 0, errors.New("engine is not started", http.StatusServiceUnavailable)
		}
		return indices[rand.IntN(len(indices))], nil
	}
	// Declared weights win over the doorbell: a replica placing only on the shards it dispatches would send each
	// shard its own dispatchers' creates whatever the weights say, so any weight other than 1 places over every
	// non-cordoned shard. A flow placed on a shard this replica does not dispatch waits a dispatcher's scan
	// before its first step.
	passes := []bool{true, false}
	for _, spec := range specs {
		if !spec.Cordoned && placementWeight(spec) != 1 {
			passes = []bool{false}
			break
		}
	}
	weights := make([]float64, len(specs))
	total := 0.0
	// leftOut is the pass's candidates excluded only for blindness; it stays empty unless a shard reads blind.
	var leftOut []int
	for _, skipBlind := range []bool{true, false} {
		for _, dispatchedOnly := range passes {
			leftOut = leftOut[:0]
			for i, spec := range specs {
				weights[i] = 0
				if spec.Cordoned || (dispatchedOnly && !e.dispatchesOn(spec.Index)) {
					continue
				}
				if skipBlind && e.blindOn(spec.Index) {
					leftOut = append(leftOut, spec.Index)
					continue
				}
				weights[i] = placementWeight(spec)
				total += weights[i]
			}
			if total > 0 {
				break
			}
		}
		if total > 0 {
			break
		}
		leftOut = leftOut[:0]
	}
	if total == 0 {
		return 0, errors.New("all shards are cordoned", http.StatusServiceUnavailable)
	}
	for _, shard := range leftOut {
		e.metricPlacementSkipped(ctx, shard)
	}
	r := rand.Float64() * total
	last := -1
	for i, w := range weights {
		if w == 0 {
			continue
		}
		if r < w {
			return specs[i].Index, nil
		}
		r -= w
		last = i
	}
	// Floating-point rounding can leave r at the very end of the range; the last eligible shard takes it. Never
	// a cordoned or zero-weight one.
	return specs[last].Index, nil
}

// shardFacts returns, for every open shard, its sizing - from its spec, and the RTT and server connection limit
// measured when it opened - and the RTT the worker ceiling and the peer profile take. The budget takes the RAW
// probe (an unprobed shard takes the near budget), while the ceiling takes the same-zone constant for a failed
// probe - each the safe direction for what it sizes. Every input is fixed for the run, so the result is too.
func (e *Engine) shardFacts() (sizing map[int]shardSizing, rtts map[int]float64) {
	e.shardsLock.Lock()
	declared := maps.Clone(e.shardSpecs)
	e.shardsLock.Unlock()
	indices := e.db.Indices()
	sizing = make(map[int]shardSizing, len(indices))
	rtts = make(map[int]float64, len(indices))
	for _, idx := range indices {
		sh, err := e.db.Shard(idx)
		if err != nil {
			continue
		}
		rttMs := float64(sh.RTT()) / float64(time.Millisecond)
		// A zero-value spec is the default shard's sizing.
		sizing[idx] = sizeShard(declared[idx], rttMs, sh.MaxConnections(), sh.DB().DriverName())
		if rttMs <= 0 {
			rttMs = defaultRTTMs
		}
		rtts[idx] = rttMs
	}
	return sizing, rtts
}
