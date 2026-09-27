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

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// The connection-budget and capacity constants below were measured by the cloud benchmark campaigns
// (Cloud SQL PostgreSQL, tiers 1-64 vCPU; see docs/benchmark-cloud.md). They are engine knowledge - the
// operator provides the facts (ShardSpec.VirtualCPUs), the engine owns the constants.
const (

	// defaultVirtualCPUs is assumed when a ShardSpec does not declare VirtualCPUs. 2 vCPUs is the FLOOR of
	// every current-generation AWS RDS class (db.t4g/m7g/r7g all start at 2), so on RDS the assumption
	// cannot undershoot the real machine.
	//
	// WHERE IT CAN OVERSHOOT - Cloud SQL's 1-vCPU db-custom-1-* and its shared-core tiers - THE COST IS NOW
	// BOUNDED BY MEASUREMENT. The guess yields 7 connections at the reference distance, and a 1-vCPU shard
	// was measured to need 3 and to run clean at 8, 10 and 14 (390 steps/s, ~70% of that tier's peak). So
	// an undeclared shard on the smallest machine Cloud SQL sells lands above what it needs and well below
	// anything that hurt it. Declaring VirtualCPUs: 1 yields 3.
	//
	// Declare it anyway: the guess cannot use a large database. An 8-vCPU shard left undeclared gets 7
	// connections where it needs 40.
	defaultVirtualCPUs = 2

	// workersPerConnBudget sizes the RESIDENT worker set (and the candidate cache) from the aggregate
	// connection budget: dispatch is database-bound, and useful dispatching workers = conns x T/db,
	// with T/db measured ~3 for no-op tasks. 8 is deliberately generous for short tasks. Workers beyond
	// this are spawned on demand (see workerCeiling): a worker blocked in a long ExecuteTask holds no
	// connection, so it must not inflate the cache/refill scan, which serves dispatch only.
	workersPerConnBudget = 8

	// turnstilePassesPerConn sizes each shard's turnstile: the number of turns that may be out at once,
	// as a multiple of that shard's connection pool.
	//
	// IT IS 8x, AND A QUEUE AT THE POOL IS WHY. One turn per connection was the first cut and it is
	// CATASTROPHIC: measured at 600 flows/s on a local Postgres, 281 steps/s against the 1,687 the previous
	// gate managed on the same rig, a 6x collapse. With exactly as many turns as connections there are
	// exactly as many candidates for a connection, so every gap between a turn-holder finishing and the next
	// waiter being woken, scheduled and asking the pool is IDLE CONNECTION TIME - on the critical path of all
	// ~9.6 round trips a step makes. The queue this was "fixing" is the same margin workersPerConnBudget
	// deliberately keeps, and for the same reason: a resource with nobody queued for it runs below capacity.
	//
	// So the turnstile does not remove the wait for a connection and does not empty the pool's queue - a
	// caller holding a turn still competes for one, and still waits when they are all busy. What it changes
	// is WHO gets to compete and in what ORDER: the population at the pool is bounded by the turn count, and
	// admission into it is by band and then by age.
	//
	// 8 is the same multiple workersPerConnBudget uses, which is not a coincidence - it is what keeps the
	// contending population at the size the cache and the resident worker set are already sized for. Raising
	// it recovers a little more (10x and 12x measured 1,688 and 1,734 against 8x's 1,559) and grows the crew
	// with it (480 -> 572 -> 686), but per-connection service time was identical across all of them, so what
	// the multiple buys past 8 is queue depth rather than throughput. Treat the ordering between 8, 10 and 12
	// as UNSETTLED: those arms differed by less than the rig's own RTT drift, and only the 1x collapse and
	// the direction are established.
	turnstilePassesPerConn = 8

	// The turnstile bands. Lower is served first, and STRICTLY: a band is exhausted before the next is
	// looked at, so a band may only be given to a source that cannot flood it. Within a band, order is by
	// how long the job asking has been running.
	//
	// priorityRefill is the only band above the common one, and it is reserved for the piston because the
	// piston is the one caller bounded by construction - it cycles on a derived period and takes two turns
	// per cycle per shard, so it cannot starve what sits below it. It is ahead of everything else because
	// candidate supply runs only 1.04-1.47x ahead of consumption: a refill cycle that queues behind the
	// dispatch it feeds starves the workers it is filling for.
	//
	// Everything else shares priorityCommon and is separated by age alone. That is deliberate rather than
	// unfinished: age ordering is starvation-free (every claim eventually becomes the oldest), while any
	// second band handed to a caller that can arrive faster than it is served starves the band below it -
	// which is the shape of the measured 3x short-task collapse that killed the single-permit-pool design.
	// internal/turnstile/CLAUDE.md records the specific band that keeps being proposed - one for EXITING
	// workers - and why it is not here.
	//
	// The values are spaced, and are ordinals only: nothing reads the distance between them. A band can
	// therefore be inserted between two in use without renumbering the callers of either.
	priorityRefill = 0
	priorityCommon = 10
	priorityWorker = priorityCommon

	// completionRoundTrips is the number of database round trips in a step's post-task phase: the
	// standalone completed-UPDATE plus the transition transaction (lock-grab UPDATE, successor INSERT,
	// successor_id UPDATE, flow step_id UPDATE, COMMIT). Multiplied by the measured RTT it gives the
	// network half of the completion cost.
	completionRoundTrips = 7

	// completionServerMs is the server-side half of a completion: row work plus the group-committed
	// WAL fsync, measured ~3ms (cross-checked by the whole-step fit db = 12.1 x RTT + 4.4ms).
	completionServerMs = 3.0

	// defaultRTTMs is the same-zone round-trip time (measured 0.28-0.34ms on GCP private IP), used only
	// when the Startup probe fails. Deliberately the OPTIMISTIC value: a failed probe should not silently
	// inflate the worker ceiling, and a small RTT yields a small txTime... which yields a LARGER ceiling.
	// So the fallback is paired with the safety factor below; a persistently unprobeable database is a
	// louder problem than a mis-sized pool.
	defaultRTTMs = 0.3

	// minDispatcherPool is the smallest pool worth dispatching a shard from - a piston's two queries plus
	// workers enough to use them - and so what sets how many replicas share a shard's budget
	// (dispatcherSlots). An estimate, not a measurement: it makes a 2-vCPU shard's two dispatchers and an
	// 8-vCPU shard's four. Lowering it spreads a shard across more replicas with smaller pools each.
	minDispatcherPool = 12.0

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
//	txTime = completionRoundTrips x RTT + completionServerMs
//
// Every input is engine-visible: M is the derived pool, the margin is the engine's own constant, and
// RTT is measured at Startup (probeRTT). Nothing here needs the task duration T, which is exactly why
// this - and not `M x T/db` - is the number the engine can derive for itself.
func workerCeiling(open int, rttMs float64) int {
	txTimeMs := completionRoundTrips*rttMs + completionServerMs
	if txTimeMs <= 0 {
		txTimeMs = completionServerMs
	}
	marginMs := float64(30 * time.Second / time.Millisecond)
	return max(1, int(float64(open)*marginMs/txTimeMs*workerSafetyFactor))
}

// probeRTT measures the round-trip time to a shard with a few `SELECT 1`s, returning the MINIMUM of
// the samples: the minimum approximates pure network RTT, where a mean is polluted by scheduler jitter
// and warmup. The first sample is discarded - it pays connection establishment. Measured once at
// Startup on connections the pool is opening anyway (~10ms), never adapted: a latency change
// re-derives on restart, the same posture as the shard set itself. A failed probe returns 0, and the
// caller falls back to the same-zone constant rather than mis-sizing on a transient error.
func probeRTT(ctx context.Context, db *sequel.DB) float64 {
	const samples = 6
	best := math.MaxFloat64
	for i := range samples {
		start := time.Now()
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		elapsed := time.Since(start)
		if err != nil {
			return 0
		}
		if i == 0 {
			continue // discard: pays connection establishment
		}
		best = min(best, float64(elapsed.Nanoseconds())/1e6)
	}
	if best == math.MaxFloat64 {
		return 0
	}
	return best
}

// shardPool returns the idle/open pool sizes for one shard, with a warm idle core of half the open
// ceiling. VirtualCPUs (defaulted, see effectiveVirtualCPUs) and the shard's probed RTT pick the ratio, and
// the resulting per-DATABASE budget is split across the shard's DISPATCHERS - slots of them, this replica
// among them. slots == 0 means this replica does not dispatch the shard and holds the small fixed pool its
// reads and heartbeat need.
//
// The split rounds to the nearest connection rather than flooring: each dispatcher gains under half a
// connection, so the shard exceeds its budget by at most slots/2, inside the margin poolSafetyMargin
// already builds into every budget.
//
// The explicit SetMaxOpenConns override wins and pins the pool to exactly that size (the
// benchmarking/external-pooler path): it is the operator's exact per-replica number, is never divided,
// and bypasses the RTT term with it.
//
// rttMs is the value probed at Startup and held, never a live reading - a latency change re-derives on
// restart, the same posture as the shard set itself.
func shardPool(spec ShardSpec, override int, slots int, rttMs float64) (idle, open int) {
	if override > 0 {
		return override, override
	}
	if slots <= 0 {
		return readerIdleConns, readerOpenConns
	}
	vcpus := effectiveVirtualCPUs(spec.VirtualCPUs)
	open = max(2, int(math.Round(float64(shardBudget(vcpus, rttMs))/float64(slots))))
	return max(2, open/2), open
}

// dispatcherSlots is how many replicas dispatch one shard: enough that each holds a pool worth running a
// piston and workers on, never more than the replicas there are, and never fewer than two once there are
// two, so one replica dying does not stop the shard while its slot passes to the next rank.
//
// IT MUST AGREE ACROSS THE FLEET, which is why the budget is taken at defaultRTTMs rather than at the RTT
// this replica probed: the replicas ranked below this number are the shard's dispatchers, and two replicas
// that disagreed on it would both claim, or both leave, the same slot. VirtualCPUs is declared fleet-wide,
// and replicas is read from one registry, so every replica derives the same answer.
func dispatcherSlots(spec ShardSpec, replicas int) int {
	replicas = max(1, replicas)
	ref := shardBudget(effectiveVirtualCPUs(spec.VirtualCPUs), defaultRTTMs)
	want := int(math.Round(float64(ref) / minDispatcherPool))
	return min(replicas, max(want, min(2, replicas)))
}

// slotsOn is this replica's role on one shard: how many replicas dispatch it when this one is among them,
// or 0 when it is not. It is the pool divisor, the piston's on/off switch and the doorbell's gate in one
// number.
//
// The slots are derived from the replicas that MAY dispatch, not from every registered one: an await-only
// replica holds connections but takes no rank, and counting it would divide the budget among more
// dispatchers than exist - one worker beside an await-only frontend would run on half its shard's budget.
//
// Every doubt resolves toward dispatching, because the two errors are not symmetric: a replica wrongly
// dispatching over-connects the shard by one share, while a shard nobody dispatches runs nothing at all.
// A shard with no Sonar dispatches solo. Under a SetMaxOpenConns override every replica dispatches - the
// pools are pinned and never divided, so there is no budget for the ranking to protect - and the count is
// every candidate, which is who drains the shard.
func (e *Engine) slotsOn(shard int, spec ShardSpec) int {
	if e.zeroWorkers() {
		return 0
	}
	s := e.sonarFor(shard)
	if s == nil {
		return 1
	}
	rank, candidates := s.Rank()
	if e.maxOpenConns.Load() != 0 {
		return candidates
	}
	slots := dispatcherSlots(spec, candidates)
	if rank >= slots {
		return 0
	}
	return slots
}

// slotsByShard is every open shard's current role (slotsOn), read once so that everything derived from
// the roles in one pass is derived from the same reading.
func (e *Engine) slotsByShard(specs map[int]ShardSpec) map[int]int {
	slots := make(map[int]int, len(specs))
	for _, idx := range e.db.Indices() {
		slots[idx] = e.slotsOn(idx, specs[idx])
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

// dispatchFlag is one shard's role flag in the current run's map, or nil before the first Startup and
// for a shard that run did not open.
func (e *Engine) dispatchFlag(shard int) *atomic.Bool {
	flags := e.dispatching.Load()
	if flags == nil {
		return nil
	}
	return (*flags)[shard]
}

// readMaxConnections returns the server's connection limit for one shard, or 0 when it cannot say - SQLite
// has none, and a failed read is treated the same, since the check it feeds is advisory. SQL Server reports
// 32767 unless "user connections" is configured, and Azure SQL always does, so there the check effectively
// never fires. Read once at
// Startup and held: Postgres and SQL Server change the limit only on a server restart, and a managed
// service's resize restarts or fails the server over.
func readMaxConnections(ctx context.Context, db *sequel.DB) int {
	var q string
	switch db.DriverName() {
	case "pgx":
		q = "SELECT CAST(setting AS INTEGER) FROM pg_settings WHERE name='max_connections'"
	case "mysql":
		q = "SELECT @@max_connections"
	case "mssql":
		q = "SELECT @@MAX_CONNECTIONS"
	default:
		return 0
	}
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0
	}
	return max(0, n)
}

// checkFleetFits warns when the connections the fleet can hold on one shard approach the server's limit:
// the dispatchers' budget plus up to readerOpenConns for every replica that does not dispatch there.
// Advisory and edge-triggered - it never resizes anything, since shrinking dispatchers to make room for
// idle readers is the wrong trade, and a fleet that size wants an external pooler. It is evaluated on
// every reconcile tick, because it is the replica count that moves it and a non-dispatcher joining
// changes no pool. Called under poolsLock, which owns fleetOverLimit.
func (e *Engine) checkFleetFits(shard int, spec ShardSpec, rttMs float64, maxConns int) {
	if maxConns <= 0 {
		return
	}
	replicas := e.replicasOn(shard)
	candidates := 1
	if s := e.sonarFor(shard); s != nil {
		_, candidates = s.Rank()
	}
	budget := shardBudget(effectiveVirtualCPUs(spec.VirtualCPUs), rttMs)
	held := budget + readerOpenConns*max(0, replicas-dispatcherSlots(spec, candidates))
	over := held > maxConns*fleetLimitPercent/100
	if over == e.fleetOverLimit[shard] {
		return
	}
	e.fleetOverLimit[shard] = over
	if over {
		e.logger.Warn("Fleet may exceed the database's connection limit; consider an external pooler",
			"shard", shard, "replicas", replicas, "connections", held, "maxConnections", maxConns)
		return
	}
	e.logger.Info("Fleet back within the database's connection limit",
		"shard", shard, "replicas", replicas, "connections", held, "maxConnections", maxConns)
}

// poolRTTBuckets are the distances poolRatio is indexed by, in ms. UNEVENLY SPACED ON PURPOSE: they are
// where the sweeps actually measured, and connsPerVCPUFor interpolates everything between. Finer columns
// would be arithmetic dressed as evidence - the sweeps resolved a minimum to +/-10% at best, so a 0.25ms
// grid across the whole range implied a precision nothing here has.
//
// THE AXIS STOPS AT 2ms, which is not a rounding convenience: past there a large instance cannot be
// compensated at all (a 32-vCPU shard at 4ms needs ~1,163 connections to reach its own base peak, and its
// knee was never reached by 900), so any further column would extrapolate into the collapse these ratios
// are backed off from, for no throughput. At that distance the answer is more shards, not a bigger pool.
var poolRTTBuckets = [5]float64{0.25, 0.50, 1.00, 1.50, 2.00}

// poolSafetyMargin is applied by connsPerVCPUFor to every cell of poolRatio, so the table holds RAW
// MINIMA and the margin stays one number in one place. Keeping them separate matters because they answer
// to different evidence: a cell changes when someone re-measures that tier, and this changes when someone
// argues about how much slack a pool needs.
//
// 1.2 is what the sweeps support. At every distance the bare minimum ran a materially worse tail than a
// pool ~20% above it - 16 vCPU at 1 ms measured p99 1,344 ms at the minimum against 203 ms at +26%, and
// the 8-vCPU base minimum of 40 measured p99 170 ms against 92 ms at 48. Below ~1.1 the tail degrades;
// above ~1.5 the extra connections buy nothing and start costing (see the over-provisioning note in
// connsPerVCPUFor).
const poolSafetyMargin = 1.2

// tierRow is one measured shard-size tier: its vCPU count and its ratio at each poolRTTBuckets column.
type tierRow struct {
	vcpus  int
	ratios [5]float64 // 0.25, 0.50, 1.00, 1.50, 2.00 ms
}

// poolRatio is the RAW MINIMUM connections per vCPU - the smallest pool that sustained the load, before
// poolSafetyMargin - by shard size and distance, one column per poolRTTBuckets entry. connsPerVCPUFor
// documents how the cells are derived and which of them rest on measurement.
var poolRatio = []tierRow{
	{1, [5]float64{3.00, 5.00, 7.00, 9.00, 11.00}},  // MEASURED
	{2, [5]float64{3.00, 6.00, 7.00, 10.00, 11.00}}, // MEASURED
	{4, [5]float64{3.00, 6.00, 6.00, 8.00, 10.00}},  // MEASURED
	{8, [5]float64{5.00, 8.00, 8.00, 11.00, 11.00}}, // MEASURED
	{16, [5]float64{5.00, 7.00, 7.00, 9.00, 9.00}},  // MEASURED
	{32, [5]float64{3.75, 4.70, 5.60, 8.10, 8.75}},  // MEASURED
	{64, [5]float64{2.25, 3.50, 5.00, 6.00, 6.00}},  // MEASURED
}

// connsPerVCPUFor is the connection-per-vCPU ratio for a shard of a given size at a given distance. Both
// axes are load-bearing: a connection held while a packet is in flight does the server no good, so what a
// database sees is the duty cycle M*s/(k*RTT+s), not M.
//
// THE TABLE STOPS AT 64 vCPU, DELIBERATELY - every row in it is measured, and there is no modelled row
// past it. A shard declared larger than 64 vCPU falls back to the 64-vCPU row (connsPerVCPUFor takes the
// last row once vcpus exceeds every tabulated tier): the safe direction, since 64 vCPU is the LOWEST
// ratio measured and under-connecting costs throughput rather than collapsing the database. The model
// this table replaces derives each cell as M* = B*(k*RTT+s*)/s* derated 10%, with k = 9.11 round trips
// per step and B* = 15*vCPU^0.72 backends at the knee. Pool sweeps on Cloud SQL found it over-provisions
// worst at SHORT RTT - 8.63x and 7.34x where 5.0x and 5.1x sufficed - which is where nearly every
// deployment sits (same-zone RTT is 0.05-0.96 ms). The correction is NOT extrapolated to an untested row:
// reasoning-by-inheritance is what put the wrong numbers here to begin with. Sweep a tier before adding
// its row.
//
// Every cell is the MINIMUM pool that sustained that distance's achievable throughput. poolSafetyMargin
// is applied on top by this function, so the table stays a record of measurement and the slack stays one
// number in one place.
//
//	      1 vCPU      2 vCPU        4 vCPU        8 vCPU        16 vCPU       32 vCPU        64 vCPU
//	RTT   rate min  x   rate min  x   rate  min  x  rate  min  x  rate  min  x  rate   min  x   rate   min  x
//	~0.1   390   3  3.0   770   6  3.0 1,750  12 3.0 3,500  40 5.0 7,000  82 5.1 14,000 120 3.75 20,000 144 2.25
//	~0.5   390   5  5.0   770  12  6.0 1,750  24 6.0 3,500  64 8.0 7,000 120 7.5 14,000 150 4.70 20,000 224 3.50
//	~1.0   390   7  7.0   660  14  7.0 1,500  24 6.0 3,000  64 8.0 6,000 115 7.2 12,000 180 5.60 17,000 320 5.00
//	~1.5   390   9  9.0   660  20 10.0 1,500  32 8.0 3,000  88 11.0 6,000 145 9.1 12,000 260 8.10 17,000 384 6.00
//	~2.0   390  11 11.0   660  22 11.0 1,500  40 10.0 3,000 88 11.0 5,000 145 9.1 10,000 280 8.75 14,000 384 6.00
//	(rate in steps/s - each tier's own achievable throughput at that distance, not one fixed rate)
//
// WHAT THE SWEEPS ESTABLISH IS THE LEVEL, NOT THE SHAPE ACROSS TIERS. Every measured minimum is far below
// its modelled cell at short RTT (4 vCPU 3.0 vs 8.50, 8 vCPU 5.0 vs 7.19, 16 vCPU 5.1 vs 6.12, 32 vCPU
// 3.75 vs 5.36, 64 vCPU 2.25 vs 4.42), so the model over-provisions there on every tier tried, regardless
// of how each was loaded.
//
// The ratio's trend ACROSS tiers is NOT established. The measured base ratios run 3.0 / 5.0 / 5.1 / 3.75 /
// 2.25 at 4 / 8 / 16 / 32 / 64 vCPU, which is not monotonic in either direction - and the arms were not
// held at a comparable fraction of each tier's own ceiling (~70% / ~71% / ~93% / ~66% / ~78%). Since s
// inflates as a tier approaches its ceiling, part of that spread measures how hard each was pushed rather
// than the tier itself. Comparing rows needs arms at matched utilisation, which no campaign has run.
//
// THE RTT SLOPE IS NOT UNIFORM ACROSS TIERS EITHER, and 64 vCPU is where that became visible. It rises
// 2.7x from base to 2ms (2.25 -> 6.00) against 1.8x at 16 and 2.3x at 32, so the model is wrong in BOTH
// directions on this tier at once: over-provisioning 2.0x at base (4.42 vs 2.25) while under-compensating
// at distance. Reading only the far column hides the first error behind the second.
//
// THE CURVE IS A STEP, NOT A RAMP, and it plateaus because two terms cancel. Duty cycle pushes the
// requirement UP with distance while the throughput the tier can deliver falls. Every measured tier
// plateaus somewhere: 4 vCPU between 0.5 and 1 ms, 8 and 16 vCPU between 1.5 and 2 ms, 64 vCPU likewise
// (6.00 at both). A table that only ever rises with distance models one term.
//
// ADDING A ROW PAST 64 vCPU IS GATED ON THE LOAD GENERATOR, NOT THE DATABASE - size it before booking a
// rig, or the run reports the generator's ceiling as the tier's.
//
// COMPENSATION IS PARTIAL - NO POOL RECOVERS WHAT DISTANCE COSTS. The sustained column falls because
// past ~1 ms there is a load no pool size can serve: 8 vCPU could not hold 3,500 st/s at 1 ms at ANY
// pool (80 starved, 96 contended, nothing between), and 16 vCPU could not hold 7,000 st/s there either.
// The connection knob runs out before the distance does. Size for the throughput the distance allows.
//
// HEADROOM SETS THE WIDTH OF THE SAFE BAND, which is why over-provisioning is only sometimes fatal. At
// 1.6 ms the 16-vCPU tier ran 6,000 st/s - ~98% of capacity - and the band was 145-155 wide: pool 160
// CONTENDED. At 2.0 ms with 5,000 st/s the same 160 ran clean. Read a narrow band as a statement about
// headroom, not about distance, and keep production off the ceiling.
//
// THE MULTIPLIER IS NOT MONOTONIC IN RTT, and that is a property of the system rather than noise. Two
// terms fight: duty cycle pushes connections UP with distance, while the throughput a tier can actually
// deliver falls with it. At 1.05 ms the second term won - the minimum FELL from 120 to 115 because the
// sustainable rate dropped from 7,000 to 6,000. A table that only ever rises with distance is modelling
// one term and ignoring the other.
//
// BOTH AXES INTERPOLATE, BUT NOT THE SAME WAY. The RTT axis is smooth in RTT, so a bucket boundary is an
// artifact of storing a curve as a table and a 0.75ms path lands halfway between the 0.50 and 1.00
// columns - a plain linear blend. The tier axis is smooth in log(vCPUs), not vCPUs - B* is a power law
// and the rows are roughly an octave apart, uniformly spaced in LOG space, not raw space - so a 24-vCPU
// shard is blended between the 16- and 32-vCPU rows in LOG-LOG space (interpolating log(ratio) against
// log(vCPUs), then exponentiating back): exact at every tabulated tier (f lands at exactly 0 or 1 there),
// and it tracks the power-law shape between them instead of guessing with one curve's row for a shard
// that sits between two. Below the first tier or at/beyond the last, there is no second point to
// interpolate against, so the boundary row is taken flat - clamped, not extrapolated (see "THE TABLE
// STOPS AT 64 vCPU" above for why the top clamp is the safe direction).
// An unprobed shard (rttMs <= 0) lands in the first column: a failed probe must not inflate a pool.
//
// A COLLAPSED ARM IS A TRANSIENT CAUGHT BY THE WINDOW, NOT AN UPPER BOUND ON THE POOL - so no cell here
// is placed below one, and none should be. The stall lasts ~30s and recurs; below the ceiling it clears
// itself with the offered rate unchanged, so whether a 120s window reports 46% or 98% of command is partly
// a question of where the window landed. Measured on the 64-vCPU sweep, at one pool and one rate: 9,183
// steps/s with a 75s p50 in one window, 19,685 (98.4%) in another. Three distance arms then showed a
// collapsed pool bracketed by WORKING pools on both sides (256 between 224 and 320 at 0.5ms; 320 between
// 256 and 384 at 1.5ms), which no threshold can produce.
//
// EVERY MINIMUM IN THIS TABLE WAS MEASURED ON A 120s WINDOW, so read each as the smallest pool that
// sustained in every arm it ran, with a tail comparable to larger pools - not as an edge. The starvation
// side is the trustworthy half: it is monotonic and reproducible (at 0.5ms on 64 vCPU, 40/70/85/97% of
// command as the pool climbed 128/160/192/224), which is why the cells are placed from where starvation
// ENDS rather than from where a collapse begins.
//
// The cells also sit well below every collapse observed for their tier, which is the direction to keep
// them in: 8 vCPU at 0.25ms derives 48 against the one 8-vCPU arm that ever collapsed, at M=70; 1 vCPU
// derives 3 against a peak at M=16. So what a re-measurement should look for is starvation at the top of
// a tier's range, not collapse - and it should state its window length beside every throughput number.
//
// DO NOT READ A LARGE-RTT CELL AS AN OVER-CONNECTION WITHOUT APPLYING THE DUTY CYCLE. The 2.00ms cell for
// 32 vCPU is 336 connections against a collapse observed at 430, but only ~41% are inside the database at
// once - ~138 backends, fewer than the 183 the healthy knee carried. Any cap belongs on backends or on
// the operator's configured max_connections, never on a connection count read without its RTT.
//
// This is a lookup on declared facts and one probe taken at Startup, not a controller: it reads nothing
// that moves while the engine runs, converges on nothing, and cannot oscillate. Do not grow it into one.
func connsPerVCPUFor(vcpus int, rttMs float64) float64 {
	lo, hi, f := tierBracket(vcpus)
	loRatio := ratioAtRTT(lo.ratios, rttMs)
	if f <= 0 {
		// Below the first tier or at/beyond the last: no second point to blend against, so the
		// boundary row stands alone.
		return loRatio * poolSafetyMargin
	}
	hiRatio := ratioAtRTT(hi.ratios, rttMs)
	// Log-log: the ratio is a power law in vCPUs, so it is linear in log(ratio) against
	// log(vCPUs), not in the raw values. f==1 lands exactly on hiRatio (every tabulated tier is
	// therefore exact, never blended), and this is mathematically identical whether the blend is
	// done on the ratio or on the raw connection count (ratio*vCPUs) - the vCPU term cancels.
	logRatio := math.Log(loRatio) + f*(math.Log(hiRatio)-math.Log(loRatio))
	return math.Exp(logRatio) * poolSafetyMargin
}

// tierBracket finds the two poolRatio rows bracketing vcpus and the log-log interpolation fraction
// between them (0 at lo, 1 at hi). Below the first row or at/above the last, hi==lo and f==0: there is
// no second point to interpolate against, so the caller takes the boundary row flat rather than
// extrapolating past it.
func tierBracket(vcpus int) (lo, hi tierRow, f float64) {
	if vcpus <= poolRatio[0].vcpus {
		return poolRatio[0], poolRatio[0], 0
	}
	last := poolRatio[len(poolRatio)-1]
	if vcpus >= last.vcpus {
		return last, last, 0
	}
	for i := 1; i < len(poolRatio); i++ {
		if vcpus <= poolRatio[i].vcpus {
			lo, hi = poolRatio[i-1], poolRatio[i]
			f = (math.Log(float64(vcpus)) - math.Log(float64(lo.vcpus))) / (math.Log(float64(hi.vcpus)) - math.Log(float64(lo.vcpus)))
			return lo, hi, f
		}
	}
	return last, last, 0 // unreachable given the >= last.vcpus check above
}

// ratioAtRTT applies the RTT-bucket interpolation (poolRTTBuckets) to one tier row's ratios, clamped at
// both ends - a plain linear blend, since the sweeps establish the requirement is smooth in RTT (see
// "BOTH AXES INTERPOLATE" above for why the tier axis needs the log-log form instead). Shared by every
// lookup, including the two rows a tier interpolation brackets.
func ratioAtRTT(ratios [5]float64, rttMs float64) float64 {
	if rttMs <= poolRTTBuckets[0] {
		return ratios[0]
	}
	for i := 1; i < len(poolRTTBuckets); i++ {
		if rttMs <= poolRTTBuckets[i] {
			f := (rttMs - poolRTTBuckets[i-1]) / (poolRTTBuckets[i] - poolRTTBuckets[i-1])
			return ratios[i-1] + f*(ratios[i]-ratios[i-1])
		}
	}
	return ratios[len(poolRTTBuckets)-1]
}

// shardBudget is the whole-database connection budget for a shard of a given size at a given distance,
// before it is split across the replicas holding connections to it. It is the quantity the measured
// knees are expressed in, so tests asserting on the budget derive it from here rather than restating a
// ratio that varies by tier and distance.
func shardBudget(vcpus int, rttMs float64) int {
	return max(2, int(connsPerVCPUFor(vcpus, rttMs)*float64(vcpus)))
}

// effectiveVirtualCPUs resolves a shard's declared CPU count, substituting defaultVirtualCPUs when the
// operator did not declare one. The vCPU count is a fact off the machine's spec sheet - something an
// operator KNOWS rather than guesses - so the default exists for the zero-config case, not as an
// invitation to leave it unset.
func effectiveVirtualCPUs(declared int) int {
	if declared > 0 {
		return declared
	}
	return defaultVirtualCPUs
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

// slowPoolPushDelay is how long the FaultSlowPoolPush seam stalls a recompute between reading R and pushing
// the derived sizes. Test-only (the fault is inert in production); a var so it stays adjustable.
var slowPoolPushDelay = 200 * time.Millisecond

// recomputePools re-derives every shard's role and connection pool from that shard's OWN registry reading
// and pushes the sizes to the open shards (sequel's pool setters are hot/atomic), then re-derives the worker
// ceiling, which is a function of those pools. Called by the reconcile loop on the Sonars' cadence.
// No-ops when the engine is not running and when no shard's role has moved since the last application.
// (Startup itself sizes the pools directly from what it read, not through here, and records the roles.)
//
// Under a SetMaxOpenConns override the roles are still applied - every replica dispatches then, and one
// that was a reader when the override landed must start - but the pinned pools are left alone:
// SetMaxOpenConns pushed them, and it owns everything that follows from them.
//
// The divisor is PER SHARD because the budget is: it belongs to the shard's database, so the replicas that
// matter are the ones dispatching from THAT database. One shard's fleet changing must not re-push another's
// unchanged sizes, and one shard's role staying put must not mask a change elsewhere.
func (e *Engine) recomputePools() {
	// poolsLock is held across the whole read-then-push, not just the dedupe: lastAppliedSlots keeps a no-op
	// recompute from touching the pools, but it does not ORDER two live ones, so without this a push derived
	// from an older reading could land after a newer one (over-connecting the fleet), and a concurrent
	// SetMaxOpenConns could have its pinned pools overwritten by derived ones. The poolsLock -> shardsLock
	// order below cannot cycle (the roles are lock-free reads of the Sonars' published state).
	e.poolsLock.Lock()
	defer e.poolsLock.Unlock()
	if !e.started.Load() {
		return
	}
	e.shardsLock.Lock()
	specs := maps.Clone(e.shardSpecs)
	rtts := maps.Clone(e.shardRTTMs)
	e.shardsLock.Unlock()
	override := int(e.maxOpenConns.Load())
	// Read every shard's role first and compare as a whole: a push is all-or-nothing, so a single shard
	// moving is enough to re-derive, and nothing moving is the cheap common case.
	observed := make(map[int]int, len(e.lastAppliedSlots))
	changed := false
	for _, idx := range e.db.Indices() {
		if override == 0 {
			e.checkFleetFits(idx, specs[idx], rtts[idx], e.shardMaxConns[idx])
		}
		slots := e.slotsOn(idx, specs[idx])
		observed[idx] = slots
		if prev, ok := e.lastAppliedSlots[idx]; !ok || prev != slots {
			changed = true
		}
	}
	if !changed && len(observed) == len(e.lastAppliedSlots) {
		return
	}
	e.lastAppliedSlots = observed
	// The window poolsLock closes: the roles have been read, the sizes are not yet pushed. A test stalls one
	// recompute here to hold a stale reading while a peer's fresher one races past (see TestPoolSizing_-
	// ConcurrentRecomputeAppliesLatestR). Deliberately a FAULT, not a checkpoint: a breakpoint would freeze
	// the racing recompute at this same site too, and the test needs it to run through.
	if e.seams.IsFault(FaultSlowPoolPush) {
		time.Sleep(slowPoolPushDelay)
	}
	postSplitConns := 0
	for _, idx := range e.db.Indices() {
		db, err := e.db.Shard(idx)
		if err != nil {
			continue
		}
		slots := observed[idx]
		// A replica leaving a shard stops dispatching BEFORE its pool shrinks, and one joining grows its pool
		// BEFORE it dispatches, so no worker is ever handed a shard through a pool sized for reading.
		if slots == 0 {
			e.applyRole(idx, false)
		}
		if override != 0 {
			if slots > 0 {
				postSplitConns += override
			}
		} else {
			// Zero-value spec = the default shard's sizing; a missing RTT (an unprobed shard) falls to the
			// uncompensated bucket, which is the under-connecting direction.
			idle, open := shardPool(specs[idx], 0, slots, rtts[idx])
			db.SetMaxOpenConns(open)
			db.SetMaxIdleConns(idle)
			if e.seams.Enabled() { // Enabled gates the assembled name and the boxed value in production
				e.seams.Variable(seamsJoin(VariablePoolIdle, strconv.Itoa(idx)), idle)
			}
			// The turnstile follows the pool for the same reason the cache and the worker ceiling do: it
			// orders access to the connections this replica actually holds, and the pool just changed. Resize
			// moves the available count by the DELTA, so a turn held by an in-flight worker is never handed
			// out twice.
			if e.turnstiles != nil {
				e.turnstiles.Resize(idx, turnstilePassesPerConn*open)
			}
			if slots > 0 {
				postSplitConns += open
			}
		}
		if slots > 0 {
			e.applyRole(idx, true)
		}
	}
	// Under an override the pools were pushed by SetMaxOpenConns, but what follows from WHICH shards this
	// replica dispatches still moves with the roles: a replica promoted by the override landing was sized at
	// Startup as a reader, with the minimum cache and no refill period on shards it now dispatches.
	//
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
	// one that would have to serialize against a shrink in flight. The cache and the ceiling below do not
	// self-correct, which is why they are re-derived and this is not.
	dispatch := max(64, workersPerConnBudget*postSplitConns)
	e.cache.Resize(min(dispatch, int(e.workers.Load())))
	// The refill scan floor is measured against the cache's capacity, so it follows the same split -
	// the same rule the dispatch count and worker ceiling obey just above. Reuses the specs/rtts already
	// locked-and-cloned above rather than paying for a second (and, below, a third) lock/clone pass over
	// the same shardSpecs/shardRTTMs maps.
	e.recomputeRefillIntervalsWith(specs, rtts, observed)
	e.logger.Info("Derived pools recomputed", "slots", observed, "dispatch", dispatch)
	e.recomputeWorkerCeilingWith(e.lifetimeCtx, specs, rtts, observed)
}

// applyRole starts or stops this replica dispatching one shard: the doorbell's gate and the piston's idle
// mode move together. Called under poolsLock, in the order recomputePools gives it.
func (e *Engine) applyRole(shard int, dispatches bool) {
	if b := e.dispatchFlag(shard); b != nil && b.Swap(dispatches) != dispatches {
		e.logger.Info("Shard dispatch role changed", "shard", shard, "dispatching", dispatches)
	}
	if p := e.pistons[shard]; p != nil {
		p.SetIdle(!dispatches || e.zeroWorkers())
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
// Locks shardsLock and clones the shard facts itself; recomputePools has already done both by the time it
// gets here, so it calls recomputeWorkerCeilingWith directly with its own clones rather than paying for a
// second lock/clone pass over the same maps.
func (e *Engine) recomputeWorkerCeiling(ctx context.Context) {
	e.shardsLock.Lock()
	specs := maps.Clone(e.shardSpecs)
	rtts := maps.Clone(e.shardRTTMs)
	e.shardsLock.Unlock()
	e.recomputeWorkerCeilingWith(ctx, specs, rtts, e.slotsByShard(specs))
}

// recomputeWorkerCeilingWith is recomputeWorkerCeiling's core, taking the shard specs, RTTs and roles as
// already-read snapshots rather than re-acquiring shardsLock or re-reading the roles to fetch them.
func (e *Engine) recomputeWorkerCeilingWith(ctx context.Context, specs map[int]ShardSpec, rtts map[int]float64, slotsBy map[int]int) {
	override := int(e.maxOpenConns.Load())
	ceiling := math.MaxInt
	for idx, rttMs := range rtts {
		// Each dispatched shard's own pool, since each is divided by its own dispatchers - and the worst
		// shard's number wins, because a storm drains through whichever pool is tightest. A shard this replica
		// only reads runs no step, so its small pool drains no storm and must not bound the workers.
		slots := slotsBy[idx]
		if slots == 0 {
			continue
		}
		_, open := shardPool(specs[idx], override, slots, rttMs)
		ceiling = min(ceiling, workerCeiling(open, rttMs))
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
		e.logger.WarnContext(ctx, "Worker count exceeds the lease-margin ceiling: a synchronized completion storm may re-execute tasks",
			"workers", n, "ceiling", ceiling)
	}
}

// capacityWeight maps a shard's VirtualCPUs to its new-flow placement weight, proportional to the
// measured steps/s ceiling of the tier: ~flat up to 2 vCPUs (1- and 2-vCPU tiers both ceiling near
// 745 steps/s - raw-CPU proportionality would over-place on 2 vCPUs), then ~450 steps/s per vCPU.
// An undeclared count resolves to defaultVirtualCPUs, so every shard carries a positive weight.
func capacityWeight(virtualCPUs int) int {
	if v := effectiveVirtualCPUs(virtualCPUs); v <= 2 {
		return 745
	} else {
		return 450 * v
	}
}

// pickShard selects the shard for a new top-level flow: a weighted-random pick over the non-cordoned
// shards, in proportion to capacityWeight. Placement is the engine's only load-balancing moment (flows
// are shard-pinned for life), so heterogeneous fleets must be loaded in capacity proportion - uniform
// placement saturates the smallest shard first while larger ones idle.
//
// The pick is among the shards this replica DISPATCHES when there are any, because the doorbell rings only
// for those: a flow placed elsewhere waits for a dispatcher's scan before its first step runs. The ranking
// spreads dispatch slots across the fleet, so placement stays roughly capacity-proportional fleet-wide. A
// replica that dispatches nothing picks among every non-cordoned shard.
func (e *Engine) pickShard() (int, error) {
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
		// indices). The second is not API misuse but an ordinary shutdown race - a host still serving while
		// it tears the engine down, or a Create in flight when Shutdown lands - and indexing the empty slice
		// panicked the host's process for it. A library owes that caller an error.
		indices := e.db.Indices()
		if len(indices) == 0 {
			return 0, errors.New("engine is not started", http.StatusServiceUnavailable)
		}
		return indices[rand.IntN(len(indices))], nil
	}
	weights := make([]int, len(specs))
	total := 0
	for _, dispatchedOnly := range []bool{true, false} {
		for i, spec := range specs {
			weights[i] = 0
			if spec.Cordoned || (dispatchedOnly && !e.dispatchesOn(spec.Index)) {
				continue
			}
			weights[i] = capacityWeight(spec.VirtualCPUs)
			total += weights[i]
		}
		if total > 0 {
			break
		}
	}
	if total == 0 {
		return 0, errors.New("all shards are cordoned", http.StatusServiceUnavailable)
	}
	r := rand.IntN(total)
	for i, w := range weights {
		if w == 0 {
			continue
		}
		if r < w {
			return specs[i].Index, nil
		}
		r -= w
	}
	return specs[len(specs)-1].Index, nil // unreachable; defensive
}
