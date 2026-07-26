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
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// The connection-budget and capacity constants below were measured by the cloud benchmark campaign
// (Cloud SQL PostgreSQL, tiers 1-8 vCPU; see docs/benchmark-cloud.md). They are engine knowledge - the
// operator provides the facts (ShardSpec.VirtualCPUs), the engine owns the constants.
const (
	// connsPerVCPU sizes a shard's connection pool at the measured knee: throughput stops improving
	// beyond ~6x the database's CPU count (range 4-8 across tiers), and on small servers over-connection
	// actively collapses throughput (745 -> 212 steps/s at 1 vCPU between 16 and 96 connections).
	connsPerVCPU = 6

	// defaultVirtualCPUs is assumed when a ShardSpec does not declare VirtualCPUs. Two facts make this
	// guess safe rather than reckless (the failure mode of a WRONG guess is the over-connection collapse):
	//   - 2 vCPUs is the FLOOR of every current-generation AWS RDS class (db.t4g/m7g/r7g all start at 2),
	//     so on RDS the assumption cannot undershoot the real machine.
	//   - Where smaller machines do exist (Cloud SQL's 1-vCPU db-custom-1-*, and its shared-core tiers),
	//     the pool this yields (6 x 2 = 12) still sits below their measured knee: the 1-vCPU tier peaked
	//     at M=16 (856 steps/s) and only collapsed from M=32 up. An operator on such a machine declares
	//     VirtualCPUs: 1 and gets a pool of 6.
	// So the cost of the assumption is bounded, and it buys a real default instead of a timid one.
	defaultVirtualCPUs = 2

	// workersPerConnBudget sizes the RESIDENT worker set (and the candidate cache) from the aggregate
	// connection budget: dispatch is database-bound, and useful dispatching workers = conns x T/db,
	// with T/db measured ~3 for no-op tasks. 8 is deliberately generous for short tasks. Workers beyond
	// this are spawned on demand (see workerCeiling): a worker blocked in a long ExecuteTask holds no
	// connection, so it must not inflate the cache/refill scan, which serves dispatch only.
	workersPerConnBudget = 8

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

// shardPool returns the idle/open pool sizes for one shard: VirtualCPUs (defaulted, see
// effectiveVirtualCPUs) derives the open ceiling at the measured knee, with a warm idle core of half.
// The explicit SetMaxOpenConns override wins and pins the pool to exactly that size (the
// benchmarking/expert path). The derived budgets are per DATABASE, so they are split across the
// OBSERVED engine replicas (the peer-discovery count; see peers.go); the override is the operator's
// exact per-replica number and is never divided.
func shardPool(spec ShardSpec, override int, replicas int) (idle, open int) {
	if override > 0 {
		return override, override
	}
	replicas = max(1, replicas)
	open = max(2, connsPerVCPU*effectiveVirtualCPUs(spec.VirtualCPUs)/replicas)
	return max(2, open/2), open
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

// recomputePools re-derives every shard's connection pool from that shard's OWN replica count and pushes
// the sizes to the open shards (sequel's pool setters are hot/atomic), then re-derives the worker
// ceiling, which is a function of those pools. Called by the reconcile loop on the Sonars' cadence.
// No-ops when the engine is not running, when the SetMaxOpenConns override pins the pools (an exact
// per-replica number, never divided), and when no shard's count has moved since the last application.
// (Startup itself sizes the pools directly from what it read, not through here, and records the counts.)
//
// The divisor is PER SHARD because the budget is: it belongs to the shard's database, so the replicas that
// matter are the ones holding connections to THAT database. One shard's fleet changing must not re-push
// another's unchanged sizes, and one shard's count staying put must not mask a change elsewhere.
func (e *Engine) recomputePools() {
	// poolsLock is held across the whole read-then-push, not just the dedupe: lastAppliedR keeps a no-op
	// recompute from touching the pools, but it does not ORDER two live ones, so without this an R=2 push
	// could land after an R=3 push (over-connecting a fleet of 3), and a concurrent SetMaxOpenConns could
	// have its pinned pools overwritten by derived ones. The poolsLock -> shardsLock order below cannot
	// cycle (the counts are lock-free reads of the Sonars' published state).
	e.poolsLock.Lock()
	defer e.poolsLock.Unlock()
	if !e.started.Load() || e.maxOpenConns.Load() != 0 {
		return
	}
	// Read every shard's count first and compare as a whole: a push is all-or-nothing, so a single shard
	// moving is enough to re-derive, and nothing moving is the cheap common case.
	observed := make(map[int]int, len(e.lastAppliedR))
	changed := false
	for _, idx := range e.db.Indices() {
		r := e.replicasOn(idx)
		observed[idx] = r
		if prev, ok := e.lastAppliedR[idx]; !ok || prev != r {
			changed = true
		}
	}
	if !changed && len(observed) == len(e.lastAppliedR) {
		return
	}
	e.lastAppliedR = observed
	// The window poolsLock closes: R has been read, the sizes are not yet pushed. A test stalls one recompute
	// here to hold a stale R while a peer's fresher one races past (see TestPoolSizing_ConcurrentRecompute-
	// AppliesLatestR). Deliberately a FAULT, not a checkpoint: a breakpoint would freeze the racing recompute
	// at this same site too, and the test needs it to run through.
	if e.seams.IsFault(FaultSlowPoolPush) {
		time.Sleep(slowPoolPushDelay)
	}
	e.shardsLock.Lock()
	specs := maps.Clone(e.shardSpecs)
	e.shardsLock.Unlock()
	postSplitConns := 0
	for _, idx := range e.db.Indices() {
		db, err := e.db.Shard(idx)
		if err != nil {
			continue
		}
		idle, open := shardPool(specs[idx], 0, observed[idx]) // zero-value spec = the default shard's sizing
		db.SetMaxOpenConns(open)
		db.SetMaxIdleConns(idle)
		postSplitConns += open
	}
	// The candidate cache follows the pool split, for the same reason the worker ceiling does: it is sized from
	// what this replica can actually CLAIM, and the pool it claims through just shrank by R.
	//
	// Startup derives the dispatch count with R=1 (peer discovery has not run yet), so it is the FULL per-database
	// budget. Left alone, a replica in a fleet of 8 keeps a cache sized for 8x the connections it now holds - and
	// the refiller scans up to the cache's capacity per fairness key and wholesale-replaces it, so it is handed far
	// more candidates than it can ever claim. Stale hints whose claim CAS loses to a peer, and wasted round-trips,
	// exactly when the fleet is busiest. This is the same "never size the cache from more than the replica can
	// claim" rule the worker ceiling is kept away from, arrived at through a different door.
	//
	// The RESIDENT worker count is deliberately NOT resized: it is a bounded, non-compounding over-provision
	// (surplus workers queue on the pool and the growth trigger counts workers OFFSITE, not saturation), and
	// shrinking it needs a worker-retirement protocol whose only prize is goroutine stacks.
	dispatch := max(64, workersPerConnBudget*postSplitConns)
	e.cache.Resize(min(dispatch, int(e.workers.Load())))
	// The refill scan floor is measured against the cache's capacity, so it follows the same split -
	// the same rule the dispatch count and worker ceiling obey just above.
	e.recomputeRefillIntervals()
	e.logger.Info("Derived pools recomputed", "replicas", observed, "dispatch", dispatch)
	e.recomputeWorkerCeiling(e.lifetimeCtx)
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
func (e *Engine) recomputeWorkerCeiling(ctx context.Context) {
	override := int(e.maxOpenConns.Load())
	e.shardsLock.Lock()
	specs := maps.Clone(e.shardSpecs)
	rtts := maps.Clone(e.shardRTTMs)
	e.shardsLock.Unlock()

	ceiling := math.MaxInt
	for idx, rttMs := range rtts {
		// Each shard's own count, since each shard's pool is divided by its own fleet - and the worst shard's
		// number wins, because a storm drains through whichever pool is tightest.
		_, open := shardPool(specs[idx], override, e.replicasOn(idx))
		ceiling = min(ceiling, workerCeiling(open, rttMs))
	}
	if ceiling == math.MaxInt {
		ceiling = 64 // no shard was probed (an unopened or test-mode engine)
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
	total := 0
	weights := make([]int, len(specs))
	for i, spec := range specs {
		if spec.Cordoned {
			continue
		}
		weights[i] = capacityWeight(spec.VirtualCPUs)
		total += weights[i]
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
