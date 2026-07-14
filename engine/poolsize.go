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

// recomputePools re-derives every shard's connection pool from the observed replica count and pushes
// the sizes to the open shards (sequel's pool setters are hot/atomic), then re-derives the worker
// ceiling, which is a function of those pools. Called whenever the peer map changes (hello/ping of a
// new id, goodbye, heartbeat prune). No-ops when the engine is not running, when the SetMaxOpenConns
// override pins the pools (an exact per-replica number, never divided), and when R is unchanged since
// the last application.
func (e *Engine) recomputePools() {
	if !e.started.Load() || e.maxOpenConns.Load() != 0 {
		return
	}
	replicas := e.observedReplicas()
	if int32(replicas) == e.lastAppliedR.Swap(int32(replicas)) {
		return
	}
	e.shardsLock.Lock()
	specs := maps.Clone(e.shardSpecs)
	e.shardsLock.Unlock()
	for _, idx := range e.db.Indices() {
		db, err := e.db.Shard(idx)
		if err != nil {
			continue
		}
		idle, open := shardPool(specs[idx], 0, replicas) // zero-value spec = the default shard's sizing
		db.SetMaxOpenConns(open)
		db.SetMaxIdleConns(idle)
	}
	e.logger.Info("Derived pools recomputed", "replicas", replicas)
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
	replicas := e.observedReplicas()
	override := int(e.maxOpenConns.Load())
	e.shardsLock.Lock()
	specs := maps.Clone(e.shardSpecs)
	rtts := maps.Clone(e.shardRTTMs)
	e.shardsLock.Unlock()

	ceiling := math.MaxInt
	for idx, rttMs := range rtts {
		_, open := shardPool(specs[idx], override, replicas)
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
