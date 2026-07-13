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

	// defaultPoolSize is the pool for a shard with unknown VirtualCPUs: measured safe on every tier
	// (M=8 ran clean even at 1 vCPU), deliberately conservative - honest ignorance beats a guessed
	// CPU count whose failure mode is the over-connection collapse.
	defaultPoolSize = 8

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

// shardPool returns the idle/open pool sizes for one shard. The explicit SetMaxOpenConns override wins
// (the pool is pinned to exactly that size - the benchmarking/expert path); otherwise VirtualCPUs derives
// the open ceiling at the measured knee with a warm idle core; otherwise the measured-safe default.
// The derived budgets are per DATABASE, so they are split across the OBSERVED engine replicas (the
// peer-discovery count; see peers.go); the override is the operator's exact per-replica number and is
// never divided.
func shardPool(spec ShardSpec, override int, replicas int) (idle, open int) {
	replicas = max(1, replicas)
	switch {
	case override > 0:
		return override, override
	case spec.VirtualCPUs > 0:
		open = max(2, connsPerVCPU*spec.VirtualCPUs/replicas)
		return max(2, open/2), open
	default:
		open = max(2, defaultPoolSize/replicas)
		return open, open
	}
}

// recomputePools re-derives every shard's connection pool from the observed replica count and pushes
// the sizes to the open shards (sequel's pool setters are hot/atomic). Called whenever the peer map
// changes (hello/ping of a new id, goodbye, heartbeat prune). No-ops when the engine is not running,
// when the SetMaxOpenConns override pins the pools (an exact per-replica number, never divided), and
// when R is unchanged since the last application.
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
}

// capacityWeight maps a shard's VirtualCPUs to its new-flow placement weight, proportional to the
// measured steps/s ceiling of the tier: ~flat up to 2 vCPUs (1- and 2-vCPU tiers both ceiling near
// 745 steps/s - raw-CPU proportionality would over-place on 2 vCPUs), then ~450 steps/s per vCPU.
// 0 (unknown) is resolved by the caller to the most conservative weight in the set.
func capacityWeight(virtualCPUs int) int {
	switch {
	case virtualCPUs <= 0:
		return 0
	case virtualCPUs <= 2:
		return 745
	default:
		return 450 * virtualCPUs
	}
}

// pickShard selects the shard for a new top-level flow: a weighted-random pick over the non-cordoned
// shards, in proportion to capacityWeight. Placement is the engine's only load-balancing moment (flows
// are shard-pinned for life), so heterogeneous fleets must be loaded in capacity proportion - uniform
// placement saturates the smallest shard first while larger ones idle. A shard with unknown
// VirtualCPUs gets the smallest known weight (conservative: never over-load the shard known least),
// or a uniform pick when no shard declares CPUs.
func (e *Engine) pickShard() (int, error) {
	e.shardsLock.Lock()
	specs := make([]ShardSpec, 0, len(e.shardSpecs))
	for _, spec := range e.shardSpecs {
		specs = append(specs, spec)
	}
	e.shardsLock.Unlock()
	if len(specs) == 0 {
		// No shard was registered: Startup opened the single default shard.
		indices := e.db.Indices()
		return indices[rand.IntN(len(indices))], nil
	}
	minKnown := 0
	for _, spec := range specs {
		if w := capacityWeight(spec.VirtualCPUs); w > 0 && (minKnown == 0 || w < minKnown) {
			minKnown = w
		}
	}
	if minKnown == 0 {
		minKnown = 1 // no shard declares CPUs: uniform
	}
	total := 0
	weights := make([]int, len(specs))
	for i, spec := range specs {
		if spec.Cordoned {
			continue
		}
		w := capacityWeight(spec.VirtualCPUs)
		if w == 0 {
			w = minKnown
		}
		weights[i] = w
		total += w
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
