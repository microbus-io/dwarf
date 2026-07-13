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
	"maps"
	"math/rand/v2"
	"net/http"

	"github.com/microbus-io/errors"
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

	// workersPerConnBudget derives the default worker count from the aggregate connection budget:
	// useful workers = conns x T/db, and T/db was measured ~3 for no-op tasks and grows with task time.
	// 8 is deliberately generous - an over-provisioned worker idles cheaply, an under-provisioned pool
	// caps throughput below the database's ceiling.
	workersPerConnBudget = 8
)

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
