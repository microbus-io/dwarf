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
	"time"

	"github.com/microbus-io/dwarf/internal/pipeline"
)

// The piston cycle period - the supply control for every shard, measured start of scan to start of scan.
// Derived per shard at Startup and fixed thereafter: nothing here reads an observed rate, because supply
// set from measured consumption oscillates (consumption is min(demand, supply), so the actuation
// contaminates its own measurement).
const (
	// refillSupplyHeadroom is the margin the buffer carries over the sustained drain. 2.0 is the measured
	// throughput optimum: at a tighter margin ordinary drain-rate jitter briefly empties the buffer and
	// stalls workers.
	//
	// Do NOT re-derive it from waste (discarded/selected). Waste runs ~2% and nearly flat across the whole
	// good interval range, so it cannot distinguish the optimum; throughput can.
	refillSupplyHeadroom = 2.0
	// sustainedDrainPerVCPU is the measured sustained per-shard drain in steps/s/vCPU: ~120 steps/s per
	// connection x connsPerVCPU. NOT capacityWeight's 450, which is the PEAK placement ceiling - the period
	// wants the SUSTAINED rate, and conflating the two undershoots the drain and overshoots the period into
	// a starved regime.
	sustainedDrainPerVCPU = 720
	// refillIntervalCap bounds priority latency, and is the only thing that does. A better band arriving
	// here does not preempt - Offer appends it at the tail - so it becomes servable when a cycle plans it,
	// and peers plan it a cycle after that. Priority ORDER is never inverted regardless, since every cycle
	// plans the global minimum band; what this bounds is when better work starts, not whether it wins.
	refillIntervalCap = 1 * time.Second
)

// deriveRefillInterval computes ONE shard's cycle period:
//
//	bufferShare = capacity/N        the most one cycle can hand this partition
//	drain       = sustainedDrainPerVCPU * min(poolConns/connsPerVCPU, vCPUs/R)
//	T           = bufferShare / (headroom * drain)
//
// The drain takes the TIGHTER of two channels, since sustained throughput cannot exceed either: this
// replica's connection pool, or the shard's database CPU split across the fleet. They are equal by
// construction in the derived path, so the min bites only when SetMaxOpenConns pins a pool independently
// of the declared vCPUs - without it, a large pinned pool with vCPUs undeclared derives its drain from the
// default 2 and overshoots to the cap (a starved refiller), and a small pooler-capped pool over-scans on a
// drain it cannot sustain. vCPUs <= 0 is undeclared: the CPU ceiling is unknown, so the drain falls to the
// connection channel alone.
//
// It stays a formula rather than the ~67ms it evaluates to at the reference config, because bufferShare
// tracks the cache-sizing constants: a change to worker or cache sizing rescales the period with it,
// instead of leaving a pinned number that exceeds what the buffer can cover.
func deriveRefillInterval(bufferShare, virtualCPUs, poolConns, replicas int) time.Duration {
	drain := float64(sustainedDrainPerVCPU) * float64(poolConns) / float64(connsPerVCPU) // connection channel
	if virtualCPUs > 0 {                                                                 // cap by the CPU ceiling, when it is known
		drain = min(drain, float64(sustainedDrainPerVCPU)*float64(virtualCPUs)/float64(max(1, replicas)))
	}
	if bufferShare <= 0 || drain <= 0 {
		return refillIntervalCap
	}
	t := float64(bufferShare) / (refillSupplyHeadroom * drain) // seconds: buffer covers headroom x the drain
	return min(time.Duration(t*float64(time.Second)), refillIntervalCap)
}

// recomputeRefillIntervals re-derives every piston's cycle period and pushes it. Called at Startup and
// from recomputePools - the same "every path that changes a pool must re-derive what depends on it" rule
// the worker ceiling and the candidate cache already obey, since the period is measured against the
// cache's capacity. Pushing is live: a piston reads its interval once per cycle rather than capturing it.
func (e *Engine) recomputeRefillIntervals() {
	n := max(1, e.db.NumShards())
	// max(1, ...) because a cache smaller than the shard count divides to zero, which reaches the degenerate
	// guard and answers with the 1s cap - backwards for a tiny cache, which drains instantly and wants
	// frequent scans. The case is a small cache, not an unknown one.
	share := max(1, e.cache.Capacity()/n)
	replicas := max(1, int(e.observedR.Load()))
	override := time.Duration(e.refillIntervalOverride.Load())
	pinned := int(e.maxOpenConns.Load()) // >0 when SetMaxOpenConns pins every shard's pool
	e.shardsLock.Lock()
	specs := make(map[int]ShardSpec, len(e.shardSpecs))
	for idx, spec := range e.shardSpecs {
		specs[idx] = spec
	}
	e.shardsLock.Unlock()
	for idx, p := range e.pistons {
		if override > 0 {
			p.SetInterval(override)
			// The gap is the fuse against a 100%-duty-cycle scan loop, and a bench sweep measuring below it
			// is the one caller entitled to say so explicitly. Only lowered, never raised: a 500ms pinned
			// interval keeps the ordinary 20ms gap.
			p.SetMinGap(min(pipeline.DefaultMinGap, override))
			continue
		}
		// Pass the shard's ACTUAL pool (shardPool resolves the SetMaxOpenConns pin) and its RAW declared
		// vCPUs (0 = undeclared), so the drain is bounded by whichever channel is real - the pinned pool,
		// not a defaulted vCPU count. An unconfigured shard's zero-value spec falls to the conn channel.
		spec := specs[idx]
		_, pool := shardPool(spec, pinned, replicas)
		p.SetInterval(deriveRefillInterval(share, spec.VirtualCPUs, pool, replicas))
		p.SetMinGap(pipeline.DefaultMinGap)
	}
}
