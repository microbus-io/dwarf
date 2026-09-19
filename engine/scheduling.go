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
	// sustainedDrainPerVCPU is the measured sustained per-shard drain in steps/s/vCPU. NOT capacityWeight's
	// 450, which is the PEAK placement ceiling - the period wants the SUSTAINED rate, and conflating the two
	// undershoots the drain and overshoots the period into a starved regime.
	sustainedDrainPerVCPU = 720
	// sustainedDrainPerConn is the same measurement expressed per CONNECTION - the quantity the connection
	// channel below actually wants, measured roughly flat across connection counts, instance sizes and
	// backlog volumes. It is stated as its own constant rather than as sustainedDrainPerVCPU divided by a
	// connection ratio, because that ratio varies with both instance size and distance (see
	// connsPerVCPUFor), so dividing by it would silently mean a different thing per tier and per shard -
	// and the per-connection drain does not vary with how many connections the pool was allotted.
	//
	// It is flat across connection counts, instance sizes and backlog volumes on a disk with IOPS headroom,
	// which is the deployment the operator guidance asks for - NOT across disks. A throttled disk drains
	// slower, so its optimum interval is LONGER: an interval crawl left to find each instance's own optimum
	// settled at ~24ms on an IOPS-rich instance and ~51ms on a throttled one, and a laptop Postgres wanted
	// 170-280ms. The constant is deliberately the healthy-disk value, because being short there costs only
	// wasted scans while being long starves dispatch (~50% of throughput at the old 141ms).
	sustainedDrainPerConn = 120
	// refillIntervalCap bounds priority latency, and is the only thing that does. A better band arriving
	// here does not preempt - Offer appends it at the tail - so it becomes servable when a cycle plans it,
	// and peers plan it a cycle after that. Priority ORDER is never inverted regardless, since every cycle
	// plans the global minimum band; what this bounds is when better work starts, not whether it wins.
	refillIntervalCap = 1 * time.Second
	// supplyGapDivisor turns the supply loop's derived period into its quiet-time fuse.
	//
	// THE GAP IS A FRACTION OF THE DRAIN TIME, not a constant, because a constant cannot stay a fuse. The
	// period is the time the workers take to drain one buffer's worth (see deriveRefillInterval), and a fixed
	// 20ms gap silently BECOMES the supply rate wherever that period lands under it - which is any pinned
	// SetWorkers, where it under-supplies a small cache against its own drain. It is applied under a
	// min(DefaultMinGap, ...) so it only ever SCALES THE GAP DOWN - the measured-good 20ms stands wherever the
	// period is 60ms or longer, and the divisor bites only below that. Raising it instead would throttle
	// supply on precisely the slowest shards (333ms at the 1s refillIntervalCap, where a 900ms fetch would
	// cost a 1.233s period against 1s), which is the opposite of what the fuse is for. Three is not a tuned
	// number: it is the coarsest divisor that stays clear of the period at the derived low end.
	//
	// The TALLY loop keeps the flat constant. It fills no buffer, so it has no drain to derive from, and its
	// fuse is against a scan that outruns its own interval - a different quantity entirely.
	supplyGapDivisor = 3
)

// deriveRefillInterval computes ONE shard's cycle period:
//
//	bufferShare = capacity/N        the most one cycle can hand this partition
//	drain       = min(sustainedDrainPerConn * min(poolConns, dispatchers),
//	                  sustainedDrainPerVCPU * vCPUs/R)
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
func deriveRefillInterval(bufferShare, virtualCPUs, poolConns, replicas, dispatchers int) time.Duration {
	// WORKERS BOUND THE DRAIN, and leaving them out is what makes a pinned SetWorkers derive a nonsense
	// period. They are what CONSUMES candidates: one worker cannot drain a connection budget's worth however
	// many connections exist, and it holds no connection while its task runs. Without this term the buffer
	// shrinks with the worker count (capacity is twice it) while the assumed drain does not, so the period
	// collapses - measured at 0.28ms for SetWorkers(1) against a 30-connection pool, i.e. far under the gap,
	// which then silently becomes the supply rate instead of the fuse it is meant to be. A worker that never
	// waits for a connection drains at the same per-connection rate, which is why one constant serves both.
	drain := float64(sustainedDrainPerConn) * float64(min(poolConns, max(1, dispatchers)))
	if virtualCPUs > 0 { // cap by the CPU ceiling, when it is known
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
//
// Locks shardsLock and clones the shard facts itself; recomputePools has already done both by the time it
// gets here, so it calls recomputeRefillIntervalsWith directly with its own clones rather than paying for a
// second lock/clone pass over the same maps.
func (e *Engine) recomputeRefillIntervals() {
	e.shardsLock.Lock()
	specs := make(map[int]ShardSpec, len(e.shardSpecs))
	for idx, spec := range e.shardSpecs {
		specs[idx] = spec
	}
	rtts := maps.Clone(e.shardRTTMs)
	e.shardsLock.Unlock()
	e.recomputeRefillIntervalsWith(specs, rtts)
}

// recomputeRefillIntervalsWith is recomputeRefillIntervals' core, taking the shard specs and RTTs as
// already-read snapshots rather than re-acquiring shardsLock to fetch them.
func (e *Engine) recomputeRefillIntervalsWith(specs map[int]ShardSpec, rtts map[int]float64) {
	n := max(1, e.db.NumShards())
	// max(1, ...) because a cache smaller than the shard count divides to zero, which reaches the degenerate
	// guard and answers with the 1s cap - backwards for a tiny cache, which drains instantly and wants
	// frequent scans. The case is a small cache, not an unknown one.
	capacity := e.cache.Capacity() // read ONCE: share and dispatchers are two views of one number
	share := max(1, capacity/n)
	// The workers draining THIS shard's partition, which is share/2 because the cache is sized as twice the
	// whole crew - read back off it rather than re-derived, so it cannot drift from what the buffer was built
	// for. EVERY term here is per-shard (the pool is this shard's, the vCPU ceiling is this shard's), and the
	// drain must be too: one crew pops across all N partitions, so crediting the whole of it to each one
	// over-states the drain by exactly N and collapses the period - 1.04ms on 8 shards, against the 8.33ms the
	// worker term exists to hold, i.e. straight back into the over-supply regime it was added to close.
	dispatchers := max(1, share/2)
	override := time.Duration(e.refillIntervalOverride.Load())
	pinned := int(e.maxOpenConns.Load()) // >0 when SetMaxOpenConns pins every shard's pool
	for idx, p := range e.pistons {
		if override > 0 {
			// The gap is the fuse against a 100%-duty-cycle loop, and a bench sweep measuring below it is the
			// one caller entitled to say so explicitly. Only lowered, never raised: a 500ms pinned interval
			// keeps the ordinary 20ms gap. An override pins BOTH loops - it exists to measure the refiller
			// unpaced, and pacing half of it would measure neither arm.
			gap := min(pipeline.DefaultMinGap, override)
			p.SetTallyCadence(override, gap)
			p.SetSupplyCadence(override, gap)
			continue
		}
		// Pass the shard's ACTUAL pool (shardPool resolves the SetMaxOpenConns pin) and its RAW declared
		// vCPUs (0 = undeclared), so the drain is bounded by whichever channel is real - the pinned pool,
		// not a defaulted vCPU count. An unconfigured shard's zero-value spec falls to the conn channel.
		spec := specs[idx]
		// This shard's own replica count: the pool it drains through was divided by that one, so deriving the
		// period from any other shard's fleet would measure the buffer against the wrong drain rate.
		replicas := e.replicasOn(idx)
		_, pool := shardPool(spec, pinned, replicas, rtts[idx])
		derived := deriveRefillInterval(share, spec.VirtualCPUs, pool, replicas, dispatchers)
		p.SetTallyCadence(derived, pipeline.DefaultMinGap)
		p.SetSupplyCadence(derived, min(pipeline.DefaultMinGap, derived/supplyGapDivisor))
	}
}
