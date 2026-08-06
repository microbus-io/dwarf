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
// ceiling. VirtualCPUs (defaulted, see effectiveVirtualCPUs) and the shard's probed RTT pick the ratio,
// and the resulting per-DATABASE budget is split across the OBSERVED engine replicas holding connections
// to that database (the peer-discovery count; see peers.go).
//
// The explicit SetMaxOpenConns override wins and pins the pool to exactly that size (the
// benchmarking/external-pooler path): it is the operator's exact per-replica number, is never divided,
// and bypasses the RTT term with it.
//
// rttMs is the value probed at Startup and held, never a live reading - a latency change re-derives on
// restart, the same posture as the shard set itself.
func shardPool(spec ShardSpec, override int, replicas int, rttMs float64) (idle, open int) {
	if override > 0 {
		return override, override
	}
	replicas = max(1, replicas)
	vcpus := effectiveVirtualCPUs(spec.VirtualCPUs)
	open = max(2, shardBudget(vcpus, rttMs)/replicas)
	return max(2, open/2), open
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

// poolRatio is the RAW MINIMUM connections per vCPU - the smallest pool that sustained the load, before
// poolSafetyMargin - by shard size and distance, one column per poolRTTBuckets entry. connsPerVCPUFor
// documents how the cells are derived and which of them rest on measurement.
var poolRatio = []struct {
	vcpus  int
	ratios [5]float64 // 0.25, 0.50, 1.00, 1.50, 2.00 ms
}{
	{1, [5]float64{3.00, 5.00, 7.00, 9.00, 11.00}},  // MEASURED
	{2, [5]float64{3.00, 6.00, 7.00, 10.00, 11.00}}, // MEASURED
	{4, [5]float64{3.00, 6.00, 6.00, 8.00, 10.00}},  // MEASURED
	{8, [5]float64{5.00, 8.00, 8.00, 11.00, 11.00}}, // MEASURED
	{16, [5]float64{5.00, 7.00, 7.00, 9.00, 9.00}},  // MEASURED
	{32, [5]float64{3.75, 4.70, 5.60, 8.10, 8.75}},  // MEASURED
	{64, [5]float64{4.42, 5.21, 6.55, 7.64, 8.54}},
	{96, [5]float64{3.94, 4.65, 5.85, 6.83, 7.63}},
	{128, [5]float64{3.63, 4.29, 5.40, 6.29, 7.03}},
}

// connsPerVCPUFor is the connection-per-vCPU ratio for a shard of a given size at a given distance. Both
// axes are load-bearing: a connection held while a packet is in flight does the server no good, so what a
// database sees is the duty cycle M*s/(k*RTT+s), not M.
//
// THE 1- THROUGH 32-vCPU ROWS ARE MEASURED; 64, 96 AND 128 REMAIN MODELLED, DELIBERATELY. The model derives each cell as
// M* = B*(k*RTT+s*)/s* derated 10%, with k = 9.11 round trips per step and B* = 15*vCPU^0.72 backends at
// the knee. Pool sweeps on Cloud SQL found it over-provisions worst at SHORT RTT - 8.63x and 7.34x where
// 5.0x and 5.1x sufficed - which is where nearly every deployment sits (same-zone RTT is 0.05-0.96 ms).
// The correction is NOT extrapolated to the untested rows: two tiers are not evidence about seven others,
// and reasoning-by-inheritance is what put the wrong numbers here to begin with. Sweep a tier before
// changing its row.
//
// Every cell is the MINIMUM pool that sustained that distance's achievable throughput. poolSafetyMargin
// is applied on top by this function, so the table stays a record of measurement and the slack stays one
// number in one place.
//
//	      1 vCPU      2 vCPU        4 vCPU        8 vCPU        16 vCPU       32 vCPU
//	RTT   rate min  x   rate min  x   rate  min  x  rate  min  x  rate  min  x  rate   min  x
//	~0.1   390   3  3.0   770   6  3.0 1,750  12 3.0 3,500  40 5.0 7,000  82 5.1 14,000 120 3.75
//	~0.5   390   5  5.0   770  12  6.0 1,750  24 6.0 3,500  64 8.0 7,000 120 7.5 14,000 150 4.70
//	~1.0   390   7  7.0   660  14  7.0 1,500  24 6.0 3,000  64 8.0 6,000 115 7.2 12,000 180 5.60
//	~1.5   390   9  9.0   660  20 10.0 1,500  32 8.0 3,000  88 11.0 6,000 145 9.1 12,000 260 8.10
//	~2.0   390  11 11.0   660  22 11.0 1,500  40 10.0 3,000 88 11.0 5,000 145 9.1 10,000 280 8.75
//	(rate in steps/s - each tier's own achievable throughput at that distance, not one fixed rate)
//
// WHAT THE SWEEPS ESTABLISH IS THE LEVEL, NOT THE SHAPE ACROSS TIERS. Every measured minimum is far below
// its modelled cell at short RTT (4 vCPU 3.0 vs 8.50, 8 vCPU 5.0 vs 7.19, 16 vCPU 5.1 vs 6.12, 32 vCPU
// 3.75 vs 5.36), so the model over-provisions there on every tier tried, regardless of how each was
// loaded.
//
// The ratio's trend ACROSS tiers is NOT established, and an earlier version of this comment claimed it
// was. The measured base ratios run 3.0 / 5.0 / 5.1 / 3.75 at 4 / 8 / 16 / 32 vCPU, which is not
// monotonic in either direction - and the arms were not held at a comparable fraction of each tier's own
// ceiling (~70% / ~71% / ~93% / ~66%). The gap to the model also NARROWS with distance on every tier
// (32 vCPU: 30% under at base, 13% under at 1.5 ms), so the error is concentrated at short RTT - which is
// where same-zone deployments actually sit (0.05-0.96 ms). Since s inflates as a tier approaches its ceiling, part of that
// spread measures how hard each was pushed rather than the tier itself. Comparing rows needs arms at
// matched utilisation, which no campaign has run.
//
// THE CURVE IS A STEP, NOT A RAMP, and it plateaus because two terms cancel. Duty cycle pushes the
// requirement UP with distance while the throughput the tier can deliver falls, so between 0.5 and 1 ms
// the multiplier holds flat on both tiers. A table that only ever rises with distance models one term.
//
// WHY 64/96/128 WERE LEFT MODELLED RATHER THAN CORRECTED. The model's error is concentrated at SMALL
// tiers and has already decayed by 32 vCPU - over-provisioning runs 4.1x / 3.4x / 2.8x / 1.4x / 1.2x /
// 1.4x at 1/2/4/8/16/32. Its remaining cells are 4.42 / 3.94 / 3.63 at 64/96/128, already inside the
// 3.0-5.1x band every measured tier occupies and inside the conventional Postgres 3-5x guidance; its RTT
// slope there (1.94x base to 2ms) matches what 16 and 32 vCPU measured (1.8x, 2.3x). Extrapolating the
// small-tier correction upward would push those cells BELOW 3x - outside anything measured, in the
// STARVING direction. A future sweep should start at 64 and re-check 96/128 only if 64 disagrees.
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
// The 0.75 / 1.25 / 1.75 buckets are interpolated between measured neighbours, not measured.
//
// THE RTT AXIS INTERPOLATES, THE TIER AXIS ROUNDS UP. The requirement is smooth in RTT, so a bucket
// boundary is an artifact of storing a curve as a table and a 0.75ms path lands halfway between the 0.50
// and 1.00 columns. It is NOT smooth in vCPUs - B* is a power law and the rows are an octave apart - so a
// 24-vCPU shard takes the 32-vCPU row, under-connecting rather than guessing between two curves.
// An unprobed shard (rttMs <= 0) lands in the first column: a failed probe must not inflate a pool.
//
// TWO CELLS SIT CLOSE TO A MEASURED FAILURE and are the ones a re-measurement should check first:
// 8 vCPU at 0.25ms derives 69, and the one 8-vCPU arm that ever collapsed did so at M=70 (bracketed by
// healthy arms at M=50 and M=90, n=1); 1 vCPU derives 14 against a peak at M=16 and a collapse from M=32.
//
// DO NOT READ A LARGE-RTT CELL AS AN OVER-CONNECTION WITHOUT APPLYING THE DUTY CYCLE. The 2.00ms cell for
// 32 vCPU is 398 connections against a collapse observed at 430, but only ~41% are inside the database at
// once - ~163 backends, fewer than the 183 the healthy knee carried. Any cap belongs on backends or on
// the operator's configured max_connections, never on a connection count read without its RTT.
//
// This is a lookup on declared facts and one probe taken at Startup, not a controller: it reads nothing
// that moves while the engine runs, converges on nothing, and cannot oscillate. Do not grow it into one.
func connsPerVCPUFor(vcpus int, rttMs float64) float64 {
	row := poolRatio[len(poolRatio)-1] // larger than every tabulated tier: take the smallest ratio
	for _, r := range poolRatio {
		if vcpus <= r.vcpus {
			row = r
			break
		}
	}
	// Linear interpolation between the two bracketing buckets, clamped at both ends. The buckets are
	// unevenly spaced, so the position cannot be computed arithmetically the way an even grid allows.
	// The margin rides on the result, so the table itself stays a record of what was measured.
	if rttMs <= poolRTTBuckets[0] {
		return row.ratios[0] * poolSafetyMargin
	}
	for i := 1; i < len(poolRTTBuckets); i++ {
		if rttMs <= poolRTTBuckets[i] {
			f := (rttMs - poolRTTBuckets[i-1]) / (poolRTTBuckets[i] - poolRTTBuckets[i-1])
			return (row.ratios[i-1] + f*(row.ratios[i]-row.ratios[i-1])) * poolSafetyMargin
		}
	}
	return row.ratios[len(poolRTTBuckets)-1] * poolSafetyMargin
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
	rtts := maps.Clone(e.shardRTTMs)
	e.shardsLock.Unlock()
	postSplitConns := 0
	for _, idx := range e.db.Indices() {
		db, err := e.db.Shard(idx)
		if err != nil {
			continue
		}
		// Zero-value spec = the default shard's sizing; a missing RTT (an unprobed shard) falls to the
		// uncompensated bucket, which is the under-connecting direction.
		idle, open := shardPool(specs[idx], 0, observed[idx], rtts[idx])
		db.SetMaxOpenConns(open)
		db.SetMaxIdleConns(idle)
		if e.seams.Enabled() { // Enabled gates the assembled name and the boxed value in production
			e.seams.Variable(seamsJoin(VariablePoolIdle, strconv.Itoa(idx)), idle)
		}
		// The turnstile follows the pool for the same reason the cache and the worker ceiling do: it orders
		// access to the connections this replica actually holds, and the pool just changed. Resize moves the
		// available count by the DELTA, so a turn held by an in-flight worker is never handed out twice.
		if e.turnstiles != nil {
			e.turnstiles.Resize(idx, turnstilePassesPerConn*open)
		}
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
	// The RESIDENT worker count is deliberately NOT resized, because the crew shrinks ITSELF: a worker that
	// spent too little of its own recent wall clock holding a candidate retires on a coin flip, so the surplus
	// this smaller budget creates finds itself idle and goes, down to the floor Start was given. Pushing a new
	// resident count here would be a second, coarser actuator on a quantity that already self-corrects - and
	// one that would have to serialize against a shrink in flight. The cache and the ceiling below do not
	// self-correct, which is why they are re-derived and this is not.
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
		_, open := shardPool(specs[idx], override, e.replicasOn(idx), rttMs)
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
