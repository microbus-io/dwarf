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
	"database/sql"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// workerLoop pops candidates from the cache and executes them.
//
// It no longer nudges a refiller. Each shard's piston cycles on a fixed cadence of its own, so there is
// nothing to ring: the low-water signal Pop still returns has no consumer here, and the post-processStep
// nudge is covered by the next cycle. What replaced the nudge's liveness role - "the scan after a
// completion sees the freed slot" - is that the cycle is unconditional, plus the doorbell's Offer, which
// now admits into an empty partition so a chain's next step dispatches without waiting for a cycle at all.
func (e *Engine) workerLoop(ctx context.Context) {
	for {
		j, ok, _ := e.cache.Pop()
		if !ok {
			return
		}
		e.logger.DebugContext(ctx, "Worker popped", "stepID", j.StepID, "shard", j.Shard)
		// A sibling worker in this process reserved this step within the last ~second - its claim CAS may
		// still be in flight, or may have committed already (the reservation deliberately outlives the CAS
		// to span selection -> pop; see internal/claimstracker). Either way the piston re-selected it
		// because an uncommitted claim reads `pending`, so issuing our own claim would cost a round trip to
		// be told we lost. Popping the next candidate costs nothing. Checked HERE rather than inside
		// processStep so a skip does not pay for its setup, and the cache has already removed the entry.
		if !e.claims.TryClaim(j.Shard, j.StepID) {
			e.metricStepClaimPreempted(ctx)
			continue
		}
		err := errors.CatchPanic(func() error {
			return e.processStep(ctx, j.StepID, j.Shard)
		})
		if err != nil {
			e.logger.ErrorContext(ctx, "Failed to process step", "stepID", j.StepID, "error", err)
		}
	}
}

// timerLoop sleeps until nextPoll, then polls.
func (e *Engine) timerLoop(ctx context.Context) {
	for {
		e.nextPollLock.Lock()
		deadline := e.nextPoll
		e.nextPollLock.Unlock()

		delay := max(0, min(time.Until(deadline), maxPollInterval))

		select {
		case <-e.timerStop:
			return
		case <-time.After(delay):
		case <-e.wakeTimer:
			continue
		}

		e.pollPendingSteps(ctx)
	}
}

// pollPendingSteps recovers expired-lease steps and sizes the wake timer.
func (e *Engine) pollPendingSteps(ctx context.Context) {
	var mu sync.Mutex
	var nearestDelay time.Duration = -1
	var sizingErr bool // a sizing query failed (e.g. transient DB error); re-poll soon, don't sleep maxPollInterval

	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		res, err := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, updated_at=NOW_UTC() WHERE status='"+workflow.StatusRunning+"' AND parked=0 AND lease_expires<=NOW_UTC()",
			workflow.StatusPending,
		)
		if err == nil {
			if recovered, _ := res.RowsAffected(); recovered > 0 {
				e.metricStepsRecovered(ctx, int(recovered))
			}
		}
		shardErr := err != nil

		// FaultPollSizingErr simulates a transient sizing-query failure so the test proves the poll clamps
		// nextPoll to pollErrorRetryInterval (re-poll soon) instead of sleeping maxPollInterval on an
		// unknown backlog.
		if e.seams.IsFault(FaultPollSizingErr) {
			shardErr = true
		}

		var nearestMs sql.NullFloat64
		if err := db.QueryRowContext(ctx,
			"SELECT DATE_DIFF_MILLIS(MIN(not_before), NOW_UTC()) FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before>NOW_UTC() AND not_before<=DATE_ADD_MILLIS(NOW_UTC(), ?) AND lease_expires<=NOW_UTC()",
			maxPollInterval.Milliseconds(),
		).Scan(&nearestMs); err != nil && err != sql.ErrNoRows {
			shardErr = true
		}
		var shardNearestDelay time.Duration = -1
		if nearestMs.Valid && nearestMs.Float64 > 0 {
			shardNearestDelay = time.Duration(nearestMs.Float64 * float64(time.Millisecond))
		}

		// There is deliberately NO due-backlog existence probe here. It used to cap nextPoll at
		// backlogPollInterval (1 minute) so an idle replica that missed a doorbell still re-scanned, and
		// it cost a whole round-trip per shard per poll to set that one cap. Each shard's piston now scans
		// unconditionally on its own cadence, which covers the same case per shard far sooner and with one
		// band scan instead of this probe plus the poll's other three statements. Do not reintroduce it:
		// this poll is nudged sub-second under load, so anything added here is paid on every nudge.

		// Wake at the soonest future lease expiry of a running step so crash-recovery
		// of a worker that died holding the lease happens promptly, rather than waiting
		// for the next maxPollInterval sweep.
		var leaseMs sql.NullFloat64
		if err := db.QueryRowContext(ctx,
			"SELECT DATE_DIFF_MILLIS(MIN(lease_expires), NOW_UTC()) FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusRunning+"' AND parked=0 AND lease_expires>NOW_UTC() AND lease_expires<=DATE_ADD_MILLIS(NOW_UTC(), ?)",
			maxPollInterval.Milliseconds(),
		).Scan(&leaseMs); err != nil && err != sql.ErrNoRows {
			shardErr = true
		}
		if leaseMs.Valid && leaseMs.Float64 > 0 {
			leaseDelay := time.Duration(leaseMs.Float64 * float64(time.Millisecond))
			if shardNearestDelay < 0 || leaseDelay < shardNearestDelay {
				shardNearestDelay = leaseDelay
			}
		}

		mu.Lock()
		if shardNearestDelay >= 0 && (nearestDelay < 0 || shardNearestDelay < nearestDelay) {
			nearestDelay = shardNearestDelay
		}
		if shardErr {
			sizingErr = true
		}
		mu.Unlock()
		return nil
	})

	// A failed sizing query (typically a transient DB error such as a momentary connection-limit
	// rejection) leaves the backlog unknown for that shard. Treating "unknown" as "nothing pending"
	// would park the timer for maxPollInterval (minutes) while a due step sits undispatched - the
	// wedge a swallowed error used to cause. Re-poll soon instead so the doorbell fires again once
	// the blip clears; sequel already waits out a brief connection-limit rejection underneath us, so
	// reaching here means it outlasted that window and a prompt re-poll is the right recovery.
	if sizingErr && (nearestDelay < 0 || nearestDelay > pollErrorRetryInterval) {
		nearestDelay = pollErrorRetryInterval
	}

	now := time.Now()
	var proposed time.Time
	if nearestDelay >= 0 {
		proposed = now.Add(nearestDelay)
	} else {
		proposed = now.Add(maxPollInterval)
	}
	e.nextPollLock.Lock()
	if e.nextPoll.Before(now) || proposed.Before(e.nextPoll) {
		e.nextPoll = proposed
	}
	e.nextPollLock.Unlock()
}

// Refill SCAN RATE. This is the supply control for every shard's piston: the period of its cycle, start
// of scan to start of scan. It is derived from static configuration and then FIXED - deliberately not
// adaptive. Two adaptive designs were built and measured on a 6-shard rig, and both are recorded here
// because both keep re-suggesting themselves:
//
//   - An adaptive fetch DEPTH (size the batch from observed demand) was INERT: swept across margins
//     of 1.25/1.5/2 the batch moved 179 -> 173 -> 190, because the batch is set by the available
//     backlog and this shard's slice of the plan, never by a target.
//   - A DERIVED interval (T = (capacity/N)/c / 2, from the measured drain rate c) was actively
//     HARMFUL: same scan count and same batch size as a fixed 150ms, but ~1,000x the discard (38-63k
//     vs 47-468) and a 2.4x worse p99. It OSCILLATES, and unavoidably so: supply is set from measured
//     consumption, but consumption is min(demand, supply), so the actuation contaminates its own
//     measurement. Over-supply -> discard -> consumed reads low -> interval grows -> buffer runs dry
//     -> consumed reads high -> interval shrinks. High discard AND high p99 together is the signature.
//
// So the rate is a constant, and the derivation below is how an OPERATOR picks it, not something the
// engine recomputes at runtime.
//
// WHY A RATE LIMIT AT ALL. A piston cycles unconditionally, so without a period it runs at a 100% duty
// cycle - measured, back when a trigger re-armed by every completed step produced exactly that: every
// refiller scanning back to back for a whole 60s window. The merged pass was accidentally self-limiting
// because its straggler wait made it slow; deleting the barrier made each pass fast and the loop hot,
// raising phase-1 scan load 3.4x. Phase 1 costs per DUE ROW regardless of how many rows the cycle then
// fetches, which is why sizing the batch can never substitute for scanning less often.
//
// HOW TO PICK IT. Workers drain a partition at c candidates/sec and a pass hands it at most its share
// of the cache, capacity/N, so covering the gap between passes requires
//
//	T <= (capacity/N) / c
//
// Half that leaves 2x supply headroom. On the measured rig (capacity 4608, 6 shards, ~17k steps/s)
// the ceiling is ~271ms and half of it ~136ms; 150ms measured +18% throughput AND a 10% BETTER tail
// than the barriered build, both resolved. 300ms starved exactly as the bound predicts (supply B/T
// matched its degraded throughput to within 1%), and 0ms - unlimited - cost 8% candidate churn and a
// 77% worse p99 while buying no throughput.
//
// WHAT THE VALUE DOES **NOT** HAVE TO COVER: a drained buffer on a chain's next step. The doorbell's
// Offer admits into an empty partition, so a sequential hop dispatches without waiting for a cycle at
// all - which is what lets this be a steady period rather than a latency budget.
const (
	// refillSupplyHeadroom is the supply margin over the sustained drain, chosen for MAXIMUM THROUGHPUT.
	// 2.0 is the measured optimum: on a database whose disk is not the bottleneck, throughput peaks at a
	// derived floor of ~140ms (this headroom) and falls off ~15% by ~260ms (headroom 1.1) - because near
	// the supply ceiling ordinary drain-rate jitter briefly empties the buffer and stalls workers, and a
	// ~2x buffer absorbs that jitter where a ~1.1x one does not.
	//
	// Do NOT re-derive this from "waste" (discarded/selected). Waste looked like the tuning lever on an
	// IOPS-throttled disk, where slow dispatch piled up a deep pending backlog and the refiller
	// over-supplied it (waste ran 25-50%, and it seemed to trade against throughput). That was a disk
	// artifact: on a healthy disk steps dispatch fast, the pending backlog stays shallow, the refiller
	// supplies close to consumption, and waste is ~2% across the whole good interval range - so waste is
	// nearly flat and does not distinguish the optimum. Throughput does. (Confirmed after a 1TB / 16-vCPU
	// single-shard sweep showed throughput bimodality collapse from ~2x to ~4%, making the peak
	// resolvable at ~110-150ms with ~2% waste.)
	refillSupplyHeadroom = 2.0
	// sustainedDrainPerVCPU is the MEASURED sustained per-shard drain in steps/s/vCPU: the per-connection
	// rate x connsPerVCPU. The measured per-connection rate is ~120 steps/s, roughly constant across
	// connection counts, instance sizes, and backlog volumes - which is what lets one constant stand in for
	// it - so 120 x connsPerVCPU(6) = 720. With headroom 2.0 this derives a 96/(2*720) ~= 67ms interval (see
	// deriveRefillInterval). NOT capacityWeight's 450, the PEAK placement ceiling: placement wants the peak,
	// the cycle period wants the SUSTAINED rate, and conflating them undershoots the drain and overshoots
	// the period (the earlier 340 - ~57/conn - put it at 141ms, a starved regime giving ~half the throughput
	// of the good band at high connection counts).
	sustainedDrainPerVCPU = 720
	// refillIntervalCap bounds priority latency - it is the only thing that does, so it is load-bearing.
	//
	// The arriving high-priority step itself does not wait: Offer HEAD-INSERTS a strictly-better band, so
	// it is popped next. What waits out the interval is (a) the rest of an arriving burst, since only one
	// pioneer is admitted per band-opening, and (b) the cross-shard publish - this shard must rescan before
	// its new band reaches the planner, and peers plan on their own next cycle, so a peer can serve
	// worse-band work for up to two intervals. Both are bounded by this cap and are ~67ms in the derived
	// path. Priority ORDER is never inverted regardless: a cycle always plans the global minimum band, so
	// however slowly a shard scans, better work is still selected first.
	refillIntervalCap = 1 * time.Second
)

// There is deliberately NO minimum here, and there used to be (refillScanFloorMin, 20ms). The fuse it
// provided - never scan one shard twice in quick succession, however the formula's inputs degenerate -
// now belongs to the pipeline's MinGap, which enforces the same 20ms as a gap between the END of one
// cycle and the START of the next. That is the stronger form: a floor measured start-to-start cannot
// bound a cycle that outruns it, which is exactly the deep-backlog case the fuse exists for.

// deriveRefillInterval computes ONE shard's cycle period from static configuration - capacity, that shard's
// pool and declared vCPUs, and the observed replica count. It is arithmetic over values known at
// Startup, NOT a controller: nothing here reads an observed rate, which is the distinction that matters,
// because the version that DID (interval set from measured consumption) oscillated badly and was removed.
//
//	bufferShare = capacity/N        the most one pass can hand this partition
//	drain       = sustainedDrainPerVCPU * min(poolConns/connsPerVCPU, vCPUs/R)   this replica's drain
//	T           = bufferShare / (headroom * drain)
//
// The drain is bounded by the TIGHTER of two channels, because sustained throughput cannot exceed
// either: the connection pool (this replica can only push poolConns/connsPerVCPU vCPUs of work through
// its pool) or the database CPU (the shard's vCPUs, split across the fleet). In the DERIVED path the two
// are equal by construction (pool = connsPerVCPU*vCPUs/R, so poolConns/connsPerVCPU == vCPUs/R) and the
// min is a no-op - it bites only when an operator pins the pool with SetMaxOpenConns *independently* of
// the declared vCPUs, which is exactly the footgun it protects against:
//   - a large pinned pool with vCPUs left undeclared no longer derives its drain from the default 2
//     vCPUs (which made the buffer/drain terms disagree and overshot the period to the 1s cap, starving
//     the refiller - the rig's 20-80s fan-out latency);
//   - a small pooler-capped pool (many vCPUs, few connections) no longer over-scans on a drain the pool
//     cannot actually sustain.
//
// vCPUs <= 0 means undeclared: the CPU ceiling is unknown, so the drain falls to the connection channel
// alone (the best estimate available, erring toward over-scan rather than starvation).
//
// Substituting the engine's own constants makes the configuration terms CANCEL in the derived path -
// bufferShare is 2*8*6*vCPUs/R = 96*vCPUs/R and drain is 720*vCPUs/R, so T = 96/(2*720) ~= 67ms at any
// vCPU count or replica count. So it evaluates to a CONSTANT (~67ms) there; the reason to keep it a
// formula rather than hardcode 67ms is that bufferShare tracks the cache-sizing constants (connsPerVCPU,
// workersPerConnBudget, the 2x cache), so a change to worker/cache sizing rescales the period
// automatically. (Nearly shipped: campaign 11 found workersPerConnBudget overshoots ~4x. Had it been
// "corrected", capacity would have fallen 4x and a hardcoded period would have exceeded what the buffer
// could cover - the measured i300 starvation mode. The overshoot was kept as throughput-neutral, but
// the near-miss is the argument for computing this rather than pinning a number.)
//
// The cap governs where the cancellation breaks - workersDispatch's max(64, ...) floor at small or
// high-R configurations - and is a lost-signal backstop (see the constant).
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
	// max(1, ...): a cache SMALLER than the shard count integer-divides to zero, and zero reaches
	// deriveRefillInterval's degenerate guard, which answers with the 1s cap - the slowest period there
	// is. That is exactly backwards. A tiny cache drains in an instant and wants frequent scans; the case
	// is a small cache, not an unknown one. Left at zero it strands a small-cache configuration on
	// second-long scan intervals, which reads as a wedged fleet rather than a mis-tuned one.
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
			continue
		}
		// Pass the shard's ACTUAL pool (shardPool resolves the SetMaxOpenConns pin) and its RAW declared
		// vCPUs (0 = undeclared), so the drain is bounded by whichever channel is real - the pinned pool,
		// not a defaulted vCPU count. An unconfigured shard's zero-value spec falls to the conn channel.
		spec := specs[idx]
		_, pool := shardPool(spec, pinned, replicas)
		p.SetInterval(deriveRefillInterval(share, spec.VirtualCPUs, pool, replicas))
	}
}
