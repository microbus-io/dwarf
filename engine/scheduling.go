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
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// workerLoop pops candidates from the cache and executes them. Refill nudges are shard-targeted: the
// low-water signal names the partition that drained (the popped job's shard), and the post-processStep
// nudge names the shard holding the freed slot and any successor the step just inserted (same-flow
// shard affinity) - preserving the liveness guarantee's "post-completion scan sees the freed slot"
// property per shard.
func (e *Engine) workerLoop(ctx context.Context) {
	for {
		j, ok, needRefill := e.cache.Pop()
		if needRefill {
			e.requestRefill(j.Shard)
		}
		if !ok {
			return
		}
		e.logger.DebugContext(ctx, "Worker popped", "stepID", j.StepID, "shard", j.Shard, "needRefill", needRefill)
		// A sibling worker in this process already has a claim CAS in flight on this step - the refiller
		// re-selected it because the uncommitted claim still reads `pending`. Popping the next candidate
		// costs nothing; issuing the claim would cost a round trip to be told we lost. Checked HERE rather
		// than inside processStep so a skip does not pay for its setup, and the cache has already removed
		// the entry, so nothing re-pops it this generation.
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
		e.requestRefill(j.Shard)
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

// refillOutcome is what one shard's refill pass reports back to its refillerLoop, which turns it
// into pacing, a band back-off, or nothing.
type refillOutcome int

const (
	// refillIdle: nothing due on this shard, a partial plan (light load), or an error path - consume
	// the next trigger immediately.
	refillIdle refillOutcome = iota
	// refillFull: the GLOBAL plan reached capacity - the deep-backlog signal - so the loop pauses
	// refillPace before the next pass. The signal is deliberately the plan, never "my slice was big":
	// a partial plan means the whole backlog fits in the cache, where pacing adds dispatch latency
	// for nothing.
	refillFull
	// refillAboveBand: this shard HAS due work, but above the global-minimum band, so its slice of
	// the plan is empty by strict priority. Distinct from refillIdle ("nothing due") because parking
	// on the doorbell would spin hot: the always-armed trigger re-runs the scan immediately, at full
	// rate, producing nothing - what releases this shard is the global band rising, which is observed
	// only through the census. The loop backs off and watches for that (awaitBandRelease).
	refillAboveBand
	// refillStarved: this shard is AT the global band with due work, but a capacity-bound plan gave
	// every slot to shards holding more (or older) of the planned keys. Like refillAboveBand this is
	// an empty slice, but the release condition is different - the band is already right, what must
	// change is the competitors DRAINING - so it gets its own short timed retry (awaitStarvedRetry)
	// rather than the census watch.
	//
	// It must never park on the doorbell, and that is the whole reason this outcome exists. Every
	// trigger-arming site is shard-LOCAL (a pop, a completed step, a doorbell for a step on this
	// shard), so a starved shard with an empty partition and nothing in flight arms nothing and
	// generates nothing: it would sleep until pollPendingSteps' fleet-wide backstop, up to
	// backlogPollInterval (1 MINUTE), while its due work sat there - and once the competitors drained,
	// every worker would block in Pop with due steps present. Its census entry would also age past
	// the TTL meanwhile, making its keys invisible to peers' plans, so it could not win slots even in
	// principle. The retry keeps both the dispatch latency and the entry fresh.
	refillStarved
)

// refillerLoop runs one SHARD's selection scans, one per trigger - there is one such loop per shard,
// each with its own single-slot trigger, and no barrier anywhere between them (the point of the
// decoupling: a merged pass returned when the slowest shard did, an order-statistics tax measured at
// 2.02x by 6 shards, on the path that is the engine's throughput ceiling).
//
// After a pass whose global plan reached capacity - the deep-backlog regime - it pauses refillPace
// before consuming the next trigger. Unpaced, the always-armed trigger (workers re-arm it after every
// step on this shard) makes the refiller run back-to-back full-backlog scans, and each wholesale
// partition replace re-delivers steps whose claim CAS is still in flight on another worker. The gate
// and the bound are both load-bearing:
//   - Paced ONLY on a full plan: a partial plan means the backlog fits in the cache (light load),
//     where pacing would add dispatch latency for nothing - a sequential flow's next step must not
//     wait out a pace interval. Light-load refills stay immediate.
//   - refillPace must stay well under the cache's drain time (capacity = 2x workers, each candidate
//     occupying a worker for a full step), or the refillers become the throughput ceiling
//     (<= capacity/pace pops per second): over-pacing was measured to invert the gain. At the default
//     pace a full cache outlives the pause by a wide margin, and the armed trigger means the next scan
//     starts the moment the pause ends - a drained-early partition waits at most the remainder.
func (e *Engine) refillerLoop(ctx context.Context, shard int) {
	trigger := e.refillTriggers[shard]
	for {
		select {
		case <-e.refillStop:
			return
		case <-trigger:
			passStart := time.Now()
			var outcome refillOutcome
			err := errors.CatchPanic(func() error {
				outcome = e.runShardRefill(ctx, shard)
				return nil
			})
			if err != nil {
				e.logger.ErrorContext(ctx, "Refilling candidate cache", "shard", shard, "error", err)
			}
			// Hold off until the scan floor has elapsed, measured from the pass START so a slow pass
			// pays for itself rather than stacking its duration on top of the floor. An URGENT nudge
			// (empty partition, or a better band head-inserted) cuts the wait short - which is what
			// makes this a debounce rather than a timer that has to be short enough to cover an event.
			if wait := e.refillState[shard].wait(passStart); wait > 0 {
				select {
				case <-e.refillStop:
					return
				case <-e.refillDemand[shard]:
				case <-time.After(wait):
				}
			}
			switch outcome {
			case refillFull:
				if e.refillPace > 0 {
					select {
					case <-e.refillStop:
						return
					case <-time.After(e.refillPace):
					}
				}
			case refillAboveBand:
				if !e.awaitBandRelease(shard, trigger) {
					return
				}
				e.requestRefill(shard)
			case refillStarved:
				if !e.awaitStarvedRetry(trigger) {
					return
				}
				e.requestRefill(shard)
			}
		}
	}
}

// awaitBandRelease parks an above-band refiller until something suggests its shard can dispatch
// again, then reports true (false means the engine is stopping). The release signals, in order of
// arrival: the shard's own trigger (a doorbell - new local work may have opened a better band); the
// in-memory census showing the global band risen to this shard's own band (the band-holding shard
// drained and published); or this shard's own census entry aging past censusRefreshInterval, which
// forces a real rescan so the entry stays fresh for the fleet (a stale entry would otherwise age out
// of peers' snapshots and take this shard's band claim with it). The ticks between checks are
// in-memory only - no query - so the "spins hot" failure (a full-rate scan producing nothing, with
// the pace gate never engaging because the batch is never full) cannot occur; the database is
// touched at most once per censusRefreshInterval while parked.
func (e *Engine) awaitBandRelease(shard int, trigger chan struct{}) bool {
	tick := e.refillPace
	if tick <= 0 {
		tick = 20 * time.Millisecond
	}
	for {
		select {
		case <-e.refillStop:
			return false
		case <-trigger:
			return true
		case <-time.After(tick):
			e.censusLock.Lock()
			own := e.census[shard]
			e.censusLock.Unlock()
			if own == nil || time.Since(own.at) >= censusRefreshInterval {
				return true
			}
			entries := e.censusSnapshot()
			globalBand := math.MaxInt
			for _, ce := range entries {
				if len(ce.census.rows) > 0 && ce.census.band < globalBand {
					globalBand = ce.census.band
				}
			}
			if globalBand >= own.band {
				return true
			}
		}
	}
}

// awaitStarvedRetry backs a starved (at-band, zero-slot) refiller off for one pace interval, then
// reports true so it rescans (false means the engine is stopping); its own trigger releases it early.
//
// One short tick, no state, no condition to get wrong - what this shard is waiting for is simply the
// competitors draining, which the very next plan will reflect. The interval is the pace, so a starved
// shard scans at the same cadence a busy one does: that is not extra load but a RESTORATION of the
// pre-decoupling baseline, where the merged pass scanned every shard on every cycle regardless of
// who won the slots. Keeping it scanning is also what keeps its census entry inside the TTL, and
// therefore its keys inside its peers' plans - a shard that stops scanning stops being planned for,
// which is the trap the doorbell park fell into.
func (e *Engine) awaitStarvedRetry(trigger chan struct{}) bool {
	tick := e.refillPace
	if tick <= 0 {
		tick = 20 * time.Millisecond
	}
	select {
	case <-e.refillStop:
		return false
	case <-trigger:
		return true
	case <-time.After(tick):
		return true
	}
}

// pollPendingSteps recovers expired-lease steps, sizes the wake timer, and rings the doorbell.
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

		// faultPollSizingErr simulates a transient sizing-query failure so the test proves the poll clamps
		// nextPoll to pollErrorRetryInterval (re-poll soon) instead of sleeping maxPollInterval on an
		// unknown backlog.
		if e.seams.IsFault(faultPollSizingErr) {
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

		// Existence probe: is any due pending step waiting? The ORDER BY is not for ordering (any match
		// suffices) - it is REQUIRED because LIMIT_OFFSET compiles to OFFSET/FETCH on SQL Server, a
		// syntax error without an ORDER BY. Do not remove it to "optimize" the existence check.
		var dueExists sql.NullInt64
		err = db.QueryRowContext(ctx,
			"SELECT 1 FROM dwarf_steps WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC() ORDER BY step_id LIMIT_OFFSET(1, 0)",
		).Scan(&dueExists)
		if err != nil && err != sql.ErrNoRows {
			shardErr = true
		} else if err == nil && (shardNearestDelay < 0 || shardNearestDelay > backlogPollInterval) {
			shardNearestDelay = backlogPollInterval
		}

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

	// The backstop cannot know which shard's backlog it observed, so it is the one nudge that rings
	// every shard's refiller.
	e.requestRefillAll()
}

// --- The census: each shard's phase-1 result, shared across the per-shard refillers ---

// The census is the shared map the barrier used to be: phase 1's per-shard query output already IS
// the per-key census, and retaining it costs one struct and zero extra queries. Each refiller
// publishes its own shard's entry after every scan and plans phase 2 GLOBALLY over a snapshot of all
// live entries - the exact same cross-shard merge + weighted pick the merged pass ran - then fetches
// only its own shard's slice of the plan (phase 3). Fairness semantics are exactly the barrier's; the
// only degradation is that each refiller sees the OTHER shards up to one of their passes stale, which
// fairness (slowly-varying) tolerates and dispatch (per-shard, always fresh) never sees.

// censusTTLFloor is the minimum age at which a shard's census entry is dropped from snapshots. A dead
// shard's stale entry would otherwise poison the global band forever: its last entry says band 1, so
// every other shard computes globalBand=1, finds nothing there, and plans nothing - the fleet wedges
// on a shard that is not there. The TTL scales with the slowest observed pass (a deep backlog scan
// can legitimately take seconds) so a slow shard is not mistaken for a dead one.
const (
	censusTTLFloor        = 5 * time.Second
	censusTTLPassMultiple = 8
	censusRefreshInterval = time.Second // an above-band shard rescans at least this often to keep its entry live
)

// shardCensus is one shard's published phase-1 result: its minimum due band and one row per fairness
// key at that band. Entries are immutable once published - a refiller REPLACES its shard's entry,
// never mutates it - so snapshot readers need no copy.
type shardCensus struct {
	band  int            // the shard's minimum due band; math.MaxInt when nothing is due
	rows  []censusRow    // one row per fairness key at band, in scan order (stable for planBatch)
	byKey map[string]int // index into rows, for the slice rule's per-key lookups
	at    time.Time      // publish time; entries past the TTL are dropped from snapshots
}

// censusRow is one fairness key's aggregate on one shard: its count of due steps, and the age and
// fairness weight of its OLDEST due step.
type censusRow struct {
	key    string
	weight float64
	ageMs  float64
	count  int
}

// censusShard pairs a census entry with its shard for snapshot iteration in shard-ordinal order
// (map iteration is not stable, and planBatch relies on a stable key order).
type censusShard struct {
	shard  int
	census *shardCensus
}

// publishCensus replaces this shard's census entry with a fresh, timestamped one.
func (e *Engine) publishCensus(shard int, band int, rows []censusRow) {
	sc := &shardCensus{band: band, rows: rows, at: time.Now()}
	if len(rows) > 0 {
		sc.byKey = make(map[string]int, len(rows))
		for i, r := range rows {
			sc.byKey[r.key] = i
		}
	}
	e.censusLock.Lock()
	e.census[shard] = sc
	e.censusLock.Unlock()
}

// censusTTL is the age past which an entry is considered dead (see censusTTLFloor).
func (e *Engine) censusTTL() time.Duration {
	ttl := censusTTLPassMultiple * time.Duration(e.maxRefillPassNs.Load())
	if ttl < censusTTLFloor {
		ttl = censusTTLFloor
	}
	return ttl
}

// censusSnapshot returns the live (non-TTL'd) census entries sorted by shard ordinal. The lock is
// held for the map copy only - never across a query or a plan computation, which would re-couple the
// refill cycles and hand the decoupling win straight back.
func (e *Engine) censusSnapshot() []censusShard {
	ttl := e.censusTTL()
	now := time.Now()
	e.censusLock.Lock()
	entries := make([]censusShard, 0, len(e.census))
	for s, sc := range e.census {
		if now.Sub(sc.at) > ttl {
			continue
		}
		entries = append(entries, censusShard{shard: s, census: sc})
	}
	e.censusLock.Unlock()
	sort.Slice(entries, func(a, b int) bool { return entries[a].shard < entries[b].shard })
	return entries
}

// mergeCensus computes the globally-minimum due band and the merged per-key aggregates at that band -
// the same merge the barriered scanBandKeys ran, over the census instead of a synchronized fan-out. A
// tenant's steps can span shards; sum the counts, and keep the globally-oldest step's weight (max age
// wins). Shards at a worse band contribute nothing (lower bands materialize only once the higher
// drains). Shard-ordinal iteration + insertion order preserves a stable key order for planBatch's
// deterministic iteration.
func mergeCensus(entries []censusShard) (band int, keys []bandKeyAgg) {
	band = math.MaxInt
	for _, ce := range entries {
		if len(ce.census.rows) > 0 && ce.census.band < band {
			band = ce.census.band
		}
	}
	if band == math.MaxInt {
		return band, nil
	}
	byKey := map[string]*bandKeyAgg{}
	var order []string
	for _, ce := range entries {
		if ce.census.band != band {
			continue
		}
		for _, r := range ce.census.rows {
			agg := byKey[r.key]
			if agg == nil {
				byKey[r.key] = &bandKeyAgg{key: r.key, weight: r.weight, ageMs: r.ageMs, count: r.count}
				order = append(order, r.key)
				continue
			}
			agg.count += r.count
			if r.ageMs > agg.ageMs {
				agg.ageMs = r.ageMs
				agg.weight = r.weight
			}
		}
	}
	keys = make([]bandKeyAgg, 0, len(order))
	for _, k := range order {
		keys = append(keys, *byKey[k])
	}
	return band, keys
}

// bandKeyAgg is one fairness key's cross-shard aggregate at the global band: its count of due steps,
// and the age and fairness weight of its OLDEST due step. The picker keys a tenant's weight off its
// oldest step (so a tenant cannot self-promote by queueing newer high-weight tasks), and the age both
// fixes that weight during the census merge (the globally-oldest step wins) and steers the slice
// rule's first slot; the actual oldest-first ordering happens on the fetched steps in
// fetchShardBandSteps.
type bandKeyAgg struct {
	key    string
	weight float64
	ageMs  float64
	count  int
}

// scanShardBandKeys returns ONE shard's minimum due priority band and, for every fairness key at that
// band, a single aggregate row (count + oldest step's age/weight). This is phase 1 of a three-phase
// refill; it returns O(distinct keys) rows, NOT O(backlog). Phase 2 (planBatch over the census merge)
// decides per-key demand from these aggregates and phase 3 (fetchShardBandSteps) fetches only the
// selected steps.
//
// The per-key count is CAPPED at the cache capacity (min(count, capacity)) rather than exact: it is
// computed as MAX(rn) under a `rn <= capacity` cut, not COUNT(*) OVER. The cap is lossless - planBatch
// can never assign one key more than the whole batch (capacity), so a count above capacity is
// indistinguishable from capacity - and it is what lets the scan stop touching a key's rows past
// capacity instead of scanning the whole partition to count it (the O(backlog) cost that made a
// single-key flood scan millions of rows every pass). See engine/CLAUDE.md "The scan is capped, not
// exact" for the cross-dialect rationale (why COUNT(*) OVER was O(backlog), why rn<=cap is the fix, and
// why the sibling phase-3 window was left alone).
// partitionPredicate restricts a selection scan to this replica's residue class of step_id, so R
// replicas sharing a database select DISJOINT candidates instead of racing for the same rows. It
// returns ("", nil) - selecting everything, exactly as before replicas were partitioned - whenever
// partitioning must not apply (a solo replica, or an ordinal this replica cannot determine).
//
// WHY IT IS NEEDED AT ALL. planBatch's key pick is independently random per replica, but within a key
// the fetch is deterministic (`ORDER BY created_at, step_id`), so every replica that picks a key
// fetches the SAME oldest rows and all but one lose the claim CAS. Measured: claim miss rose 25% -> 63%
// from R=1 to R=8 while throughput FELL 42%, because a lost claim is a full round trip that produces no
// work. Sizing the candidate cache down by R (poolsize.go) bounds how many candidates a replica holds
// but not WHICH, so it does not help; disjoint selection does.
//
// `step_id % R` is not sargable - the scan still walks the band and filters - so this reduces claims,
// not scan cost. That is the intended trade: the scan was never the contended resource.
//
// The residue class is a RESIDENCY, not a lock: the claim CAS remains the only thing that grants a
// step, so a stale or overlapping (R, ordinal) costs a lost claim, never correctness.
func (e *Engine) partitionPredicate() (string, []any) {
	replicas, ordinal, ok := e.observedPartition()
	if !ok {
		return "", nil
	}
	return " AND step_id % ? = ?", []any{replicas, ordinal}
}

func (e *Engine) scanShardBandKeys(ctx context.Context, shard int) (band int, rows []censusRow, err error) {
	// faultRefillScanErr simulates the band scan failing so the test proves runShardRefill logs and
	// shortens the next poll (re-scan soon) instead of refilling empty and idling the shard's workers.
	if e.seams.IsFault(faultRefillScanErr) {
		return math.MaxInt, nil, errors.New("injected fault: " + faultRefillScanErr)
	}
	db, err := e.db.Shard(shard)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	started := time.Now()
	defer func() { e.metricRefillQuery(ctx, shard, refillPhaseBandKeys, time.Since(started)) }()
	// One row per fairness key at this shard's minimum due band. rn numbers a key's due steps oldest-first;
	// MAX(rn) under the rn<=capacity cut is the capped count; the rn=1 row carries the oldest step's
	// age/weight (the weight the picker must use). All inner rows are filtered to the min band, so
	// MAX(priority) is that band. On Postgres the rn<=? cut becomes a run-condition early-stop; on the
	// other dialects it still avoids the extra COUNT(*) OVER aggregation pass (see engine/CLAUDE.md).
	// The partition filters the ROWS this replica censuses, but deliberately NOT the MIN(priority)
	// subquery: the band is a cluster-wide fact (strict priority is global), so mining it from one
	// replica's slice would let replicas disagree on which band is open. A replica holding nothing at
	// the global band therefore censuses zero rows and parks - correct, since its own work is at a worse
	// band that must not be served until the better one drains.
	part, partArgs := e.partitionPredicate()
	scanArgs := make([]any, 0, len(partArgs)+1)
	scanArgs = append(scanArgs, partArgs...)
	scanArgs = append(scanArgs, e.cache.Capacity())
	qrows, err := db.QueryContext(ctx,
		"SELECT fairness_key, MAX(rn) AS cnt,"+
			" MAX(CASE WHEN rn=1 THEN age_ms ELSE NULL END) AS age_ms,"+
			" MAX(CASE WHEN rn=1 THEN weight ELSE NULL END) AS weight,"+
			" MAX(priority) AS priority FROM ("+
			"SELECT fairness_key, priority,"+
			" DATE_DIFF_MILLIS(NOW_UTC(), created_at) AS age_ms,"+
			" fairness_weight AS weight,"+
			" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn"+
			" FROM dwarf_steps"+
			" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
			part+
			" AND priority=(SELECT MIN(priority) FROM dwarf_steps"+
			" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC())"+
			") t WHERE rn<=? GROUP BY fairness_key",
		scanArgs...,
	)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	defer qrows.Close()
	band = math.MaxInt
	for qrows.Next() {
		var r censusRow
		var prio int
		if err := qrows.Scan(&r.key, &r.count, &r.ageMs, &r.weight, &prio); err != nil {
			return 0, nil, errors.Trace(err)
		}
		if r.weight <= 0 {
			r.weight = 1
		}
		band = prio
		rows = append(rows, r)
	}
	if err := qrows.Err(); err != nil {
		return 0, nil, errors.Trace(err)
	}
	return band, rows, nil
}

// planBatch runs the weighted fairness pick over the band's per-key aggregates and returns the ordered
// sequence of keys to dispatch - one entry per selected step, at most capacity. Each round weighted-
// randomly picks a key (Efraimidis-Spirakis over the *keys*, re-rolled per step so a key's expected
// share is proportional to its weight and independent of backlog depth) and consumes one of its due
// steps, capped at the key's count. Every refiller rolls its OWN plan from its own census snapshot and
// executes only its shard's slice - the rolls are independent, which changes nothing in expectation
// and requires no cross-refiller coordination.
func planBatch(keys []bandKeyAgg, capacity int) []string {
	remaining := make([]int, len(keys))
	for i := range keys {
		remaining[i] = keys[i].count
	}
	plan := make([]string, 0, capacity)
	for len(plan) < capacity {
		best, bestScore := -1, -1.0
		for i := range keys {
			if remaining[i] <= 0 {
				continue
			}
			score := math.Pow(rand.Float64(), 1/keys[i].weight)
			if score > bestScore {
				bestScore = score
				best = i
			}
		}
		if best < 0 {
			break
		}
		plan = append(plan, keys[best].key)
		remaining[best]--
	}
	return plan
}

// sliceDemand applies the slice rule: it splits each planned key's demand across the shards whose
// census entries hold it, and returns this shard's per-key quota plus the full per-occurrence
// assignment (so the batch replay preserves the plan's fairness interleave).
//
// The split, per key: the FIRST slot goes to the shard whose entry holds the key's oldest age - that
// shard's fetch is oldest-first, so it leads with the globally-oldest step, preserving the
// oldest-first property the merged fetch gave and preventing unbounded intra-tenant starvation (a key
// with one old step on a quiet shard and a constantly-replenished backlog on a busy one would see a
// purely proportional split round the quiet shard to zero, pass after pass). The REMAINING slots
// split proportional to per-shard counts, largest-remainder rounding, shard-ordinal order on ties -
// deterministic, so two refillers replaying the same plan against the same snapshot agree on every
// assignment.
//
// Deliberately NOT: per-shard lotteries, weights scaled by shard presence, or capacity allocated by
// shard counts - the global plan already priced every key's share; this function only routes it.
func sliceDemand(plan []string, entries []censusShard, band int, shard int) (myQuota map[string]int, maxNeeded int, assign map[string][]int) {
	needed := map[string]int{}
	for _, k := range plan {
		needed[k]++
	}
	assign = make(map[string][]int, len(needed))
	myQuota = map[string]int{}
	for k, n := range needed {
		type holder struct {
			shard int
			count int
			age   float64
		}
		var holders []holder
		for _, ce := range entries {
			if ce.census.band != band {
				continue
			}
			if i, ok := ce.census.byKey[k]; ok {
				r := ce.census.rows[i]
				holders = append(holders, holder{shard: ce.shard, count: r.count, age: r.ageMs})
			}
		}
		if len(holders) == 0 {
			continue
		}
		// First slot: the oldest holder (entries arrive in shard-ordinal order, so a strict > keeps
		// the lower shard on age ties).
		oldest := 0
		for i := 1; i < len(holders); i++ {
			if holders[i].age > holders[oldest].age {
				oldest = i
			}
		}
		quota := make([]int, len(holders))
		quota[oldest] = 1
		avail := make([]int, len(holders))
		totalAvail := 0
		for i := range holders {
			avail[i] = max(0, holders[i].count-quota[i])
			totalAvail += avail[i]
		}
		rem := min(n-1, totalAvail) // counts can be stale; a shortfall self-corrects next pass
		if rem > 0 {
			assigned := 0
			base := make([]int, len(holders))
			for i := range holders {
				base[i] = rem * avail[i] / totalAvail
				assigned += base[i]
			}
			order := make([]int, len(holders))
			for i := range order {
				order[i] = i
			}
			sort.SliceStable(order, func(a, b int) bool {
				ra := rem * avail[order[a]] % totalAvail
				rb := rem * avail[order[b]] % totalAvail
				if ra != rb {
					return ra > rb
				}
				return order[a] < order[b]
			})
			for _, i := range order {
				if assigned >= rem {
					break
				}
				if base[i] < avail[i] {
					base[i]++
					assigned++
				}
			}
			for i := range holders {
				quota[i] += base[i]
			}
		}
		// The per-occurrence assignment: oldest holder first, then holders in ordinal order. The
		// interleave below the head is approximate by design - per-shard fetch keeps oldest-first
		// WITHIN each shard, and the head slot carries the globally-oldest.
		seq := make([]int, 0, n)
		seq = append(seq, holders[oldest].shard)
		for i := range holders {
			extra := quota[i]
			if i == oldest {
				extra--
			}
			for range extra {
				seq = append(seq, holders[i].shard)
			}
		}
		assign[k] = seq
		mine := 0
		for _, s := range seq {
			if s == shard {
				mine++
			}
		}
		if mine > 0 {
			myQuota[k] = mine
			if mine > maxNeeded {
				maxNeeded = mine
			}
		}
	}
	return myQuota, maxNeeded, assign
}

// fetchStep is a fetched candidate carrying the age used to order a key's steps oldest-first.
type fetchStep struct {
	stepID int
	shard  int
	ageMs  float64
}

// fetchShardBandSteps loads, per chosen fairness key, up to perKey of this ONE shard's oldest due
// steps at the given band, keyed and sorted oldest-first (the order the plan replay consumes). This
// is phase 3.
//
// perKey is a UNIFORM cap - the max per-key demand across this shard's slice, not each key's exact
// demand. That keeps the fetch a single IN-list query (an exact per-key cap would need a per-key
// VALUES/LATERAL join, non-trivial across the four SQL dialects). The cost is at most
// len(chosen)*perKey rows, and crucially BOTH factors are bounded by the cache capacity (at most
// capacity distinct keys can be chosen, since each contributes >=1 step; perKey <= capacity) - so the
// fetch is bounded by capacity^2 regardless of how many fairness keys exist. That independence from
// key cardinality is the whole point: at high cardinality perKey is ~1, so the fetch is ~capacity.
func (e *Engine) fetchShardBandSteps(ctx context.Context, shard int, band int, chosen []string, perKey int) (map[string][]fetchStep, error) {
	if len(chosen) == 0 || perKey <= 0 {
		return nil, nil
	}
	db, err := e.db.Shard(shard)
	if err != nil {
		return nil, errors.Trace(err)
	}
	started := time.Now()
	defer func() { e.metricRefillQuery(ctx, shard, refillPhaseFetchSteps, time.Since(started)) }()
	placeholders := strings.Repeat("?,", len(chosen)-1) + "?"
	// The SAME partition the census used. Phase 1 alone would not do: it only shapes the per-key counts,
	// while THIS query returns the step ids the workers claim - unfiltered, every replica would fetch the
	// same globally-oldest rows per key and collide exactly as before.
	part, partArgs := e.partitionPredicate()
	args := make([]any, 0, len(chosen)+len(partArgs)+2)
	args = append(args, band)
	for _, k := range chosen {
		args = append(args, k)
	}
	args = append(args, partArgs...)
	args = append(args, perKey)
	// Band is bound (priority=?), not the min-subquery: the plan committed to this band, and re-mining
	// here could pick a lower band that arrived between phases and mismatch the chosen keys. A binding
	// priority does not defeat the selection index (only a bound status would - status stays inlined).
	rows, err := db.QueryContext(ctx,
		"SELECT step_id, fairness_key, age_ms FROM ("+
			"SELECT step_id, fairness_key,"+
			" DATE_DIFF_MILLIS(NOW_UTC(), created_at) AS age_ms,"+
			" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn"+
			" FROM dwarf_steps"+
			" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
			" AND priority=? AND fairness_key IN ("+placeholders+")"+
			part+
			") t WHERE rn<=? ORDER BY step_id",
		args...,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	byKey := map[string][]fetchStep{}
	for rows.Next() {
		var fs fetchStep
		var key string
		if err := rows.Scan(&fs.stepID, &key, &fs.ageMs); err != nil {
			return nil, errors.Trace(err)
		}
		fs.shard = shard
		byKey[key] = append(byKey[key], fs)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	// Sort each key oldest-first: age desc, then step_id for determinism.
	for k := range byKey {
		list := byKey[k]
		sort.Slice(list, func(a, b int) bool {
			x, y := list[a], list[b]
			if x.ageMs != y.ageMs {
				return x.ageMs > y.ageMs
			}
			return x.stepID < y.stepID
		})
	}
	return byKey, nil
}

// runShardRefill replaces ONE shard's cache partition with a fresh priority+fairness batch, in three
// phases: scanShardBandKeys (this shard's per-key aggregates, published to the census), a GLOBAL
// planBatch over the census merge (the same weighted pick the barriered pass ran - see "The census"
// above), and fetchShardBandSteps (fetch only this shard's slice of the plan). It reports the outcome
// the refillerLoop paces or backs off on.
func (e *Engine) runShardRefill(ctx context.Context, shard int) refillOutcome {
	capacity := e.cache.Capacity()
	started := time.Now()

	// Phase 1: one aggregate row per fairness key at this shard's minimum due band (or MaxInt band
	// when nothing is due here), published to the census either way - "nothing due" is information
	// the other refillers need (it may raise the global band).
	band, rows, err := e.scanShardBandKeys(ctx, shard)
	if err != nil {
		// The scan failed (typically a transient DB error). Log it and re-poll soon - the same
		// pollErrorRetryInterval (1s) policy pollPendingSteps applies to its own sizing-query failures -
		// so the doorbell fires again and the refiller retries once the blip clears. Return WITHOUT
		// refilling: a failed scan means "unknown", not "nothing is due", and Refill is a wholesale
		// partition replace that honors an empty batch, so falling through would hand a healthy
		// partition's candidates to the garbage collector because the database blipped, idling workers
		// in Pop until the 1s re-poll. Keeping the existing hints costs nothing - a worker popping a
		// stale one just loses its claim CAS. The shard's census entry is NOT republished, so a
		// persistently failing shard ages out of its peers' snapshots (TTL) and stalls only its own
		// partition.
		e.logger.ErrorContext(ctx, "Scanning priority band for refill", "shard", shard, "error", err)
		e.shortenNextPoll(time.Now().Add(pollErrorRetryInterval))
		return refillIdle
	}
	e.publishCensus(shard, band, rows)

	// Phase 2: merge the live census and run the global weighted pick.
	entries := e.censusSnapshot()
	globalBand, keys := mergeCensus(entries)

	// Record the global band and its distinct-fairness-key count for the dwarf_steps_fairness_keys
	// observable gauge (read at metric-collection time).
	e.lastRefillLock.Lock()
	e.lastRefillBand = globalBand
	e.lastRefillKeys = len(keys)
	e.lastRefillLock.Unlock()
	e.lastGlobalBand.Store(int64(globalBand))

	if band == math.MaxInt {
		// Nothing due on this shard at all: wholesale-replace the partition with an empty batch,
		// exactly as draining it dry does - a still-cached candidate would be a dead hint - and park
		// on the doorbell (NOT the band back-off: there is nothing here to dispatch at any band).
		e.metricRefillPass(ctx, shard, time.Since(started), 0, e.cache.Refill(shard, nil, math.MaxInt))
		return refillIdle
	}
	if band > globalBand {
		// Due work here, but above the global band: strict priority says this shard dispatches
		// nothing until the band-holding shards drain. The partition's old hints are for a band this
		// shard no longer holds due work at, so clear them; then back off (refillAboveBand) rather
		// than re-scanning at full rate.
		e.metricRefillPass(ctx, shard, time.Since(started), 0, e.cache.Refill(shard, nil, math.MaxInt))
		return refillAboveBand
	}

	plan := planBatch(keys, capacity)
	if len(plan) == 0 {
		e.metricRefillPass(ctx, shard, time.Since(started), 0, e.cache.Refill(shard, nil, math.MaxInt))
		return refillIdle
	}
	full := len(plan) == capacity

	// The slice rule: which of the plan's slots land on this shard.
	myQuota, maxNeeded, assign := sliceDemand(plan, entries, globalBand, shard)
	if len(myQuota) == 0 {
		// At the band, but a capacity-bound plan gave every slot to shards holding more (or older) of
		// the planned keys - so this shard dispatches nothing this cycle. The partition holds hints the
		// plan no longer backs; clear it. Report STARVED, never idle/full: those park on a trigger that
		// only this shard's own activity can arm, and it has none (see refillStarved).
		e.metricRefillPass(ctx, shard, time.Since(started), 0, e.cache.Refill(shard, nil, math.MaxInt))
		return refillStarved
	}
	chosen := make([]string, 0, len(myQuota))
	for k := range myQuota {
		chosen = append(chosen, k)
	}

	// Phase 3: fetch only this shard's selected steps (up to maxNeeded oldest per chosen key).
	stepsByKey, err := e.fetchShardBandSteps(ctx, shard, globalBand, chosen, maxNeeded)
	if err != nil {
		e.logger.ErrorContext(ctx, "Fetching band steps for refill", "shard", shard, "error", err)
		e.shortenNextPoll(time.Now().Add(pollErrorRetryInterval))
		return refillIdle
	}

	// Assemble the batch by replaying the plan: each occurrence assigned to this shard pops its key's
	// oldest remaining fetched step, preserving the plan's fairness interleave within the slice. A key
	// that came up short (a step got claimed/completed between phases) just skips - the batch runs a
	// touch short, self-corrected on the next refill.
	batch := make([]candidatecache.Job, 0, len(plan))
	occ := map[string]int{}
	idx := map[string]int{}
	for _, k := range plan {
		seq := assign[k]
		o := occ[k]
		occ[k]++
		if o >= len(seq) || seq[o] != shard {
			continue
		}
		list := stepsByKey[k]
		i := idx[k]
		if i >= len(list) {
			continue
		}
		batch = append(batch, candidatecache.Job{StepID: list[i].stepID, Shard: shard})
		idx[k]++
	}

	e.logger.DebugContext(ctx, "Refill batch", "shard", shard, "band", globalBand, "distinctKeys", len(keys), "size", len(batch))
	// The floor is the batch's actual band so the doorbell's priority-preemption decision
	// (head-insert when a strictly more important step arrives) is made against the right threshold.
	discarded := e.cache.Refill(shard, batch, globalBand)
	dur := time.Since(started)
	e.metricRefillPass(ctx, shard, dur, len(batch), discarded)
	e.notePassDuration(dur)
	if full {
		return refillFull
	}
	return refillIdle
}

// Refill SCAN RATE. This is the refiller's supply control, and it is a FIXED floor on how often a
// shard may scan - deliberately not a derived or adaptive one. Two adaptive designs were built and
// measured on a 6-shard rig, and both are recorded here because both keep re-suggesting themselves:
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
// WHY A RATE LIMIT AT ALL. The trigger is re-armed by every step this shard completes, so an
// unlimited refiller runs at a 100% duty cycle - measured, in BOTH builds: every refiller scanning
// back to back for a whole 60s window. The merged pass was accidentally self-limiting because its
// straggler wait made it slow; deleting the barrier made each pass fast and the loop hot, raising
// phase-1 scan load 3.4x. Phase 1 costs per DUE ROW regardless of how many rows the pass then
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
// WHAT THE VALUE DOES **NOT** HAVE TO COVER: priority latency or a drained buffer. Those are events,
// and events are handled by waking on them (see the demand-nudge interrupt in refillerLoop), not by
// standing scan frequency. That is what keeps this a debounce floor rather than a policy timer.
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
	// it - so 120 x connsPerVCPU(6) = 720. With headroom 2.0 this derives a 96/(2*720) ~= 67ms floor (see
	// deriveScanFloor). NOT capacityWeight's 450, the PEAK placement ceiling: placement wants the peak, the
	// scan floor wants the SUSTAINED rate, and conflating them undershoots the drain and overshoots the
	// floor (the earlier 340 - ~57/conn - put it at 141ms, a starved regime giving ~half the throughput of
	// the good band at high connection counts).
	sustainedDrainPerVCPU = 720
	// refillScanFloorCap is a lost-signal backstop, NOT a priority timer. Priority latency is handled by
	// the escalation bypass (routeRefill wakes the refiller the instant a strictly-higher-priority step
	// arrives), so the floor never has to be short enough to cover a priority event, and the cap only
	// bounds how long a genuinely-lost doorbell can delay a scan.
	refillScanFloorCap = 1 * time.Second
	refillScanFloorMin = 5 * time.Millisecond // degenerate-configuration guard only
)

// deriveScanFloor computes ONE shard's scan floor from static configuration - capacity, that shard's
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
//     vCPUs (which made the buffer/drain terms disagree and overshot the floor to the 1s cap, starving
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
// workersPerConnBudget, the 2x cache), so a change to worker/cache sizing rescales the floor
// automatically. (Nearly shipped: campaign 11 found workersPerConnBudget overshoots ~4x. Had it been
// "corrected", capacity would have fallen 4x and a hardcoded floor would have exceeded what the buffer
// could cover - the measured i300 starvation mode. The overshoot was kept as throughput-neutral, but
// the near-miss is the argument for computing this rather than pinning a number.)
//
// The cap governs where the cancellation breaks - workersDispatch's max(64, ...) floor at small or
// high-R configurations - and is a lost-signal backstop (see the constant).
func deriveScanFloor(bufferShare, virtualCPUs, poolConns, replicas int) time.Duration {
	drain := float64(sustainedDrainPerVCPU) * float64(poolConns) / float64(connsPerVCPU) // connection channel
	if virtualCPUs > 0 {                                                                 // cap by the CPU ceiling, when it is known
		drain = min(drain, float64(sustainedDrainPerVCPU)*float64(virtualCPUs)/float64(max(1, replicas)))
	}
	if bufferShare <= 0 || drain <= 0 {
		return refillScanFloorCap
	}
	t := float64(bufferShare) / (refillSupplyHeadroom * drain) // seconds: buffer covers headroom x the drain
	return min(max(time.Duration(t*float64(time.Second)), refillScanFloorMin), refillScanFloorCap)
}

// recomputeScanFloors re-derives every shard's scan floor. Called at Startup and from recomputePools -
// the same "every path that changes a pool must re-derive what depends on it" rule the worker ceiling
// and the candidate cache already obey, since this floor is measured against the cache's capacity.
func (e *Engine) recomputeScanFloors() {
	n := max(1, e.db.NumShards())
	share := e.cache.Capacity() / n
	replicas := max(1, int(e.observedR.Load()))
	override := time.Duration(e.refillScanFloorOverride.Load())
	pinned := int(e.maxOpenConns.Load()) // >0 when SetMaxOpenConns pins every shard's pool
	e.shardsLock.Lock()
	specs := make(map[int]ShardSpec, len(e.shardSpecs))
	for idx, spec := range e.shardSpecs {
		specs[idx] = spec
	}
	e.shardsLock.Unlock()
	for idx, st := range e.refillState {
		if override > 0 {
			st.setFloor(override)
			continue
		}
		// Pass the shard's ACTUAL pool (shardPool resolves the SetMaxOpenConns pin) and its RAW declared
		// vCPUs (0 = undeclared), so the drain is bounded by whichever channel is real - the pinned pool,
		// not a defaulted vCPU count. An unconfigured shard's zero-value spec falls to the conn channel.
		spec := specs[idx]
		_, pool := shardPool(spec, pinned, replicas)
		st.setFloor(deriveScanFloor(share, spec.VirtualCPUs, pool, replicas))
	}
}

// shardRefillState is one shard's rate state, created before the refillers start.
//
// It IS locked, despite each entry having exactly one writer in production (its own refiller
// goroutine). A white-box test driving runShardRefill directly - which several do - is a second
// caller, and an unsynchronized read/write pair there is a genuine race the detector rightly fails.
type shardRefillState struct {
	mu    sync.Mutex
	floor time.Duration // derived scan floor for this shard (see deriveScanFloor)
}

// setFloor publishes a newly-derived floor.
func (s *shardRefillState) setFloor(d time.Duration) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.floor = d
	s.mu.Unlock()
}

// wait returns how long this refiller should hold off before its next scan, measured from the pass
// start so a slow pass pays for itself rather than stacking its duration on top of the floor.
func (s *shardRefillState) wait(passStart time.Time) time.Duration {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	floor := s.floor
	s.mu.Unlock()
	if floor <= 0 {
		return 0
	}
	return floor - time.Since(passStart)
}

// notePassDuration tracks the slowest observed refill pass, which scales the census TTL.
func (e *Engine) notePassDuration(d time.Duration) {
	for {
		cur := e.maxRefillPassNs.Load()
		if int64(d) <= cur || e.maxRefillPassNs.CompareAndSwap(cur, int64(d)) {
			return
		}
	}
}

// Refiller scan phases, the `phase` attribute on dwarf_refill_query_duration_seconds.
const (
	refillPhaseBandKeys   = "band_keys"   // phase 1: one aggregate row per fairness key at the min due band
	refillPhaseFetchSteps = "fetch_steps" // phase 3: the selected steps for the chosen keys
)
