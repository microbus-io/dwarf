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

// workerLoop pops candidates from the cache and executes them.
func (e *Engine) workerLoop(ctx context.Context) {
	for {
		j, ok, needRefill := e.cache.Pop()
		if needRefill {
			e.requestRefill()
		}
		if !ok {
			return
		}
		e.logger.DebugContext(ctx, "Worker popped", "stepID", j.StepID, "shard", j.Shard, "needRefill", needRefill)
		err := errors.CatchPanic(func() error {
			return e.processStep(ctx, j.StepID, j.Shard)
		})
		if err != nil {
			e.logger.ErrorContext(ctx, "Failed to process step", "stepID", j.StepID, "error", err)
		}
		e.requestRefill()
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

// refillerLoop runs one selection scan per trigger. After a scan that filled the cache to capacity -
// the deep-backlog regime - it pauses refillPace before consuming the next trigger. Unpaced, the
// always-armed trigger (workers re-arm it after every step) makes the refiller run back-to-back
// full-backlog scans, and each wholesale cache replace re-delivers steps whose claim CAS is still in
// flight on another worker: at high backlog ~half of all pops (and their claim round-trips) were
// measured stale, and the scan itself streamed the entire due backlog every few ms. Pacing the
// deep-backlog case lets in-flight claims commit before candidates are re-selected.
//
// The gate and the bound are both load-bearing:
//   - Paced ONLY on a full batch: a partial batch means the backlog fits in the cache (light load),
//     where pacing would add dispatch latency for nothing - a sequential flow's next step must not
//     wait out a pace interval. Light-load refills stay immediate.
//   - refillPace must stay well under the cache's drain time (capacity = 2x workers, each candidate
//     occupying a worker for a full step), or the refiller itself becomes the throughput ceiling
//     (<= capacity/pace pops per second): over-pacing was measured to invert the gain. At the default
//     pace a full cache outlives the pause by a wide margin, and the armed trigger means the next scan
//     starts the moment the pause ends - a drained-early cache waits at most the remainder.
func (e *Engine) refillerLoop(ctx context.Context) {
	for {
		select {
		case <-e.refillStop:
			return
		case <-e.refillTrigger:
			var full bool
			err := errors.CatchPanic(func() error {
				full = e.runRefill(ctx)
				return nil
			})
			if err != nil {
				e.logger.ErrorContext(ctx, "Refilling candidate cache", "error", err)
			}
			if full && e.refillPace > 0 {
				select {
				case <-e.refillStop:
					return
				case <-time.After(e.refillPace):
				}
			}
		}
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

	e.requestRefill()
}

// bandKeyAgg is one fairness key's aggregate at the scanned band: its count of due steps, and the age
// and fairness weight of its OLDEST due step. The picker keys a tenant's weight off its oldest step (so
// a tenant cannot self-promote by queueing newer high-weight tasks), and the age both fixes that weight
// during cross-shard merge (the globally-oldest step wins) and is discarded thereafter - the actual
// oldest-first ordering happens on the fetched steps in fetchBandSteps.
type bandKeyAgg struct {
	key    string
	weight float64
	ageMs  float64
	count  int
}

// scanBandKeys returns the globally-minimum due priority band and, for every fairness key at that band,
// a single aggregate row (count + oldest step's age/weight). This is phase 1 of a three-phase refill,
// and its point is that it returns O(distinct keys) rows, NOT O(backlog): the earlier one-query scan
// cut each key at the cache capacity and streamed up to capacity*keys rows across the wire - with high
// fairness-key cardinality (thousands of tenants) that materialized hundreds of thousands of rows every
// refillPace to pick only `capacity` of them. Here the per-key COUNT/ROW_NUMBER window collapses each
// key to one row server-side, so the wire and heap cost scales with key cardinality alone. planBatch
// (phase 2) then decides per-key demand from these aggregates and fetchBandSteps (phase 3) fetches only
// the selected steps.
func (e *Engine) scanBandKeys(ctx context.Context) (band int, keys []bandKeyAgg, err error) {
	// faultRefillScanErr simulates the band scan failing so the test proves runRefill logs and shortens
	// the next poll (re-scan soon) instead of refilling empty and idling every worker.
	if e.seams.IsFault(faultRefillScanErr) {
		return math.MaxInt, nil, errors.New("injected fault: " + faultRefillScanErr)
	}
	type shardKey struct {
		key    string
		weight float64
		ageMs  float64
		count  int
	}
	type shardResult struct {
		band int
		rows []shardKey
	}
	_, pos := e.shardOrdinals()
	results := make([]*shardResult, len(pos))
	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		// One row per fairness key at this shard's minimum due band: COUNT(*) OVER the key's due steps,
		// and the age/weight of its oldest (rn=1). All rows are already filtered to the min band, so the
		// returned priority is that band. rn=1 selects the oldest step whose fairness_weight is the one
		// the picker must use.
		rows, err := db.QueryContext(ctx,
			"SELECT fairness_key, cnt, age_ms, weight, priority FROM ("+
				"SELECT fairness_key, priority,"+
				" COUNT(*) OVER (PARTITION BY fairness_key) AS cnt,"+
				" DATE_DIFF_MILLIS(NOW_UTC(), created_at) AS age_ms,"+
				" fairness_weight AS weight,"+
				" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn"+
				" FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
				" AND priority=(SELECT MIN(priority) FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC())"+
				") t WHERE rn=1",
		)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		var sr *shardResult
		for rows.Next() {
			var sk shardKey
			var prio int
			if err := rows.Scan(&sk.key, &sk.count, &sk.ageMs, &sk.weight, &prio); err != nil {
				return errors.Trace(err)
			}
			if sk.weight <= 0 {
				sk.weight = 1
			}
			if sr == nil {
				sr = &shardResult{band: prio}
			}
			sr.rows = append(sr.rows, sk)
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		if sr != nil {
			results[pos[shard]] = sr
		}
		return nil
	})
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	band = math.MaxInt
	for _, sr := range results {
		if sr != nil && len(sr.rows) > 0 && sr.band < band {
			band = sr.band
		}
	}
	if band == math.MaxInt {
		return band, nil, nil
	}
	// Merge keys across shards at the global band. A tenant's steps can span shards; sum the counts, and
	// keep the globally-oldest step's weight (max age wins) - the same oldest-weins rule the old inline
	// pick applied. Shards at a worse band contribute nothing (lower bands materialize only once the
	// higher drains). Insertion order preserves a stable key order for planBatch's deterministic iteration.
	byKey := map[string]*bandKeyAgg{}
	var order []string
	for _, sr := range results {
		if sr == nil || sr.band != band {
			continue
		}
		for _, sk := range sr.rows {
			agg := byKey[sk.key]
			if agg == nil {
				byKey[sk.key] = &bandKeyAgg{key: sk.key, weight: sk.weight, ageMs: sk.ageMs, count: sk.count}
				order = append(order, sk.key)
				continue
			}
			agg.count += sk.count
			if sk.ageMs > agg.ageMs {
				agg.ageMs = sk.ageMs
				agg.weight = sk.weight
			}
		}
	}
	keys = make([]bandKeyAgg, 0, len(order))
	for _, k := range order {
		keys = append(keys, *byKey[k])
	}
	return band, keys, nil
}

// planBatch runs the weighted fairness pick over the band's per-key aggregates and returns the ordered
// sequence of keys to dispatch - one entry per selected step, at most capacity. Each round weighted-
// randomly picks a key (Efraimidis-Spirakis over the *keys*, re-rolled per step so a key's expected
// share is proportional to its weight and independent of backlog depth) and consumes one of its due
// steps, capped at the key's count. This is the identical selection the old inline pick made, just run
// over counts instead of materialized rows - the actual steps are fetched afterward and replayed against
// this plan oldest-first.
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

// fetchStep is a fetched candidate carrying the age used to order a key's steps oldest-first across shards.
type fetchStep struct {
	stepID int
	shard  int
	ageMs  float64
}

// fetchBandSteps loads, per chosen fairness key, up to perKey of its oldest due steps at the given band
// across all shards, keyed and sorted oldest-first (the order planBatch's plan replays). This is phase 3.
//
// perKey is a UNIFORM cap - the max per-key demand across the plan, not each key's exact demand. That
// keeps the fetch a single IN-list query per shard (an exact per-key cap would need a per-key VALUES/
// LATERAL join, non-trivial across the four SQL dialects). The cost is at most len(chosen)*perKey rows
// per shard, and crucially BOTH factors are bounded by the cache capacity (at most capacity distinct
// keys can be chosen, since each contributes >=1 step; perKey <= capacity) - so the fetch is bounded by
// capacity^2 regardless of how many fairness keys exist. That independence from key cardinality is the
// whole point: at high cardinality perKey is ~1, so the fetch is ~capacity. Over-fetch only appears
// under extreme weight skew among many keys, the low-cardinality regime that never had a scaling problem.
//
// The uniform cap is still COMPLETE for every key: a key's globally-oldest needed steps live at most
// `needed` (<=perKey) on any single shard - they are that shard's oldest for the key, so the per-shard
// rn<=perKey cut captures all of them. The cross-shard merge then sorts by age and the replay takes each
// key's true oldest.
func (e *Engine) fetchBandSteps(ctx context.Context, band int, chosen []string, perKey int) (map[string][]fetchStep, error) {
	if len(chosen) == 0 || perKey <= 0 {
		return nil, nil
	}
	placeholders := strings.Repeat("?,", len(chosen)-1) + "?"
	var mu sync.Mutex
	byKey := map[string][]fetchStep{}
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		args := make([]any, 0, len(chosen)+2)
		args = append(args, band)
		for _, k := range chosen {
			args = append(args, k)
		}
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
				") t WHERE rn<=? ORDER BY step_id",
			args...,
		)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		var local []fetchStep
		var localKeys []string
		for rows.Next() {
			var fs fetchStep
			var key string
			if err := rows.Scan(&fs.stepID, &key, &fs.ageMs); err != nil {
				return errors.Trace(err)
			}
			fs.shard = shard
			local = append(local, fs)
			localKeys = append(localKeys, key)
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		mu.Lock()
		for i := range local {
			byKey[localKeys[i]] = append(byKey[localKeys[i]], local[i])
		}
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Sort each key oldest-first across shards: age desc, then (shard, step_id) for determinism - the
	// same comparator the old inline pick used. age is the only cross-shard-comparable ordering signal
	// (step_id is a per-shard auto-increment).
	for k := range byKey {
		list := byKey[k]
		sort.Slice(list, func(a, b int) bool {
			x, y := list[a], list[b]
			if x.ageMs != y.ageMs {
				return x.ageMs > y.ageMs
			}
			if x.shard != y.shard {
				return x.shard < y.shard
			}
			return x.stepID < y.stepID
		})
	}
	return byKey, nil
}

// runRefill replaces the candidate cache with a fresh priority+fairness batch drawn from the single
// globally-minimum due band, in three phases: scanBandKeys (per-key aggregates), planBatch (weighted
// pick over those aggregates), fetchBandSteps (fetch only the selected steps). Splitting the old single
// scan this way keeps the wire/heap cost off the fairness-key cardinality - see scanBandKeys.
// runRefill reports whether it filled the cache to capacity - the deep-backlog signal the refiller's
// pacing gates on.
func (e *Engine) runRefill(ctx context.Context) (full bool) {
	capacity := e.cache.Capacity()

	// Phase 1: one aggregate row per fairness key at the global-minimum due band (or MaxInt band when
	// nothing is due).
	band, keys, err := e.scanBandKeys(ctx)
	if err != nil {
		// The scan failed (typically a transient DB error). Log it and re-poll soon - the same
		// pollErrorRetryInterval (1s) policy pollPendingSteps applies to its own sizing-query failures -
		// so the doorbell fires again and the refiller retries once the blip clears. Return WITHOUT
		// refilling: a failed scan means "unknown", not "nothing is due", and Refill is a wholesale
		// replace that honors an empty batch, so falling through would hand a healthy cache's candidates
		// to the garbage collector because the database blipped, idling every worker in Pop until the 1s
		// re-poll. Keeping the existing hints costs nothing - a worker popping a stale one just loses its
		// claim CAS.
		e.logger.ErrorContext(ctx, "Scanning priority band for refill", "error", err)
		e.shortenNextPoll(time.Now().Add(pollErrorRetryInterval))
		return false
	}

	// Record this refill's selected band and its distinct-fairness-key count for the
	// dwarf_steps_fairness_keys observable gauge (read at metric-collection time). len(keys) is every key
	// in contention at the band, matching the old metric (which counted every key the scan returned).
	e.lastRefillLock.Lock()
	e.lastRefillBand = band
	e.lastRefillKeys = len(keys)
	e.lastRefillLock.Unlock()

	if band == math.MaxInt || len(keys) == 0 {
		// Nothing due: wholesale-replace with an empty batch (MaxInt floor), exactly as draining the cache
		// dry does - a still-cached candidate would be a dead hint under a floor advertising a band the
		// cache no longer holds.
		e.cache.Refill(nil, math.MaxInt)
		return false
	}

	// Phase 2: weighted pick over the aggregates -> ordered plan of keys, and the per-key demand it implies.
	plan := planBatch(keys, capacity)
	if len(plan) == 0 {
		e.cache.Refill(nil, math.MaxInt)
		return false
	}
	needed := map[string]int{}
	maxNeeded := 0
	for _, k := range plan {
		needed[k]++
		if needed[k] > maxNeeded {
			maxNeeded = needed[k]
		}
	}
	chosen := make([]string, 0, len(needed))
	for k := range needed {
		chosen = append(chosen, k)
	}

	// Phase 3: fetch only the selected steps (up to maxNeeded oldest per chosen key).
	stepsByKey, err := e.fetchBandSteps(ctx, band, chosen, maxNeeded)
	if err != nil {
		e.logger.ErrorContext(ctx, "Fetching band steps for refill", "error", err)
		e.shortenNextPoll(time.Now().Add(pollErrorRetryInterval))
		return false
	}

	// Assemble the batch by replaying the plan: each entry pops its key's oldest remaining fetched step.
	// A key that came up short (a step got claimed/completed between phases) just skips - the batch runs a
	// touch short, self-corrected on the next refill. The result is the same batch the old inline pick
	// produced from a materialized band.
	batch := make([]candidatecache.Job, 0, capacity)
	idx := map[string]int{}
	for _, k := range plan {
		list := stepsByKey[k]
		i := idx[k]
		if i >= len(list) {
			continue
		}
		batch = append(batch, candidatecache.Job{StepID: list[i].stepID, Shard: list[i].shard})
		idx[k]++
	}

	e.logger.DebugContext(ctx, "Refill batch", "band", band, "distinctKeys", len(keys), "size", len(batch))
	// The floor is the cached batch's actual band so the doorbell's priority-preemption decision
	// (head-insert when a strictly more important step arrives) is made against the right threshold.
	e.cache.Refill(batch, band)
	return len(batch) == capacity
}
