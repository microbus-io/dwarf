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

// candidateRow is a candidate step considered for admission.
type candidateRow struct {
	stepID int
	shard  int
	task   string
	key    string
	weight float64
	ageMs  float64
}

// scanPriorityBand returns the due rows of the globally-minimum priority band, at most perKeyLimit per
// fairness key (oldest first). perKeyLimit is the cache capacity: the picker takes at most that many
// steps in one refill, so it can never want more than that from any single key - the bound is free of
// selection consequences and keeps the scan's cost off the size of the backlog.
func (e *Engine) scanPriorityBand(ctx context.Context, perKeyLimit int) (int, []candidateRow, error) {
	// faultRefillScanErr simulates the priority-band scan failing so the test proves runRefill logs and
	// shortens the next poll (re-scan soon) instead of refilling empty and idling every worker.
	if e.seams.IsFault(faultRefillScanErr) {
		return math.MaxInt, nil, errors.New("injected fault: " + faultRefillScanErr)
	}
	type shardResult struct {
		band int
		rows []candidateRow
	}
	_, pos := e.shardOrdinals()
	results := make([]*shardResult, len(pos))
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		// Bounded per fairness key: ROW_NUMBER() over each key's due steps, oldest first, cut at
		// perKeyLimit (the cache capacity). Without the cut this streamed the ENTIRE due band - every row
		// of every key - across the wire and allocated a candidateRow for each, only to discard all but
		// `capacity` of them in the weighted pick below. Its cost grew with the BACKLOG, so under a deep
		// one (the case the refiller exists for) it re-scanned hundreds of thousands of rows every
		// refillPace (20ms).
		//
		// The cut is per KEY, not a plain LIMIT, because fairness is the whole point of this query: a
		// global `ORDER BY created_at LIMIT n` would let one tenant's old backlog fill the window and
		// starve every other key. Per key, the picker can never consume more than `capacity` steps in one
		// refill (it takes at most `capacity` steps total), so this drops nothing it could have used - the
		// batch is identical, it is just no longer built by materializing the backlog first.
		rows, err := db.QueryContext(ctx,
			"SELECT step_id, task_url, fairness_key, fairness_weight, priority, age_ms FROM ("+
				"SELECT step_id, task_url, fairness_key, fairness_weight, priority,"+
				" DATE_DIFF_MILLIS(NOW_UTC(), created_at) AS age_ms,"+
				" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn"+
				" FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
				" AND priority=(SELECT MIN(priority) FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC())"+
				") t WHERE rn<=? ORDER BY step_id",
			perKeyLimit,
		)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		var sr *shardResult
		for rows.Next() {
			var c candidateRow
			var prio int
			err := rows.Scan(&c.stepID, &c.task, &c.key, &c.weight, &prio, &c.ageMs)
			if err != nil {
				return errors.Trace(err)
			}
			if c.weight <= 0 {
				c.weight = 1
			}
			c.shard = shard
			if sr == nil {
				sr = &shardResult{band: prio}
			}
			sr.rows = append(sr.rows, c)
		}
		err = rows.Err()
		if err != nil {
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
	globalBand := math.MaxInt
	for _, sr := range results {
		if sr != nil && len(sr.rows) > 0 && sr.band < globalBand {
			globalBand = sr.band
		}
	}
	if globalBand == math.MaxInt {
		return globalBand, nil, nil
	}
	var atBand []candidateRow
	for _, sr := range results {
		if sr == nil || sr.band != globalBand {
			continue
		}
		atBand = append(atBand, sr.rows...)
	}
	return globalBand, atBand, nil
}

// runRefill replaces the candidate cache with a fresh priority+fairness batch drawn from the single
// globally-minimum due band.
// runRefill reports whether it filled the cache to capacity - the deep-backlog signal the refiller's
// pacing gates on.
func (e *Engine) runRefill(ctx context.Context) (full bool) {
	capacity := e.cache.Capacity()
	batch := make([]candidatecache.Job, 0, capacity)

	// One band per refill: scanPriorityBand returns the strict global-minimum due band's rows (or MaxInt
	// when nothing is due). The earlier per-band advance loop was vestige of the removed saturation gating,
	// which could post-filter a band's rows to empty and force a scan of the next band up; without that
	// filter a non-MaxInt band always yields rows (every row makes a fairness-key bucket), so the scan runs
	// exactly once - lower bands stay materialized only after the current one drains, by design.
	chosenBand := math.MaxInt
	band, rows, err := e.scanPriorityBand(ctx, capacity)
	if err != nil {
		// The scan failed (typically a transient DB error), so this refill produces an empty batch and
		// every worker blocks in Pop. Swallowing the error would leave that stall unretried until the
		// next doorbell or the backlog backstop (up to a minute). Log it and re-poll soon - the same
		// pollErrorRetryInterval (1s) policy pollPendingSteps applies to its own sizing-query failures -
		// so the doorbell fires again and the refiller retries once the blip clears.
		e.logger.ErrorContext(ctx, "Scanning priority band for refill", "error", err)
		e.shortenNextPoll(time.Now().Add(pollErrorRetryInterval))
	}
	if err == nil && band != math.MaxInt {
		type keyBucket struct {
			weight    float64
			oldestAge float64
			steps     []candidateRow
		}
		byKey := map[string]*keyBucket{}
		order := []string{}
		for _, c := range rows {
			kb := byKey[c.key]
			if kb == nil {
				kb = &keyBucket{weight: c.weight, oldestAge: c.ageMs}
				byKey[c.key] = kb
				order = append(order, c.key)
			} else if c.ageMs > kb.oldestAge {
				kb.oldestAge = c.ageMs
				kb.weight = c.weight
			}
			kb.steps = append(kb.steps, c)
		}
		e.logger.DebugContext(ctx, "Refill selecting", "band", band, "distinctKeys", len(order))
		// Record this refill's selected band and its distinct-fairness-key count for the
		// dwarf_steps_fairness_keys observable gauge (read at metric-collection time).
		e.lastRefillLock.Lock()
		e.lastRefillBand = band
		e.lastRefillKeys = len(order)
		e.lastRefillLock.Unlock()
		for _, kb := range byKey {
			sort.Slice(kb.steps, func(a, b int) bool {
				x, y := kb.steps[a], kb.steps[b]
				if x.ageMs != y.ageMs {
					return x.ageMs > y.ageMs
				}
				if x.shard != y.shard {
					return x.shard < y.shard
				}
				return x.stepID < y.stepID
			})
		}
		for len(batch) < capacity {
			bestKey, bestScore := "", -1.0
			for _, k := range order {
				kb := byKey[k]
				if len(kb.steps) == 0 {
					continue
				}
				score := math.Pow(rand.Float64(), 1/kb.weight)
				if score > bestScore {
					bestScore = score
					bestKey = k
				}
			}
			if bestScore < 0 {
				break
			}
			kb := byKey[bestKey]
			c := kb.steps[0]
			kb.steps = kb.steps[1:]
			batch = append(batch, candidatecache.Job{StepID: c.stepID, Shard: c.shard})
		}
		chosenBand = band
	}

	e.logger.DebugContext(ctx, "Refill batch", "size", len(batch))
	// The floor is the cached batch's actual band so the doorbell's priority-preemption decision
	// (head-insert when a strictly more important step arrives) is made against the right threshold.
	// chosenBand stays MaxInt when no band was selected (empty batch), matching the empty-cache case.
	e.cache.Refill(batch, chosenBand)
	return len(batch) == capacity
}
