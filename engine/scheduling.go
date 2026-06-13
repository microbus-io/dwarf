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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// workerLoop pops candidates from the cache and executes them.
func (e *Engine) workerLoop(ctx context.Context) {
	for {
		j, ok, needRefill := e.cache.pop()
		if needRefill {
			e.requestRefill()
		}
		if !ok {
			return
		}
		e.logger.LogDebug(ctx, "Worker popped", "stepID", j.stepID, "shard", j.shard, "needRefill", needRefill)
		err := errors.CatchPanic(func() error {
			return e.processStep(ctx, j.stepID, j.shard)
		})
		if err != nil {
			e.logger.LogError(ctx, "Failed to process step", "stepID", j.stepID, "error", err)
		}
		e.requestRefill()
	}
}

// timerLoop sleeps until the sooner of nextPoll/nextProbe, then polls.
func (e *Engine) timerLoop(ctx context.Context) {
	for {
		e.nextPollLock.Lock()
		deadline := e.nextPoll
		e.nextPollLock.Unlock()

		if probeNanos := e.nextProbe.Load(); probeNanos != 0 {
			probe := time.Unix(0, probeNanos)
			if floor := time.Now().Add(breakerInitialProbeDelay); probe.Before(floor) {
				probe = floor
			}
			if probe.Before(deadline) {
				deadline = probe
			}
		}

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

// refillerLoop runs one selection scan per trigger.
func (e *Engine) refillerLoop(ctx context.Context) {
	for {
		select {
		case <-e.refillStop:
			return
		case <-e.refillTrigger:
			err := errors.CatchPanic(func() error {
				e.runRefill(ctx)
				return nil
			})
			if err != nil {
				e.logger.LogError(ctx, "Refilling candidate cache", "error", err)
			}
		}
	}
}

// pollPendingSteps recovers expired-lease steps, sizes the wake timer, and rings the doorbell.
func (e *Engine) pollPendingSteps(ctx context.Context) {
	var mu sync.Mutex
	var nearestDelay time.Duration = -1

	e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		db.ExecContext(ctx,
			"UPDATE microbus_steps SET status=?, updated_at=NOW_UTC() WHERE status=? AND parked=0 AND lease_expires<=NOW_UTC()",
			workflow.StatusPending, workflow.StatusRunning,
		)

		var nearestMs sql.NullFloat64
		db.QueryRowContext(ctx,
			"SELECT DATE_DIFF_MILLIS(MIN(not_before), NOW_UTC()) FROM microbus_steps"+
				" WHERE status=? AND parked=0 AND not_before>NOW_UTC() AND not_before<=DATE_ADD_MILLIS(NOW_UTC(), ?) AND lease_expires<=NOW_UTC()",
			workflow.StatusPending, maxPollInterval.Milliseconds(),
		).Scan(&nearestMs)
		var shardNearestDelay time.Duration = -1
		if nearestMs.Valid && nearestMs.Float64 > 0 {
			shardNearestDelay = time.Duration(nearestMs.Float64 * float64(time.Millisecond))
		}

		var dueExists sql.NullInt64
		err := db.QueryRowContext(ctx,
			"SELECT 1 FROM microbus_steps WHERE status=? AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC() ORDER BY step_id LIMIT_OFFSET(1, 0)",
			workflow.StatusPending,
		).Scan(&dueExists)
		if err == nil && (shardNearestDelay < 0 || shardNearestDelay > backlogPollInterval) {
			shardNearestDelay = backlogPollInterval
		}

		// Wake at the soonest future lease expiry of a running step so crash-recovery
		// of a worker that died holding the lease happens promptly, rather than waiting
		// for the next maxPollInterval sweep.
		var leaseMs sql.NullFloat64
		db.QueryRowContext(ctx,
			"SELECT DATE_DIFF_MILLIS(MIN(lease_expires), NOW_UTC()) FROM microbus_steps"+
				" WHERE status=? AND parked=0 AND lease_expires>NOW_UTC() AND lease_expires<=DATE_ADD_MILLIS(NOW_UTC(), ?)",
			workflow.StatusRunning, maxPollInterval.Milliseconds(),
		).Scan(&leaseMs)
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
		mu.Unlock()
		return nil
	})

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

// scanPriorityBand returns the rows of the next priority band above prevBand.
func (e *Engine) scanPriorityBand(ctx context.Context, prevBand int) (int, []candidateRow, error) {
	type shardResult struct {
		band int
		rows []candidateRow
	}
	numShards := e.numDBShards()
	results := make([]*shardResult, numShards+1)
	err := e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		rows, err := db.QueryContext(ctx,
			"SELECT step_id, task_name, fairness_key, fairness_weight, priority, DATE_DIFF_MILLIS(NOW_UTC(), created_at) FROM microbus_steps"+
				" WHERE status=? AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC() AND priority>?"+
				" AND priority=(SELECT MIN(priority) FROM microbus_steps"+
				" WHERE status=? AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC() AND priority>?)"+
				" ORDER BY step_id",
			workflow.StatusPending, prevBand, workflow.StatusPending, prevBand,
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
		if err = rows.Err(); err != nil {
			return errors.Trace(err)
		}
		if sr != nil {
			results[shard] = sr
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

// runRefill replaces the candidate cache with a fresh priority+fairness batch.
func (e *Engine) runRefill(ctx context.Context) {
	now := time.Now()
	capacity := e.cache.capacity()
	batch := make([]job, 0, capacity)
	valveDropped := false

	admittable := func(task string) bool {
		if !e.valvePeek(task, now) {
			valveDropped = true
			return false
		}
		return true
	}

	prevBand := -1
	chosenBand := math.MaxInt
	for {
		band, rows, err := e.scanPriorityBand(ctx, prevBand)
		if err != nil || band == math.MaxInt {
			break
		}
		type keyBucket struct {
			weight    float64
			oldestAge float64
			steps     []candidateRow
		}
		byKey := map[string]*keyBucket{}
		order := []string{}
		for _, c := range rows {
			if !admittable(c.task) {
				continue
			}
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
		if len(byKey) == 0 {
			e.logger.LogDebug(ctx, "Refill band saturated, advancing", "band", band, "rows", len(rows))
			prevBand = band
			continue
		}
		e.logger.LogDebug(ctx, "Refill selecting", "band", band, "distinctKeys", len(order))
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
				for len(kb.steps) > 0 && !admittable(kb.steps[0].task) {
					kb.steps = kb.steps[1:]
				}
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
			batch = append(batch, job{stepID: c.stepID, shard: c.shard})
			e.valveCommit(c.task, now)
		}
		chosenBand = band
		break
	}

	e.logger.LogDebug(ctx, "Refill batch", "size", len(batch))
	// The floor is the cached batch's actual band so the doorbell's priority-preemption decision
	// (head-insert when a strictly more important step arrives) is made against the right threshold.
	// chosenBand stays MaxInt when no band was selected (empty batch), matching the empty-cache case.
	e.cache.refill(batch, chosenBand)
	if valveDropped {
		e.shortenNextPoll(now.Add(time.Second))
	}
}
