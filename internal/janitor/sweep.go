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

package janitor

import (
	"context"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/metric"
)

// DefaultSweepOverdue is how long past its due time a root waits before a sweep behind a fast job takes it: the steal
// grace, long enough for what failed a root - a Fork holding it, contention, a connection lost - to have cleared.
const DefaultSweepOverdue = 30 * time.Second

// sweepFailureLimit is how many batches a round of a sweep may fail before it gives way to the job's idle wait.
const sweepFailureLimit = 8

// heldLogLimit is how many held roots a sweep logs; the gauge counts the rest. A sweep through an outage holds every
// root it takes, up to sweepFailureLimit a second.
const heldLogLimit = 8

// The kinds of the sweep gauges, one per sweep job: the trees it sweeps.
const (
	kindClosed    = "closed"
	kindDeleted   = "deleted"
	kindRetention = "retention"
)

// HeldRootsDescription is the sweep gauge's description, the same for every kind.
const HeldRootsDescription = "Roots a sweep failed on singly: the larger of the current sweep's count and the last complete one's. Per replica, shard, kind and job: a replica's jobs of one kind hold disjoint roots, but two replicas can hold the same one, so aggregate replicas with max, not sum. Resets at restart or handoff."

// sweep is a sweep job's account of its sweeps: whether one is under way, how many roots its next batch takes, and what
// its gauges report. The job's goroutine alone touches sweeping, size and front; the rest is under mu, which the gauge
// callback takes.
type sweep struct {
	sweeping bool
	size     int // 1 after a failure, doubling after a batch that failed nothing

	mu sync.Mutex
	// failed counts the roots the current sweep held, lastFailed the last complete sweep's.
	failed, lastFailed int
}

// begin begins a sweep: its first batch takes one root.
func (w *sweep) begin() {
	w.sweeping, w.size = true, 1
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failed = 0
}

// drop ends the sweep under way without completing it: its held roots still count.
func (w *sweep) drop() {
	w.sweeping = false
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastFailed = max(w.lastFailed, w.failed)
}

// complete ends the sweep under way: nothing is left behind its end.
func (w *sweep) complete() {
	w.sweeping = false
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastFailed, w.failed = w.failed, 0
}

// limit is how many roots the next batch takes, at most batchRoots.
func (w *sweep) limit(batchRoots int) int {
	return max(min(w.size, batchRoots), 1)
}

// grow doubles the next batch, up to batchRoots, after one that failed nothing.
func (w *sweep) grow(batchRoots int) {
	w.size = min(2*w.size, max(batchRoots, 1))
}

// hold counts a root the sweep failed on alone, which it passes; the next sweep comes back to it. The first heldLogLimit
// of a sweep are logged on j, by the token-free correlation id, never by a flow key - unless ctx ended: a stop is no
// failure.
func (w *sweep) hold(ctx context.Context, j *Janitor, root dueRoot, err error) {
	w.mu.Lock()
	w.failed++
	n := w.failed
	w.mu.Unlock()
	if n > heldLogLimit || ctx.Err() != nil {
		return
	}
	j.diags.Logger().ErrorContext(ctx, "Janitor: a sweep could not remove a tree; the next sweep takes it again",
		"shard", j.shard.Index(), "correlation_id", keys.CorrelationID(j.shard.Index(), root.createdHour, root.flowID),
		"error", err)
}

// forget forgets every sweep and what the gauge reports.
func (w *sweep) forget() {
	w.sweeping, w.size = false, 1
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failed, w.lastFailed = 0, 0
}

// held is what the sweep gauge reports: the larger of the current sweep's held count and the last complete one's.
func (w *sweep) held() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return max(w.failed, w.lastFailed)
}

// sweepGauge is the gauge every sweep job reports, under its kind.
type sweepGauge struct {
	held metric.Int64ObservableGauge
}

// newSweepGauge creates the sweep gauge on meter.
func newSweepGauge(meter metric.Meter) (g sweepGauge, err error) {
	g.held, err = meter.Int64ObservableGauge("dwarf_archive_held_roots", metric.WithUnit("{root}"),
		metric.WithDescription(HeldRootsDescription))
	return g, errors.Trace(err)
}

// observe reports w's held count under attrs, the shard and the kind.
func (g sweepGauge) observe(o metric.Observer, w *sweep, attrs metric.ObserveOption) {
	o.ObserveInt64(g.held, int64(w.held()), attrs)
}

// outcome is what one batch of a sweep came to.
type outcome int

const (
	idle     outcome = iota // nothing done: nothing to take, the day closed, or a read failed
	moved                   // progress: trees moved, deleted, stamped or given an attempt, or a chunk deleted
	passed                  // roots passed and nothing done: each held, or not the sweep's - the round takes another
	faltered                // a batch failed: the round takes another at once, up to sweepFailureLimit
)

// sweepRound runs a round of a sweep job: batch after batch of run, until one makes progress, one finds nothing to do,
// or sweepFailureLimit have failed. Passing roots is not progress, though the round goes on past them: a root that stays
// behind a fast job's position is passed by every sweep, and counted as progress it would keep the job from ever idling.
func sweepRound(ctx context.Context, batch batchFunc, run func(ctx context.Context, h handle) outcome) (progress bool) {
	for failures := 0; failures < sweepFailureLimit; {
		var o outcome
		if err := batch(ctx, func(ctx context.Context, h handle) error {
			o = run(ctx, h)
			return nil
		}); err != nil || ctx.Err() != nil {
			return false // the batch did not run, or the job was stopped
		}
		switch o {
		case faltered:
			failures++
		case passed:
		default:
			return o == moved
		}
	}
	return false
}
