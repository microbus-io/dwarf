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
	"slices"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/janitor"
)

// startJanitors makes and starts a janitor for every shard, for the run, on ctx: active on a shard this replica
// dispatches, paused on the others. applyRole resumes and pauses them as the roles change, and the reconcile loop
// hands them their residue class (pushResidency). A batch's turn is on the shard at dispatch's band, priorityCommon:
// a lower one could starve the jobs at peak load and let the live tables grow.
func (e *Engine) startJanitors(ctx context.Context) error {
	set := e.archives.Load()
	janitors := map[int]*janitor.Janitor{}
	for _, shard := range e.db.Indices() {
		db, err := e.db.Shard(shard)
		if err != nil {
			return err
		}
		var archive *archiver.Archive
		if set != nil {
			archive = set.byShard[shard]
		}
		j := janitor.New(shard, db, archive, e.retentionDays)
		j.SetLogger(e.logger)
		j.SetSeams(e.seams)
		j.SetTurnstile(e.turnstiles, priorityCommon)
		if e.meter != nil {
			if err := j.SetMeter(e.meter); err != nil {
				e.logger.ErrorContext(ctx, "Creating the janitor's instruments", "shard", shard, "error", err)
			}
		}
		j.SetManual(e.janitorManual)
		j.SetIdle(e.janitorIdle)
		e.configureJanitor(j, e.archiveGrace)
		count, ordinal, ok := e.partitionOn(shard)
		j.SetResidency(count, ordinal, ok)
		j.Start(ctx)
		if e.dispatchesOn(shard) {
			j.Resume()
		}
		janitors[shard] = j
	}
	e.janitorMu.Lock()
	e.janitors = janitors
	e.janitorMu.Unlock()
	return nil
}

// configureJanitor sets j's settings from the engine's fields, with grace for the archive grace.
func (e *Engine) configureJanitor(j *janitor.Janitor, grace time.Duration) {
	j.SetGrace(grace)
	j.SetBatch(e.reapBatch, e.reapBatchRows, e.reapBatchBytes)
	j.SetStaging(e.reapStageRows, e.reapStageBytes)
	j.SetQuietMargin(!e.noReapQuiet)
}

// stopJanitors stops every janitor and returns once they all have.
func (e *Engine) stopJanitors() {
	for _, j := range e.allJanitors() {
		j.Stop()
	}
}

// janitorOn is the janitor of shard, or nil when it has none.
func (e *Engine) janitorOn(shard int) *janitor.Janitor {
	e.janitorMu.Lock()
	defer e.janitorMu.Unlock()
	return e.janitors[shard]
}

// allJanitors returns every janitor, by shard.
func (e *Engine) allJanitors() []*janitor.Janitor {
	e.janitorMu.Lock()
	defer e.janitorMu.Unlock()
	shards := make([]int, 0, len(e.janitors))
	for shard := range e.janitors {
		shards = append(shards, shard)
	}
	slices.Sort(shards)
	js := make([]*janitor.Janitor, 0, len(shards))
	for _, shard := range shards {
		js = append(js, e.janitors[shard])
	}
	return js
}

// pushResidency hands shard's janitor its residue class among the shard's dispatchers when it differs from the one it
// was last handed (last, updated). Called by the reconcile loop, which owns last.
func (e *Engine) pushResidency(shard int, last map[int][3]int) {
	count, ordinal, ok := e.partitionOn(shard)
	okInt := 0
	if ok {
		okInt = 1
	}
	pair := [3]int{count, ordinal, okInt}
	if seen, known := last[shard]; known && seen == pair {
		return
	}
	last[shard] = pair
	if j := e.janitorOn(shard); j != nil {
		j.SetResidency(count, ordinal, ok)
	}
}

// drainJanitors runs every active janitor's jobs until they make no progress (janitor.Drain), shard by shard, with the
// engine's settings as they stand and grace for the archive grace: what a test drives to have the jobs do what is due.
// The settings stay as the engine's after it.
func (e *Engine) drainJanitors(ctx context.Context, grace time.Duration) {
	for _, j := range e.allJanitors() {
		e.configureJanitor(j, grace)
		j.Drain(ctx)
		e.configureJanitor(j, e.archiveGrace)
	}
}
