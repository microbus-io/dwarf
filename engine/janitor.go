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

	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/sequel"
)

// janitorSupervision is how often the supervisor reads the dispatch roles.
const janitorSupervision = time.Second

// janitorLoop is the janitors' supervisor: it keeps a janitor on every shard this replica dispatches and none on the
// others, reading the roles every janitorSupervision. A janitor starts with nothing learned, so one that stops and
// starts again on a shard loses only hints. On ctx's end, which cancels every janitor, it joins them and returns.
func (e *Engine) janitorLoop(ctx context.Context) {
	ticker := time.NewTicker(janitorSupervision)
	defer ticker.Stop()
	for {
		e.superviseJanitors()
		select {
		case <-ctx.Done():
			e.janitorMu.Lock()
			defer e.janitorMu.Unlock()
			for shard, j := range e.janitors {
				j.Stop()
				delete(e.janitors, shard)
			}
			return
		case <-ticker.C:
		}
	}
}

// superviseJanitors starts a janitor on each shard this replica dispatches that has none, and stops the janitor of
// each shard it no longer dispatches. It starts none once the supervisor's context has ended.
func (e *Engine) superviseJanitors() {
	e.janitorMu.Lock()
	defer e.janitorMu.Unlock()
	if e.janitorCtx == nil || e.janitorCtx.Err() != nil {
		return
	}
	for _, shard := range e.db.Indices() {
		j, running := e.janitors[shard]
		dispatching := e.dispatchesOn(shard)
		switch {
		case dispatching && !running:
			db, err := e.db.Shard(shard)
			if err != nil {
				e.logger.ErrorContext(e.janitorCtx, "Starting a janitor", "shard", shard, "error", err)
				continue
			}
			j = janitor.New(e.janitorConfig(shard, db))
			j.Start(e.janitorCtx)
			e.janitors[shard] = j
		case !dispatching && running:
			delete(e.janitors, shard)
			j.Stop()
		}
	}
}

// janitorConfig is the configuration of a new janitor on shard. A batch's turn is on the shard at dispatch's band,
// priorityCommon: a lower one could starve the jobs at peak load and let the live tables grow.
func (e *Engine) janitorConfig(shard int, db *sequel.DB) janitor.Config {
	cfg := janitor.Config{
		Shard: shard,
		DB:    db,
		Turn: func(ctx context.Context) (context.Context, func()) {
			return e.dbTurn(ctx, shard)
		},
		Logger: e.logger,
		Idle:   e.janitorIdle,
		Manual: e.janitorManual,
	}
	if e.metrics != nil {
		cfg.Instruments = e.metrics.janitor
	}
	if e.janitorTestJobs != nil {
		cfg.Jobs = e.janitorTestJobs(shard, db)
	}
	return cfg
}

// janitorOn is the janitor of shard, or nil when it has none.
func (e *Engine) janitorOn(shard int) *janitor.Janitor {
	e.janitorMu.Lock()
	defer e.janitorMu.Unlock()
	return e.janitors[shard]
}

// drainJanitors brings the janitors in line with the dispatch roles, then runs each one's jobs until they make no
// progress (janitor.Drain), shard by shard: what a test drives to have the jobs do what is due.
func (e *Engine) drainJanitors(ctx context.Context) {
	e.superviseJanitors()
	e.janitorMu.Lock()
	shards := make([]int, 0, len(e.janitors))
	for shard := range e.janitors {
		shards = append(shards, shard)
	}
	slices.Sort(shards)
	js := make([]*janitor.Janitor, 0, len(shards))
	for _, shard := range shards {
		js = append(js, e.janitors[shard])
	}
	e.janitorMu.Unlock()
	for _, j := range js {
		j.Drain(ctx)
	}
}
