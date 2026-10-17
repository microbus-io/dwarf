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
	"slices"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// backlogInterval is how often a shard's leader refreshes the per-band backlog gauges. A var only so a test
// can shorten it.
var backlogInterval = 30 * time.Second

// backlogQueryTimeout bounds one shard's backlog aggregate, the wait for its turn included. A refresh that
// fails keeps the previous reading.
const backlogQueryTimeout = 10 * time.Second

// backlogStaleIntervals is how many refresh intervals old a reading may be and still be reported. Past it -
// every refresh since has failed, typically timing out against a saturated shard with a deep backlog - the
// series goes absent rather than holding a count that froze while the backlog grew.
const backlogStaleIntervals = 3

// shardBacklog is one shard's last per-band backlog reading, keyed by priority band.
type shardBacklog struct {
	pending map[int]int64 // due pending steps
	oldestS map[int]int64 // seconds since the band's longest-due step became due
	at      time.Time     // when the reading was taken
}

// backlogLoop refreshes the per-band backlog gauges on its own cadence, for the shards this replica leads.
// A single goroutine, so a slow aggregate delays the next refresh rather than overlapping it. It ends on
// ctx, which drainRuntime cancels.
//
// It queries nothing until a metrics collection has asked for the gauges: with no meter provider configured
// the callback never runs, and the engine pays nothing for instruments nobody reads.
func (e *Engine) backlogLoop(ctx context.Context) {
	ticker := time.NewTicker(backlogInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if e.backlogWanted.Load() {
			e.refreshBacklog(ctx)
		}
	}
}

// refreshBacklog runs the backlog aggregate on every shard this replica leads and caches the result for the
// gauge callback. A shard it no longer leads is dropped from the cache, so a former leader stops reporting.
//
// A band this replica has reported before and that is now empty is cached as zero rather than left out. The
// aggregate returns no row for an empty band, and an absent series is not a zero: a chart would hold the
// band's last count until the series went stale.
func (e *Engine) refreshBacklog(ctx context.Context) {
	_ = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.leadsBacklog(shard) {
			e.backlogMu.Lock()
			delete(e.backlogByShard, shard)
			e.backlogMu.Unlock()
			return nil
		}
		reading, err := e.queryBacklog(ctx, db, shard)
		if err != nil {
			if ctx.Err() == nil {
				e.logger.WarnContext(ctx, "Refreshing backlog gauges", "shard", shard, "error", err)
			}
			return nil // keep the previous reading
		}
		e.backlogMu.Lock()
		defer e.backlogMu.Unlock()
		bands := e.backlogBands[shard]
		if bands == nil {
			bands = map[int]bool{}
			e.backlogBands[shard] = bands
		}
		for priority := range reading.pending {
			bands[priority] = true
		}
		for priority := range bands {
			if _, ok := reading.pending[priority]; !ok {
				reading.pending[priority] = 0
				reading.oldestS[priority] = 0
			}
		}
		reading.at = time.Now()
		e.backlogByShard[shard] = reading
		return nil
	})
}

// backlogSQL is the per-band backlog aggregate. parked=0 is what the gauge MEANS: a parked step is excluded from
// selection, so counting one would report a backlog no worker will ever pick up. The age is measured from
// not_before - a step that slept for an hour has been waiting only since it came due - which also makes
// MIN(not_before) a seek on idx_dwarf_steps_due per band. step_id>0 is always true and is the conjunct that
// index's partial predicate carries besides the status: without it this query cannot use the index.
const backlogSQL = "SELECT priority, COUNT(*), DATE_DIFF_MILLIS(NOW_UTC(), MIN(not_before)) FROM dwarf_steps" +
	" WHERE status='" + workflow.StatusPending + "' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()" +
	" AND step_id>0 GROUP BY priority"

// queryBacklog counts one shard's due pending steps per priority band and how long each band's longest-due
// step has been due.
//
// It is O(due backlog): a COUNT over every due row, which no index prefix can shorten. That is affordable
// only because it runs on one replica per shard on a slow cadence, against a band scan that reads the same
// rows several times a second. Do not move it into the metrics callback, which a collector may drive at
// any rate and which every replica runs.
func (e *Engine) queryBacklog(ctx context.Context, db *sequel.DB, shard int) (shardBacklog, error) {
	ctx, cancel := context.WithTimeout(ctx, backlogQueryTimeout)
	defer cancel()
	// The common band, not the refiller's: this aggregate costs O(backlog), and a band may only be given to a
	// caller that cannot flood it - floods are measured in cost, not in count. A late reading is still a
	// correct one.
	ctx, doneTurn := e.dbTurn(ctx, shard)
	defer doneTurn()
	rows, err := db.QueryContext(ctx, backlogSQL, parkedNone)
	if err != nil {
		return shardBacklog{}, errors.Trace(err)
	}
	defer rows.Close()
	reading := shardBacklog{pending: map[int]int64{}, oldestS: map[int]int64{}}
	for rows.Next() {
		var priority int
		var count int64
		var dueMs sql.NullFloat64
		err := rows.Scan(&priority, &count, &dueMs)
		if err != nil {
			return shardBacklog{}, errors.Trace(err)
		}
		reading.pending[priority] = count
		if dueMs.Valid {
			reading.oldestS[priority] = int64(dueMs.Float64 / 1000)
		}
	}
	err = rows.Err()
	if err != nil {
		return shardBacklog{}, errors.Trace(err)
	}
	return reading, nil
}

// leadsBacklog reports whether this replica refreshes a shard's backlog gauges: the first replica in the
// shard's dispatcher window. Every replica derives that place from the same registry, so the aggregate runs
// once per shard however wide the fleet. Each derives it from its own reading, on its own 30s tick, so a
// handoff can leave two replicas reporting a shard for up to an interval (the documented max absorbs it) or
// none (the series is absent for up to an interval).
//
// A shard with no Sonar is dispatched solo, so its only dispatcher leads.
func (e *Engine) leadsBacklog(shard int) bool {
	if !e.dispatchesOn(shard) {
		return false
	}
	s := e.sonarFor(shard)
	if s == nil {
		return true
	}
	st := s.Standing()
	place := slices.Index(slices.Sorted(slices.Values(e.db.Indices())), shard)
	slots := dispatcherSlots(st.Candidates)
	return st.InCut && st.Rank == place*slots%st.Candidates
}
