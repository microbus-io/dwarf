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
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/latch"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/internal/peers"
	"github.com/microbus-io/dwarf/internal/piston"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/errors"
)

// shardActors are one shard's actors in one run, and this replica's role there. Startup builds every open shard's
// before anything reads them, and publishes the run's map whole (Engine.actors), so a call straddling a
// Shutdown/Startup restart reads one run's map or the other's, never one being filled. No field is written after
// the map is published: what changes during the run changes inside an actor or the role.
type shardActors struct {
	// sonar owns this replica's row in the shard's dwarf_peers registry. Nil when it could not be built, which leaves
	// the shard dispatched solo, with its whole budget and unpartitioned selection.
	sonar *peers.Sonar
	// piston supplies the shard's candidates.
	piston *piston.Piston
	// janitor runs the shard's fast archiving and fast deletion while this replica dispatches it.
	janitor *janitor.Janitor
	// archive is the shard's archive day tables.
	archive *archiver.Archive
	// latch is the board Await callers on the shard park on.
	latch *latch.Board
	// linker decides which state fields a successor step on the shard stores by reference, and resolves them.
	linker *staterefs.Linker
	// role is this replica's standing on the shard.
	role shardRole
}

// actorsOn is shard's actors in the current run, or nil before the first Startup and for a shard the run did not
// open.
func (e *Engine) actorsOn(shard int) *shardActors {
	m := e.actors.Load()
	if m == nil {
		return nil
	}
	return (*m)[shard]
}

// allActors is the current run's actors by shard, nil before the first Startup. Read-only.
func (e *Engine) allActors() map[int]*shardActors {
	m := e.actors.Load()
	if m == nil {
		return nil
	}
	return *m
}

// dayLength is the length of the run's archive day, 0 before the first Startup.
func (e *Engine) dayLength() time.Duration {
	return time.Duration(e.runDayLength.Load())
}

// buildActors builds every open shard's actors for the run, none of them running, and publishes them with the day's
// length. The planner and the state cache the pistons and linkers share are built first, fresh for the run: a tally
// left from the last run would claim a band nobody serves. A shard whose piston cannot be built fails Startup: it
// would have no supply cycle, so nothing on it would ever be dispatched while the replica went on advertising itself
// there.
func (e *Engine) buildActors(ctx context.Context, dayLength time.Duration) error {
	e.planner = planner.New()
	cacheBudget, cacheSource := e.stateCacheBudget()
	e.stateCache = staterefs.NewCache(cacheBudget)
	e.diags.Logger().InfoContext(ctx, "State cache sized", "bytes", cacheBudget, "source", cacheSource)
	settings := e.janitorSettingsFor(e.archiveGrace)
	actors := make(map[int]*shardActors, len(e.db.Indices()))
	release := func() {
		for _, a := range actors {
			a.janitor.Stop()
		}
	}
	for _, idx := range e.db.Indices() {
		sh, err := e.db.Shard(idx)
		if err != nil {
			release()
			return errors.New("resolving shard %d: %w", idx, err)
		}
		db := sh.DB()
		p, err := piston.New(sh, e.diags, e.planner, &e.cache)
		if err != nil {
			release()
			return errors.New("building piston for shard %d: %w", idx, err)
		}
		p.SetMaxPriority(e.maxPriority)
		a := &shardActors{
			piston:  p,
			archive: archiver.New(db, dayLength, migrations.Archive()),
			latch:   e.newLatchBoard(idx),
			// The linker needs the shard's driver name - the only dialect-dependent term in the size policy - and
			// caches resolved payload bytes under step ids, which are unique only within a shard.
			linker: staterefs.New(db.DriverName(), idx, e.stateCache),
		}
		a.janitor = janitor.New(sh, e.diags)
		a.janitor.SetManual(e.janitorManual)
		a.janitor.SetIdle(e.janitorIdle)
		e.addJanitorJobs(a.janitor, a.archive, settings)
		actors[idx] = a
	}
	e.buildSonars(ctx, actors)
	for _, a := range actors {
		// The two halves of the shard's peer accounting meet through the shard and here, rather than through either
		// package knowing the other: the piston selects within the residue class the Sonar publishes on the shard,
		// and the Sonar stamps this replica as SERVING the shard only when the piston says it is turning.
		if a.sonar != nil {
			a.sonar.SetEvidence(a.piston.Liveness)
		}
	}
	e.janitorMu.Lock()
	e.janitorJobsBuilt = settings
	e.janitorMu.Unlock()
	e.runDayLength.Store(int64(dayLength))
	e.actors.Store(&actors)
	return nil
}

// newLatchBoard returns a fresh Await board for shard, whose detector asks the shard which parked flows settled. Fresh
// per run: the last run's was closed at its drain, and a closed board turns every Await away.
func (e *Engine) newLatchBoard(shard int) *latch.Board {
	// The resolver is non-nil, so New cannot fail.
	board, _ := latch.New(func(ctx context.Context, flowKeys []string) (map[string]string, error) {
		return e.resolveStoppedFlows(ctx, shard, flowKeys)
	})
	// The park rendezvous, wired only under test so a production board holds no hook and Latch pays one atomic load.
	// Fired unscoped and scoped by flow key, for the same reason signalStop fires both: a scoped fire does not wake an
	// unscoped waiter, and a test may be waiting for THIS caller to park while a subgraph child's or a peer's Await
	// parks alongside it.
	if e.diags.Seams().Enabled() {
		board.SetOnPark(func(flowKey string) {
			e.diags.Seams().Checkpoint(e.lifetimeCtx, diagnostics.CheckpointAwaitParked)
			e.diags.Seams().Checkpoint(e.lifetimeCtx, diagnostics.Join(diagnostics.CheckpointAwaitParked, flowKey))
		})
	}
	return board
}
