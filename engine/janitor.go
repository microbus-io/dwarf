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
	"maps"
	"slices"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/janitor"
)

// startJanitors makes and starts a janitor for every shard, for the run, on ctx, with its jobs: active on a shard this
// replica dispatches, paused on the others. applyRole resumes and pauses them as the roles change; each job reads its
// residue class from its shard, where the shard's Sonar publishes it.
func (e *Engine) startJanitors(ctx context.Context) error {
	settings := e.janitorSettingsFor(e.archiveGrace)
	janitors := map[int]*janitor.Janitor{}
	for _, shard := range e.db.Indices() {
		sh, err := e.db.Shard(shard)
		if err != nil {
			return err
		}
		j := janitor.New(sh, e.diags)
		j.SetManual(e.janitorManual)
		j.SetIdle(e.janitorIdle)
		e.addJanitorJobs(j, shard, settings)
		j.Start(ctx)
		if e.dispatchesOn(shard) {
			j.Resume()
		}
		janitors[shard] = j
	}
	e.janitorMu.Lock()
	e.janitors = janitors
	e.janitorJobsBuilt = settings
	e.janitorMu.Unlock()
	return nil
}

// janitorSettings are what the engine builds a janitor's jobs with, taken from its fields.
type janitorSettings struct {
	grace                             time.Duration
	batchRoots, batchRows, batchBytes int
	stageRows, stageBytes             int
	noQuietMargin                     bool
}

// janitorSettingsFor returns the settings the engine's fields give, with grace for the archive grace.
func (e *Engine) janitorSettingsFor(grace time.Duration) janitorSettings {
	return janitorSettings{
		grace:      grace,
		batchRoots: e.reapBatch, batchRows: e.reapBatchRows, batchBytes: e.reapBatchBytes,
		stageRows: e.reapStageRows, stageBytes: e.reapStageBytes,
		noQuietMargin: e.noReapQuiet,
	}
}

// addJanitorJobs adds shard's janitor its jobs - fast archiving into the shard's archive, and fast deletion - built
// with s, replacing the ones it has, which start over.
func (e *Engine) addJanitorJobs(j *janitor.Janitor, shard int, s janitorSettings) {
	var archive *archiver.Archive
	if set := e.archives.Load(); set != nil {
		archive = set.byShard[shard]
	}
	fa := janitor.NewFastArchiving(archive, e.retentionDays)
	fa.SetGrace(s.grace)
	fa.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	fa.SetStaging(s.stageRows, s.stageBytes)
	fa.SetQuietMargin(!s.noQuietMargin)
	j.AddJob(janitor.JobFastArchiving, fa)
	fd := janitor.NewFastDeletion()
	fd.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	j.AddJob(janitor.JobFastDeletion, fd)
}

// stopJanitors stops every janitor and returns once they all have.
func (e *Engine) stopJanitors() {
	e.janitorMu.Lock()
	janitors := e.janitors
	e.janitorMu.Unlock()
	for _, j := range janitors {
		j.Stop()
	}
}

// janitorOn is the janitor of shard, or nil when it has none.
func (e *Engine) janitorOn(shard int) *janitor.Janitor {
	e.janitorMu.Lock()
	defer e.janitorMu.Unlock()
	return e.janitors[shard]
}

// drainJanitors runs every active janitor's jobs until they make no progress (janitor.Drain), shard by shard, with
// grace for the archive grace and the engine's other settings as they stand: what a test drives to have the jobs do
// what is due. A test changes the engine's fields on a running engine, so the jobs are rebuilt when the settings
// differ from the ones they were built with, and rebuilt with the engine's own after a drain under another grace.
func (e *Engine) drainJanitors(ctx context.Context, grace time.Duration) {
	e.janitorMu.Lock()
	janitors, built := e.janitors, e.janitorJobsBuilt
	e.janitorMu.Unlock()
	shards := slices.Sorted(maps.Keys(janitors))
	want := e.janitorSettingsFor(grace)
	for _, shard := range shards {
		if want != built {
			e.addJanitorJobs(janitors[shard], shard, want)
		}
		janitors[shard].Drain(ctx)
	}
	restore := e.janitorSettingsFor(e.archiveGrace)
	if restore != want {
		for _, shard := range shards {
			e.addJanitorJobs(janitors[shard], shard, restore)
		}
	}
	e.janitorMu.Lock()
	e.janitorJobsBuilt = restore
	e.janitorMu.Unlock()
}
