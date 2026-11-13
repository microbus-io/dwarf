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

// startJanitors starts every shard's janitor for the run, on ctx: active on a shard this replica dispatches, paused on
// the others. applyRole resumes and pauses them as the roles change; each job reads its residue class from its shard,
// where the shard's Sonar publishes it.
func (e *Engine) startJanitors(ctx context.Context) {
	for shard, a := range e.allActors() {
		a.janitor.Start(ctx)
		if e.dispatchesOn(shard) {
			a.janitor.Resume()
		}
	}
}

// janitorSettings are what the engine builds a janitor's jobs with, taken from its fields.
type janitorSettings struct {
	grace, overdue                    time.Duration
	batchRoots, batchRows, batchBytes int
	stageRows, stageBytes             int
	noQuietMargin                     bool
}

// janitorSettingsFor returns the settings the engine's fields give, with grace for the archive grace.
func (e *Engine) janitorSettingsFor(grace time.Duration) janitorSettings {
	return janitorSettings{
		grace: grace, overdue: e.sweepOverdue,
		batchRoots: e.reapBatch, batchRows: e.reapBatchRows, batchBytes: e.reapBatchBytes,
		stageRows: e.reapStageRows, stageBytes: e.reapStageBytes,
		noQuietMargin: e.noReapQuiet,
	}
}

// addJanitorJobs adds a shard's janitor its jobs - archive retention; fast archiving, sweep archiving behind it and
// large-tree archiving into the shard's archive; fast deletion and sweep deletion behind it; live retention; the
// archive deletion sweep - built with s, replacing the ones it has, which start over. Archive retention comes first: a
// drain runs the jobs in that order, and fast archiving archives only into days archive retention has made.
func (e *Engine) addJanitorJobs(j *janitor.Janitor, archive *archiver.Archive, s janitorSettings) {
	j.AddJob(janitor.JobArchiveRetention, janitor.NewArchiveRetention(archive, e.retentionDays))
	fa := janitor.NewFastArchiving(archive, e.retentionDays)
	fa.SetGrace(s.grace)
	fa.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	fa.SetStaging(s.stageRows, s.stageBytes)
	fa.SetQuietMargin(!s.noQuietMargin)
	j.AddJob(janitor.JobFastArchiving, fa)
	sa := janitor.NewSweepArchiving(fa, archive, e.retentionDays)
	sa.SetGrace(s.grace)
	sa.SetOverdue(s.overdue)
	sa.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	sa.SetStaging(s.stageRows, s.stageBytes)
	sa.SetQuietMargin(!s.noQuietMargin)
	j.AddJob(janitor.JobSweepArchiving, sa)
	la := janitor.NewLargeArchiving(archive, e.retentionDays)
	la.SetBatch(s.batchRows, s.batchBytes)
	la.SetQuietMargin(!s.noQuietMargin)
	j.AddJob(janitor.JobLargeArchiving, la)
	fd := janitor.NewFastDeletion()
	fd.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	j.AddJob(janitor.JobFastDeletion, fd)
	sd := janitor.NewSweepDeletion(fd)
	sd.SetOverdue(s.overdue)
	sd.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	j.AddJob(janitor.JobSweepDeletion, sd)
	lr := janitor.NewLiveRetention(archive, e.retentionDays)
	lr.SetGrace(s.grace)
	lr.SetBatch(s.batchRoots)
	j.AddJob(janitor.JobLiveRetention, lr)
	dm := janitor.NewDeleteMarked(archive, e.retentionDays)
	dm.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
	j.AddJob(janitor.JobArchiveDeletionSweep, dm)
}

// stopJanitors stops every shard's janitor and returns once they all have. A stopped janitor releases what its jobs
// registered, its gauges among them, whether or not it was started.
func (e *Engine) stopJanitors() {
	for _, a := range e.allActors() {
		a.janitor.Stop()
	}
}

// janitorOn is the janitor of shard, or nil when it has none.
func (e *Engine) janitorOn(shard int) *janitor.Janitor {
	if a := e.actorsOn(shard); a != nil {
		return a.janitor
	}
	return nil
}

// drainJanitors runs every active janitor's jobs until they make no progress (janitor.Drain), shard by shard, with
// grace for the archive grace and the engine's other settings as they stand: what a test drives to have the jobs do
// what is due. A test changes the engine's fields on a running engine, so the jobs are rebuilt when the settings
// differ from the ones they were built with, and rebuilt with the engine's own after a drain under another grace.
func (e *Engine) drainJanitors(ctx context.Context, grace time.Duration) {
	e.janitorMu.Lock()
	built := e.janitorJobsBuilt
	e.janitorMu.Unlock()
	actors := e.allActors()
	shards := slices.Sorted(maps.Keys(actors))
	want := e.janitorSettingsFor(grace)
	for _, shard := range shards {
		a := actors[shard]
		if want != built {
			e.addJanitorJobs(a.janitor, a.archive, want)
		}
		a.janitor.Drain(ctx)
	}
	restore := e.janitorSettingsFor(e.archiveGrace)
	if restore != want {
		for _, shard := range shards {
			e.addJanitorJobs(actors[shard].janitor, actors[shard].archive, restore)
		}
	}
	e.janitorMu.Lock()
	e.janitorJobsBuilt = restore
	e.janitorMu.Unlock()
}
