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

/*
Package reaper removes closed trees from one shard's live tables: it archives each closed root's tree into the
archive day it is filed in, deletes the trees stamped for deletion, and deletes an unarchived tree once its retention
has passed. It also deletes archived trees for a caller that removes them (DeleteArchivedRoots).

A Reaper holds what its passes learned, per shard: where each walk over the due roots stands, since when archiving
has been failing, and the archive lag. Build one per run with New, before anything reads it, and drive it:

	r := reaper.New()
	r.SetLogger(logger)
	r.SetSeams(seams)
	if err := r.SetMeter(meter); err != nil { ... }
	// On each tick, for each shard this replica dispatches:
	r.Pass(ctx, cfg, reaper.Shard{Index: shard, DB: db, Archive: archive, Residency: residency})
	// On a shard this replica stopped dispatching:
	r.Forget(shard)
	// For the gauges:
	obs := r.Observe(shard, time.Now())

Concurrency: Pass on different shards may run at once, and Observe and Forget may overlap a Pass. Two Passes on one
shard are safe too, though each walk's position is then a hint either may overwrite; a caller runs one per shard.
The Reaper takes no admission of its own on a shard: a caller that meters its connections does so around each call.
*/
package reaper

import (
	"context"
	"log/slog"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/metric"
)

// The kinds of due roots a pass walks, as Observe reports them.
const (
	KindClosed    = "closed"    // closed roots past the archive grace: archived, or deleted at a retention of 0
	KindDeleted   = "deleted"   // roots stamped for deletion: deleted, never archived
	KindRetention = "retention" // unarchived closed roots past their retention: deleted
)

// The reasons a pass deletes flows without archiving them, the reason attribute of dwarf_flows_reaped.
const (
	ReasonDeleted       = "deleted"
	ReasonNoRetention   = "no_retention"
	ReasonArchiveFailed = "archive_failed"
)

// The faults and checkpoints a Reaper consults, inert unless its seams are enabled. A targeted fault is the name
// joined with a root's flow id by a colon.
const (
	FaultMidTree       = "reapMidTree"      // a move or delete fails after its steps, before its flows; optionally targeted
	FaultSizeErr       = "reapSizeErr"      // sizing a batch fails; targeted
	FaultSelectErr     = "reapSelectErr"    // a due-root scan fails
	FaultContention    = "reapContention"   // an archive move fails on lock contention
	FaultArchiveErr    = "reapArchiveErr"   // an archive move or staging chunk fails with an error that is not lock contention
	FaultArchiveDelete = "archiveDeleteErr" // an archived tree's deletion fails before it commits; targeted
	FaultStageChunk    = "reapStageChunk"   // a staging chunk fails under its root's lock, before it writes; targeted
	FaultStageLost     = "reapStageLost"    // a staging chunk commits, then reports an error, as a lost response; targeted

	CheckpointBatchLocked      = "reapBatchLocked"      // a batch has locked its due roots, inside its transaction
	CheckpointBeforeRootDelete = "reapBeforeRootDelete" // a chunked deletion is about to recheck and delete its root
	CheckpointChunkDone        = "reapChunkDone"        // a chunk of a chunked deletion has committed and been counted
	CheckpointStageChunkDone   = "reapStageChunkDone"   // a staging chunk, or the publication, has committed
	CheckpointArchiveDeleted   = "reapArchiveDeleted"   // a transaction deleting archived trees has committed
)

// Config is one pass's settings. Nothing in it is kept between passes.
type Config struct {
	// Batch is how many due roots one batch considers.
	Batch int
	// BatchRows is about how many rows, flows and steps alike, one transaction moves or deletes, and at least one
	// tree.
	BatchRows int
	// BatchBytes is about how many payload bytes one transaction moves or deletes, and at least one tree or step; 0
	// sets no byte limit. A transaction stops at whichever of BatchRows and BatchBytes it reaches first.
	BatchBytes int
	// Attempt bounds each database attempt: a scan, a sizing, a day lookup, a tree transaction.
	Attempt time.Duration
	// Slice is how long one walker runs before it yields to the next.
	Slice time.Duration
	// Budget is how long a pass keeps working through a healthy backlog.
	Budget time.Duration
	// Grace is how long after its close a root is archived.
	Grace time.Duration
	// StealGrace is how long past due a root waits for the dispatcher whose residue class it is before any takes it.
	StealGrace time.Duration
	// RetentionDays is how many days the archive keeps; 0 keeps no archive.
	RetentionDays int
	// DayLength is the archive day's length, for the retention cutoff on a shard with no Archive.
	DayLength time.Duration
	// NoQuietMargin lets a pass archive into its day however near the day's end.
	NoQuietMargin bool
	// StageRows is the size, in rows, above which a closed tree is archived in stages rather than in one transaction;
	// 0 never starts a staged archive. A tree whose staged archive has started is archived in stages whatever its size.
	StageRows int
	// StageBytes is the size, in payload bytes, above which a closed tree is archived in stages, when StageRows starts
	// staged archives at all; 0 sets no byte threshold.
	StageBytes int
}

// ResidencyFunc returns this replica's residue class among a shard's dispatchers: the count of dispatchers and this
// one's ordinal. ok false, a count under 2 or an ordinal out of range restricts nothing.
type ResidencyFunc func() (count, ordinal int, ok bool)

// Shard is the shard a pass works on.
type Shard struct {
	// Index is the shard's number.
	Index int
	// DB is the shard's handle.
	DB *sequel.DB
	// Archive is the shard's archive, or nil when it has none: closed roots then stay live at a positive retention.
	Archive *archiver.Archive
	// Residency is read on every walk; nil restricts nothing.
	Residency ResidencyFunc
}

// Observation is what a Reaper knows of one shard, for its gauges.
type Observation struct {
	// Failing is how long archiving has been failing, or 0.
	Failing time.Duration
	// Lag is how long past the grace the oldest due closed root had waited at the last scan; LagOK is false before
	// the first.
	Lag   time.Duration
	LagOK bool
	// Kinds holds every kind, in the order closed, deleted, retention, zero for a kind not walked yet.
	Kinds [3]KindObservation
	// CleanSweep is when the last repair sweep of closed roots that completed with no failure began; zero before the
	// first.
	CleanSweep time.Time
	// Pending is the number of discovered marked trees left by the last DeleteMarked. PendingUnknown counts
	// retained days it could not examine. PendingOK is false before the first completed pass.
	Pending        int
	PendingUnknown int
	PendingOK      bool
}

// KindObservation is what the repair walk of one kind knows.
type KindObservation struct {
	Kind string
	// Held is how many roots repair could not remove, the larger of the current sweep's count and the last one's.
	Held int
	// OldestHeld is the age of the oldest due root at the current sweep's start, aging since.
	OldestHeld time.Duration
	// Sweep is how long the current repair sweep has run, or 0 between sweeps.
	Sweep time.Duration
	// OldestAttempt is the age of the oldest unpublished archive attempt the repair sweep has seen, the larger of the
	// current sweep's and the last complete one's, aging since it was read; 0 for none. The closed kind's only.
	OldestAttempt time.Duration
}

// Reaper removes closed trees, one shard per Pass.
type Reaper struct {
	logger   *slog.Logger
	seams    *seamster.Seamster
	metrics  *instruments
	walks    walks
	failures failures
	lag      lag
	cleanup  archiveCleanups
	stages   stages
	prefer   preferences
}

// New returns a Reaper that knows nothing yet, logs nowhere and records no metric.
func New() *Reaper {
	return &Reaper{logger: slog.New(slog.DiscardHandler), seams: seamster.New(false)}
}

// SetLogger sets where the Reaper logs. Call it before the first Pass.
func (r *Reaper) SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.New(slog.DiscardHandler)
	}
	r.logger = l
}

// SetSeams sets the seams the Reaper consults; nil consults none. Call it before the first Pass.
func (r *Reaper) SetSeams(s *seamster.Seamster) {
	if s == nil {
		s = seamster.New(false)
	}
	r.seams = s
}

// SetMeter creates the Reaper's instruments on meter: dwarf_flows_archived, dwarf_flows_reaped,
// dwarf_archive_move_failures and dwarf_reap_pass_seconds. Call it before the first Pass.
func (r *Reaper) SetMeter(meter metric.Meter) error {
	m, err := newInstruments(meter)
	r.metrics = m
	return err
}

// Pass removes what is due on one shard: the trees past their retention, then the stamped trees and the closed ones,
// batch by batch, until none is due, the budget is spent, or ctx is done. Each transaction locks the roots it removes
// first, FOR UPDATE or by flipping their touch column, so a caller's conflicting lock on a root holds its tree back.
func (r *Reaper) Pass(ctx context.Context, cfg Config, s Shard) {
	(&pass{r: r, cfg: cfg, shard: s}).run(ctx)
}

// Forget forgets what the Reaper knows of shard, for a replica that stopped dispatching it: a replica that dispatches
// it again starts afresh.
func (r *Reaper) Forget(shard int) {
	r.failures.forget(shard)
	r.lag.forget(shard)
	r.walks.forget(shard)
	r.cleanup.forget(shard)
	r.stages.forget(shard)
	r.prefer.forget(shard)
}

// Observe returns what the Reaper knows of shard at now.
func (r *Reaper) Observe(shard int, now time.Time) Observation {
	var o Observation
	o.Failing = r.failures.failingFor(shard, now)
	o.Lag, o.LagOK = r.lag.of(shard)
	o.CleanSweep = r.walks.get(walkKey{shard, KindClosed, true}).cleanSweep
	cleanup := r.cleanup.get(shard)
	o.Pending, o.PendingUnknown, o.PendingOK = cleanup.pending, cleanup.unknown, cleanup.observed
	for i, kind := range []string{KindClosed, KindDeleted, KindRetention} {
		w := r.walks.get(walkKey{shard, kind, true})
		k := KindObservation{Kind: kind, Held: max(w.failed, w.lastFailed)}
		if w.oldest > 0 {
			k.OldestHeld = w.oldest + max(now.Sub(w.observed), 0)
		}
		if !w.started.IsZero() {
			k.Sweep = max(now.Sub(w.started), 0)
		}
		for _, a := range []struct {
			age  time.Duration
			seen time.Time
		}{{w.attemptAge, w.attemptSeen}, {w.lastAttemptAge, w.lastAttemptSeen}} {
			if a.age > 0 {
				k.OldestAttempt = max(k.OldestAttempt, a.age+max(now.Sub(a.seen), 0))
			}
		}
		o.Kinds[i] = k
	}
	return o
}
