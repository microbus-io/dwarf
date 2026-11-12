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
Package janitor runs one shard's background jobs on the live and archive tables, each job on a goroutine of its own.
Its jobs are fast archiving - closed trees moved into the archive, or deleted at a retention of 0, as they come due -
and fast deletion - the trees stamped for deletion deleted as they come due.

A Janitor serves one shard on one replica for the replica's run. It works only while it is active - while the replica
dispatches the shard - and a paused one holds no goroutine and no connection:

	j := janitor.New(shard, diags, archive, retentionDays)
	if err := j.SetMeter(meter); err != nil { ... }
	j.Start(ctx)
	j.Resume() // this replica dispatches the shard
	j.Pause()  // it stopped dispatching the shard
	...
	j.Stop()

Each job is a loop of rounds: one that makes progress is followed at once by the next, one that does not by the job's
idle wait. A round works in batches, each under a turn on the shard taken fresh for it, and on one connection, taken
from the pool once, for all of its statements.

For a test that drives the jobs itself, SetManual starts no goroutine, and Drain runs the jobs' rounds until they
stop making progress.

Concurrency: every method may be called from any goroutine, a Drain beside the job goroutines included: one job's
rounds never overlap.
*/
package janitor

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// The reason a closed tree is deleted without archiving, the reason attribute of dwarf_flows_reaped.
const ReasonNoRetention = "no_retention"

// Janitor runs one shard's jobs while it is active.
type Janitor struct {
	shard         *database.Shard
	archive       *archiver.Archive
	retentionDays int
	diags         *diagnostics.Diags
	metrics       *instruments
	manual        bool
	idle          time.Duration // replaces every job's idle wait when not 0

	jobs []*job
	fast *fastArchiving
	del  *fastDeletion

	// mu guards the settings, which the jobs read at every round.
	mu       sync.Mutex
	settings settings

	// life guards the lifecycle: the run's context, whether the janitor is active, the active period's context.
	life     sync.Mutex
	run      context.Context
	active   context.Context
	cancel   context.CancelFunc
	stopped  bool
	routines sync.WaitGroup
}

// settings are what the jobs read at every round.
type settings struct {
	grace                             time.Duration
	batchRoots, batchRows, batchBytes int
	stageRows, stageBytes             int
	noQuietMargin                     bool
}

// residency is this replica's residue class among the shard's dispatchers, as the shard reports it at a round.
type residency struct {
	count, ordinal int
	ok             bool
}

// job is one of the janitor's jobs: its name, its label in logs and metrics; how long it waits after a round without
// progress; its round, which does one round of work through batch and reports whether it made progress; and its
// reset, which forgets where the job stood.
type job struct {
	name  string
	idle  time.Duration
	round func(ctx context.Context, batch batchFunc) (progress bool)
	reset func()
	// mu is held across a round and a reset, so a Drain, the job's goroutine and a Resume never overlap one.
	mu sync.Mutex
	// progress is the Unix time in nanoseconds of the last round that made progress, or of the last Resume.
	progress atomic.Int64
}

// batchFunc runs fn as one batch: under a turn on the shard taken for it, on one connection taken from the pool for
// it, with the batch's timings recorded. fn runs every statement of the batch on h and the context it is given -
// reading each query's rows to the end before the next statement - and the connection and the turn are returned when
// fn does. A batch that cannot take its connection fails without calling fn. ctx must carry no turn claim - the
// round's own context, never a batch's - or the batch fails: the claim would keep its age, and a turn taken while
// another is held deadlocks against the shard's other holders.
type batchFunc func(ctx context.Context, fn func(ctx context.Context, h handle) error) error

// handle is what a batch runs its statements on.
type handle interface {
	sequel.Executor
	Transact(ctx context.Context, fn func(tx *sequel.Tx) error) error
}

// The default settings, the engine's.
const (
	defaultGrace      = time.Minute
	defaultBatchRoots = 512
	defaultBatchRows  = 4096
	defaultBatchBytes = 16 << 20
)

// New returns a paused Janitor of shard, reporting through d - its logger, its seams, and its meter, on which it
// creates dwarf_janitor_batch_seconds, dwarf_flows_archived, dwarf_archive_move_failures and dwarf_flows_reaped;
// an instrument it cannot create is logged and records nothing. Its archive is archive - nil when the shard has
// none, and closed roots then stay live at a positive retention - keeping retentionDays of archive: 0 keeps none,
// and closed trees are deleted.
func New(shard *database.Shard, d *diagnostics.Diags, archive *archiver.Archive, retentionDays int) *Janitor {
	j := &Janitor{
		shard:         shard,
		archive:       archive,
		retentionDays: retentionDays,
		diags:         d,
		settings: settings{
			grace:      defaultGrace,
			batchRoots: defaultBatchRoots,
			batchRows:  defaultBatchRows,
			batchBytes: defaultBatchBytes,
		},
	}
	m, err := newInstruments(d.Meter())
	if err != nil {
		j.diags.Logger().Error("Creating the janitor's instruments", "shard", shard.Index(), "error", err)
	}
	j.metrics = m
	j.fast = &fastArchiving{j: j}
	j.del = &fastDeletion{j: j}
	j.jobs = []*job{
		{name: JobFastArchiving, idle: time.Second, round: j.fast.round, reset: j.fast.reset},
		{name: JobFastDeletion, idle: time.Second, round: j.del.round, reset: j.del.reset},
	}
	return j
}

// SetGrace sets how long after its close a closed root is archived (a minute).
func (j *Janitor) SetGrace(d time.Duration) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.settings.grace = d
}

// SetBatch sets one batch: how many due roots it considers (512), and about how many rows, flows and steps alike
// (4,096), and payload bytes (16 MiB; 0 sets no byte limit) one transaction moves - at least one tree.
func (j *Janitor) SetBatch(roots, rows, bytes int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.settings.batchRoots, j.settings.batchRows, j.settings.batchBytes = roots, rows, bytes
}

// SetStaging sets the size above which a closed tree is archived in stages rather than in one transaction, and passed
// by fast archiving: in rows (0, none staged), and in payload bytes beside it (0, no byte threshold).
func (j *Janitor) SetStaging(rows, bytes int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.settings.stageRows, j.settings.stageBytes = rows, bytes
}

// SetQuietMargin sets whether archiving stops a margin before its day ends (on): off, a round archives into its day
// however near the day's end. For tests.
func (j *Janitor) SetQuietMargin(on bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.settings.noQuietMargin = !on
}

// SetManual sets whether the janitor runs its jobs only in Drain, starting no goroutine. For tests; call it before
// Start.
func (j *Janitor) SetManual(manual bool) {
	j.manual = manual
}

// SetIdle replaces every job's wait after a round without progress; 0 restores each job's own. For tests; call it
// before Start.
func (j *Janitor) SetIdle(d time.Duration) {
	j.idle = d
}

// current returns the settings and the residue class the shard reports (Shard.Residency): ok false, a count under
// 2 or an ordinal out of range restricts nothing, and a class other than the one a fast job walked in makes it start
// over.
func (j *Janitor) current() (settings, residency) {
	count, ordinal, ok := j.shard.Residency()
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.settings, residency{count, ordinal, ok}
}

// Start begins the janitor's run on ctx, paused; Stop, or ctx's end, ends it. Call it once.
func (j *Janitor) Start(ctx context.Context) {
	j.life.Lock()
	defer j.life.Unlock()
	j.run = ctx
}

// Resume makes the janitor active: its jobs start over - every position forgotten, since a replica that dispatches
// the shard again saw nothing of what happened meanwhile - each on a goroutine of its own unless it is Manual. A no-op
// while active, before Start and after Stop.
func (j *Janitor) Resume() {
	j.life.Lock()
	defer j.life.Unlock()
	if j.run == nil || j.stopped || j.active != nil {
		return
	}
	// The last active period's goroutines are cancelled already; they must be gone before the jobs start over.
	j.routines.Wait()
	now := time.Now().UnixNano()
	for _, jb := range j.jobs {
		jb.mu.Lock()
		jb.reset()
		jb.progress.Store(now)
		jb.mu.Unlock()
	}
	j.active, j.cancel = context.WithCancel(j.run)
	if j.manual {
		return
	}
	active := j.active
	for _, jb := range j.jobs {
		j.routines.Go(func() {
			j.loop(active, jb)
		})
	}
}

// Pause makes the janitor inactive: its jobs are cancelled, and a batch in flight is abandoned - its statement
// cancelled, its transaction rolled back. It does not wait for them.
func (j *Janitor) Pause() {
	j.life.Lock()
	defer j.life.Unlock()
	if j.active == nil {
		return
	}
	j.cancel()
	j.active, j.cancel = nil, nil
}

// Stop ends the run: it pauses the janitor and returns once its goroutines have. A stopped janitor never resumes.
func (j *Janitor) Stop() {
	j.life.Lock()
	j.stopped = true
	if j.cancel != nil {
		j.cancel()
	}
	j.active, j.cancel = nil, nil
	j.life.Unlock()
	j.routines.Wait()
}

// activeContext returns the active period's context, or nil while paused.
func (j *Janitor) activeContext() context.Context {
	j.life.Lock()
	defer j.life.Unlock()
	return j.active
}

// Drain runs the jobs' rounds while the janitor is active, each job's until a round makes no progress, over and over
// until a pass over every job makes none, and returns there: where a running job would wait. A job whose candidates
// keep failing or are held therefore does not keep it going. It returns early when ctx ends or the janitor pauses.
func (j *Janitor) Drain(ctx context.Context) {
	active := j.activeContext()
	if active == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(active, cancel)()
	for ctx.Err() == nil {
		progress := false
		for _, jb := range j.jobs {
			for ctx.Err() == nil && j.round(ctx, jb) {
				progress = true
			}
		}
		if !progress {
			return
		}
	}
}

// SweptClean reports a sweep of every due closed root, begun at start, that failed on none: it ends fast archiving's
// failure episode unless a failure was recorded at or after start. A root a failure left behind fast archiving's
// position is the sweep's, so only a clean sweep, or a later move, says archiving has recovered.
func (j *Janitor) SweptClean(start time.Time) {
	if !start.IsZero() {
		j.fast.clearUnlessFailedSince(start)
	}
}

// ArchiveLag returns how long past the grace the oldest due closed root after fast archiving's position had waited at
// its last scan, and false before its first scan since the janitor resumed.
func (j *Janitor) ArchiveLag() (lag time.Duration, ok bool) {
	return j.fast.lagReading()
}

// ArchiveFailing returns how long archiving has been failing at now, or 0.
func (j *Janitor) ArchiveFailing(now time.Time) time.Duration {
	return j.fast.failingFor(now)
}

// JobAges returns, by job, how long at now since its last round that made progress, or since the janitor resumed.
func (j *Janitor) JobAges(now time.Time) map[string]time.Duration {
	ages := make(map[string]time.Duration, len(j.jobs))
	for _, jb := range j.jobs {
		if at := jb.progress.Load(); at != 0 {
			ages[jb.name] = max(now.Sub(time.Unix(0, at)), 0)
		}
	}
	return ages
}

// loop is one job's goroutine: rounds, back to back while they make progress, its idle wait after one that does not.
func (j *Janitor) loop(ctx context.Context, jb *job) {
	idle := jb.idle
	if j.idle > 0 {
		idle = j.idle
	}
	wait := time.NewTimer(0)
	defer wait.Stop()
	<-wait.C
	for ctx.Err() == nil {
		if j.round(ctx, jb) {
			continue
		}
		wait.Reset(idle)
		select {
		case <-ctx.Done():
			return
		case <-wait.C:
		}
	}
}

// round runs one round of jb, and records the time if it made progress.
func (j *Janitor) round(ctx context.Context, jb *job) bool {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	if !jb.round(ctx, j.batch(jb)) {
		return false
	}
	jb.progress.Store(time.Now().UnixNano())
	return true
}

// batch returns jb's batchFunc.
func (j *Janitor) batch(jb *job) batchFunc {
	return func(ctx context.Context, fn func(ctx context.Context, h handle) error) error {
		if database.HasClaim(ctx) {
			return errors.New("a batch's context carries a turn claim already")
		}
		start := time.Now()
		ctx, pass := j.shard.WaitTurn(ctx)
		defer pass.Return()
		turned := time.Now()
		conn, err := j.shard.DB().Conn(ctx)
		if err == nil && j.diags.Seams().IsFault(diagnostics.FaultJanitorConnErr) {
			conn.Close()
			conn, err = nil, errors.New("injected fault: "+diagnostics.FaultJanitorConnErr)
		}
		acquired := time.Now()
		if err != nil {
			j.metrics.batch(ctx, j.shard.Index(), jb.name, turned.Sub(start), acquired.Sub(turned), -1)
			return errors.Trace(err)
		}
		defer conn.Close()
		j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointJanitorBatchConn)
		err = fn(ctx, conn)
		j.metrics.batch(ctx, j.shard.Index(), jb.name, turned.Sub(start), acquired.Sub(turned), time.Since(acquired))
		return err
	}
}
