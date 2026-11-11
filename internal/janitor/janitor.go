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

A Janitor serves one shard on one replica. Build one when the replica starts dispatching the shard, and stop it when
the replica stops, or shuts down:

	j := janitor.New(janitor.Config{
		Shard:     shard,
		DB:        db,
		Turn:      turn,     // a turn on the shard's connections, for one batch
		Settings:  settings, // read at every round
		Archive:   archive,
		Residency: residency,
	})
	j.Start(ctx)
	...
	j.Stop()

A job is a loop of rounds. A round that reports progress is followed at once by the next; one that does not is
followed by an idle wait (Config.Idle). A round does its work in batches, each through the Batch it is handed: a turn
taken fresh for the batch and one connection, taken from the pool once, for all of its statements.

For a test that drives the jobs itself, Config.Manual starts no goroutine, and Drain runs the jobs' rounds until they
stop making progress. For the gauges, Observe reports how long since each job last made progress.

Concurrency: Observe may be called at any time, and Drain beside the goroutines Start runs: one job's rounds never
overlap.
*/
package janitor

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/metric"
)

// DefaultIdle is how long a job waits after a round that made no progress, when Config.Idle is 0.
const DefaultIdle = time.Second

// The reason a closed tree is deleted without archiving, the reason attribute of dwarf_flows_reaped.
const ReasonNoRetention = "no_retention"

// The faults and checkpoints the jobs consult, inert unless Config.Seams is enabled. A targeted fault is the name
// joined with a root's flow id by a colon.
const (
	FaultMidTree    = "reapMidTree"    // a move fails after its steps, before its flows; optionally targeted
	FaultSizeErr    = "reapSizeErr"    // sizing a batch fails; targeted
	FaultSelectErr  = "reapSelectErr"  // a scan of due roots fails
	FaultContention = "reapContention" // an archive move fails on lock contention
	FaultArchiveErr = "reapArchiveErr" // an archive move fails with an error that is not lock contention
	FaultConnErr    = "janitorConnErr" // a batch cannot take its connection

	CheckpointBatchLocked = "reapBatchLocked" // a batch has locked its due roots, inside its transaction
)

// Config is a Janitor's settings, fixed for its life.
type Config struct {
	// Shard is the shard's number.
	Shard int
	// DB is the shard's handle.
	DB *sequel.DB
	// Turn takes a turn on the shard's connections for one batch, and returns the context the batch runs on and the
	// release. It is called with a context that carries no turn claim, so each batch's claim is new.
	Turn func(ctx context.Context) (context.Context, func())
	// Logger is where the jobs log; nil logs nowhere.
	Logger *slog.Logger
	// Instruments records the batches' timings; nil records nothing.
	Instruments *Instruments
	// Idle is how long a job waits after a round that made no progress; 0 is DefaultIdle.
	Idle time.Duration
	// Manual starts no goroutine: the jobs run only in Drain.
	Manual bool
	// Settings returns the settings of the janitor's own jobs, read at the start of every round with the round's
	// context. Nil runs none of them: only Jobs.
	Settings func(ctx context.Context) Settings
	// Archive is the shard's archive, or nil when it has none: closed roots then stay live at a positive retention.
	Archive *archiver.Archive
	// Residency returns this replica's residue class among the shard's dispatchers, re-read every round: the count of
	// dispatchers and this one's ordinal. Nil, ok false, a count under 2 or an ordinal out of range restricts nothing.
	Residency func() (count, ordinal int, ok bool)
	// Seams are the faults and checkpoints the jobs consult; nil consults none.
	Seams *seamster.Seamster
	// Jobs are run beside the janitor's own.
	Jobs []Job
}

// Settings are the settings of the janitor's own jobs.
type Settings struct {
	// Grace is how long after its close a closed root is archived.
	Grace time.Duration
	// RetentionDays is how many days the archive keeps; 0 keeps no archive, and closed trees are deleted.
	RetentionDays int
	// BatchRoots is how many due roots one batch considers.
	BatchRoots int
	// BatchRows is about how many rows, flows and steps alike, one transaction moves, and at least one tree.
	BatchRows int
	// BatchBytes is about how many payload bytes one transaction moves, and at least one tree; 0 sets no byte limit.
	BatchBytes int
	// StageRows is the size, in rows, above which a closed tree is archived in stages rather than in one transaction,
	// and passed by fast archiving; 0 stages none.
	StageRows int
	// StageBytes is the size, in payload bytes, above which a closed tree is archived in stages, when StageRows
	// stages any; 0 sets no byte threshold.
	StageBytes int
	// NoQuietMargin lets a round archive into its day however near the day's end.
	NoQuietMargin bool
}

// Job is one of a janitor's jobs: a name, the job's label in logs and metrics, and its round.
type Job struct {
	Name string
	// Round does one round of the job's work through batch, and reports whether it made progress: work done that
	// moved the job forward. A round whose candidates were all held or failed made none. A round runs on the job's
	// context, which ends when the janitor stops.
	Round func(ctx context.Context, batch Batch) (progress bool)
}

// Batch runs fn as one batch: under a turn on the shard taken for it, on one connection taken from the shard's pool
// for it, with the batch's timings recorded. fn runs every statement of the batch on h and the context it is given -
// reading each query's rows to the end before the next statement - and the connection and the turn are returned when
// fn does. A batch that cannot take its connection fails without calling fn. ctx must carry no turn claim - pass the round's own context, never a batch's - or the batch fails: the claim
// it carries would keep its age, and a turn taken while another is held deadlocks against the shard's other holders.
type Batch func(ctx context.Context, fn func(ctx context.Context, h Handle) error) error

// Handle is what a batch runs its statements on.
type Handle interface {
	sequel.Executor
	Transact(ctx context.Context, fn func(tx *sequel.Tx) error) error
}

// Observation is what a Janitor knows, for its gauges.
type Observation struct {
	// Jobs holds a reading of every job, in the order of the jobs.
	Jobs []Reading
	// Lag is how long past the grace the oldest due closed root after fast archiving's position had waited at its last
	// scan; LagOK is false before the first.
	Lag   time.Duration
	LagOK bool
	// Failing is how long archiving has been failing, or 0.
	Failing time.Duration
}

// Reading is what Observe reports of one job.
type Reading struct {
	Job string
	// SinceProgress is how long since the job's last round that made progress, or since Start.
	SinceProgress time.Duration
}

// Janitor runs one shard's jobs.
type Janitor struct {
	cfg    Config
	jobs   []*job
	fast   *fastArchiving
	del    *fastDeletion
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// job is a Job with what the janitor keeps of it.
type job struct {
	Job
	// mu is held across a round, so a Drain and the job's goroutine never run one at once.
	mu sync.Mutex
	// progress is the Unix time in nanoseconds of the last round that made progress, or of Start.
	progress atomic.Int64
	// The attribute sets of the job's batch timings, made once.
	turnAttrs    metric.MeasurementOption
	acquireAttrs metric.MeasurementOption
	workAttrs    metric.MeasurementOption
}

// New returns a Janitor for cfg, not started.
func New(cfg Config) *Janitor {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Idle <= 0 {
		cfg.Idle = DefaultIdle
	}
	if cfg.Seams == nil {
		cfg.Seams = seamster.New(false)
	}
	j := &Janitor{cfg: cfg}
	jobs := slices.Clone(cfg.Jobs)
	if cfg.Settings != nil {
		j.fast = &fastArchiving{j: j}
		j.del = &fastDeletion{j: j}
		jobs = append([]Job{
			{Name: JobFastArchiving, Round: j.fast.round},
			{Name: JobFastDeletion, Round: j.del.round},
		}, jobs...)
	}
	for _, jb := range jobs {
		j.jobs = append(j.jobs, &job{
			Job:          jb,
			turnAttrs:    batchAttrs(cfg.Shard, jb.Name, partTurn),
			acquireAttrs: batchAttrs(cfg.Shard, jb.Name, partAcquire),
			workAttrs:    batchAttrs(cfg.Shard, jb.Name, partWork),
		})
	}
	return j
}

// Start starts the jobs, each on a goroutine of its own unless the janitor is Manual, on a child of ctx that Stop
// cancels. Every job counts as having made progress at Start. Call it once.
func (j *Janitor) Start(ctx context.Context) {
	j.ctx, j.cancel = context.WithCancel(ctx)
	now := time.Now().UnixNano()
	for _, jb := range j.jobs {
		jb.progress.Store(now)
	}
	if j.cfg.Manual {
		return
	}
	for _, jb := range j.jobs {
		j.wg.Go(func() {
			j.run(j.ctx, jb)
		})
	}
}

// Stop cancels the jobs and returns once their goroutines have. A batch in flight is abandoned: its statements are
// cancelled, and a transaction is rolled back.
func (j *Janitor) Stop() {
	if j.cancel != nil {
		j.cancel()
	}
	j.wg.Wait()
}

// Drain runs the jobs' rounds, each job's until a round makes no progress, over and over until a pass over every job
// makes none, and returns there: where a running job would wait. A job whose candidates keep failing or are held
// therefore does not keep it going. It returns early when ctx ends or the janitor stops.
func (j *Janitor) Drain(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if j.ctx != nil {
		defer context.AfterFunc(j.ctx, cancel)()
	}
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
	if j.fast != nil && !start.IsZero() {
		j.fast.clearUnlessFailedSince(start)
	}
}

// Observe reports what the janitor knows at now: how long since each job last made progress, and archiving's lag and
// failure.
func (j *Janitor) Observe(now time.Time) Observation {
	var o Observation
	for _, jb := range j.jobs {
		at := jb.progress.Load()
		if at == 0 {
			continue // not started
		}
		o.Jobs = append(o.Jobs, Reading{Job: jb.Name, SinceProgress: max(now.Sub(time.Unix(0, at)), 0)})
	}
	if j.fast != nil {
		j.fast.observe(&o, now)
	}
	return o
}

// run is one job's goroutine: rounds, back to back while they make progress, an idle wait after one that does not.
func (j *Janitor) run(ctx context.Context, jb *job) {
	idle := time.NewTimer(0)
	defer idle.Stop()
	<-idle.C
	for ctx.Err() == nil {
		if j.round(ctx, jb) {
			continue
		}
		idle.Reset(j.cfg.Idle)
		select {
		case <-ctx.Done():
			return
		case <-idle.C:
		}
	}
}

// round runs one round of jb, and records the time if it made progress.
func (j *Janitor) round(ctx context.Context, jb *job) bool {
	jb.mu.Lock()
	defer jb.mu.Unlock()
	if !jb.Round(ctx, j.batch(jb)) {
		return false
	}
	jb.progress.Store(time.Now().UnixNano())
	return true
}

// batch returns jb's Batch.
func (j *Janitor) batch(jb *job) Batch {
	return func(ctx context.Context, fn func(ctx context.Context, h Handle) error) error {
		if _, stamped := turnstile.ClaimFrom(ctx); stamped {
			return errors.New("a batch's context carries a turn claim already")
		}
		start := time.Now()
		turnCtx, release := j.cfg.Turn(ctx)
		defer release()
		turned := time.Now()
		conn, err := j.cfg.DB.Conn(turnCtx)
		if err == nil && j.cfg.Seams.IsFault(FaultConnErr) {
			conn.Close()
			conn, err = nil, errors.New("injected fault: "+FaultConnErr)
		}
		acquired := time.Now()
		if err != nil {
			j.cfg.Instruments.batch(ctx, jb, turned.Sub(start), acquired.Sub(turned), -1)
			return errors.Trace(err)
		}
		defer conn.Close()
		err = fn(turnCtx, conn)
		j.cfg.Instruments.batch(ctx, jb, turned.Sub(start), acquired.Sub(turned), time.Since(acquired))
		return err
	}
}
