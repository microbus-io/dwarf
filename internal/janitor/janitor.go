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
Its jobs are fast archiving (NewFastArchiving) - closed trees moved into the archive, or deleted at a retention of 0,
as they come due - large-tree archiving (NewLargeArchiving) - trees too large for one transaction copied into the
archive in stages, then published - and fast deletion (NewFastDeletion) - the trees stamped for deletion deleted as
they come due.

A Janitor serves one shard on one replica for the replica's run. It works only while it is active - while the replica
dispatches the shard - and a paused one holds no goroutine and no connection:

	j := janitor.New(shard, diags)
	fa := janitor.NewFastArchiving(archive, retentionDays)
	fa.SetGrace(grace)
	j.AddJob(janitor.JobFastArchiving, fa)
	j.AddJob(janitor.JobFastDeletion, janitor.NewFastDeletion())
	j.Start(ctx)
	j.Resume() // this replica dispatches the shard
	j.Pause()  // it stopped dispatching the shard
	...
	j.Stop()

A job's settings are its own, set before it is added. To change them, add a new job under the same name: it
replaces the old one, which starts over.

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
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Janitor runs one shard's jobs while it is active.
type Janitor struct {
	shard        *database.Shard
	diags        *diagnostics.Diags
	batchSeconds metric.Float64Histogram
	manual       bool
	idle         time.Duration // replaces every job's idle wait when not 0

	// life guards the lifecycle and the jobs: the run's context, the active period's context while active, and the
	// jobs in the order they were added.
	life       sync.Mutex
	run        context.Context
	active     context.Context
	stopActive context.CancelFunc
	stopped    bool
	jobs       []*runningJob
}

// runningJob is a job added to a janitor, by its name.
type runningJob struct {
	name string
	job  Job
	// mu is held across a round and a reset, so a Drain, the job's goroutine and a Resume never overlap one.
	mu sync.Mutex
	// progress is the Unix time in nanoseconds of the last round that made progress, or of the last Resume.
	progress atomic.Int64
	// cancel ends the job's goroutine, and done waits for it.
	cancel context.CancelFunc
	done   sync.WaitGroup
}

// New returns a paused Janitor of shard, with no job, reporting through d: its logger, its seams, and its meter, on
// which it creates dwarf_janitor_batch_seconds and its jobs create theirs. An instrument that cannot be created is
// logged and records nothing.
func New(shard *database.Shard, d *diagnostics.Diags) *Janitor {
	j := &Janitor{shard: shard, diags: d}
	var err error
	j.batchSeconds, err = d.Meter().Float64Histogram("dwarf_janitor_batch_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time one batch of a janitor's job took, by shard, job and part: turn is the wait for a turn on the shard's connections, which the batch shares with dispatch; acquire is the wait for its one connection from the pool; work is its statements, every one on that connection. A job falling behind with most of its time in turn or acquire is held back by the shard's load; with most in work, by its statements."),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60))
	if err != nil {
		d.Logger().Error("Creating the janitor's instruments", "shard", shard.Index(), "error", err)
	}
	return j
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

// AddJob adds job under name, running it at once if the janitor is active. A job already under name is removed first,
// so adding a job is how its settings are changed: the new one starts over. A job is added to one janitor only, and
// nothing is added to a stopped janitor.
func (j *Janitor) AddJob(name string, job Job) {
	j.RemoveJob(name)
	// Attached and detached outside life: registering or unregistering a gauge waits for the meter's lock, which a
	// collection holds while it calls the gauge's callback, which takes life to ask whether the janitor is active.
	job.attach(j, name)
	j.life.Lock()
	if j.stopped {
		j.life.Unlock()
		job.detach()
		return
	}
	rn := &runningJob{name: name, job: job}
	if j.active != nil {
		rn.progress.Store(time.Now().UnixNano())
	}
	j.jobs = append(j.jobs, rn)
	if j.active != nil && !j.manual {
		j.goLocked(rn)
	}
	j.life.Unlock()
}

// RemoveJob removes the job under name, cancelling it - a batch in flight is abandoned, its statement cancelled and
// its transaction rolled back - and returns once its goroutine has. A name with no job is a no-op.
func (j *Janitor) RemoveJob(name string) {
	j.life.Lock()
	i := slices.IndexFunc(j.jobs, func(rn *runningJob) bool { return rn.name == name })
	if i < 0 {
		j.life.Unlock()
		return
	}
	rn := j.jobs[i]
	j.jobs = slices.Delete(j.jobs, i, i+1)
	if rn.cancel != nil {
		rn.cancel()
	}
	j.life.Unlock()
	rn.done.Wait()
	rn.job.detach()
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
	now := time.Now().UnixNano()
	for _, rn := range j.jobs {
		// The last active period's goroutine is cancelled already; it must be gone before the job starts over.
		rn.done.Wait()
		rn.mu.Lock()
		rn.job.reset()
		rn.progress.Store(now)
		rn.mu.Unlock()
	}
	j.active, j.stopActive = context.WithCancel(j.run)
	if j.manual {
		return
	}
	for _, rn := range j.jobs {
		j.goLocked(rn)
	}
}

// goLocked starts rn's goroutine in the active period. Called under life.
func (j *Janitor) goLocked(rn *runningJob) {
	ctx, cancel := context.WithCancel(j.active)
	rn.cancel = cancel
	rn.done.Go(func() {
		j.loop(ctx, rn)
	})
}

// Pause makes the janitor inactive: its jobs are cancelled, and a batch in flight is abandoned - its statement
// cancelled, its transaction rolled back. It does not wait for them.
func (j *Janitor) Pause() {
	j.life.Lock()
	defer j.life.Unlock()
	if j.active == nil {
		return
	}
	j.stopActive()
	j.active, j.stopActive = nil, nil
}

// Stop ends the run: it pauses the janitor, returns once its jobs' goroutines have, and releases what its jobs
// registered. A stopped janitor never resumes.
func (j *Janitor) Stop() {
	j.life.Lock()
	j.stopped = true
	if j.stopActive != nil {
		j.stopActive()
	}
	j.active, j.stopActive = nil, nil
	jobs := slices.Clone(j.jobs)
	j.life.Unlock()
	for _, rn := range jobs {
		rn.done.Wait()
		rn.job.detach()
	}
}

// isActive reports whether the janitor is active.
func (j *Janitor) isActive() bool {
	return j.activeContext() != nil
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
	j.life.Lock()
	active, jobs := j.active, slices.Clone(j.jobs)
	j.life.Unlock()
	if active == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(active, cancel)()
	for ctx.Err() == nil {
		progress := false
		for _, rn := range jobs {
			for ctx.Err() == nil && j.round(ctx, rn) {
				progress = true
			}
		}
		if !progress {
			return
		}
	}
}

// JobAges returns, by job, how long at now since its last round that made progress, or since the janitor resumed or
// the job was added to it while active; nothing for a job before then.
func (j *Janitor) JobAges(now time.Time) map[string]time.Duration {
	j.life.Lock()
	jobs := slices.Clone(j.jobs)
	j.life.Unlock()
	ages := make(map[string]time.Duration, len(jobs))
	for _, rn := range jobs {
		if at := rn.progress.Load(); at != 0 {
			ages[rn.name] = max(now.Sub(time.Unix(0, at)), 0)
		}
	}
	return ages
}

// loop is one job's goroutine: rounds, back to back while they make progress, its idle wait after one that does not.
func (j *Janitor) loop(ctx context.Context, rn *runningJob) {
	idle := rn.job.idleWait()
	if j.idle > 0 {
		idle = j.idle
	}
	wait := time.NewTimer(0)
	defer wait.Stop()
	<-wait.C
	for ctx.Err() == nil {
		if j.round(ctx, rn) {
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

// round runs one round of rn, and records the time if it made progress.
func (j *Janitor) round(ctx context.Context, rn *runningJob) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if !rn.job.round(ctx, j.batch(rn.name)) {
		return false
	}
	rn.progress.Store(time.Now().UnixNano())
	return true
}

// batch returns the batchFunc of the job named name.
func (j *Janitor) batch(name string) batchFunc {
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
			j.recordBatch(ctx, name, turned.Sub(start), acquired.Sub(turned), -1)
			return errors.Trace(err)
		}
		defer conn.Close()
		j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointJanitorBatchConn)
		err = fn(ctx, conn)
		j.recordBatch(ctx, name, turned.Sub(start), acquired.Sub(turned), time.Since(acquired))
		return err
	}
}

// The parts of a batch's time, the part attribute of dwarf_janitor_batch_seconds.
const (
	partTurn    = "turn"    // waiting for a turn on the shard's connections
	partAcquire = "acquire" // waiting for the batch's connection from the pool
	partWork    = "work"    // the batch's statements
)

// recordBatch records one batch's time in its parts; a negative work, for a batch that could not take its connection,
// records none.
func (j *Janitor) recordBatch(ctx context.Context, job string, turn, acquire, work time.Duration) {
	shard := attribute.String("shard", strconv.Itoa(j.shard.Index()))
	name := attribute.String("job", job)
	j.batchSeconds.Record(ctx, turn.Seconds(), metric.WithAttributes(shard, name, attribute.String("part", partTurn)))
	j.batchSeconds.Record(ctx, acquire.Seconds(), metric.WithAttributes(shard, name, attribute.String("part", partAcquire)))
	if work >= 0 {
		j.batchSeconds.Record(ctx, work.Seconds(), metric.WithAttributes(shard, name, attribute.String("part", partWork)))
	}
}
