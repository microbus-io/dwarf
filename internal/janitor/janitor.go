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

A Janitor serves one shard on one replica. Build one when the replica starts dispatching the shard, and stop it when
the replica stops, or shuts down:

	j := janitor.New(janitor.Config{
		Shard: shard,
		DB:    db,
		Turn:  turn, // a turn on the shard's connections, for one batch
	})
	j.Start(ctx)
	...
	j.Stop()

A job is a loop of rounds. A round that reports progress is followed at once by the next; one that does not is
followed by an idle wait (Config.Idle). A round does its work in batches, each through the Batch it is handed: a turn
taken fresh for the batch and one handle on the shard for all of its statements.

For a test that drives the jobs itself, Config.Manual starts no goroutine, and Drain runs the jobs' rounds until they
stop making progress. For the gauges, Observe reports how long since each job last made progress.

Concurrency: Observe may be called at any time, and Drain beside the goroutines Start runs: one job's rounds never
overlap.
*/
package janitor

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/metric"
)

// DefaultIdle is how long a job waits after a round that made no progress, when Config.Idle is 0.
const DefaultIdle = time.Second

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
	// Jobs are run beside the janitor's own.
	Jobs []Job
}

// Job is one of a janitor's jobs: a name, the job's label in logs and metrics, and its round.
type Job struct {
	Name string
	// Round does one round of the job's work through batch, and reports whether it made progress: work done that
	// moved the job forward. A round whose candidates were all held or failed made none. A round runs on the job's
	// context, which ends when the janitor stops.
	Round func(ctx context.Context, batch Batch) (progress bool)
}

// Batch runs fn as one batch: under a turn on the shard taken for it, on one handle, with the batch's timings
// recorded. fn runs every statement of the batch on h and the context it is given, and the turn is returned when fn
// does. ctx must carry no turn claim - pass the round's own context, never a batch's - or the batch fails: the claim
// it carries would keep its age, and a turn taken while another is held deadlocks against the shard's other holders.
type Batch func(ctx context.Context, fn func(ctx context.Context, h Handle) error) error

// Handle is what a batch runs its statements on.
type Handle interface {
	sequel.Executor
	Transact(ctx context.Context, fn func(tx *sequel.Tx) error) error
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
	turnAttrs metric.MeasurementOption
	workAttrs metric.MeasurementOption
}

// New returns a Janitor for cfg, not started.
func New(cfg Config) *Janitor {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Idle <= 0 {
		cfg.Idle = DefaultIdle
	}
	j := &Janitor{cfg: cfg}
	for _, jb := range cfg.Jobs {
		j.jobs = append(j.jobs, &job{
			Job:       jb,
			turnAttrs: batchAttrs(cfg.Shard, jb.Name, partTurn),
			workAttrs: batchAttrs(cfg.Shard, jb.Name, partWork),
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

// Observe reports how long since each job last made progress, in the order of the jobs.
func (j *Janitor) Observe(now time.Time) []Reading {
	readings := make([]Reading, 0, len(j.jobs))
	for _, jb := range j.jobs {
		at := jb.progress.Load()
		if at == 0 {
			continue // not started
		}
		readings = append(readings, Reading{Job: jb.Name, SinceProgress: max(now.Sub(time.Unix(0, at)), 0)})
	}
	return readings
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
		err := fn(turnCtx, j.handle())
		j.cfg.Instruments.batch(ctx, jb, turned.Sub(start), time.Since(turned))
		return err
	}
}

// handle is what a batch runs on: the shard's pool, so each statement takes a connection of its own.
func (j *Janitor) handle() Handle {
	return j.cfg.DB
}
