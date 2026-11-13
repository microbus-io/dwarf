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

package janitor

import (
	"context"
	"time"

	"github.com/microbus-io/sequel"
)

// Job is a job a Janitor runs: one NewFastArchiving or NewFastDeletion returns.
type Job interface {
	// attach readies the job to run on j, creating its instruments on j's meter.
	attach(j *Janitor)
	// detach releases what attach registered.
	detach()
	// round does one round of work through batch and reports whether it made progress: work done that moved the
	// job forward.
	round(ctx context.Context, batch batchFunc) (progress bool)
	// reset forgets where the job stood and what it observed.
	reset()
	// idleWait is how long the job waits after a round without progress.
	idleWait() time.Duration
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

// stealGrace is how long past due a root waits for the dispatcher whose residue class it is before any dispatcher's job
// takes it.
const stealGrace = 30 * time.Second
