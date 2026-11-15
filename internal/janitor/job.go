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

// Job is a job a Janitor runs: one this package's constructors return - NewFastArchiving, NewRecovery and the rest.
type Job interface {
	// attach readies the job to run on j under name, creating its instruments on j's meter; name labels what they record.
	attach(j *Janitor, name string)
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

// batchFunc runs fn as one batch: under a turn on the shard taken for it, with the batch's timings recorded, and
// returns fn's error. fn runs every statement of the batch on h, the shard's DB, and the context it is given; each
// statement takes a connection from the pool and returns it, a transaction holds one until it ends, and the turn is
// returned when fn does. Inside a transaction every statement runs on its Tx, and a query's rows are read to the end
// before the next statement: either way a statement on h would take a second connection while one is held. ctx must
// carry no turn claim - the round's own context, never a batch's - or the batch fails without calling fn: the claim
// would keep its age, and a turn taken while another is held deadlocks against the shard's other holders.
type batchFunc func(ctx context.Context, fn func(ctx context.Context, h handle) error) error

// handle is what a batch runs its statements on: the shard's DB, or a test's.
type handle interface {
	sequel.Executor
	Transact(ctx context.Context, fn func(tx *sequel.Tx) error) error
}

// stealGrace is how long past due a root waits for the dispatcher whose residue class it is before any dispatcher's job
// takes it.
const stealGrace = 30 * time.Second

// The descriptions of the counters more than one job creates, each word for word the same in every one of them: two instruments of one name and different descriptions are exported as two streams of that name.
// The idle waits: how long a job waits after a round without progress. A fast job polls for work that keeps arriving.
// A repair job - a sweep, large-tree archiving - normally has none: polled every second it would cost a turn and its
// reads every second on every shard, and retry a root or tree that keeps failing every second - a
// large tree's failing chunk copied again each time. A round with progress is followed at once by the next whatever
// the wait, so a repair job with work is not slowed.
const (
	fastIdle   = time.Second
	repairIdle = 30 * time.Second
)

const (
	archivedDescription   = "Counts flows moved from the live tables into the archive, by shard: each root and every subgraph flow of its tree. Its rate plus dwarf_flows_reaped's, against dwarf_flows_closed, is archiving keeping up - a closed root waits the archive grace (a minute) in the live tables, so the two track each other a minute apart."
	stagedRowsDescription = "Counts rows a staged archive copied into the archive, by shard: the flows and steps of a tree too large to archive in one transaction, written chunk by chunk before it is published. A chunk run again counts only what was not there. Read beside dwarf_archive_oldest_attempt_seconds: an age that grows while this moves is a large tree copying; one that grows while this is flat is a copy that is stuck, and the logs say why."
	failingDescription    = "Per shard and archiving job: time since the job's archiving began failing - a fast_archiving job's ended by a later move that archives something, or once every root of its share due at its last failure has left the live tables; a sweep_archiving job's by a sweep that completes failing nothing; a large_archiving job's by a round over every started archive of its share that fails none. Retained trees remain live. Aggregate with max."
	reapedDescription     = "Counts flows deleted from the live tables WITHOUT archiving them, by shard and by reason: deleted (a tree Delete or DeleteOnCompletion scheduled for deletion, a tree expiration terminated, a tree a retention of 0 stamped for deletion as too large for one transaction, or a tree past its retention, counted under archive_failed too when it was stamped), no_retention (a retention of 0 keeps no archive), and archive_failed (the flows of an unarchived tree past its retention cutoff, counted when it is stamped for deletion). archive_failed is records LOST from the archive and must stay zero; dwarf_archive_failing_seconds says since when it has been failing, and the logs say why."
)
