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
	"strings"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// Persisting a step's outcome is the one write the engine cannot simply give up on. The task has ALREADY RUN -
// its side effects have fired - so the only record that it ran is the write we are holding. Before this existed,
// a database error here left the step `running` with `error=”` and `attempt=0` (reading as perfectly healthy),
// and lease recovery re-dispatched it every `budget + leaseMargin`, RE-EXECUTING THE TASK, forever. A silent,
// eternal loop, invisible to detectOrphanedFlows because a non-terminal step does exist.
//
// The fix is to retry the WRITE, never the task:
//
//  1. Retry in place, holding the lease. An ephemeral database error (a failover, a dropped connection, a
//     momentary connection-limit rejection) clears in seconds, so a short exponential lands the write with ZERO
//     re-execution - which is the whole point, and what re-dispatching could never give us.
//  2. If the retries are exhausted, ask the database to classify the failure for us - see failOnPersistError.
//
// Lock contention is NOT handled here: `sequel.Transact` already retries it to exhaustion, and re-litigating it
// with a second loop would terminalize flows during a contention storm - exactly backwards.
const (
	// persistLeaseExtensionMs holds the step across the retry window. Without it a task that consumed most of
	// its time budget has only `leaseMargin` left, so sleeping through the backoff would put us PAST the lease:
	// a peer re-claims the step, re-executes the task, and our late write is fenced and discarded - precisely
	// the re-execution this exists to prevent. It is deliberately modest (the whole window is ~7s): a crash
	// mid-retry strands the step for this long before lease recovery can take it.
	persistLeaseExtensionMs = 30_000
)

// persistBackoff is short and exponential because the errors it exists for resolve in SECONDS. A minutes-long
// backoff would be slow in both directions - slow to recover from a blip that already cleared, and slow to
// report a permanent failure we could have named after the second attempt. (A `var`, not a `const`, only so a
// test can shorten it - the same reason awaitPollInterval and deletionGrace are.)
var persistBackoff = []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}

var (
	// errPersistFenced: a peer re-claimed the step while we were failing. We are a zombie; abandon silently.
	errPersistFenced = errors.New("step lease was re-granted to a peer")
	// errPersistDrained: the engine is shutting down. We released the lease so a peer can take the step now
	// rather than waiting out its expiry.
	errPersistDrained = errors.New("engine is draining; step lease released")
)

// persist runs a step-persistence write and, on a non-contention database error, retries THE WRITE - never the
// task - in place while holding the lease.
//
// write must be idempotent with respect to the database (all callers re-run one fenced UPDATE or one whole
// transaction, both of which re-derive their own state), and must not re-execute the task.
//
// Returns nil when the write landed. errPersistFenced / errPersistDrained are terminal, benign, and the caller
// simply returns. Any other error means the write kept failing and the caller must classify it - see
// failOnPersistError.
func (e *Engine) persist(ctx context.Context, db *sequel.DB, shardNum, stepID, leaseSeq int, write func() error) error {
	err := write()
	if err == nil {
		return nil
	}
	if sequel.IsLockContentionError(err) {
		// Transact already retried this to exhaustion; a second loop here would only prolong a contention storm.
		return errors.Trace(err)
	}

	// Extend the lease BEFORE sleeping, so the backoff cannot outlive our ownership of the step. This write
	// carries no payload, so it is not subject to whatever is killing the real one.
	//
	// A zero-row match (the query SUCCEEDED but matched nothing) means our generation is stale - a peer already
	// re-claimed the step and is re-running it. Abandon silently: retrying would be a zombie's write, and the
	// fence would reject it anyway. An ERROR here means the database is unreachable, which is exactly the case
	// the retry loop is for - so fall through and try, with whatever lease we still hold (the margin is 30s and
	// the window is ~7s, so there is room unless the task burned its entire budget).
	res, xerr := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE step_id=? AND lease_seq=?",
		persistLeaseExtensionMs, stepID, leaseSeq,
	)
	if xerr == nil {
		if n, _ := res.RowsAffected(); n == 0 {
			return errPersistFenced
		}
	}

	for _, backoff := range persistBackoff {
		select {
		case <-e.drainStop:
			// Shutting down. Hand the step back NOW rather than making a peer wait out the lease we just
			// extended. This DOES re-execute the task on the peer - it is the at-least-once contract, and it is
			// what would have happened at lease expiry anyway, only sooner.
			e.releaseLease(ctx, db, stepID, leaseSeq)
			return errPersistDrained
		case <-time.After(backoff):
		}
		e.metricStepWriteRetried(ctx, shardNum)
		err = write()
		if err == nil {
			return nil
		}
		if sequel.IsLockContentionError(err) {
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
}

// releaseLease hands a step back for immediate re-dispatch, fenced on our lease generation. It resets from
// `running` (the task ran but its outcome never landed) or `completed` (the outcome landed but the transition
// did not) - the two states a worker can be holding a step in. A terminal step (cancelled by a racing Cancel) is
// deliberately not matched: it is immutable.
func (e *Engine) releaseLease(ctx context.Context, db *sequel.DB, stepID, leaseSeq int) {
	_, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC()"+
			" WHERE step_id=? AND lease_seq=? AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusCompleted+"')",
		workflow.StatusPending, stepID, leaseSeq,
	)
	if err != nil {
		// Best-effort: if we cannot even release it, the lease will lapse and recovery takes it. No worse than
		// not trying.
		e.logger.DebugContext(ctx, "Releasing step lease on drain", "step", stepID, "error", err)
	}
}

// failOnPersistError decides whether a write that would not land is PERMANENT or TRANSIENT - and it does so
// without a per-driver error taxonomy, by asking the database itself.
//
// It attempts the CLEAN terminal write (failStep records a status and an error message; it carries none of the
// payload that the failing write carried):
//
//   - the clean write LANDS -> the database is reachable, so the database is not the problem: the PAYLOAD is.
//     The failure is permanent, and re-running the task would only reproduce it. The step fails, naming the
//     driver's actual error, and the flow terminates honestly. The task has run exactly once.
//   - the clean write ALSO FAILS -> the database is unreachable, so nothing at all was recorded and nothing can
//     be. Leave the step exactly as it is; lease recovery re-dispatches it when the database returns
//     (`dwarf_steps_recovered` counts that). Re-execution here is unavoidable AND correct - from the database's
//     point of view the step never ran.
//
// The two attempts are milliseconds apart and differ only in what they carry, which is what makes the
// classification sound. Guessing the other way - terminalizing on any unknown error - would kill live flows on
// every routine failover, and a terminal flow is immutable (recovery is a human running Fork).
func (e *Engine) failOnPersistError(ctx context.Context, shardNum, stepID, leaseSeq, flowID int, flowToken string, writeErr error, taskName string) error {
	fenced, ferr := e.failStep(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, writeErr, taskName)
	if ferr != nil {
		// Cannot write even a payload-free failure: the DATABASE is down, not the payload. Surface the original
		// error and leave the step to lease recovery.
		e.logger.ErrorContext(ctx, "Persisting step outcome failed and the step could not be failed either; leaving it for lease recovery",
			"step", stepID, "task", taskName, "writeError", writeErr, "failError", ferr)
		return errors.Trace(writeErr)
	}
	if fenced {
		return nil
	}
	// A nonzero dwarf_steps_write_failed means a write the engine could not land was permanent - a latent bug
	// (an unstorable payload, a column/packet limit, a constraint violation), not a transient blip. It is an
	// alarm, like dwarf_steps_unwedged.
	e.metricStepWriteFailed(ctx, taskName)
	e.logger.ErrorContext(ctx, "Step failed: its outcome could not be persisted and the database is reachable, so the failure is permanent",
		"step", stepID, "task", taskName, "error", writeErr)
	return nil
}

// sanitizeErrorMessage strips control bytes from a message bound for a text column. A driver error can quote the
// offending value back at you, so the message carrying "this value is unstorable" can itself be unstorable - and
// on Postgres a NUL is rejected in `text` just as it is in `jsonb`. Replacement, not rejection: the message is
// diagnostic, and a mangled one still names the failure.
func sanitizeErrorMessage(msg string) string {
	if !strings.ContainsFunc(msg, func(r rune) bool { return r < 0x20 && r != '\n' && r != '\t' }) {
		return msg
	}
	return strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\n' && r != '\t' {
			return '�'
		}
		return r
	}, msg)
}

// persistStep is persist plus the disposition every caller in processStep shares: a lost lease or a drain is a
// benign no-op, lock contention goes to the recovery defer (never the classifier - terminalizing a flow because the
// database was busy is exactly backwards), and anything else that would not land is classified.
func (e *Engine) persistStep(ctx context.Context, db *sequel.DB, shardNum, stepID, leaseSeq, flowID int, flowToken, taskName string, write func() error) error {
	err := e.persist(ctx, db, shardNum, stepID, leaseSeq, write)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errPersistFenced), errors.Is(err, errPersistDrained):
		return nil // a peer owns the step now; abandon it silently
	case sequel.IsLockContentionError(err):
		return errors.Trace(err) // the recovery defer rewinds and re-polls
	default:
		return errors.Trace(e.failOnPersistError(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName))
	}
}
