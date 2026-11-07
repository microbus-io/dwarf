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
	"math/rand/v2"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// Persisting a step's outcome is the one write the engine cannot simply give up on. The task has ALREADY RUN -
// its side effects have fired - so the only record that it ran is the write we are holding. Left unhandled, a
// database error here leaves the step `running` with `error=”` and `attempt=0` (reading as perfectly healthy),
// and lease recovery re-dispatches it every `budget + leaseMargin`, RE-EXECUTING THE TASK, forever. A silent,
// eternal loop, invisible to recoverOrphanedFlows because a non-terminal step does exist.
//
// The fix is to retry the WRITE, never the task:
//
//  1. Retry in place, holding the lease. An ephemeral database error (a failover, a dropped connection, a
//     momentary connection-limit rejection) clears in seconds to tens of seconds, so an exponential ladder lands
//     the write with ZERO re-execution - which is the whole point, and what re-dispatching could never give us.
//  2. If the retries are exhausted, ask the database to classify the failure for us - see failOnPersistError.
//
// Lock contention is NOT handled here: it is retried before it reaches persist - by `sequel.Transact` for a
// transaction, by retryContention for a statement run on its own - and re-litigating it with a second loop
// would terminalize flows during a contention storm - exactly backwards. A persist write outside a
// transaction MUST go through retryContention: plain `ExecContext`/`QueryRowContext` retry nothing, so one
// deadlock would reach processStep's recovery defer, which rewinds the still-running step and RE-RUNS THE TASK.
const (
	// contentionMaxAttempts matches Transact's retry depth, so a statement run on its own survives the same
	// contention a transaction does.
	contentionMaxAttempts = 8
)

var (
	// errPersistFenced: a peer re-claimed the step while we were failing. We are a zombie; abandon silently.
	errPersistFenced = errors.New("step lease was re-granted to a peer")
	// errPersistDrained: the engine is shutting down. We released the lease so a peer can take the step now
	// rather than waiting out its expiry.
	errPersistDrained = errors.New("engine is draining; step lease released")
)

// retryContention runs a write that is not inside a transaction, retrying it on lock contention with the same
// depth and jittered millisecond backoff Transact gives a transaction. The write must be safe to run again
// after a contention error: an autocommit statement that lost applied nothing, and a statement that landed
// before a later one in the same write lost must be one that re-applies cleanly.
func retryContention(ctx context.Context, stmt func() error) error {
	var err error
	for attempt := range contentionMaxAttempts {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case <-time.After(time.Duration(attempt)*time.Millisecond + time.Duration(rand.IntN(3))*time.Millisecond):
			}
		}
		err = stmt()
		if err == nil || !sequel.IsLockContentionError(err) {
			return err
		}
	}
	return err
}

// persist runs a step-persistence write and, on a non-contention database error, retries THE WRITE - never the
// task - in place while holding the lease.
//
// write must be idempotent with respect to the database (all callers re-run one fenced UPDATE or one whole
// transaction, both of which re-derive their own state), and must not re-execute the task.
//
// Returns nil when the write landed. errPersistFenced / errPersistDrained are terminal, benign, and the caller
// simply returns. Any other error means the write kept failing and the caller must classify it - see
// failOnPersistError.
// yield hands back whatever the caller is holding for the duration of a wait that is not the database -
// see the backoff below. Nil means the caller holds nothing.
func (e *Engine) persist(ctx context.Context, db *sequel.DB, shardNum, stepID, leaseSeq int, write func() error, yield func(func())) error {
	err := write()
	if err == nil {
		return nil
	}
	if errors.Is(err, errLeaseFenced) {
		// The write's own lease fence rolled it back: we are a zombie, and a retry would be one too.
		return errPersistFenced
	}
	if isPermanentError(err) {
		return err // deterministic: a retry would compute the same failure
	}
	if sequel.IsLockContentionError(err) {
		// Already retried to exhaustion (Transact, or retryContention); a second loop would only prolong a storm.
		return errors.Trace(err)
	}

	for _, backoff := range e.persistBackoff {
		// Renew the lease BEFORE each sleep, so the retry cannot outlive our ownership of the step: the renewal covers
		// the sleep, then persistLeaseExtension - its first half for the write, its second for failOnPersistError to
		// fail the step. Renewing per retry rather than once keeps the whole ladder, which sleeps a minute in all, on a
		// lease we hold. This write carries no payload, so it is not subject to whatever is killing the real one.
		//
		// The status guard is load-bearing, not decoration. It matches the two states a worker legitimately HOLDS a
		// step in - `running` (its outcome has not landed) and `completed` (it landed, but the follow-up transition
		// has not) - exactly as releaseLease does, because persist sites of both kinds reach here: the completion
		// write, a Retry rewind, a subgraph spawn and an interrupt retry while the step is `running`, the transition
		// writes while it is `completed`. Restricting to `running` alone would wrongly fence the transition retry.
		// A write that already landed moves the step out of both (a rewind to `pending`, an interrupt to
		// `interrupted`, a spawn to parked), so a retry after an ambiguous commit finds the step not ours and stops.
		//
		// What the guard excludes: lease recovery resets an expired step to `pending` WITHOUT
		// bumping `lease_seq` (only a claim grants a new generation), so the generation fence alone still matches a
		// step recovery has already taken back. Stamping a FUTURE `lease_expires` onto that pending row would strand
		// it OUTRIGHT: every claim and selection predicate requires `lease_expires<=NOW` on a pending step, so no piston cycle would ever
		// select it, and lease recovery could not rescue it either - that sweep only resets `running` rows. The step
		// would simply sit `pending` and invisible until its future lease lapsed on its own.
		//
		// A zero-row match (the query SUCCEEDED but matched nothing) therefore means we no longer own the step - a peer
		// re-claimed it (lease_seq bumped), recovery reset it to `pending`, or a racing Terminate terminalized it. Abandon
		// silently: retrying would be a zombie's write, and the fence would reject it anyway. An ERROR here means the
		// database is unreachable, which is exactly the case the retry loop is for - so fall through and try on whatever
		// lease we still hold. Do not break to the classifier on a renewal error instead: a failover fails the renewal
		// too, and a blip would then cost a re-execution.
		held, xerr := e.extendLease(ctx, db, stepID, leaseSeq, backoff+e.persistLeaseExtension)
		if xerr == nil && !held {
			e.logLostLease(ctx, stepID, err)
			return errPersistFenced
		}
		// The sleep is not database work, so whatever admission the caller is holding goes back for its
		// duration - about a minute across the whole ladder. Holding it would park a turn on a timer, which is
		// the one thing the admission rule forbids, and it would do so during a database blip: exactly when
		// the remaining turns are most contended.
		drained := false
		waited := func() {
			select {
			case <-e.drainStop:
				drained = true
			case <-time.After(backoff):
			}
		}
		if yield != nil {
			yield(waited)
		} else {
			waited()
		}
		if drained {
			// Shutting down. drainStop is the ONLY signal available here: the lifetime ctx is deliberately
			// left live until every worker has drained, so that in-flight database work always commits, which
			// means a worker asleep in this backoff has nothing else to watch.
			//
			// Hand the step back NOW rather than making a peer wait out the lease we just extended. This DOES
			// re-execute the task on the peer - it is the at-least-once contract, and it is what would have
			// happened at lease expiry anyway, only sooner.
			e.releaseLease(ctx, db, stepID, leaseSeq)
			return errPersistDrained
		}
		e.metricStepWriteRetried(ctx, shardNum)
		err = write()
		if err == nil {
			return nil
		}
		if errors.Is(err, errLeaseFenced) {
			return errPersistFenced
		}
		if isPermanentError(err) {
			return err
		}
		if sequel.IsLockContentionError(err) {
			return errors.Trace(err)
		}
	}
	return errors.Trace(err)
}

// logLostLease reports a step whose outcome write failed for a reason of its own and that was then no longer this
// worker's. Usually lease recovery has taken it and the task runs again; a write that fails every time makes that a
// loop, which shows here and as dwarf_steps_recovered climbing. It is a warning, not an error: a write that landed
// with its acknowledgement lost, or a racing Terminate or Cancel, also moves the step out of this worker's hands, and
// then nothing runs again. A zombie whose write the fence rejected never gets here: persist returns before it.
func (e *Engine) logLostLease(ctx context.Context, stepID int, writeErr error) {
	e.logger.WarnContext(ctx, "Step is no longer this worker's after its outcome write failed; it may run again",
		"step", stepID, "error", writeErr)
}

// extendLease ensures a step's lease runs at least d into the future, fenced on the lease
// generation and on the two states a worker holds a step in (see persist for why the status guard is load-bearing),
// unparked: a parked caller waits on its child and belongs to no worker, so a persist that finds its own write
// parked the step has landed it and must stop.
// It never shortens a lease: a step claimed with a long budget keeps it. It reports whether the step is still this
// worker's: false, with no error, means it was re-claimed, rewound or terminalized.
//
// The UPDATE matches only a lease that needs extending, so its new expiry always differs from the old and
// RowsAffected is honest on MySQL. A zero-row match is therefore ambiguous - taken back, or already long enough -
// and only then a read settles which.
func (e *Engine) extendLease(ctx context.Context, db *sequel.DB, stepID, leaseSeq int, d time.Duration) (bool, error) {
	if e.seams.Enabled() && e.seams.IsFault(engineseams.FaultExtendLeaseErr) {
		return false, errors.New("injected fault: " + engineseams.FaultExtendLeaseErr)
	}
	const owned = " WHERE step_id=? AND lease_seq=? AND parked=? AND status IN ('" + workflow.StatusRunning + "', '" + workflow.StatusCompleted + "')"
	res, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), ?)"+owned+" AND lease_expires < DATE_ADD_MILLIS(NOW_UTC(), ?)",
		d.Milliseconds(), stepID, leaseSeq, parkedNone, d.Milliseconds(),
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return true, nil
	}
	var held int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps"+owned, stepID, leaseSeq, parkedNone).Scan(&held); err != nil {
		return false, errors.Trace(err)
	}
	return held > 0, nil
}

// releaseLease hands a step back for immediate re-dispatch, fenced on our lease generation. It resets from
// `running` (the task ran but its outcome never landed) or `completed` (the outcome landed but the transition
// did not) - the two states a worker can be holding a step in. A terminal step (terminated by a racing
// Terminate) is deliberately not matched: it is immutable. Nor is a parked one, which waits on its child: reset to
// pending while parked, nothing would select or recover it.
func (e *Engine) releaseLease(ctx context.Context, db *sequel.DB, stepID, leaseSeq int) {
	_, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC()"+
			" WHERE step_id=? AND lease_seq=? AND parked=? AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusCompleted+"')",
		workflow.StatusPending, stepID, leaseSeq, parkedNone,
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
// classification sound. Guessing the other way - terminalizing on any unknown error - would kill open flows on
// every routine failover, and a closed flow is immutable (recovery is a human running Fork).
func (e *Engine) failOnPersistError(ctx context.Context, d *dispatch, writeErr error, held stepHeld) error {
	stepID, taskName := d.stepID, d.row.taskName
	fenced, ferr := e.failStep(ctx, d, writeErr, held)
	if ferr != nil {
		// Cannot write even a payload-free failure: the DATABASE is down, not the payload. Surface the original
		// error and leave the step to lease recovery.
		e.logger.ErrorContext(ctx, "Persisting step outcome failed and the step could not be failed either; leaving it for lease recovery",
			"step", stepID, "task", taskName, "writeError", writeErr, "failError", ferr)
		return errors.Trace(writeErr)
	}
	if fenced {
		e.logLostLease(ctx, stepID, writeErr)
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

// maxErrorBytes bounds an error message stored in a text column. MySQL's TEXT holds 65,535 bytes and refuses
// more under its default strict mode; well below that, every listed flow and History step carries the message.
const maxErrorBytes = 16 << 10

// truncatedMark ends a message cut to maxErrorBytes.
const truncatedMark = " ... [truncated]"

// sanitizeErrorMessage makes a message storable in a text column: control bytes and invalid UTF-8 are replaced,
// and a message over maxErrorBytes is cut on a rune boundary and marked. A driver error can quote the offending
// value back at you, so the message carrying "this value is unstorable" can itself be unstorable - Postgres
// rejects a NUL and invalid UTF-8 in `text` just as in `jsonb`, and MySQL a value over the column's size. Replace
// and cut, never reject: the message is diagnostic, and a mangled one still names the failure, while a rejected
// one leaves failStep unable to land, which the classifier reads as the database being down.
func sanitizeErrorMessage(msg string) string {
	if !utf8.ValidString(msg) || strings.ContainsFunc(msg, func(r rune) bool { return r < 0x20 && r != '\n' && r != '\t' }) {
		// Ranging over invalid UTF-8 yields utf8.RuneError, so Map replaces it too.
		msg = strings.Map(func(r rune) rune {
			if r < 0x20 && r != '\n' && r != '\t' {
				return '�'
			}
			return r
		}, msg)
	}
	if len(msg) > maxErrorBytes {
		cut := maxErrorBytes - len(truncatedMark)
		for cut > 0 && !utf8.RuneStart(msg[cut]) {
			cut--
		}
		msg = msg[:cut] + truncatedMark
	}
	return msg
}

// persistStep is persist plus the disposition every caller in processStep shares: a lost lease or a drain is a
// benign no-op, lock contention goes to the recovery defer (never the classifier - terminalizing a flow because the
// database was busy is exactly backwards), and anything else that would not land is classified. held is the state
// the write starts from (see stepHeld).
func (e *Engine) persistStep(ctx context.Context, d *dispatch, held stepHeld, write func() error, yield func(func())) error {
	_, err := e.settlePersist(ctx, d, e.persist(ctx, d.db, d.shard, d.stepID, d.row.leaseSeq, write, yield), held)
	return err
}

// permanentError marks a failure of the engine's own computation inside a persisted write - deterministic, so a
// retry would compute it again, and not a storage fault. A fan-in's fold of its branches' contributions that fails
// on a value of the wrong type under a combining reducer is one. persist does not retry it, and
// settlePersist fails the step with it without counting a write that could not be stored. Wrap only a failure
// that is a pure function of stored data: a database error from the same computation is transient, and wrapping
// it would fail an open flow over a blip.
//
// It is a Go type rather than an error property (the marker workflow.IsCancelled reads) because it must NOT
// survive serialization: it is raised inside the write and inspected by persist and settlePersist in the same
// processStep call, and means nothing past it. A property would ride the failed flow's error into its caller's
// subgraph_error and onErr, handing task code an engine-internal marker.
type permanentError struct{ err error }

func (p *permanentError) Error() string { return p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// isPermanentError reports whether err carries a permanentError, or is a state ref the stored rows cannot satisfy
// (staterefs.IsUnresolvable) - the same kind of failure, raised where the engine's own wrapping does not reach: a
// final-state merge, a fan-in fold, a Fork leaf. Left transient, Terminate, Delete and Purge of a flow with such a
// row would retry it forever, and nothing could stop the flow.
func isPermanentError(err error) bool {
	var pe *permanentError
	return errors.As(err, &pe) || staterefs.IsUnresolvable(err)
}

// settlePersist applies persistStep's disposition to the error persist returned, and reports whether the write
// landed - the only case in which the caller goes on. Contention is NOT a persistence failure and must never be
// classified as one: terminalizing on it would kill open flows during a contention storm. It was already retried
// to exhaustion (by Transact, or by retryContention), so it goes to the recovery defer, which rewinds the step
// and re-polls.
func (e *Engine) settlePersist(ctx context.Context, d *dispatch, err error, held stepHeld) (landed bool, _ error) {
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errPersistFenced), errors.Is(err, errPersistDrained):
		return false, nil // a peer owns the step now; abandon it silently
	case sequel.IsLockContentionError(err):
		return false, errors.Trace(err)
	case isPermanentError(err):
		// Not a storage fault: the step fails with it, without the write alarm.
		_, ferr := e.failStep(ctx, d, err, held)
		return false, errors.Trace(ferr)
	default:
		return false, errors.Trace(e.failOnPersistError(ctx, d, err, held))
	}
}

// persistYielding is persist for a dispatch holding its turn (d.pass): it hands the turn back across the retry
// backoff, which is a sleep rather than database work, and takes a fresh one before the next attempt.
func (e *Engine) persistYielding(ctx context.Context, d *dispatch, write func() error) error {
	return e.persist(ctx, d.db, d.shard, d.stepID, d.row.leaseSeq, write, func(wait func()) {
		yieldTurn(ctx, &d.pass, wait)
	})
}

// persistStepYielding is persistStep for a dispatch holding its turn - see persistYielding.
func (e *Engine) persistStepYielding(ctx context.Context, d *dispatch, held stepHeld, write func() error) error {
	return e.persistStep(ctx, d, held, write, func(wait func()) {
		yieldTurn(ctx, &d.pass, wait)
	})
}
