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
	"database/sql"
	"slices"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// arrivalBatchMax bounds the arrivals one transaction lands. Each member is one fenced UPDATE in the transaction, which
// holds the spawn row's write lock until it commits, so a larger batch holds every other replica's arrivals at that
// row for longer.
const arrivalBatchMax = 256

// arrivalKey names one cohort. A spawn step's id is unique only within its shard, and stripes are shared across
// shards and cohorts.
type arrivalKey struct{ shard, spawn int }

// arrivalOutcome is what an arrival batch did with one member.
type arrivalOutcome uint8

const (
	// arrivalSolo: the member runs its own transaction. The zero value, so a batch that never ran, failed or panicked
	// leaves every member it had not landed to do so.
	arrivalSolo arrivalOutcome = iota
	// arrivalLanded: the member's arrival committed; its dispatch holds what the transaction set.
	arrivalLanded
	// arrivalFenced: the member's step was no longer its worker's; nothing of it was written.
	arrivalFenced
)

// arrivalBatch is the pure arrivals of one cohort queued on its stripe behind whoever holds it. The first to queue
// leads: it waits its turn on the stripe and then lands every member's arrival in one transaction. The others wait on
// done and never touch the database meanwhile but to keep their leases. A member's dispatch is the leader's to read
// and write until done closes; outcomes is written before it closes.
type arrivalBatch struct {
	key      arrivalKey
	members  []*dispatch // members[0] leads
	outcomes []arrivalOutcome
	done     chan struct{}
}

// arriveInBatch lands a pure arrival together with the arrivals of its cohort queued beside it, and reports what
// happened to it. arrivalSolo means it must run its own transaction; holdsStripe then says whether it already holds
// the stripe, which it does when it found the stripe free and so had nothing to join.
//
// Its wait holds no turn, as a stripe waiter's does; it keeps its lease alive on the same schedule
// (extendWaitingLease) and never abandons, because the leader may be using its dispatch. A member whose lease was taken
// back meanwhile is fenced by the batch's own write.
func (e *Engine) arriveInBatch(ctx context.Context, stripe *cohortStripe, d *dispatch) (outcome arrivalOutcome, holdsStripe bool) {
	b, idx := stripe.join(arrivalKey{shard: d.shard, spawn: d.cohortSpawnID}, d)
	if b == nil {
		return arrivalSolo, true
	}
	if idx > 0 {
		yieldTurn(ctx, &d.pass, func() { e.awaitBatch(ctx, b, d) })
		return b.outcomes[idx], false
	}
	e.leadBatch(ctx, stripe, b, d)
	return b.outcomes[0], false
}

// awaitBatch waits for a member's batch to finish, keeping the member's lease alive meanwhile.
func (e *Engine) awaitBatch(ctx context.Context, b *arrivalBatch, d *dispatch) {
	lead := e.leaseLead()
	timer := time.NewTimer(time.Until(e.leaseDeadline(d)) - lead)
	defer timer.Stop()
	for {
		select {
		case <-b.done:
			return
		case <-timer.C:
			// A lease that is gone ticks no more: the batch fences the member.
			if next, held := e.extendWaitingLease(ctx, d, lead); held {
				timer.Reset(next)
			}
		}
	}
}

// awaitStripeToLead waits for the stripe on behalf of a batch, keeping the leader's lease alive meanwhile. It never
// abandons: the members wait on the leader, and a leader whose own lease is gone still lands theirs.
func (e *Engine) awaitStripeToLead(ctx context.Context, stripe *cohortStripe, d *dispatch) {
	lead := e.leaseLead()
	lost := false
	stripe.lock(time.Until(e.leaseDeadline(d))-lead, func() (time.Duration, bool) {
		if !lost {
			next, held := e.extendWaitingLease(ctx, d, lead)
			if held {
				return next, true
			}
			lost = true
		}
		return time.Hour, true
	})
}

// leadBatch waits for the stripe on behalf of a batch and lands its arrivals while its leader d holds the stripe and
// its turn, then releases the stripe and the members. The batch is sealed once served, so an arrival queuing after
// that starts the next one.
//
// Its cleanup covers the wait as well as the work: a leader that panics anywhere - in a lease tick while it waits, in
// the transaction - still seals the batch, so no arrival joins it after, and wakes every member it had not landed to
// run its own transaction (arrivalSolo), and hands on the stripe if it held it.
//
// The transition's test seams are consulted once per member before anything is written, as each member's own
// transaction would; a member whose fault fires leaves the batch and runs its own, which returns the fault first
// (dispatch.firedFault). A chunk that will not commit leaves its members, and every later one, to run their own: their
// transactions classify the failure exactly as an arrival that never batched does.
func (e *Engine) leadBatch(ctx context.Context, stripe *cohortStripe, b *arrivalBatch, d *dispatch) {
	held := false
	defer func() {
		members := stripe.seal(b)
		if b.outcomes == nil {
			b.outcomes = make([]arrivalOutcome, len(members))
		}
		if held {
			stripe.unlock()
		}
		close(b.done)
	}()
	yieldTurn(ctx, &d.pass, func() {
		e.awaitStripeToLead(ctx, stripe, d)
		held = true
	})
	members := stripe.seal(b)
	b.outcomes = make([]arrivalOutcome, len(members))
	leader := members[0]
	batched := make([]int, 0, len(members))
	for i, m := range members {
		if err := e.transitionFault(m.row.taskName); err != nil {
			m.firedFault = err
			continue
		}
		batched = append(batched, i)
	}
	group := make([]*dispatch, 0, min(len(batched), arrivalBatchMax))
	fenced := make([]bool, 0, cap(group))
	for chunk := range slices.Chunk(batched, arrivalBatchMax) {
		group = group[:0]
		for _, i := range chunk {
			group = append(group, members[i])
		}
		fenced = fenced[:len(group)]
		err := leader.db.Transact(ctx, func(tx *sequel.Tx) error {
			return e.arrivalBatchTx(ctx, tx, group, fenced)
		})
		if err == nil && e.diags.Seams().Enabled() && e.diags.Seams().IsFault(diagnostics.Join(diagnostics.FaultArrivalBatchCommitLost, leader.row.taskName)) {
			err = e.commitLostErr(ctx, diagnostics.FaultArrivalBatchCommitLost, leader.row.taskName)
		}
		if err != nil {
			e.diags.Logger().DebugContext(ctx, "Cohort arrival batch did not commit; its members arrive one by one",
				"spawnStep", b.key.spawn, "members", len(group), "error", err)
			return
		}
		for j, i := range chunk {
			if fenced[j] {
				b.outcomes[i] = arrivalFenced
			} else {
				b.outcomes[i] = arrivalLanded
			}
		}
	}
}

// arrivalBatchTx lands the arrivals of members, all pure arrivals at one cohort, in one transaction: the spawn's
// counter rises by their arrivals, each member's own row clears its advancing flag fenced on its lease generation -
// so each arrival still commits with its own row - and the arrival that resolves the cohort, the last member not
// fenced, resolves it as a lone arrival would (resolveCohort). fenced reports the members whose fence matched nothing;
// their arrivals are taken back off the counter before anything reads it. Transact may run it more than once, so it
// starts every member from a clean slate.
//
// The spawn row is written first, as a lone arrival's bump writes it: one statement there locks the spawn and the
// arriving step together.
func (e *Engine) arrivalBatchTx(ctx context.Context, tx *sequel.Tx, members []*dispatch, fenced []bool) error {
	spawnID := members[0].cohortSpawnID
	total := 0
	for i, m := range members {
		fenced[i] = false
		m.newStepIDs = m.newStepIDs[:0]
		m.fanInLineageID = 0
		m.loss = flowLoss{}
		m.txBytes = stateByteCount{}
		m.flowRowLocked = false
		total += m.fanInArrivals
	}
	batch := newTxBatch(ctx, tx)
	batch.queue(func(res *sequel.BatchResults) error {
		bump, err := res.Exec()
		if err != nil {
			return errors.Trace(err)
		}
		if n, _ := bump.RowsAffected(); n == 0 {
			return errors.Trace(sql.ErrNoRows) // the spawn row is gone, as the lone bump reports it
		}
		return nil
	}, "UPDATE dwarf_steps SET cohort_arrivals=cohort_arrivals+? WHERE step_id=?", total, spawnID)
	for i, m := range members {
		// MySQL counts CHANGED rows, which is honest here: a match flips the flag from 1 to 0.
		batch.queue(func(res *sequel.BatchResults) error {
			clear, err := res.Exec()
			if err != nil {
				return errors.Trace(err)
			}
			if n, _ := clear.RowsAffected(); n == 0 {
				fenced[i] = true
			}
			return nil
		}, "UPDATE dwarf_steps SET advancing=0 WHERE step_id=? AND lease_seq=? AND advancing=1", m.stepID, m.row.leaseSeq)
	}
	var c cohortCounts
	batch.queue(func(res *sequel.BatchResults) error {
		return errors.Trace(res.QueryRow().Scan(&c.arrivals, &c.size, &c.failures, &c.cancellations, &c.spawnLineageID))
	}, "SELECT cohort_arrivals, cohort_size, cohort_failures, cohort_cancellations, lineage_id FROM dwarf_steps WHERE step_id=?", spawnID)
	if err := batch.flush(); err != nil {
		return err
	}
	var resolver *dispatch
	fencedArrivals := 0
	for i, m := range members {
		if fenced[i] {
			fencedArrivals += m.fanInArrivals
		} else {
			resolver = m
		}
	}
	if fencedArrivals > 0 {
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_arrivals=cohort_arrivals-? WHERE step_id=?", fencedArrivals, spawnID); err != nil {
			return errors.Trace(err)
		}
		c.arrivals -= fencedArrivals
	}
	if resolver == nil {
		return nil
	}
	succ := e.successorsOf(resolver)
	resolve := newTxBatch(ctx, tx)
	if err := e.resolveCohort(ctx, resolver, tx, resolve, &succ, true, c); err != nil {
		return err
	}
	return resolve.flush()
}
