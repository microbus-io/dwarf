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
	"maps"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// bumpCohortArrivals adds n arrivals to the cohort spawned by spawnID and returns the spawn row's counters as
// updated. The same statement clears the arriving step's advancing flag, fenced on leaseSeq and on the flag still
// being set (see the successor write in processStep for why both); a fenced arrival returns errLeaseFenced (the
// caller's transaction must roll back, undoing the bump), and a missing spawn row returns sql.ErrNoRows.
//
// One statement covers two rows rather than adding a second one for the arriving step, because the bump holds
// the spawn row's lock until commit and every sibling of the cohort queues on it. The arriving row is its own
// worker's and uncontended, and it always sorts after its spawn (a later insert, a larger step_id), so the lock
// order is the one every other writer takes. A fan-out source arriving at its own fan-in is its own spawn
// (spawnID == stepID): the one row takes both writes, and the source's own fenced write has already run.
func bumpCohortArrivals(ctx context.Context, tx *sequel.Tx, spawnID, n, stepID, leaseSeq int) (arrivals, size, failures, cancellations, spawnLineageID int, err error) {
	const set = "SET cohort_arrivals = cohort_arrivals + CASE WHEN step_id=? THEN ? ELSE 0 END, advancing = CASE WHEN step_id=? THEN 0 ELSE advancing END"
	const where = " WHERE step_id IN (?, ?) AND (step_id=? OR (lease_seq=? AND advancing=1))"
	const cols = "step_id, cohort_arrivals, cohort_size, cohort_failures, cohort_cancellations, lineage_id"
	args := []any{spawnID, n, stepID, spawnID, stepID, spawnID, leaseSeq}
	var rows *sequel.Rows
	switch tx.DriverName() {
	case "pgx", "sqlite":
		rows, err = tx.QueryContext(ctx, "UPDATE dwarf_steps "+set+where+" RETURNING "+cols, args...)
	case "mssql":
		rows, err = tx.QueryContext(ctx, "UPDATE dwarf_steps "+set+
			" OUTPUT INSERTED.step_id, INSERTED.cohort_arrivals, INSERTED.cohort_size, INSERTED.cohort_failures, INSERTED.cohort_cancellations, INSERTED.lineage_id"+
			where, args...)
	default:
		// MySQL lacks RETURNING, so the bump and the read are two statements, serial on the transaction's one
		// connection. The fence is RowsAffected, which is honest here although MySQL counts CHANGED rows: every
		// row this statement matches changes - the spawn's arrivals rise by n (always at least one) and the arriving
		// step's flag goes 1 -> 0, the fence requiring it set. The read then needs only the spawn row, which this
		// transaction just wrote, so it sees the bump whatever snapshot the transaction holds.
		res, xerr := tx.ExecContext(ctx, "UPDATE dwarf_steps "+set+where, args...)
		if xerr != nil {
			return 0, 0, 0, 0, 0, errors.Trace(xerr)
		}
		want := int64(2)
		if spawnID == stepID {
			want = 1
		}
		changed, _ := res.RowsAffected()
		if xerr := tx.QueryRowContext(ctx,
			"SELECT cohort_arrivals, cohort_size, cohort_failures, cohort_cancellations, lineage_id FROM dwarf_steps WHERE step_id=?", spawnID,
		).Scan(&arrivals, &size, &failures, &cancellations, &spawnLineageID); xerr != nil {
			return 0, 0, 0, 0, 0, errors.Trace(xerr) // sql.ErrNoRows for a missing spawn, as from the other dialects
		}
		if changed < want {
			return 0, 0, 0, 0, 0, errLeaseFenced
		}
		return arrivals, size, failures, cancellations, spawnLineageID, nil
	}
	if err != nil {
		return 0, 0, 0, 0, 0, errors.Trace(err)
	}
	defer rows.Close()
	spawnSeen, stepSeen := false, false
	for rows.Next() {
		var id, a, s, f, c, l int
		if err := rows.Scan(&id, &a, &s, &f, &c, &l); err != nil {
			return 0, 0, 0, 0, 0, errors.Trace(err)
		}
		if id == spawnID {
			spawnSeen = true
			arrivals, size, failures, cancellations, spawnLineageID = a, s, f, c, l
		}
		if id == stepID {
			stepSeen = true
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, 0, errors.Trace(err)
	}
	if !spawnSeen {
		return 0, 0, 0, 0, 0, sql.ErrNoRows
	}
	if !stepSeen {
		return 0, 0, 0, 0, 0, errLeaseFenced
	}
	return arrivals, size, failures, cancellations, spawnLineageID, nil
}

// fireFanInDirect creates the fan-in step immediately for a fan-out source that converges with no branches:
// an empty forEach, or a goto/onError/when/switch route straight to the source's own fan-in. The source is the
// spawn, so the fan-in runs on the source's own state and changes.
func (e *Engine) fireFanInDirect(ctx context.Context, d *dispatch, fanInTarget string) error {
	shardNum, db, flowID, flowHour, flowToken := d.shard, d.db, d.row.flowID, d.row.createdHour, d.row.flowToken
	stepID, leaseSeq, stepDepth, lineageID, fanOutOrdinal := d.stepID, d.row.leaseSeq, d.row.stepDepth, d.row.lineageID, d.row.fanOutOrdinal
	workflowURL, graph, sleepDur, inheritCancel := d.row.workflowURL, d.graph, d.sleep, d.inheritCancel
	priority, fairnessKey, fairnessWeight, timeBudgetMs := d.row.flowPriority, d.row.flowFairnessKey, d.row.flowFairnessWeight, d.row.flowTimeBudgetMs
	fanInURL := dispatchURLOf(graph, fanInTarget)
	var fanInStepID int64
	var txBytes stateByteCount
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		fanInStepID = 0
		txBytes = stateByteCount{}
		// Write-first, guarded on non-terminal status - the same lock-grab the transition tx uses. If a
		// concurrent Terminate/failStep terminalized this flow, the guard yields zero rows and this becomes a
		// clean no-op; without it we would insert a pending fan-in step and link the spawn to it on a terminal
		// flow (orphan work reaped only later by the claim-time terminal-flow guard).
		locked, flowErr := lockFlowRows(ctx, tx, liveFlowWhere, flowID)
		if flowErr != nil {
			return flowErr
		}
		if locked == 0 {
			return nil
		}

		var ourStateJSON, ourChangesJSON, ourRefsJSON []byte
		if rerr := tx.QueryRowContext(ctx, "SELECT state, changes, state_refs FROM dwarf_steps WHERE step_id=?", stepID).Scan(&ourStateJSON, &ourChangesJSON, &ourRefsJSON); rerr != nil {
			return errors.Trace(rerr)
		}
		txBytes.stateRead = len(ourStateJSON)
		txBytes.changesRead = len(ourChangesJSON)
		ourState, serr := workflow.NewState(ourStateJSON)
		if serr != nil {
			return errors.Trace(&permanentError{serr})
		}
		ourChanges, cerr := changesOf(ourChangesJSON)
		if cerr != nil {
			return errors.Trace(cerr)
		}
		ourRefs, rerr := staterefs.Parse(ourRefsJSON)
		if rerr != nil {
			return errors.Trace(&permanentError{rerr})
		}
		// With no members there is nothing more to fold: the fan-in's state is this step's changes folded onto its
		// state, exactly as fanInRow builds its base and as a linear transition does. So it mints as a linear
		// successor would, carried refs and all - except that a field the step folded needs its base, which
		// may be carried by a ref, and its folded value is in no row, so it is a list or inline.
		reducers := graph.Reducers()
		dropIdentityNulls(ourChanges, reducers)
		folded := foldedFields(ourChanges, reducers)
		if err := e.resolveReducedRefs(ctx, tx, shardNum, flowToken, ourState, ourRefs, onlyFields(reducers, folded), workflowURL); err != nil {
			return errors.Trace(err)
		}
		mergedState := ourState.Clone()
		if err := mergedState.Apply(ourChanges, reducers); err != nil {
			return errors.Trace(&permanentError{err})
		}
		folds := appendFolds(ourState, ourChanges, reducers, stepID)
		mergedJSON, refsJSON, merr := e.linkerFor(shardNum).Mint(mergedState, ourChanges, ourRefs, stepID, staterefs.Linear, withoutKeys(folded, folds), nil, folds)
		if merr != nil {
			return errors.Trace(merr)
		}
		if err := e.checkPayload("fan-in state of task '"+stripProto(fanInTarget)+"'", mergedJSON); err != nil {
			return err
		}
		txBytes.stateWritten = len(mergedJSON)
		txBytes.refsWritten = len(refsJSON)

		nextStepDepth := stepDepth + 1
		sleepMs := sleepDur.Milliseconds()
		var err error
		fanInStepID, err = tx.InsertReturnID(ctx, "step_id", successorInsertSQL,
			flowID, nextStepDepth, keys.RandomIdentifier(16), fanInTarget, fanInURL, mergedJSON, len(mergedJSON), refsJSON, emptyJSON, workflow.StatusPending, parkedNone, timeBudgetMs, lineageID, fanOutOrdinal, stepID, sleepMs, priority, fairnessKey, fairnessWeight, e.engineID, flowHour,
			inheritCancel, stepID, flowID,
		)
		if err != nil {
			return errors.Trace(err)
		}
		// The dispatched row's own write - its successor link, its (empty) cohort size and the advancing clear - is
		// ONE statement fenced exactly as in the transition transaction, and like it, it follows the INSERT: a
		// fenced attempt returns errLeaseFenced and the rollback takes the INSERT with it.
		stepRes, stepErr := tx.ExecContext(ctx, "UPDATE dwarf_steps SET successor_id=?, cohort_size=0, advancing=0 WHERE step_id=? AND lease_seq=? AND advancing=1", int(fanInStepID), stepID, leaseSeq)
		if stepErr != nil {
			return errors.Trace(stepErr)
		}
		if n, _ := stepRes.RowsAffected(); n == 0 {
			return errLeaseFenced
		}
		return nil
	})
	if err == nil && e.diags.Seams().Enabled() && e.diags.Seams().IsFault(diagnostics.Join(diagnostics.FaultFanInDirectCommitLost, d.row.taskName)) {
		err = e.commitLostErr(ctx, diagnostics.FaultFanInDirectCommitLost, d.row.taskName)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if fanInStepID == 0 {
		// The terminal-status guard bailed - the flow was terminated/failed/cancelled concurrently, no fan-in
		// step was inserted, nothing to dispatch.
		return nil
	}
	e.metricStateWriteBytes(ctx, workflowURL, "state", txBytes.stateWritten)
	e.metricStateWriteBytes(ctx, workflowURL, "state_refs", txBytes.refsWritten)
	e.metricStateReadBytes(ctx, workflowURL, "state", txBytes.stateRead)
	e.metricStateReadBytes(ctx, workflowURL, "changes", txBytes.changesRead)

	// A sleeping fan-in step is left to the piston; see the sibling site above.
	if sleepDur == 0 {
		// Priority was just bound into the fan-in INSERT; the sleep branch diverged above.
		e.enqueueStepDue(ctx, shardNum, int(fanInStepID), priority)
	}
	return nil
}

// stateByteCount accumulates state/changes payload bytes moved by a transaction, emitted to the
// dwarf_state_*_bytes counters by the caller only after the transaction commits (a contention retry
// re-runs the closure, so inline emission would double-count).
type stateByteCount struct {
	stateWritten int // "state" column: snapshots written (successor, fan-in inserts)
	refsWritten  int // "state_refs" column: refs written beside those snapshots, lists included
	stateRead    int // "state" column: snapshots read (fan-in spawn/own state)
	changesRead  int // "changes" column: deltas read (fan-in spawn + cohort members)
}

// add adds o's counts to c.
func (c *stateByteCount) add(o stateByteCount) {
	c.stateWritten += o.stateWritten
	c.refsWritten += o.refsWritten
	c.stateRead += o.stateRead
	c.changesRead += o.changesRead
}

// cohortRead is what a resolving arrival reads of its cohort: the spawn step's row and every member's.
type cohortRead struct {
	spawnTaskName      string
	spawnStateJSON     []byte
	spawnChangesJSON   []byte
	spawnRefsJSON      []byte
	spawnFanOutOrdinal int
	spawnLineageID     int
	members            []cohortMember
	changesRead        int // bytes of the members' changes columns
}

// cohortMember is one row of a cohort, in fan_out_ordinal, step_depth, step_id order. changes is decoded as the
// row is read, and only for a completed member - the only kind whose changes the fold takes.
type cohortMember struct {
	stepID      int
	taskName    string
	completed   bool
	changes     workflow.Changes
	depth       int
	successorID int
}

// queueCohortRead queues the reads of a resolving arrival's cohort into c.
func queueCohortRead(b *txBatch, flowID, cohortSpawnID int, c *cohortRead) {
	b.queue(func(res *sequel.BatchResults) error {
		err := res.QueryRow().Scan(&c.spawnStateJSON, &c.spawnChangesJSON, &c.spawnRefsJSON, &c.spawnTaskName, &c.spawnFanOutOrdinal, &c.spawnLineageID)
		return errors.Trace(err)
	}, "SELECT state, changes, state_refs, task_name, fan_out_ordinal, lineage_id FROM dwarf_steps WHERE step_id=?", cohortSpawnID)
	b.queue(func(res *sequel.BatchResults) error {
		rows, err := res.Query()
		if err != nil {
			return errors.Trace(err)
		}
		for rows.Next() {
			var m cohortMember
			var status string
			var changesJSON []byte
			if err := rows.Scan(&m.stepID, &m.taskName, &status, &changesJSON, &m.depth, &m.successorID); err != nil {
				return errors.Trace(err)
			}
			c.changesRead += len(changesJSON)
			if m.completed = status == workflow.StatusCompleted; m.completed {
				if m.changes, err = changesOf(changesJSON); err != nil {
					return errors.Trace(err)
				}
			}
			c.members = append(c.members, m)
		}
		return errors.Trace(rows.Err())
	}, "SELECT step_id, task_name, status, changes, step_depth, successor_id FROM dwarf_steps WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_depth, step_id", flowID, cohortSpawnID)
}

// fanInBase is a fan-in's accumulator base: the spawn's changes folded onto its state, the value every branch
// was handed. fanInRow builds its fold on it, and computeFinalState builds the fold of a cohort that never
// converged on it too (simulatedFanInPart), so the two cannot disagree on where a cohort starts. It drops
// identity nulls from spawnChanges in place, as fanInRow's mint needs.
//
// Only the ref'd fields a reducer will FOLD are materialized - a combining reducer (append/add/union/...) needs
// its accumulated base, and folding a delta onto an absent base would silently lose everything so far. A
// merely-carried ref is left alone, still pointing at its original anchor: resolving it would materialize the
// payload and re-anchor it at EVERY fan-in, giving back the win in exactly the fan-out graphs refs exist for. A
// reduced field the spawn OVERWROTE needs no base: its write replaces it, so reading the old value would be a
// wasted anchor read inside the transaction.
func (e *Engine) fanInBase(ctx context.Context, tx sequel.Executor, shardNum int, flowToken string, spawnState workflow.State, spawnChanges workflow.Changes, spawnRefs staterefs.Refs, reducers map[string]workflow.Reducer, workflowURL string) (workflow.State, error) {
	dropIdentityNulls(spawnChanges, reducers)
	folded := reducers
	if spawnChanges.Overwrites().Len() > 0 || spawnChanges.Additions().Len() > 0 {
		folded = make(map[string]workflow.Reducer, len(reducers)+spawnChanges.Additions().Len())
		maps.Copy(folded, reducers)
		for k := range spawnChanges.Overwrites().Names() {
			delete(folded, k)
		}
		// An addition extends the spawn's input value, so that base is needed too.
		for k := range spawnChanges.Additions().Names() {
			folded[k] = workflow.ReducerReplace
		}
	}
	// resolveReducedRefs mutates the state map in place, so it gets the live map.
	if err := e.resolveReducedRefs(ctx, tx, shardNum, flowToken, spawnState, spawnRefs, folded, workflowURL); err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	merged := spawnState.Clone()
	if err := merged.Apply(spawnChanges, reducers); err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	return merged, nil
}

// fanInRow folds a resolved cohort, as read by queueCohortRead, into its fan-in step's row. It returns the row,
// the fan-in's step_depth, the cohort-exit steps whose successor_id must point at the fan-in step, and the state
// bytes it moved (read: spawn snapshot + every cohort member's changes; written: the merged fan-in snapshot),
// which the caller emits after its transaction commits.
func (e *Engine) fanInRow(ctx context.Context, tx sequel.Executor, shardNum int, flowToken string, nextStepDepth, cohortSpawnID int, c *cohortRead, fanInTaskName string, graph *workflow.Graph, workflowURL string) (row successorRow, depth int, exitStepIDs []int, bytes stateByteCount, err error) {
	bytes = stateByteCount{stateRead: len(c.spawnStateJSON), changesRead: len(c.spawnChangesJSON) + c.changesRead}
	spawnState, err := workflow.NewState(c.spawnStateJSON)
	if err != nil {
		return row, 0, nil, bytes, errors.Trace(&permanentError{err})
	}
	spawnChanges, err := changesOf(c.spawnChangesJSON)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(err)
	}
	spawnRefs, err := staterefs.Parse(c.spawnRefsJSON)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(&permanentError{err})
	}
	reducers := graph.Reducers()
	// The members' contributions fold onto this base in branch order.
	merged, err := e.fanInBase(ctx, tx, shardNum, flowToken, spawnState, spawnChanges, spawnRefs, reducers, workflowURL)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(err)
	}
	// Fields a cohort MEMBER contributed. Their bytes are in that member's `changes` - not in the spawn's row -
	// and a reducer's COMBINED output exists in no row at all, so neither can be anchored at the spawn. They are
	// inlined into the fan-in step's own state, which becomes their anchor for everything downstream (the third
	// of the three places an anchor's bytes can sit). A stale spawn ref for such a field is dropped with them.
	// A field the spawn itself folded is the same case.
	memberWrites := map[string]bool{}
	for k := range foldedFields(spawnChanges, reducers) {
		memberWrites[k] = true
	}
	var contributions []workflow.Changes

	// A member that is itself an inner fan-in contributes what its own cohort added, which is in its STATE, not
	// its changes: its changes hold only its own task's writes. Its state records which part its cohort added
	// (foldCohort), so one row per inner fan-in carries the whole subtree below it, at any depth.
	var parts map[int]workflow.Changes
	if ids := innerFanInIDs(graph, c.members); len(ids) > 0 {
		var n int
		parts, n, err = cohortParts(ctx, tx, ids, reducers)
		if err != nil {
			return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(err)
		}
		bytes.stateRead += n
	}

	// The cohort-exit steps whose successor_id must point at the fan-in step, collected from the cohort read so
	// the caller's write can target them by primary key.
	//
	// A member qualifies as an exit only if it BOTH bears a fan-in-predecessor task name AND has not already
	// recorded a forward edge (successor_id=0). The task-name test alone is not enough: a branch that LOOPS
	// through the exit task via flow.Goto (e.g. `Work -Goto-> Work -> Join`) leaves several `Work` steps in the
	// cohort, but only the last one transitioned to the fan-in - the earlier iterations already point at their
	// next loop step. A step that transitions to a fan-in inserts no successor of its own (the fan-in step is
	// created here, or its arrival is only a counter bump), so its successor_id is still 0; a loop iteration or a
	// multi-step branch's interior step wrote its successor_id when it created that successor. Overwriting the
	// latter corrupts the execution DAG (spurious `step -> fan-in` edges in History).
	exitTaskSet := make(map[string]bool)
	for _, t := range fanInPredecessorTasks(graph, fanInTaskName) {
		exitTaskSet[t] = true
	}
	maxCohortDepth := 0
	for _, m := range c.members {
		if m.depth > maxCohortDepth {
			maxCohortDepth = m.depth
		}
		// A cohort member with an exit task name that has not yet recorded a successor is one that transitioned
		// into the fan-in (see above); a member with the same task name that already has a successor looped or
		// continued elsewhere and must keep its own forward edge.
		if exitTaskSet[m.taskName] && m.successorID == 0 {
			exitStepIDs = append(exitStepIDs, m.stepID)
		}
		contribution, ok, cerr := memberContribution(parts[m.stepID], m.changes, reducers)
		if cerr != nil {
			return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(&permanentError{cerr})
		}
		if !ok {
			continue
		}
		for k := range contribution.Names() {
			memberWrites[k] = true
		}
		contributions = append(contributions, contribution)
	}
	// One fold for the whole cohort, not one per member: a combining reducer decodes its accumulator and
	// re-encodes the result, so folding per member re-encodes a value that grows as it goes - quadratic in
	// the accumulated bytes across the cohort's width, and a fan-out is exactly where width is large.
	// Only a fan-in inside an outer cohort is ever read as a member, so only it describes what its cohort added.
	// The fan-in takes its spawn's lineage.
	rec, err := foldCohort(merged, contributions, reducers, c.spawnLineageID != 0)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(&permanentError{err})
	}

	// The fan-in sits one level below the DEEPEST cohort branch, not merely below the last sibling to
	// complete - branch lengths can differ (loops, gotos, varying chains), so step_depth must reflect the
	// deepest path the flow took. nextStepDepth (last-completer+1) is the floor for the defensive empty case.
	depth = max(maxCohortDepth+1, nextStepDepth)

	// Drop the per-branch forEach bookkeeping of THIS cohort - the one spawnTaskName fanned out - and of no
	// other. An outer cohort's bookkeeping must survive an inner fan-in: a step converging out of the inner
	// cohort is still inside the outer branch and must still see which element that branch is working on. The
	// same scoped strip runs in computeFinalState for a fan-out that FAILED (and so never reached this
	// convergence), so the two paths agree on what a fan-out's state means once its cohort is behind it.
	//
	// A stripped name must also leave the spawn's refs, or the mint carries it forward from there: in a nested
	// fan-out whose levels share an `as`, the spawn of the inner cohort holds the OUTER element as an item ref,
	// and re-emitting it would bring that element back past the inner fan-in.
	for _, k := range stripForEachBookkeeping(merged, graph, c.spawnTaskName) {
		delete(spawnRefs, k)
	}

	// Mint against the SPAWN step, not this one: a field the cohort merely carried has its bytes in the spawn's
	// row (its `state` if the spawn received it - e.g. the entry step holding the flow's initial input - or its
	// `changes` if the spawn's task wrote it), so the spawn is a legitimate anchor and the fan-in must not
	// re-copy the payload into its own row. A field that arrived at the spawn as a ref keeps that ref, so the
	// chain stays one hop. Member-contributed fields are excluded and therefore inline (above); a reduced field no
	// member touched is carried or anchored at the spawn like any other.
	// A field under append the cohort folded is a list - the spawn's base, the spawn's part, each member's - at a
	// fan-in on the trunk. A fan-in inside an outer cohort keeps every field its cohort wrote inline: the outer
	// fold projects its part from this row's raw state (projectReduced).
	var folds map[string]staterefs.Fold
	if c.spawnLineageID == 0 {
		folds = cohortAppendFolds(spawnState, spawnChanges, cohortSpawnID, c.members, parts, reducers)
	}
	mergedJSON, refsJSON, err := e.linkerFor(shardNum).Mint(merged, spawnChanges, spawnRefs, cohortSpawnID, staterefs.Linear, withoutKeys(memberWrites, folds), nil, folds)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(err)
	}
	// The fold is where a cohort's contributions meet: an append over N branches can outgrow the column though no
	// branch wrote that much.
	if err := e.checkPayload("fan-in state of task '"+stripProto(fanInTaskName)+"'", mergedJSON); err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, err
	}
	bytes.stateWritten = len(mergedJSON)
	bytes.refsWritten = len(refsJSON)
	row = successorRow{
		taskName: fanInTaskName, taskURL: dispatchURLOf(graph, fanInTaskName),
		stateJSON: mergedJSON, refsJSON: refsJSON, reducedJSON: reducedColumn(rec), ordinal: c.spawnFanOutOrdinal,
	}
	return row, depth, exitStepIDs, bytes, nil
}
