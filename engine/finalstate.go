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
	"encoding/json"
	"strings"

	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/graphcache"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// completeFlowSequential marks a flow completed when no successor exists.
//
// It does NOT touch the step. Both call sites are downstream of processStep's fenced completion UPDATE, which
// already set status='completed' (and bailed on a zero-row fence match), so the step is terminal before this
// runs. Do not add a trailing "UPDATE dwarf_steps SET status=completed" here: it re-writes that same value at
// the cost of a BEGIN/UPDATE/COMMIT on every flow completion, and its only real effect is to bump the step's
// updated_at to AFTER completeFlow's transaction, inflating the step's recorded task duration (History and the
// FlowRenderer read updated_at - started_at) by the cost of completing the flow. It would also be a
// post-execution step write with neither a status guard nor a lease_seq fence.
func (e *Engine) completeFlowSequential(ctx context.Context, shardNum int, flowID int, stepID int, flowToken string, workflowURL string) error {
	e.logger.DebugContext(ctx, "Flow completed", "workflow", workflowURL)
	_, err := e.completeFlow(ctx, shardNum, flowID, stepID, flowToken)
	return errors.Trace(err)
}

// completedTailsSQL selects a flow's completed tail steps (successor_id=0) for its final state; anyTailsSQL is the
// fallback when it finds none. Each carries the flow's payload_ver, because the final state is computed for flows
// this replica did not check at a claim (Terminate, the wedge sweep, a failStep past the deferral bound); the join
// puts the check on a read it makes anyway. Snapshot, Step and Fork check on their own reads.
//
// firstCompletedTailSQL and firstAnyTailSQL read only the first tail's lineage, which decides whether the tail rows
// are needed at all (readTerminalTails).
const (
	completedTailsSQL     = "SELECT s.state, s.changes, s.state_refs, s.lineage_id, f.payload_ver FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.flow_id=? AND s.successor_id=0 AND s.status='" + workflow.StatusCompleted + "' ORDER BY s.step_id"
	anyTailsSQL           = "SELECT s.state, s.changes, s.state_refs, s.lineage_id, f.payload_ver FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.flow_id=? AND s.successor_id=0 ORDER BY s.step_id"
	firstCompletedTailSQL = "SELECT s.lineage_id, f.payload_ver FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.flow_id=? AND s.successor_id=0 AND s.status='" + workflow.StatusCompleted + "' ORDER BY s.step_id LIMIT_OFFSET(1, 0)"
	firstAnyTailSQL       = "SELECT s.lineage_id, f.payload_ver FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.flow_id=? AND s.successor_id=0 ORDER BY s.step_id LIMIT_OFFSET(1, 0)"
)

// terminalTails is a flow's tail steps as read for its final state: the first tail's state is the merge base,
// and every tail's changes fold onto it. Rows are kept as read and decoded only by a merge that uses them: when
// the first tail is a cohort member, the final state is rebuilt from its cohort and the tails go unused.
//
// rowsSQL is set when only the first tail's lineage was read (readTerminalTails): the rows are then read only if
// the merge turns out to need them after all.
type terminalTails struct {
	found       bool
	baseState   []byte
	baseRefs    []byte
	baseLineage int
	changes     [][]byte
	rowsSQL     string
}

// scan reads one tail row.
func (t *terminalTails) scan(row interface{ Scan(dest ...any) error }) error {
	var stateJSON, changesJSON, refsJSON []byte
	var lineageID, flowPayloadVer int
	if err := row.Scan(&stateJSON, &changesJSON, &refsJSON, &lineageID, &flowPayloadVer); err != nil {
		return errors.Trace(err)
	}
	if err := checkPayloadVer(flowPayloadVer); err != nil {
		return err
	}
	if !t.found {
		t.found = true
		t.baseState, t.baseRefs, t.baseLineage = stateJSON, refsJSON, lineageID
	}
	t.changes = append(t.changes, changesJSON)
	return nil
}

// queueCompletedTails queues the read of a flow's completed tails into t. On PostgreSQL every tail row is read when
// the batch is sent, which costs one tail's state for a flow that completes normally: a validated graph converges
// every fan-out on its fan-in, so such a flow ends in a single tail.
func queueCompletedTails(b *txBatch, flowID int, t *terminalTails) {
	b.queue(func(res *sequel.BatchResults) error {
		rows, err := res.Query()
		if err != nil {
			return errors.Trace(err)
		}
		for rows.Next() {
			if err := t.scan(rows); err != nil {
				return err
			}
		}
		return errors.Trace(rows.Err())
	}, completedTailsSQL, flowID)
}

// readTerminalTails reads a flow's tails into t - its completed tails, or with all set every tail. It reads the
// first tail's lineage first: a cohort member means the final state is rebuilt from its cohort, so no tail row is
// read, which on a failed or terminated fan-out is every branch's full state and changes, under Terminate's and
// failStep's locks. Otherwise it reads the rows.
func readTerminalTails(ctx context.Context, db sequel.Executor, all bool, flowID int, t *terminalTails) error {
	firstSQL, rowsSQL := firstCompletedTailSQL, completedTailsSQL
	if all {
		firstSQL, rowsSQL = firstAnyTailSQL, anyTailsSQL
	}
	var lineageID, flowPayloadVer int
	err := db.QueryRowContext(ctx, firstSQL, flowID).Scan(&lineageID, &flowPayloadVer)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return errors.Trace(err)
	}
	if err := checkPayloadVer(flowPayloadVer); err != nil {
		return err
	}
	if lineageID != 0 {
		t.found, t.baseLineage, t.rowsSQL = true, lineageID, rowsSQL
		return nil
	}
	return readTailRows(ctx, db, rowsSQL, flowID, t)
}

// readTailRows reads every tail row by query into t.
func readTailRows(ctx context.Context, db sequel.Executor, query string, flowID int, t *terminalTails) error {
	rows, err := db.QueryContext(ctx, query, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := t.scan(rows); err != nil {
			return err
		}
	}
	return errors.Trace(rows.Err())
}

// terminalTailsOf returns a flow's tails for its terminal state: its completed tails when it has any, and every
// tail otherwise.
func terminalTailsOf(ctx context.Context, db sequel.Executor, flowID int, completed *terminalTails) (*terminalTails, error) {
	if completed.found {
		return completed, nil
	}
	t := &terminalTails{}
	if err := readTerminalTails(ctx, db, true, flowID, t); err != nil {
		return nil, errors.Trace(err)
	}
	return t, nil
}

// mergeTerminalSteps computes a flow's terminal state from the execution-DAG tail: the tails' changes fold onto
// the first tail's state through the reducers, as a linear transition's do. A flow with no tails has an empty
// state.
func (e *Engine) mergeTerminalSteps(ctx context.Context, db sequel.Executor, shardNum int, flowToken string, t *terminalTails, workflowURL string, reducers map[string]workflow.Reducer) (workflow.State, error) {
	if !t.found {
		return workflow.State{}, nil
	}
	// A tail this replica cannot read is a pure function of stored data, so it is permanent: a path that stops
	// the flow whatever it holds then falls back to an empty final state rather than failing the stop.
	base, err := workflow.NewState(t.baseState)
	if err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	baseRefs, err := staterefs.Parse(t.baseRefs)
	if err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	changes := make([]workflow.Changes, len(t.changes))
	for i, c := range t.changes {
		if changes[i], err = changesOf(c); err != nil {
			return workflow.State{}, errors.Trace(err)
		}
	}
	// FLATTEN: final_state is a flow-boundary value (a dwarf_flows column that Continue feeds into the next
	// turn, and that outlives this flow's steps - DeleteOnCompletion may reap them). A ref that escaped the
	// flow would dangle, so the terminal merge always materializes, whatever it costs on this one row.
	// It is not a regression either: a large field surviving to the end is copied into final_state today.
	// resolveStateRefs mutates the state map in place, so it gets the live map.
	if _, err := e.resolveStateRefs(ctx, db, shardNum, flowToken, base, baseRefs, nil, workflowURL); err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	// A tail's changes are its delta onto its input, folded through the reducers like any hand-off. A value a
	// reducer cannot combine is a pure function of stored data, so it is permanent.
	if err := base.ApplyAll(changes, reducers); err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	return base, nil
}

// flowGraph returns a flow's parsed graph and workflow URL, from the graph cache when the flow has been
// dispatched on this replica and from its row otherwise. The returned graph may be shared with concurrent
// dispatches and must not be mutated. A cache hit reads nothing, so it does not prove the flow row still exists.
func (e *Engine) flowGraph(ctx context.Context, db sequel.Executor, shardNum, flowID int, flowToken string) (*workflow.Graph, string, error) {
	if cg, ok := e.graphs.Flow(graphcache.Key{Shard: shardNum, FlowID: flowID, Token: flowToken}); ok {
		return cg.Graph, cg.WorkflowURL, nil
	}
	var workflowURL, graphHash string
	var graphJSON []byte
	err := db.QueryRowContext(ctx,
		"SELECT graph, graph_hash, workflow_url FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&graphJSON, &graphHash, &workflowURL)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	cg, err := e.internGraph(graphJSON, workflowURL, graphHash)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	return cg.Graph, workflowURL, nil
}

// computeFinalState computes the merged state for a flow.
func (e *Engine) computeFinalState(ctx context.Context, db sequel.Executor, shardNum, flowID int, flowToken string) ([]byte, string, error) {
	var completed terminalTails
	if err := readTerminalTails(ctx, db, false, flowID, &completed); err != nil {
		return nil, "", errors.Trace(err)
	}
	return e.computeFinalStateFrom(ctx, db, shardNum, flowID, flowToken, &completed)
}

// terminalFinalState is computeFinalState for a path that terminalizes a flow whatever its state holds -
// Terminate, Delete, Purge and the wedge sweep. A merge that cannot fold (a permanentError) yields an empty
// final_state rather than an error: the merge would fail identically on every attempt, so propagating it would
// leave the flow impossible to stop. A flow whose row is gone - reaped or deleted under a racing Terminate - needs
// no final state either: the terminalizing UPDATE's guard will match nothing. A database error still propagates.
func (e *Engine) terminalFinalState(ctx context.Context, db sequel.Executor, shardNum, flowID int, flowToken string) ([]byte, error) {
	fs, _, err := e.computeFinalState(ctx, db, shardNum, flowID, flowToken)
	if errors.Is(err, sql.ErrNoRows) {
		e.logger.WarnContext(ctx, "Computing final_state while terminalizing a flow found no row; terminalizing with empty state",
			"flow", keys.CorrelationID(shardNum, flowID), "error", err)
		return []byte("{}"), nil
	}
	if isPermanentError(err) {
		e.logger.ErrorContext(ctx, "Computing final_state while terminalizing a flow; terminalizing with empty state",
			"flow", keys.CorrelationID(shardNum, flowID), "error", err)
		return []byte("{}"), nil
	}
	return fs, errors.Trace(err)
}

// computeFinalStateFrom is computeFinalState from the flow's completed tails, already read (queueCompletedTails).
func (e *Engine) computeFinalStateFrom(ctx context.Context, db sequel.Executor, shardNum, flowID int, flowToken string, completed *terminalTails) ([]byte, string, error) {
	graph, workflowURL, err := e.flowGraph(ctx, db, shardNum, flowID, flowToken)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	tails, err := terminalTailsOf(ctx, db, flowID, completed)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// When the tails are COHORT MEMBERS (lineage_id != 0), a fan-out resolved with failures and so never reached
	// its fan-in. Rebuild the state the way the convergence would have, rather than from the tail rows - and
	// decide that from the first tail's lineage alone, before decoding, resolving or folding any tail: on a wide
	// failed fan-out that is every branch's row and the forEach array, under Terminate's and failStep's locks.
	//
	// Do not merge the tails here: mergeTerminalSteps bases on the FIRST tail's `state` and folds in every tail's
	// `changes`, which loses the intermediate output of every branch but one. In a multi-step branch
	// (`forEach -> Cell -> Enrich -> J`), Cell's output lives in Enrich's `state`, not in its `changes`: only the
	// first tail's state is consulted, so branch 0's Cell output rides in via the base while branches 1..N-1's is
	// dropped. The converged path (fanInRow) merges EVERY cohort member's `changes`, and so must this one, or the
	// two disagree on what a fan-out's terminal state means.
	//
	// Every step of a per-element sub-pipeline inherits the spawn's lineage, so "every cohort member" is one
	// indexed scan and it covers Cell AND Enrich, for every branch.
	//
	// The rebuild starts at the OUTERMOST cohort on the tail's lineage chain, whose spawn is on the trunk. The
	// tail may sit deep inside a nested fan-out whose inner cohort failed, and rebuilding only that inner cohort
	// would drop everything outside it - the flow's base state and every other branch at every level.
	outermost, err := e.outermostSpawn(ctx, db, tails.baseLineage)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	var merged workflow.State
	if outermost != 0 {
		merged, err = e.mergeCohortState(ctx, db, shardNum, flowID, flowToken, outermost, graph, workflowURL)
	} else {
		// A tail outside every cohort - the ordinary completed flow, whose terminal step is downstream of every
		// fan-in - is stripped of NOTHING: every fan-in it passed already stripped its own cohort's names. Its rows
		// are still unread only when its lineage named a spawn that is gone.
		if tails.rowsSQL != "" {
			lazy := &terminalTails{}
			if err := readTailRows(ctx, db, tails.rowsSQL, flowID, lazy); err != nil {
				return nil, "", errors.Trace(err)
			}
			tails = lazy
		}
		merged, err = e.mergeTerminalSteps(ctx, db, shardNum, flowToken, tails, workflowURL, graph.Reducers())
	}
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	data, err := json.Marshal(merged)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	// The final state is flattened - every ref resolved - so it holds the whole state, which can exceed what one
	// column stores though no step's row did. A permanentError, so the flow fails with an empty final state
	// (failFlowTx), and Terminate still stops it (terminalFinalState).
	if err := e.checkPayloadSize("final state", data); err != nil {
		return nil, "", err
	}
	return data, workflowURL, nil
}

// mergeCohortState rebuilds the state of a cohort that resolved with failures, and so never reached its fan-in.
// It runs the SAME merge fanInRow runs at a successful convergence - the spawn step's `state + changes` as
// the base, then every COMPLETED member's `changes` folded through the graph's reducers in `fan_out_ordinal,
// step_depth, step_id` order - so a fan-out's terminal state means one thing whether it converged or failed.
//
// Why the base is the SPAWN and not a tail: a branch's intermediate output lives in the NEXT step's `state`, not in
// any tail's `changes`, so merging tails alone silently drops everything but the last step of each branch (and, for
// the state half, everything but the first tail's branch). The spawn's own row is the only base that is common to
// the whole cohort, and every member's `changes` is exactly the set of per-branch outputs.
//
// Reducer ORDER matters (append/union/concat are not commutative), which is why the scan is ordered the same way
// the convergence orders it: by the branch's position in the spawn loop, not by completion order.
//
// A member contributes as it would at a convergence (cohortContributions), so an inner fan-in brings its whole
// cohort, and an inner cohort that never converged brings what its fan-in would have (simulatedFanInPart).
//
// The cohort's own per-branch bookkeeping is stripped last, exactly as fanInRow strips it, and no other cohort's:
// every inner cohort's names were already stripped from that cohort's own fold. Stripping an inner cohort's
// names HERE would hit the trunk instead, which never held them - a trunk field that shares an inner `as` name
// (`pageCount` under `forEach ... as "page"`) survives a convergence and would vanish from a failure.
func (e *Engine) mergeCohortState(ctx context.Context, db sequel.Executor, shardNum, flowID int, flowToken string, cohortSpawnID int, graph *workflow.Graph, workflowURL string) (workflow.State, error) {
	var spawnStateJSON, spawnChangesJSON, spawnRefsJSON []byte
	var spawnTask string
	err := db.QueryRowContext(ctx,
		"SELECT state, changes, state_refs, task_name FROM dwarf_steps WHERE step_id=?", cohortSpawnID,
	).Scan(&spawnStateJSON, &spawnChangesJSON, &spawnRefsJSON, &spawnTask)
	if err == sql.ErrNoRows {
		return workflow.State{}, nil
	}
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	spawnState, err := workflow.NewState(spawnStateJSON)
	if err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	spawnChanges, err := changesOf(spawnChangesJSON)
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	// FLATTEN: final_state is a flow-boundary value, so everything the spawn carried by reference is materialized
	// here - unlike the fan-in, which may pass a carried ref through. resolveStateRefs mutates the map in place.
	spawnRefs, err := staterefs.Parse(spawnRefsJSON)
	if err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	if _, err := e.resolveStateRefs(ctx, db, shardNum, flowToken, spawnState, spawnRefs, nil, workflowURL); err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	// The failed cohort's terminal state is built the SAME way convergence would (fanInRow): the spawn's
	// changes folded onto its state, as the base the completed members fold onto below.
	merged := spawnState.Clone()
	if err := merged.Apply(spawnChanges, graph.Reducers()); err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}

	contributions, err := e.cohortContributions(ctx, db, shardNum, flowID, flowToken, cohortSpawnID, graph, workflowURL)
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	// Folded in ONE pass: a combining reducer re-encodes its accumulator, so folding per member re-encodes a
	// growing value once per member - quadratic across a wide cohort.
	if err := merged.ApplyAll(contributions, graph.Reducers()); err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	stripForEachBookkeeping(merged, graph, strings.TrimSpace(spawnTask))
	return merged, nil
}

// simulatedFanInPart is what a cohort that never converged would have handed the next cohort out, had its fan-in
// fired: the same base (fanInBase), the same fold (foldCohort), the same strip of this cohort's own names, and
// the same projection the outer fold reads from a real fan-in's row (projectReduced). Only the fan-in TASK is
// missing, so nothing it would have written is.
//
// Running the fan-in's fold, strip and projection, rather than handing the members' raw contributions to the outer fold, is what
// keeps a failed nested fan-out's final_state equal to a converged one's. A member's write to its own cohort's
// `as` name projects as a delete, as it does through a real fan-in, and the strip touches this cohort's fold
// and not the trunk.
func (e *Engine) simulatedFanInPart(ctx context.Context, db sequel.Executor, shardNum, flowID int, flowToken string, spawnID int, spawnTask string, graph *workflow.Graph, workflowURL string) (workflow.Changes, error) {
	var stateJSON, changesJSON, refsJSON []byte
	err := db.QueryRowContext(ctx,
		"SELECT state, changes, state_refs FROM dwarf_steps WHERE step_id=?", spawnID,
	).Scan(&stateJSON, &changesJSON, &refsJSON)
	if err == sql.ErrNoRows {
		return workflow.Changes{}, nil
	}
	if err != nil {
		return workflow.Changes{}, errors.Trace(err)
	}
	refs, err := staterefs.Parse(refsJSON)
	if err != nil {
		return workflow.Changes{}, errors.Trace(&permanentError{err})
	}
	state, err := workflow.NewState(stateJSON)
	if err != nil {
		return workflow.Changes{}, errors.Trace(&permanentError{err})
	}
	reducers := graph.Reducers()
	changes, err := changesOf(changesJSON)
	if err != nil {
		return workflow.Changes{}, errors.Trace(err)
	}
	merged, err := e.fanInBase(ctx, db, shardNum, flowToken, state, changes, refs, reducers, workflowURL)
	if err != nil {
		return workflow.Changes{}, errors.Trace(err)
	}
	contributions, err := e.cohortContributions(ctx, db, shardNum, flowID, flowToken, spawnID, graph, workflowURL)
	if err != nil {
		return workflow.Changes{}, errors.Trace(err)
	}
	rec, err := foldCohort(merged, contributions, reducers, true)
	if err != nil {
		return workflow.Changes{}, errors.Trace(&permanentError{err})
	}
	stripForEachBookkeeping(merged, graph, strings.TrimSpace(spawnTask))
	part, err := projectReduced(merged, rec, reducers)
	if err != nil {
		return workflow.Changes{}, errors.Trace(&permanentError{err})
	}
	return part, nil
}

// cohortContributions returns what each completed member of a cohort adds to its fold, in fold order. A member
// that is an inner fan-in brings what its own cohort added (memberContribution). A member that spawned an inner
// cohort which never converged - it failed, or the flow stopped first - has no fan-in to carry that cohort's
// work, so what that fan-in would have carried follows the member's own contribution, in the place the fan-in
// step would have taken in fold order (simulatedFanInPart, at any depth).
//
// It runs only on the path that terminalizes a flow whose fan-out never converged, so reading a cohort per
// level is affordable.
func (e *Engine) cohortContributions(ctx context.Context, db sequel.Executor, shardNum, flowID int, flowToken string, cohortSpawnID int, graph *workflow.Graph, workflowURL string) ([]workflow.Changes, error) {
	type member struct {
		cohortMember
		cohortSize, arrivals, failures int
	}
	rows, err := db.QueryContext(ctx,
		"SELECT step_id, task_name, status, changes, cohort_size, cohort_arrivals, cohort_failures FROM dwarf_steps"+
			" WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_depth, step_id",
		flowID, cohortSpawnID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var members []member
	for rows.Next() {
		var m member
		var status string
		var changesJSON []byte
		if err := rows.Scan(&m.stepID, &m.taskName, &status, &changesJSON, &m.cohortSize, &m.arrivals, &m.failures); err != nil {
			rows.Close()
			return nil, errors.Trace(err)
		}
		// Only a completed member's changes contribute, matching fanInRow: a failed, terminated, or cancelled
		// branch's partial output is not a fact the flow can build on (an error voids the task's changes). An
		// inner fan-in that did not complete still brings its cohort's part (memberContribution).
		if m.completed = status == workflow.StatusCompleted; m.completed {
			var cerr error
			if m.changes, cerr = changesOf(changesJSON); cerr != nil {
				rows.Close()
				return nil, errors.Trace(cerr)
			}
		}
		members = append(members, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}

	asMembers := make([]cohortMember, len(members))
	for i, m := range members {
		asMembers[i] = m.cohortMember
	}
	reducers := graph.Reducers()
	var parts map[int]workflow.Changes
	if ids := innerFanInIDs(graph, asMembers); len(ids) > 0 {
		parts, _, err = cohortParts(ctx, db, ids, reducers)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	var contributions []workflow.Changes
	for _, m := range members {
		contribution, ok, err := memberContribution(parts[m.stepID], m.changes, reducers)
		if err != nil {
			return nil, errors.Trace(&permanentError{err})
		}
		if ok {
			contributions = append(contributions, contribution)
		}
		// A spawn's cohort exists only once the spawn completed.
		if m.completed && m.cohortSize > 0 && (m.arrivals < m.cohortSize || m.failures > 0) {
			part, err := e.simulatedFanInPart(ctx, db, shardNum, flowID, flowToken, m.stepID, m.taskName, graph, workflowURL)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if part.Len() > 0 {
				contributions = append(contributions, part)
			}
		}
	}
	return contributions, nil
}

// outermostSpawn walks a step's lineage chain upward and returns the step_id of the outermost fan-out spawn whose
// cohort the step is inside (0 for a step outside any cohort). `lineage_id` names the spawn step of the cohort a
// step belongs to (0 outside any cohort), and a spawn is itself a step with its own lineage, so nested fan-outs
// form a chain. Each level costs one primary-key lookup and the chain is as deep as the graph's fan-out nesting
// (typically zero or one), on a terminal path.
func (e *Engine) outermostSpawn(ctx context.Context, db sequel.Executor, lineageID int) (outermost int, err error) {
	seen := map[int]bool{}
	for lineageID != 0 && !seen[lineageID] {
		seen[lineageID] = true // a malformed chain must not spin
		var next int
		err := db.QueryRowContext(ctx,
			"SELECT lineage_id FROM dwarf_steps WHERE step_id=?", lineageID,
		).Scan(&next)
		if err == sql.ErrNoRows {
			return outermost, nil
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		outermost = lineageID
		lineageID = next
	}
	return outermost, nil
}

// stripForEachBookkeeping deletes the per-branch fields the engine injects into a forEach branch's state - the
// element (`<as>`) and its ordinal context (`<as>Index`, `<as>Count`) - from a materialized state map, for the
// ONE cohort spawned by spawnTask. They are a branch's private context and have no meaning once that cohort is
// behind the flow: with the Replace reducer one arbitrary branch's element would otherwise ride forward.
//
// Scoping to the spawning task is load-bearing in both directions. Do not strip every forEach in the graph: that
// (a) makes the three names of every `as` globally reserved, silently deleting a same-named field a task wrote,
// and (b) breaks nesting - at an INNER fan-in it would also delete the OUTER cohort's bookkeeping, so a step
// still inside the outer branch could no longer see which element it was working on. The names are reserved
// only WITHIN their own cohort; a workflow wanting the element past that cohort's fan-in forwards it under a different key.
//
// Applied wherever a cohort's state leaves the cohort: fanInRow (the convergence), and for a fan-out that never
// converged, mergeCohortState (its outermost cohort) and simulatedFanInPart (each inner one), so the two paths
// cannot drift on what a fan-out's state means.
//
// It returns the names it deleted, so a caller minting the stripped state can keep them from being re-emitted
// through a carried ref.
func stripForEachBookkeeping(state workflow.State, graph *workflow.Graph, spawnTask string) []string {
	if state.Len() == 0 || spawnTask == "" {
		return nil
	}
	var names []string
	for _, tr := range graph.Transitions() {
		if tr.From != spawnTask || tr.ForEach == "" || tr.As == "" {
			continue
		}
		names = append(names, tr.As, tr.As+"Index", tr.As+"Count")
	}
	state.Del(names...)
	return names
}

// completeFlow transitions a flow to completed and propagates to surgraph. stepID is the step that completed it,
// recorded as the step the flow stopped at.
func (e *Engine) completeFlow(ctx context.Context, shardNum int, flowID int, stepID int, flowToken string) (bool, error) {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}
	var workflowURL, currentStatus string
	var finalStateJSON []byte
	var surgraphFlowID, surgraphStepID, priority int
	var deleteOnCompletion bool
	completed := false
	// Test checkpoint: a breakpoint here freezes completion just before its transaction (holding no lock, so a
	// racing Terminate can commit), letting a test drive the completeFlow-vs-Terminate race in either order.
	// Placed before the transaction, not inside, for the same SQLite-deadlock reason as
	// engineseams.CheckpointResumeBeforeFlowWrite.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeCompleteFlowWrite)
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		completed = false
		// engineseams.FaultCompleteFlowCommit fails the flow-completion transaction after processStep has already
		// marked the terminal step completed, so the test proves the write-first ordering + recovery defer
		// keep the flow from stranding `running` with every step terminal. Non-retryable (a plain error).
		if e.seams.IsFault(engineseams.FaultCompleteFlowCommit) {
			return errors.New("injected fault: " + engineseams.FaultCompleteFlowCommit)
		}
		// Write-first: take the flow row's write lock before computeFinalState's reads. Without this the
		// transaction is read-first (SELECT graph + terminal steps, then UPDATE), and on SQLite with
		// cache=shared two concurrent completions both hold SHARED locks and deadlock on the upgrade to
		// write - which under load exhausts Transact's retries and errors. Because the terminal step is
		// already marked completed by processStep, the lease recovery (which only resets running rows)
		// cannot re-dispatch it, leaving the flow stranded 'running' with all steps terminal (an orphan
		// flow). Mirrors the transition transaction and fireFanInDirect, which write first for the same reason.
		// The grab takes the lock the way lockFlowRowsSQL does: a locking read on PostgreSQL and MySQL, a flip of
		// the non-indexed `touch` elsewhere, never `updated_at`, which moves only on the genuine status transition
		// below, so idx_dwarf_flows_status is not churned twice per completion.
		//
		// The lock grab also reads back the surgraph linkage, the disposable flag and the priority - needed for
		// the post-tx surgraph revival, the atomic disposable delete below and the ended counter - and it goes out in one statement batch with
		// the read of the flow's completed tails, which the final state is merged from. It stays unguarded on
		// status: the status gate is the later UPDATE, and the status read here drives the revive re-drive.
		const lockedCols = "surgraph_flow_id, surgraph_step_id, delete_on_completion, status, priority"
		const lockGrab = "UPDATE dwarf_flows SET touch=1-touch"
		const lockWhere = " WHERE flow_id=?"
		batch := newTxBatch(ctx, tx)
		readLocked := func(res *sequel.BatchResults) error {
			// A pipelined batch scans with the PostgreSQL driver's own conversions, which do not turn a SMALLINT
			// into a bool, so the flag is scanned as the integer it is stored as.
			var disposable int
			err := res.QueryRow().Scan(&surgraphFlowID, &surgraphStepID, &disposable, &currentStatus, &priority)
			deleteOnCompletion = disposable != 0
			return errors.Trace(err)
		}
		switch tx.DriverName() {
		case "pgx":
			batch.queue(readLocked, "SELECT "+lockedCols+" FROM dwarf_flows"+lockWhere+" FOR NO KEY UPDATE", flowID)
		case "mysql":
			batch.queue(readLocked, "SELECT "+lockedCols+" FROM dwarf_flows"+lockWhere+" FOR UPDATE", flowID)
		case "mssql":
			batch.queue(readLocked, lockGrab+" OUTPUT INSERTED.surgraph_flow_id, INSERTED.surgraph_step_id, INSERTED.delete_on_completion, INSERTED.status, INSERTED.priority"+lockWhere, flowID)
		default:
			batch.queue(readLocked, lockGrab+lockWhere+" RETURNING "+lockedCols, flowID)
		}
		var tails terminalTails
		queueCompletedTails(batch, flowID, &tails)
		if err := batch.flush(); err != nil {
			return err
		}
		fs, wf, err := e.computeFinalStateFrom(ctx, tx, shardNum, flowID, flowToken, &tails)
		if err != nil {
			return errors.Trace(err)
		}
		finalStateJSON = fs
		workflowURL = wf
		// A DeleteOnCompletion root schedules its own deletion by stamping delete_after_ms = deletionGrace, so
		// the reaper removes it (and its subgraph descendants, keyed on root_flow_id) after the grace window.
		// It is NOT deleted inline: the flow stays `completed` for the window so its outcome is
		// Await/Snapshot-observable (an inline delete would 404 the caller). Root-only (surgraph_flow_id=0);
		// the stamp is part of the same status transition, so delete_after_ms > 0 always implies terminal.
		deleteAfterMs := 0
		if deleteOnCompletion && surgraphFlowID == 0 {
			deleteAfterMs = int(e.deletionGrace.Milliseconds())
		}
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, final_state=?, delete_after_ms=?, stopped_step_id=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ("+terminalStatusesSQL+")",
			workflow.StatusCompleted, finalStateJSON, deleteAfterMs, stepID, flowID,
		)
		if err != nil {
			return errors.Trace(err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			completed = true
		}
		return nil
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	if !completed {
		// Our status UPDATE matched no row, so the flow was already terminal at the lock. If it is `completed`
		// with a surgraph parent, the revive still owes a re-drive - the revive-lost-on-retry case: on
		// the FIRST attempt this transaction committed the completion and the post-tx completeSurgraphFlow then
		// hit a transient DB error, so persist re-ran this idempotent closure; on this retry the status UPDATE
		// no-ops (already completed) and, without re-driving the revive here, persist would read the nil below as
		// "the write landed" and the parent's caller step would strand running+parkedSubgraph until the ~10m
		// parked-step wedge sweep (raising a false wedge alarm). completeSurgraphFlow is CAS-guarded on
		// running+parkedSubgraph, so re-driving is idempotent (a peer that already revived, or that raced us to
		// complete, makes this a harmless no-op). signalStop/metrics are deliberately NOT repeated - they fired
		// on the attempt that transitioned the flow. A failed/terminated/cancelled flow (status != completed)
		// gets no revive here; its parent is handled by failure delivery / the Terminate cascade.
		if currentStatus == workflow.StatusCompleted && surgraphFlowID != 0 {
			if rerr := e.completeSurgraphFlow(ctx, shardNum, surgraphFlowID, surgraphStepID, finalStateJSON); rerr != nil {
				return false, errors.Trace(rerr)
			}
		}
		return false, nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", workflow.StatusCompleted)
	e.metricFlowEnded(ctx, workflowURL, workflow.StatusCompleted, priority, shardNum)
	compositeID := keys.New(shardNum, flowID, flowToken)

	e.signalStop(ctx, compositeID, workflow.StatusCompleted)

	if surgraphFlowID != 0 {
		err := e.completeSurgraphFlow(ctx, shardNum, surgraphFlowID, surgraphStepID, finalStateJSON)
		if err != nil {
			return true, errors.Trace(err)
		}
	}

	return true, nil
}
