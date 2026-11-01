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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

func (e *Engine) history(ctx context.Context, flowKey string) ([]workflow.FlowStep, error) {
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "history")
	defer doneTurn()
	// delete_after_ms=0 excludes a flow scheduled for deletion: History is the full step detail a disposable
	// flow is discarding, so it 404s during the grace window (Snapshot/Await still serve the outcome). Only the
	// root is stamped, so a subgraph child's key is checked against its root's row (a root is its own root).
	var history []workflow.FlowStep
	found, err := e.queryLiveOrArchive(ctx, db, shardNum, flowID, flowHour, false, func(t flowTables) (bool, error) {
		var exists int
		err := db.QueryRowContext(ctx,
			"SELECT 1 FROM "+t.flows+" f JOIN "+t.flows+" r ON r.flow_id=f.root_flow_id"+
				" WHERE f.flow_id=? AND f.flow_token=? AND f.created_hour=? AND r.delete_after_ms=0", flowID, flowToken, flowHour).Scan(&exists)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		e.seams.Checkpoint(ctx, engineseams.CheckpointHistoryFlowRead)
		// Load the whole subgraph tree in a fixed two queries via root_flow_id (single-shard by parent-shard
		// affinity), then assemble the nested history in memory - instead of a per-step surgraph_step_id lookup,
		// an N+1 over every step in the tree. This mirrors the root_flow_id membership walks in subgraph.go.
		// Starting from a mid-tree flow (a subgraph child, which History accepts) over-fetches the ancestor and
		// sibling subtrees, but the assembly only descends from flowID, so the extra rows are never rendered.
		childByCaller, err := e.loadSubgraphChildren(ctx, db, t, shardNum, flowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		stepsByFlow, err := e.loadTreeSteps(ctx, db, t, shardNum, flowID)
		if err != nil {
			return false, errors.Trace(err)
		}
		history = assembleHistory(flowID, childByCaller, stepsByFlow)
		return true, nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	return history, nil
}

// subgraphChild is the latest child flow launched by a surgraph caller step, used to attach subhistory
// during in-memory History assembly.
type subgraphChild struct {
	flowID       int
	flowKey      string
	workflowURL  string
	workflowName string
}

// loadSubgraphChildren returns, for the whole subgraph tree flowID belongs to, a map from each surgraph
// caller step's id to the latest child flow it launched. flow.Retry rewinds a caller in place and re-spawns
// a fresh child each attempt, so several child flows can share one surgraph_step_id; ORDER BY flow_id ASC
// means the last row written for a caller step wins, giving the highest flow_id (the child whose value the
// caller actually used). Each child carries its flow key: a read handle within the tree whose History the caller
// already holds, every mutation of it is rejected.
func (e *Engine) loadSubgraphChildren(ctx context.Context, db *sequel.DB, t flowTables, shardNum int, flowID int) (map[int]subgraphChild, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, flow_token, created_hour, surgraph_step_id, workflow_url, workflow_name FROM "+t.flows+
			" WHERE root_flow_id=(SELECT root_flow_id FROM "+t.flows+" WHERE flow_id=?) AND surgraph_step_id>0 ORDER BY flow_id",
		flowID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	childByCaller := map[int]subgraphChild{}
	for rows.Next() {
		var childFlowID, childHour, callerStepID int
		var token, url, name string
		err := rows.Scan(&childFlowID, &token, &childHour, &callerStepID, &url, &name)
		if err != nil {
			return nil, errors.Trace(err)
		}
		childByCaller[callerStepID] = subgraphChild{
			flowID:       childFlowID,
			flowKey:      keys.NewFlowKey(shardNum, childHour, childFlowID, strings.TrimSpace(token)),
			workflowURL:  strings.TrimSpace(url),
			workflowName: strings.TrimSpace(name),
		}
	}
	return childByCaller, errors.Trace(rows.Err())
}

// treeStep is one step loaded by loadTreeSteps, with the step_ids that History assembly keys on.
type treeStep struct {
	id, predID, succID int
	step               workflow.FlowStep
}

// loadTreeSteps loads every step of every flow in flowID's subgraph tree (one query, keyed on the
// root_flow_id membership index), grouped by owning flow_id and ordered within each flow. DAG edges never
// leave their flow, so every predecessor_id/successor_id resolves to a key among the loaded rows.
func (e *Engine) loadTreeSteps(ctx context.Context, db *sequel.DB, t flowTables, shardNum int, flowID int) (map[int][]treeStep, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, step_id, step_token, flow_created_hour, step_depth, task_name, attempt, status, error, created_at, started_at, updated_at, predecessor_id, successor_id, parked, engine_id FROM "+t.steps+
			" WHERE flow_id IN (SELECT flow_id FROM "+t.flows+" WHERE root_flow_id=(SELECT root_flow_id FROM "+t.flows+" WHERE flow_id=?)) ORDER BY flow_id, step_depth, step_id",
		flowID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	stepsByFlow := map[int][]treeStep{}
	keyOf := map[int]string{}
	for rows.Next() {
		var step workflow.FlowStep
		var stepFlowID, stepID, stepHour, predID, succID, parked int
		var stepToken, errMsg string
		err := rows.Scan(&stepFlowID, &stepID, &stepToken, &stepHour, &step.StepDepth, &step.TaskName, &step.Attempt, &step.Status, &errMsg, &step.CreatedAt, &step.StartedAt, &step.UpdatedAt, &predID, &succID, &parked, &step.EngineID)
		if err != nil {
			return nil, errors.Trace(err)
		}
		step.Parked = parked != 0
		step.StepKey = keys.NewStepKey(shardNum, stepHour, stepFlowID, stepID, stepToken)
		step.Error = strings.TrimSpace(errMsg)
		keyOf[stepID] = step.StepKey
		stepsByFlow[stepFlowID] = append(stepsByFlow[stepFlowID], treeStep{id: stepID, predID: predID, succID: succID, step: step})
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	for _, tree := range stepsByFlow {
		for i := range tree {
			tree[i].step.PredecessorKey = keyOf[tree[i].predID]
			tree[i].step.SuccessorKey = keyOf[tree[i].succID]
		}
	}
	return stepsByFlow, nil
}

// assembleHistory builds one flow's history from the pre-loaded tree maps, recursively attaching each
// surgraph caller step's latest child subhistory. Purely in memory - no per-step query. Each flow has one
// surgraph parent, so the recursion visits every flow at most once (no cycle guard needed).
func assembleHistory(flowID int, childByCaller map[int]subgraphChild, stepsByFlow map[int][]treeStep) []workflow.FlowStep {
	tree := stepsByFlow[flowID]
	steps := make([]workflow.FlowStep, len(tree))
	for i, ts := range tree {
		steps[i] = ts.step
		child, ok := childByCaller[ts.id]
		if !ok {
			continue
		}
		sub := assembleHistory(child.flowID, childByCaller, stepsByFlow)
		if len(sub) > 0 {
			steps[i].Subgraph = true
			steps[i].SubgraphKey = child.flowKey
			steps[i].SubgraphURL = child.workflowURL
			steps[i].SubgraphName = child.workflowName
			steps[i].SubHistory = sub
		}
	}
	return steps
}

func (e *Engine) step(ctx context.Context, stepKey string) (*workflow.FlowStep, error) {
	shardNum, stepHour, stepFlowID, stepID, stepToken, err := keys.ParseStepKey(stepKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.New("step not found", http.StatusNotFound) // an unknown shard reads as an unknown step
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "step")
	defer doneTurn()
	var fs *workflow.FlowStep
	found, err := e.queryLiveOrArchive(ctx, db, shardNum, stepFlowID, stepHour, false, func(t flowTables) (bool, error) {
		var err error
		fs, err = e.stepIn(ctx, db, t, shardNum, stepKey, stepID, stepFlowID, stepToken, stepHour)
		return fs != nil, errors.Trace(err)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, errors.New("step not found", http.StatusNotFound)
	}
	return fs, nil
}

// stepIn reads the step with the given id and key parts from t, with its state refs resolved from t's steps table -
// an anchor never crosses a flow, so it is in the same tables - and its navigation keys. It returns nil when t holds
// no such step.
func (e *Engine) stepIn(ctx context.Context, db *sequel.DB, t flowTables, shardNum int, stepKey string, stepID, stepFlowID int,
	stepToken string, stepHour int) (*workflow.FlowStep, error) {
	var taskName, statusStr, errMsg, flowToken string
	var stateJSON, changesJSON, stateRefsJSON, interruptJSON []byte
	var stepDepth, attempt, predID, succID, ver int
	var createdAt, updatedAt time.Time
	err := db.QueryRowContext(ctx,
		"SELECT s.step_depth, s.task_name, s.attempt, s.state, s.changes, s.state_refs, s.interrupt_payload, s.status, s.error, s.created_at, s.updated_at, s.predecessor_id, s.successor_id, f.payload_ver, f.flow_token FROM "+t.steps+" s JOIN "+t.flows+" f ON f.flow_id=s.flow_id WHERE s.step_id=? AND s.flow_id=? AND s.step_token=? AND s.flow_created_hour=?",
		stepID, stepFlowID, stepToken, stepHour,
	).Scan(&stepDepth, &taskName, &attempt, &stateJSON, &changesJSON, &stateRefsJSON, &interruptJSON, &statusStr, &errMsg, &createdAt, &updatedAt, &predID, &succID, &ver, &flowToken)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err == nil {
		err = checkPayloadVer(ver)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.seams.Checkpoint(ctx, engineseams.CheckpointStepRead)
	fs := &workflow.FlowStep{
		StepKey:   stepKey,
		StepDepth: stepDepth,
		TaskName:  taskName,
		Attempt:   attempt,
		Status:    statusStr,
		Error:     strings.TrimSpace(errMsg),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	fs.State, err = workflow.NewState(stateJSON)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Materialize the step's carried-by-reference fields: the ref encoding is internal storage, never an
	// API-visible one, so Step reports the input the task actually saw. resolveStateRefs mutates the state map
	// in place.
	refs, err := staterefs.Parse(stateRefsJSON)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if _, err = e.resolveStateRefsIn(ctx, db, t.steps, shardNum, flowToken, fs.State, refs, nil, ""); err != nil {
		return nil, errors.Trace(err)
	}
	// An overwrite shows as its new value and a delete as a null, which is how a caller reads a delete.
	changes, err := changesOf(changesJSON)
	if err != nil {
		return nil, errors.Trace(err)
	}
	fs.Changes = changesView(changes, fs.State)
	if len(interruptJSON) > 0 {
		fs.InterruptPayload, err = workflow.NewState(interruptJSON)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	// Navigation skips the surgraph wrapper entirely: it's a routing/structural
	// step (parked while the subgraph runs) and isn't on the execution timeline
	// the user wants to walk. We resolve the effective neighbor in four steps:
	//
	//   1. Start with the intra-flow predecessor_id / successor_id.
	//   2. If the current step is a subgraph entry/exit (predID/succID == 0),
	//      stitch across the seam to the *parent's* surgraph-step's intra-flow
	//      neighbor (skipping the wrapper itself).
	//   3. If the current step is itself a surgraph (has a child flow attached),
	//      jump straight to that child flow's entry on successor.
	//   4. Repeat the "neighbor is a surgraph -> descend" walk until the
	//      effective neighbor is a regular step. Nested subgraphs naturally
	//      unwrap in one direction or the other.
	effectivePredID := predID
	effectiveSuccID := succID
	if predID == 0 || succID == 0 {
		// We may be inside a subgraph - look up our own flow's surgraph linkage.
		var surgraphStepID int
		err = db.QueryRowContext(ctx,
			"SELECT f.surgraph_step_id FROM "+t.steps+" s JOIN "+t.flows+" f ON s.flow_id = f.flow_id WHERE s.step_id=?",
			stepID,
		).Scan(&surgraphStepID)
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if surgraphStepID > 0 {
			// Cross-flow ascend: skip the surgraph wrapper and jump to its
			// intra-flow neighbor in the parent flow.
			var parentPred, parentSucc int
			err = db.QueryRowContext(ctx,
				"SELECT predecessor_id, successor_id FROM "+t.steps+" WHERE step_id=?",
				surgraphStepID,
			).Scan(&parentPred, &parentSucc)
			if err != nil && err != sql.ErrNoRows {
				return nil, errors.Trace(err)
			}
			if effectivePredID == 0 && parentPred > 0 {
				effectivePredID = parentPred
			}
			if effectiveSuccID == 0 && parentSucc > 0 {
				effectiveSuccID = parentSucc
			}
		}
	}
	// If the current step itself is a surgraph, descend on the successor side
	// (entry of its subgraph) so navigation enters the child instead of skipping
	// past it.
	var ownChildFlow int
	err = db.QueryRowContext(ctx,
		"SELECT flow_id FROM "+t.flows+" WHERE surgraph_step_id=? ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
		stepID,
	).Scan(&ownChildFlow)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Trace(err)
	}
	if ownChildFlow > 0 {
		var entry int
		err = db.QueryRowContext(ctx,
			"SELECT step_id FROM "+t.steps+" WHERE flow_id=? AND predecessor_id=0 ORDER BY step_id LIMIT_OFFSET(1, 0)",
			ownChildFlow,
		).Scan(&entry)
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if entry > 0 {
			effectiveSuccID = entry
		}
	}
	// Walk past any surgraph wrapper that the effective neighbor lands on,
	// descending into the appropriate end of the subgraph (entry for forward,
	// exit for backward). Loop in case of nested subgraphs.
	effectiveSuccID, err = e.skipSurgraphForward(ctx, db, t, effectiveSuccID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	effectivePredID, err = e.skipSurgraphBackward(ctx, db, t, effectivePredID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Resolve the DAG-edge and navigation neighbors to step keys. The cross-flow
	// fallbacks above still land on the same shard (subgraph flows have shard
	// affinity with their parent), so one IN query fetches every row.
	var ids []any
	for _, id := range []int{predID, succID, effectivePredID, effectiveSuccID} {
		if id > 0 && !slices.Contains(ids, any(id)) {
			ids = append(ids, id)
		}
	}
	if len(ids) > 0 {
		placeholders := strings.Repeat("?,", len(ids))
		placeholders = placeholders[:len(placeholders)-1]
		nrows, err := db.QueryContext(ctx,
			"SELECT step_id, step_token, flow_created_hour, flow_id FROM "+t.steps+" WHERE step_id IN ("+placeholders+")",
			ids...,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer nrows.Close()
		for nrows.Next() {
			var nid, nhour, nflow int
			var ntoken string
			err := nrows.Scan(&nid, &ntoken, &nhour, &nflow)
			if err != nil {
				return nil, errors.Trace(err)
			}
			key := keys.NewStepKey(shardNum, nhour, nflow, nid, ntoken)
			if nid == predID {
				fs.PredecessorKey = key
			}
			if nid == succID {
				fs.SuccessorKey = key
			}
			if nid == effectivePredID {
				fs.PrevKey = key
			}
			if nid == effectiveSuccID {
				fs.NextKey = key
			}
		}
		if err := nrows.Err(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return fs, nil
}

// skipSurgraphForward walks past a surgraph wrapper to the entry step of its child
// subgraph (predecessor_id=0). Loops for nested subgraphs.
func (e *Engine) skipSurgraphForward(ctx context.Context, db *sequel.DB, t flowTables, id int) (int, error) {
	for id > 0 {
		var childFlow int
		err := db.QueryRowContext(ctx,
			"SELECT flow_id FROM "+t.flows+" WHERE surgraph_step_id=? ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
			id,
		).Scan(&childFlow)
		if err == sql.ErrNoRows {
			return id, nil
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		if childFlow == 0 {
			return id, nil
		}
		var entry int
		err = db.QueryRowContext(ctx,
			"SELECT step_id FROM "+t.steps+" WHERE flow_id=? AND predecessor_id=0 ORDER BY step_id LIMIT_OFFSET(1, 0)",
			childFlow,
		).Scan(&entry)
		if err != nil {
			if err == sql.ErrNoRows {
				return id, nil
			}
			return 0, errors.Trace(err)
		}
		id = entry
	}
	return id, nil
}

// skipSurgraphBackward is the backward counterpart: if id is a surgraph
// wrapper, return the subgraph's exit step (completed, with successor_id=0).
// Loops for nested subgraphs.
func (e *Engine) skipSurgraphBackward(ctx context.Context, db *sequel.DB, t flowTables, id int) (int, error) {
	for id > 0 {
		var childFlow int
		err := db.QueryRowContext(ctx,
			"SELECT flow_id FROM "+t.flows+" WHERE surgraph_step_id=? ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
			id,
		).Scan(&childFlow)
		if err == sql.ErrNoRows {
			return id, nil
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		if childFlow == 0 {
			return id, nil
		}
		var exit int
		err = db.QueryRowContext(ctx,
			"SELECT step_id FROM "+t.steps+" WHERE flow_id=? AND successor_id=0 AND status='"+workflow.StatusCompleted+"' ORDER BY step_id DESC LIMIT_OFFSET(1, 0)",
			childFlow,
		).Scan(&exit)
		if err != nil {
			if err == sql.ErrNoRows {
				return id, nil
			}
			return 0, errors.Trace(err)
		}
		id = exit
	}
	return id, nil
}

func (e *Engine) fingerprint(ctx context.Context, flowKey string) (string, string, error) {
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "fingerprint")
	defer doneTurn()
	// The digest is the same on either side: a tree moves as it is, its updated_at with it.
	var status string
	var digest []byte
	found, err := e.queryLiveOrArchive(ctx, db, shardNum, flowID, flowHour, false, func(t flowTables) (bool, error) {
		var rootFlowID int
		err := db.QueryRowContext(ctx, "SELECT status, root_flow_id FROM "+t.flows+" WHERE flow_id=? AND flow_token=? AND created_hour=?", flowID, flowToken, flowHour).Scan(&status, &rootFlowID)
		if err == sql.ErrNoRows {
			return false, nil
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		e.seams.Checkpoint(ctx, engineseams.CheckpointFingerprintFlowRead)

		// The digest covers the steps of the flow and every subgraph flow beneath it. A root's subtree is its whole
		// tree, named by root_flow_id. A subgraph child's is found by the descendant walk, and its flow ids go out in
		// chunks, each contributing its own count and latest update; they are sorted so an unchanged subtree chunks
		// the same way twice.
		type chunk struct {
			where string
			args  []any
		}
		var chunks []chunk
		if rootFlowID == flowID {
			chunks = append(chunks, chunk{"flow_id IN (SELECT flow_id FROM " + t.flows + " WHERE root_flow_id=?)", []any{flowID}})
		} else {
			descendants, err := e.allDescendantSubgraphFlowsIn(ctx, db, t, flowID)
			if err != nil {
				return false, errors.Trace(err)
			}
			flowIDs := append([]int{flowID}, descendants...)
			slices.Sort(flowIDs)
			for ids := range slices.Chunk(flowIDs, inListChunk) {
				ph, args := inList(ids)
				chunks = append(chunks, chunk{"flow_id IN (" + ph + ")", args})
			}
		}
		digest = fmt.Appendf(nil, "%s", status)
		for _, c := range chunks {
			var count int
			// MAX(updated_at) is an untyped aggregate expression; SQLite returns it as a
			// string (no column affinity) while other dialects return a time value. Scan
			// into any and hash its string form — the fingerprint only needs a stable,
			// change-detecting digest, not a parsed timestamp.
			var maxUpdated any
			err = db.QueryRowContext(ctx, "SELECT COUNT(*), MAX(updated_at) FROM "+t.steps+" WHERE "+c.where, c.args...).Scan(&count, &maxUpdated)
			if err != nil {
				return false, errors.Trace(err)
			}
			if b, ok := maxUpdated.([]byte); ok {
				maxUpdated = string(b)
			}
			digest = fmt.Appendf(digest, "|%d|%v", count, maxUpdated)
		}
		return true, nil
	})
	if err != nil {
		return "", "", errors.Trace(err)
	}
	if !found {
		return "", "", errors.New("flow not found", http.StatusNotFound)
	}
	sum := sha256.Sum256(digest)
	return hex.EncodeToString(sum[:]), status, nil
}

func (e *Engine) shardInfo(ctx context.Context) ([]ShardSummary, error) {
	indices, pos := e.shardOrdinals()
	results := make([]ShardSummary, len(indices))
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shardIdx int) error {
		// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
		// connections like any other caller instead of taking them from work already under way.
		ctx, doneTurn := e.opTurn(ctx, shardIdx, "shard_info")
		defer doneTurn()
		r := &results[pos[shardIdx]]
		r.Index = shardIdx
		start := time.Now()
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		r.Latency = time.Since(start)
		if err != nil {
			r.Error = err.Error()
		}
		return nil
	})
	return results, nil
}
