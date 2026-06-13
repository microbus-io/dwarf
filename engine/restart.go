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
	"fmt"
	"net/http"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

func isRestartable(status string) bool {
	switch status {
	case workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled, workflow.StatusInterrupted:
		return true
	}
	return false
}

func isRestartableStep(status string) bool {
	return isRestartable(status)
}

func mergeWithOverrides(originalJSON string, overrides any) (string, error) {
	var state map[string]any
	if originalJSON != "" && originalJSON != "{}" {
		json.Unmarshal([]byte(originalJSON), &state)
	}
	if state == nil {
		state = map[string]any{}
	}
	if overrides == nil {
		out, _ := json.Marshal(state)
		return string(out), nil
	}
	overridesJSON, err := json.Marshal(overrides)
	if err != nil {
		return "", errors.Trace(err)
	}
	var ov map[string]any
	err = json.Unmarshal(overridesJSON, &ov)
	if err != nil {
		return "", errors.Trace(err)
	}
	for k, v := range ov {
		if v == nil {
			delete(state, k)
		} else {
			state[k] = v
		}
	}
	out, _ := json.Marshal(state)
	return string(out), nil
}

func (e *Engine) restart(ctx context.Context, flowKey string, stateOverrides any) error {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	var flowStatus string
	err = db.QueryRowContext(ctx,
		"SELECT status FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	if !isRestartable(flowStatus) {
		return errors.New("flow is not in a terminal status (status: %s)", flowStatus, http.StatusConflict)
	}

	var entryStepID int
	var entryState string
	err = db.QueryRowContext(ctx,
		"SELECT step_id, state FROM dwarf_steps WHERE flow_id=? AND predecessor_id=0",
		flowID,
	).Scan(&entryStepID, &entryState)
	if err == sql.ErrNoRows {
		return errors.New("flow has no entry step", http.StatusInternalServerError)
	}
	if err != nil {
		return errors.Trace(err)
	}

	mergedStateJSON, err := mergeWithOverrides(entryState, stateOverrides)
	if err != nil {
		return errors.Trace(err)
	}

	descendantFlowIDs, err := e.allDescendantSubgraphFlows(ctx, db, flowID)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if len(descendantFlowIDs) > 0 {
			ph := strings.Repeat("?,", len(descendantFlowIDs)-1) + "?"
			args := make([]any, 0, len(descendantFlowIDs))
			for _, id := range descendantFlowIDs {
				args = append(args, id)
			}
			tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+ph+")", args...)
			tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+ph+")", args...)
		}

		tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id=? AND step_id<>?", flowID, entryStepID)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, state=?, changes='{}', error='', goto_next='',"+
				" attempt=0, breakpoint_hit=0, interrupt_done=0, resume_data='{}',"+
				" subgraph_done=0, subgraph_result='{}', subgraph_error='',"+
				" successor_id=0, cohort_arrivals=0, cohort_failures=0,"+
				" not_before=NOW_UTC(), lease_expires=NOW_UTC(), created_at=NOW_UTC(), updated_at=NOW_UTC()"+
				" WHERE step_id=?",
			workflow.StatusPending, parkedNone, mergedStateJSON, entryStepID,
		)
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, step_id=?, error='', cancel_reason='', final_state='{}', created_at=NOW_UTC(), started_at=NOW_UTC(), updated_at=NOW_UTC()"+
				" WHERE flow_id=? AND flow_token=?",
			workflow.StatusRunning, entryStepID, flowID, flowToken,
		)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", flowID, "from", flowStatus, "to", workflow.StatusRunning, "via", "Restart")
	e.notifyStatusChange(flowKey, workflow.StatusRunning)
	e.enqueueStep(ctx, shardNum, entryStepID)
	return nil
}

func (e *Engine) restartFrom(ctx context.Context, stepKey string, stateOverrides any) error {
	shardNum, stepID, stepToken, err := parseStepKey(stepKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	var flowID, lineageID, predecessorID int
	var stepStatus, stepState string
	err = db.QueryRowContext(ctx,
		"SELECT flow_id, status, state, lineage_id, predecessor_id FROM dwarf_steps WHERE step_id=? AND step_token=?",
		stepID, stepToken,
	).Scan(&flowID, &stepStatus, &stepState, &lineageID, &predecessorID)
	if err == sql.ErrNoRows {
		return errors.New("step not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	stepStatus = strings.TrimSpace(stepStatus)
	if !isRestartableStep(stepStatus) {
		return errors.New("step is not in a terminal status (status: %s)", stepStatus, http.StatusConflict)
	}

	var flowStatus, flowToken string
	err = db.QueryRowContext(ctx, "SELECT status, flow_token FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&flowStatus, &flowToken)
	if err != nil {
		return errors.Trace(err)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	flowToken = strings.TrimSpace(flowToken)
	if flowStatus == workflow.StatusCancelled {
		return errors.New("flow is cancelled and cannot be restarted from a step", http.StatusConflict)
	}

	subtree, err := e.collectDAGSubtree(ctx, db, flowID, stepID)
	if err != nil {
		return errors.Trace(err)
	}

	upFlowIDs, upStepIDs, _, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	type parentLevel struct {
		flowID, callerStepID  int
		flowToken, flowStatus string
		subtree               []sweptMember
	}
	var parents []parentLevel
	for i, sid := range upStepIDs {
		parentFlowID := upFlowIDs[i+1].(int)
		callerStepID := sid.(int)
		var pTok, pStatus string
		db.QueryRowContext(ctx, "SELECT flow_token, status FROM dwarf_flows WHERE flow_id=?", parentFlowID).Scan(&pTok, &pStatus)
		pStatus = strings.TrimSpace(pStatus)
		if pStatus == workflow.StatusCancelled {
			return errors.New("ancestor surgraph flow is cancelled", http.StatusConflict)
		}
		pSub, err := e.collectDAGSubtree(ctx, db, parentFlowID, callerStepID)
		if err != nil {
			return errors.Trace(err)
		}
		parents = append(parents, parentLevel{flowID: parentFlowID, flowToken: strings.TrimSpace(pTok), flowStatus: pStatus, callerStepID: callerStepID, subtree: pSub})
	}

	mergedStateJSON, err := mergeWithOverrides(stepState, stateOverrides)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		sweepAll := func(members []sweptMember) {
			for _, m := range members {
				e.deleteSubgraphFlowsRootedAt(ctx, tx, m.stepID)
			}
		}
		sweepAll(subtree)
		for _, p := range parents {
			sweepAll(p.subtree)
		}

		type cohortDelta struct{ arrivalsDelta, failuresDelta int }
		deltaBySpawn := map[int]*cohortDelta{}
		bump := func(spawnID int, status string) {
			if spawnID == 0 {
				return
			}
			d, ok := deltaBySpawn[spawnID]
			if !ok {
				d = &cohortDelta{}
				deltaBySpawn[spawnID] = d
			}
			switch status {
			case workflow.StatusCompleted, workflow.StatusCancelled, workflow.StatusFailed:
				d.arrivalsDelta++
			}
			if status == workflow.StatusFailed {
				d.failuresDelta++
			}
		}
		for _, m := range subtree {
			bump(m.lineageID, m.status)
		}
		bump(lineageID, stepStatus)
		for _, p := range parents {
			for _, m := range p.subtree {
				bump(m.lineageID, m.status)
			}
		}

		deleteIDs := func(members []sweptMember) {
			if len(members) == 0 {
				return
			}
			ids := make([]any, 0, len(members))
			for _, m := range members {
				ids = append(ids, m.stepID)
			}
			ph := strings.Repeat("?,", len(ids)-1) + "?"
			tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE step_id IN ("+ph+")", ids...)
		}
		deleteIDs(subtree)
		for _, p := range parents {
			deleteIDs(p.subtree)
		}

		if predecessorID != 0 {
			tx.ExecContext(ctx, "UPDATE dwarf_steps SET successor_id=0 WHERE step_id=? AND successor_id<>?", predecessorID, stepID)
		}

		for spawnID, d := range deltaBySpawn {
			e.undoCohortBumps(ctx, tx, spawnID, d.arrivalsDelta, d.failuresDelta)
		}

		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, state=?, changes='{}', error='', goto_next='',"+
				" attempt=0, breakpoint_hit=0, interrupt_done=0, resume_data='{}',"+
				" subgraph_done=0, subgraph_result='{}', subgraph_error='',"+
				" successor_id=0,"+
				" not_before=NOW_UTC(), lease_expires=NOW_UTC(), created_at=NOW_UTC(), updated_at=NOW_UTC()"+
				" WHERE step_id=?",
			workflow.StatusPending, parkedNone, mergedStateJSON, stepID,
		)

		for _, p := range parents {
			tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=0, subgraph_result='{}', subgraph_error='', successor_id=0, error='', goto_next='', lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
				workflow.StatusRunning, parkedSubgraph, p.callerStepID,
			)
		}

		if flowStatus != workflow.StatusRunning {
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET status=?, error='', cancel_reason='', final_state='{}', updated_at=NOW_UTC() WHERE flow_id=? AND flow_token=? AND status<>?",
				workflow.StatusRunning, flowID, flowToken, workflow.StatusCancelled,
			)
		}
		for _, p := range parents {
			if p.flowStatus == workflow.StatusRunning {
				continue
			}
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET status=?, error='', cancel_reason='', final_state='{}', updated_at=NOW_UTC() WHERE flow_id=? AND flow_token=? AND status<>?",
				workflow.StatusRunning, p.flowID, p.flowToken, workflow.StatusCancelled,
			)
		}

		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	flowKey := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)
	if flowStatus != workflow.StatusRunning {
		e.logger.InfoContext(ctx, "Flow status transition", "flow", flowID, "from", flowStatus, "to", workflow.StatusRunning, "via", "RestartFrom", "step", stepID)
		e.notifyStatusChange(flowKey, workflow.StatusRunning)
	}
	for _, p := range parents {
		if p.flowStatus == workflow.StatusRunning {
			continue
		}
		e.notifyStatusChange(fmt.Sprintf("%d-%d-%s", shardNum, p.flowID, p.flowToken), workflow.StatusRunning)
	}
	e.enqueueStep(ctx, shardNum, stepID)
	return nil
}

type sweptMember struct {
	stepID    int
	lineageID int
	status    string
}

func (e *Engine) collectDAGSubtree(ctx context.Context, db interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}, flowID, startStepID int) ([]sweptMember, error) {
	visited := map[int]bool{startStepID: true}
	var collected []sweptMember
	frontier := []any{startStepID}
	for len(frontier) > 0 {
		ph := strings.Repeat("?,", len(frontier)-1) + "?"
		args := append([]any{flowID}, frontier...)
		query := "SELECT step_id, lineage_id, status FROM dwarf_steps WHERE flow_id=? AND (" +
			"step_id IN (SELECT successor_id FROM dwarf_steps WHERE step_id IN (" + ph + ") AND successor_id<>0)" +
			" OR predecessor_id IN (" + ph + "))"
		args = append(args, frontier...)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var nextFrontier []any
		for rows.Next() {
			var sid, lid int
			var status string
			rows.Scan(&sid, &lid, &status)
			if visited[sid] {
				continue
			}
			visited[sid] = true
			collected = append(collected, sweptMember{stepID: sid, lineageID: lid, status: strings.TrimSpace(status)})
			nextFrontier = append(nextFrontier, sid)
		}
		rows.Close()
		frontier = nextFrontier
	}
	return collected, nil
}

func (e *Engine) allDescendantSubgraphFlows(ctx context.Context, db interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}, flowID int) ([]int, error) {
	var collected []int
	current := []any{flowID}
	for len(current) > 0 {
		ph := strings.Repeat("?,", len(current)-1) + "?"
		rows, err := db.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id IN ("+ph+")", current...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		current = nil
		for rows.Next() {
			var id int
			rows.Scan(&id)
			collected = append(collected, id)
			current = append(current, id)
		}
		rows.Close()
	}
	return collected, nil
}

func (e *Engine) deleteSubgraphFlowsRootedAt(ctx context.Context, tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}, surgraphStepID int) error {
	var rootChildren []int
	rows, err := tx.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=?", surgraphStepID)
	if err != nil {
		return errors.Trace(err)
	}
	for rows.Next() {
		var id int
		rows.Scan(&id)
		rootChildren = append(rootChildren, id)
	}
	rows.Close()
	if len(rootChildren) == 0 {
		return nil
	}
	allIDs := append([]int{}, rootChildren...)
	current := make([]any, 0, len(rootChildren))
	for _, id := range rootChildren {
		current = append(current, id)
	}
	for len(current) > 0 {
		ph := strings.Repeat("?,", len(current)-1) + "?"
		nestedRows, err := tx.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id IN ("+ph+")", current...)
		if err != nil {
			return errors.Trace(err)
		}
		current = nil
		for nestedRows.Next() {
			var id int
			nestedRows.Scan(&id)
			allIDs = append(allIDs, id)
			current = append(current, id)
		}
		nestedRows.Close()
	}
	args := make([]any, 0, len(allIDs))
	for _, id := range allIDs {
		args = append(args, id)
	}
	ph := strings.Repeat("?,", len(allIDs)-1) + "?"
	tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+ph+")", args...)
	tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+ph+")", args...)
	return nil
}

func (e *Engine) undoCohortBumps(ctx context.Context, tx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}, spawnID int, arrivalsDelta int, failuresDelta int) error {
	if spawnID == 0 || (arrivalsDelta == 0 && failuresDelta == 0) {
		return nil
	}
	var priorArrivals, priorFailures, size, lineageID int
	tx.QueryRowContext(ctx,
		"SELECT cohort_arrivals, cohort_size, cohort_failures, lineage_id FROM dwarf_steps WHERE step_id=?",
		spawnID,
	).Scan(&priorArrivals, &size, &priorFailures, &lineageID)
	tx.ExecContext(ctx,
		"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals - ?, cohort_failures = cohort_failures - ? WHERE step_id=?",
		arrivalsDelta, failuresDelta, spawnID,
	)
	for priorArrivals >= size && priorFailures > 0 && lineageID != 0 {
		parent := lineageID
		tx.QueryRowContext(ctx,
			"SELECT cohort_arrivals, cohort_size, cohort_failures, lineage_id FROM dwarf_steps WHERE step_id=?",
			parent,
		).Scan(&priorArrivals, &size, &priorFailures, &lineageID)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals - 1, cohort_failures = cohort_failures - 1 WHERE step_id=?",
			parent,
		)
	}
	return nil
}
