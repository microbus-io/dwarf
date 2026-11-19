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
	"slices"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// updateFlowSteps runs "UPDATE dwarf_steps SET set WHERE flow_id IN (flowIDs) AND guard", binding setArgs and then
// flowIDs, which are at most inListChunk; guard binds nothing. One flow can hold any number of steps, so the flow list
// does not bound the rows, and a plain statement can lock the shard's other flows out. On SQL Server the ids that
// match are read first and updated by primary key, at most database.MaxListIDs a statement, in ascending order and
// under the guard again, so a row that stopped matching in between is left alone. On MySQL and MariaDB the statement
// names the flow index. It returns the rows the driver reports updated, which on MySQL and MariaDB are the rows the
// set changed.
func updateFlowSteps(ctx context.Context, ex sequel.Executor, set string, setArgs []any, flowIDs []any, guard string) (updated int64, err error) {
	ph, ids := inList(flowIDs)
	stmt := "UPDATE dwarf_steps SET " + set + " WHERE flow_id IN (" + ph + ") AND " + guard
	switch ex.DriverName() {
	case "mssql":
		return updateFlowStepsByID(ctx, ex, set, setArgs, ph, ids, guard)
	case "mysql", "mariadb":
		stmt = "UPDATE dwarf_steps FORCE INDEX (idx_dwarf_steps_flow_id) SET " + set + " WHERE flow_id IN (" + ph + ") AND " + guard
	}
	res, err := ex.ExecContext(ctx, stmt, append(slices.Clone(setArgs), ids...)...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	updated, _ = res.RowsAffected()
	return updated, nil
}

// updateFlowStepsByID is updateFlowSteps on SQL Server: it reads the ids of the steps the flow list and guard match,
// then updates them by primary key, at most database.MaxListIDs a statement.
func updateFlowStepsByID(ctx context.Context, ex sequel.Executor, set string, setArgs []any, ph string, ids []any, guard string) (updated int64, err error) {
	stepIDs, err := flowStepIDs(ctx, ex, "SELECT step_id FROM dwarf_steps WHERE flow_id IN ("+ph+") AND "+guard+" ORDER BY step_id", ids)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for chunk := range slices.Chunk(stepIDs, database.MaxListIDs) {
		sph, sids := database.IDList(chunk)
		res, err := ex.ExecContext(ctx, "UPDATE dwarf_steps SET "+set+" SEEK_FROM(dwarf_steps, 1(step_id)) WHERE step_id IN ("+sph+") AND "+guard,
			append(slices.Clone(setArgs), sids...)...)
		if err != nil {
			return updated, errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		updated += n
	}
	return updated, nil
}

// deleteFlowSteps deletes every step of flowIDs, which are at most inListChunk. On SQL Server the ids are read first and
// deleted by primary key, at most database.MaxListIDs a statement, for the reason updateFlowSteps gives: one flow can
// hold any number of steps, and a statement over the flow list escalates to a table lock past ~5,000 rows.
func deleteFlowSteps(ctx context.Context, ex sequel.Executor, flowIDs []int) error {
	ph, ids := inList(flowIDs)
	if ex.DriverName() != "mssql" {
		_, err := ex.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+ph+")", ids...)
		return errors.Trace(err)
	}
	stepIDs, err := flowStepIDs(ctx, ex, "SELECT step_id FROM dwarf_steps WHERE flow_id IN ("+ph+") ORDER BY step_id", ids)
	if err != nil {
		return errors.Trace(err)
	}
	for chunk := range slices.Chunk(stepIDs, database.MaxListIDs) {
		sph, sids := database.IDList(chunk)
		if _, err := ex.ExecContext(ctx, "DELETE FROM dwarf_steps SEEK_FROM(dwarf_steps, 1(step_id)) WHERE step_id IN ("+sph+")", sids...); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// flowStepIDs runs query, which selects one column of step ids, and returns them.
func flowStepIDs(ctx context.Context, ex sequel.Executor, query string, args []any) (stepIDs []int, err error) {
	rows, err := ex.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Trace(err)
		}
		stepIDs = append(stepIDs, id)
	}
	return stepIDs, errors.Trace(rows.Err())
}
