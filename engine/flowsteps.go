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
// names the flow index.
func updateFlowSteps(ctx context.Context, ex sequel.Executor, set string, setArgs []any, flowIDs []any, guard string) error {
	ph, ids := inList(flowIDs)
	switch ex.DriverName() {
	case "mssql":
		return updateFlowStepsByID(ctx, ex, set, setArgs, ph, ids, guard)
	case "mysql", "mariadb":
		_, err := ex.ExecContext(ctx, "UPDATE dwarf_steps FORCE INDEX (idx_dwarf_steps_flow_id) SET "+set+
			" WHERE flow_id IN ("+ph+") AND "+guard, append(slices.Clone(setArgs), ids...)...)
		return errors.Trace(err)
	}
	_, err := ex.ExecContext(ctx, "UPDATE dwarf_steps SET "+set+" WHERE flow_id IN ("+ph+") AND "+guard,
		append(slices.Clone(setArgs), ids...)...)
	return errors.Trace(err)
}

// updateFlowStepsByID is updateFlowSteps on SQL Server: it reads the ids of the steps the flow list and guard match,
// then updates them by primary key, at most database.MaxListIDs a statement.
func updateFlowStepsByID(ctx context.Context, ex sequel.Executor, set string, setArgs []any, ph string, ids []any, guard string) error {
	rows, err := ex.QueryContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id IN ("+ph+") AND "+guard+" ORDER BY step_id", ids...)
	if err != nil {
		return errors.Trace(err)
	}
	var stepIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return errors.Trace(err)
		}
		stepIDs = append(stepIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return errors.Trace(err)
	}
	for chunk := range slices.Chunk(stepIDs, database.MaxListIDs) {
		sph, sids := database.IDList(chunk)
		_, err := ex.ExecContext(ctx, "UPDATE dwarf_steps SET "+set+" SEEK_FROM(dwarf_steps, 1(step_id)) WHERE step_id IN ("+sph+") AND "+guard,
			append(slices.Clone(setArgs), sids...)...)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
