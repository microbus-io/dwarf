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

package database

// SeekPrimaryKey is the seek, for UpdateSQL and DeleteSQL, on the primary key of a table whose key column is column:
// SeekPrimaryKey("flow_id") for dwarf_flows, an archive day's flows table or the directory, SeekPrimaryKey("step_id")
// for dwarf_steps or a day's steps table.
func SeekPrimaryKey(column string) string {
	return "1(" + column + ")"
}

// SeekIndex is the seek, for UpdateSQL and DeleteSQL, on the index of table named idx_<table>_<suffix>, whose leading
// column is column: SeekIndex("dwarf_steps", "flow_id", "flow_id"), SeekIndex("dwarf_flows", "root", "root_flow_id").
// A day's archive tables name their indexes the same way.
func SeekIndex(table, suffix, column string) string {
	return "idx_" + table + "_" + suffix + "(" + column + ")"
}

// UpdateSQL returns an UPDATE that sets set on the rows of table that where matches. On SQL Server it reaches them by
// a seek on the index seek names (SeekPrimaryKey, SeekIndex), so where must restrict that index's leading column by =
// or IN, and set may end with an OUTPUT clause. Elsewhere it is the plain statement, to which a caller appends a
// RETURNING clause. Either may name the table, as a correlated subquery does.
func UpdateSQL(driverName, table, seek, set, where string) string {
	if driverName == "mssql" {
		return "UPDATE " + table + " SET " + set + " FROM " + table + " " + table + " WITH (FORCESEEK(" + seek + ")) WHERE " + where
	}
	return "UPDATE " + table + " SET " + set + " WHERE " + where
}

// DeleteSQL returns a DELETE of the rows of table that where matches. On SQL Server it reaches them by a seek on the
// index seek names, as UpdateSQL does.
func DeleteSQL(driverName, table, seek, where string) string {
	if driverName == "mssql" {
		return "DELETE " + table + " FROM " + table + " " + table + " WITH (FORCESEEK(" + seek + ")) WHERE " + where
	}
	return "DELETE FROM " + table + " WHERE " + where
}
