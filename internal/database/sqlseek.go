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

import (
	"regexp"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

func init() {
	sequel.RegisterVirtualFunc("SEEK_FROM", seekFrom)
}

// seekFromArgs is the shape of SEEK_FROM's arguments: a table, then an index as FORCESEEK names it - an index name, or
// an index id (1 is the clustered primary key) - with its leading column in parentheses.
var seekFromArgs = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_]*)\s*,\s*([A-Za-z_][A-Za-z0-9_]*|[0-9]+)\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)\s*$`)

// seekFrom expands the virtual function SEEK_FROM(table, index(column)), which an UPDATE or DELETE places where SQL
// Server takes a FROM clause: after SET (and OUTPUT), before WHERE, and in a DELETE after DELETE FROM table.
//
//	UPDATE dwarf_steps SET status=? SEEK_FROM(dwarf_steps, 1(step_id)) WHERE step_id IN (...)
//	DELETE FROM dwarf_flows SEEK_FROM(dwarf_flows, idx_dwarf_flows_root(root_flow_id)) WHERE root_flow_id IN (...)
//
// On SQL Server it is FROM table WITH (FORCESEEK(index(column))): the statement reaches its rows by a seek on that
// index, so the WHERE clause must restrict column by = or IN, and table must be the statement's own target. On every
// other driver it is nothing. Its arguments are checked on every driver, so a malformed call fails wherever it runs.
func seekFrom(driverName string, args string) (string, error) {
	m := seekFromArgs.FindStringSubmatch(args)
	if m == nil {
		return "", errors.New("SEEK_FROM requires syntax: SEEK_FROM(table, index(column))")
	}
	if driverName != "mssql" {
		return "", nil
	}
	return "FROM " + m[1] + " WITH (FORCESEEK(" + m[2] + "(" + m[3] + ")))", nil
}
