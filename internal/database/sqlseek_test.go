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
	"testing"

	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestSeekFrom_ExpandsToAHintedFromOnSQLServerOnly pins SEEK_FROM's expansion: a FROM clause forcing a seek on SQL
// Server, nothing elsewhere, and an error on every driver for arguments that are not a table and an index(column).
func TestSeekFrom_ExpandsToAHintedFromOnSQLServerOnly(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	got, err := seekFrom("mssql", "dwarf_steps, 1(step_id)")
	assert.NoError(err)
	assert.Equal("FROM dwarf_steps WITH (FORCESEEK(1(step_id)))", got)
	got, err = seekFrom("mssql", " dwarf_flows_archive_9818 ,idx_dwarf_flows_archive_9818_root( root_flow_id ) ")
	assert.NoError(err)
	assert.Equal("FROM dwarf_flows_archive_9818 WITH (FORCESEEK(idx_dwarf_flows_archive_9818_root(root_flow_id)))", got)
	for _, driver := range []string{"pgx", "mysql", "mariadb", "sqlite"} {
		got, err = seekFrom(driver, "dwarf_steps, 1(step_id)")
		assert.NoError(err, driver)
		assert.Equal("", got, driver)
	}

	for _, args := range []string{
		"",
		"dwarf_steps",
		"dwarf_steps, step_id",
		"dwarf_steps, 1(step_id), 1(flow_id)",
		"dwarf_steps; DROP TABLE dwarf_flows, 1(step_id)",
		"dwarf_steps, 1(step_id) WITH (NOLOCK)",
	} {
		for _, driver := range []string{"mssql", "pgx", "sqlite"} {
			_, err := seekFrom(driver, args)
			assert.Error(err, "%s accepted %q", driver, args)
		}
	}

	// Registered with sequel, and expanded inside a statement.
	db, err := sequel.Open("sqlite", "file:seekfrom_test?mode=memory&cache=shared")
	if !assert.NoError(err) {
		return
	}
	defer db.Close()
	unpacked, err := db.UnpackQuery("UPDATE dwarf_steps SET touch=1 SEEK_FROM(dwarf_steps, 1(step_id)) WHERE step_id IN (?, ?)")
	assert.NoError(err)
	assert.Equal("UPDATE dwarf_steps SET touch=1  WHERE step_id IN (?, ?)", unpacked)
}
