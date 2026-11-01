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

package migrations_test

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/testarossa"
)

// TestMigrations_ArchiveDayNamesOnlyTheDay pins the rewrite of the archive migrations for a day: no statement still
// names a table by its stand-in or reaches a live table, and every index is named for the day's table - an index left
// with the stand-in's name would collide across days on PostgreSQL and SQLite, where index names are per schema.
func TestMigrations_ArchiveDayNamesOnlyTheDay(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	dayFS, err := migrations.ArchiveDay(migrations.Archive(), 7)
	assert.NoError(err)
	entries, err := fs.ReadDir(dayFS, ".")
	assert.NoError(err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	assert.Equal([]string{"1.sql", "2.sql"}, names)
	for _, name := range names {
		data, err := fs.ReadFile(dayFS, name)
		assert.NoError(err)
		text := string(data)
		for _, stand := range []string{"dwarf_flows_archive", "dwarf_steps_archive"} {
			assert.Equal(strings.Count(text, stand), strings.Count(text, stand+"_7"), "%s still names %s", name, stand)
		}
		for _, index := range regexp.MustCompile(`idx_\w+`).FindAllString(text, -1) {
			assert.True(strings.Contains(index, "_archive_7_"), "index %s in %s is not named for the day", index, name)
		}
		assert.False(regexp.MustCompile(`dwarf_(flows|steps)([^_]|$)`).MatchString(text), "%s reaches a live table", name)
	}
}

// TestMigrations_LiveMakesNoArchiveTable pins the split: the live tables' migrations never name an archive table, so
// no archive table exists but a day's.
func TestMigrations_LiveMakesNoArchiveTable(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	entries, err := fs.ReadDir(migrations.Live(), ".")
	assert.NoError(err)
	assert.True(len(entries) > 0)
	for _, e := range entries {
		data, err := fs.ReadFile(migrations.Live(), e.Name())
		assert.NoError(err)
		assert.False(strings.Contains(string(data), "_archive"), "%s names an archive table", e.Name())
	}
}

// TestMigrations_DayOfTable pins the parse of a day out of a table's name: a day's tables round-trip, and no other
// table, nor a day spelled any other way, reads as one.
func TestMigrations_DayOfTable(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, name := range []string{migrations.FlowsTable(9000), migrations.StepsTable(9000)} {
		day, ok := migrations.DayOfTable(name)
		assert.True(ok, name)
		assert.Equal(9000, day)
	}
	for _, name := range []string{"dwarf_flows", "dwarf_steps", "dwarf_peers", "dwarf_flows_archive",
		"dwarf_flows_archive_09000", "dwarf_steps_archive_x", "dwarf_steps_archive_9000_old"} {
		_, ok := migrations.DayOfTable(name)
		assert.False(ok, name)
	}
}
