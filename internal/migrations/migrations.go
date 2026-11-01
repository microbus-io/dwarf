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

// Package migrations holds the engine's SQL schema: the live tables' migrations, and the migrations that make and
// alter one day's pair of archive tables.
//
//	err := db.Migrate(schemaSequenceName, migrations.Live())
//	dayFS, err := migrations.ArchiveDay(migrations.Archive(), day)
//	err = db.Migrate(migrations.SequenceName(day), dayFS)
//	rows := "SELECT ... FROM " + migrations.StepsTable(day)
//
// A day is a count of days since 2000-01-01 UTC, of whatever length the caller counts in.
package migrations

import (
	"embed"
	"io/fs"
	"strconv"
	"strings"
	"testing/fstest"
)

//go:embed live/*.sql
var liveFiles embed.FS

//go:embed archive/*.sql
var archiveFiles embed.FS

const (
	// flowsName and stepsName stand for a day's tables in the archive migrations. No table of either name exists.
	flowsName = "dwarf_flows_archive"
	stepsName = "dwarf_steps_archive"
	// sequencePrefix starts every day's migration sequence name.
	sequencePrefix = "github.com/microbus-io/dwarf/archive/"
)

var (
	live    = mustSub(liveFiles, "live")
	archive = mustSub(archiveFiles, "archive")
)

// Live returns the migrations of the live tables, numbered 1.sql, 2.sql and so on.
func Live() fs.FS {
	return live
}

// Archive returns the migrations that make and alter a day's archive tables, numbered 1.sql, 2.sql and so on as
// their own sequence. They name the day's tables dwarf_flows_archive and dwarf_steps_archive, which ArchiveDay
// replaces; every statement in them is idempotent.
func Archive() fs.FS {
	return archive
}

// ArchiveDay returns the archive migrations in fsys, Archive() outside tests, for one day: every file with the day's
// table names in place of the stand-ins, to run as the sequence SequenceName(day).
func ArchiveDay(fsys fs.FS, day int) (fs.FS, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, err
	}
	out := fstest.MapFS{}
	for _, entry := range entries {
		data, err := fs.ReadFile(fsys, entry.Name())
		if err != nil {
			return nil, err
		}
		text := strings.ReplaceAll(string(data), flowsName, FlowsTable(day))
		text = strings.ReplaceAll(text, stepsName, StepsTable(day))
		out[entry.Name()] = &fstest.MapFile{Data: []byte(text)}
	}
	return out, nil
}

// FlowsTable is the name of a day's archive table of flows.
func FlowsTable(day int) string { return flowsName + "_" + strconv.Itoa(day) }

// StepsTable is the name of a day's archive table of steps.
func StepsTable(day int) string { return stepsName + "_" + strconv.Itoa(day) }

// SequenceName is the migration sequence of a day's archive tables.
func SequenceName(day int) string { return sequencePrefix + strconv.Itoa(day) }

// DayOfTable returns the day of a day's archive table, by its name, and false for any other table.
func DayOfTable(name string) (int, bool) {
	for _, prefix := range []string{flowsName, stepsName} {
		digits, ok := strings.CutPrefix(name, prefix+"_")
		if !ok {
			continue
		}
		day, err := strconv.Atoi(digits)
		if err == nil && strconv.Itoa(day) == digits {
			return day, true
		}
	}
	return 0, false
}

func mustSub(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
