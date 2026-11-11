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

package archiver_test

import (
	"context"
	"io/fs"
	"math"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// openShard stands up an isolated, migrated database for the test and returns it.
func openShard(t *testing.T) *sequel.DB {
	t.Helper()
	var set database.ShardSet
	err := set.Open(context.Background(), database.Config{
		Shards:      map[int]database.ShardConfig{1: {MaxIdleConns: 4, MaxOpenConns: 12}},
		TestID:      database.TestID(t.Name()),
		TestConnCap: 12,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(set.Close)
	db, err := set.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// withArchiveMigration returns the archive migrations with one more file, which names a day's tables as
// dwarf_flows_archive and dwarf_steps_archive.
func withArchiveMigration(t *testing.T, name, text string) fs.FS {
	t.Helper()
	out := fstest.MapFS{}
	entries, err := fs.ReadDir(migrations.Archive(), ".")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := fs.ReadFile(migrations.Archive(), e.Name())
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = &fstest.MapFile{Data: data}
	}
	out[name] = &fstest.MapFile{Data: []byte(text)}
	return out
}

// columnsOf returns a table's columns in order.
func columnsOf(t *testing.T, db *sequel.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+table+" WHERE 1=0")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return cols
}

// indexesOf returns the names of a table's secondary indexes, on the dialects whose catalog the test reads, or nil.
func indexesOf(t *testing.T, db *sequel.DB, table string) []string {
	t.Helper()
	var stmt string
	switch db.DriverName() {
	case "pgx":
		stmt = "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() AND tablename=? AND indexname NOT LIKE '%_pkey'"
	case "sqlite":
		stmt = "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL"
	default:
		return nil
	}
	rows, err := db.QueryContext(t.Context(), stmt, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

// atHour returns a clock reading the given hour of a day of DefaultDayLength.
func atHour(day, hour int) func() int64 {
	return func() int64 { return 946684800 + int64(day)*86400 + int64(hour)*3600 }
}

// TestArchiver_EnsureMakesTheDayLikeTheLiveTables pins what a day is made of: both tables, with the live tables'
// columns in the same order - but archive_attempt_at, a live root's archive attempt, which means nothing archived -
// and an id that is written rather than drawn - an archived row keeps its live id, which state refs point at.
func TestArchiver_EnsureMakesTheDayLikeTheLiveTables(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(archiver.Ensure(ctx, a, today))
	days, err := archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{today}, days)
	assert.Equal(slices.DeleteFunc(columnsOf(t, db, "dwarf_flows"), func(c string) bool { return c == "archive_attempt_at" }),
		columnsOf(t, db, migrations.FlowsTable(today)))
	assert.Equal(columnsOf(t, db, "dwarf_steps"), columnsOf(t, db, migrations.StepsTable(today)))

	_, err = db.ExecContext(ctx, "INSERT INTO "+migrations.StepsTable(today)+
		" (step_id, flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, flow_created_hour)"+
		" VALUES (4242, 17, 1, 'tok', 'T', 'u', 'completed', 1000, 1)")
	assert.NoError(err)
	var flowID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id FROM "+migrations.StepsTable(today)+" WHERE step_id=4242").Scan(&flowID))
	assert.Equal(17, flowID)

	// Every live flows index has its archive counterpart, so a flow lookup is planned alike on either side.
	if indexes := indexesOf(t, db, "dwarf_flows"); indexes != nil {
		archived := indexesOf(t, db, migrations.FlowsTable(today))
		for _, name := range indexes {
			want := strings.Replace(name, "idx_dwarf_flows_", "idx_"+migrations.FlowsTable(today)+"_", 1)
			assert.True(slices.Contains(archived, want), "archive index for %s", name)
		}
	}

	if db.DriverName() == "pgx" {
		var opts string
		assert.NoError(db.QueryRowContext(ctx, "SELECT array_to_string(reloptions, ',') FROM pg_class WHERE relname=?",
			migrations.FlowsTable(today)).Scan(&opts))
		assert.Contains(opts, "autovacuum_freeze_min_age=0")
	}
}

// TestArchiver_EveryArchiveStatementIsIdempotent pins that a day's whole migration can run again over tables that
// exist - which a peer does when the runner that held a migration's lock died mid-way.
func TestArchiver_EveryArchiveStatementIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	assert.NoError(archiver.Ensure(ctx, a, 9000))
	dayFS, err := migrations.ArchiveDay(migrations.Archive(), 9000)
	assert.NoError(err)
	// A sequence with no records runs every migration again, over the day's tables.
	assert.NoError(db.Migrate(t.Name(), dayFS))
}

// TestArchiver_RacingEnsuresConverge pins that replicas making the same day at once all succeed with one pair of
// tables between them.
func TestArchiver_RacingEnsuresConverge(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	errs := make([]error, 8)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Go(func() {
			errs[i] = archiver.Ensure(ctx, archiver.New(db, archiver.DefaultDayLength, migrations.Archive()), 9000)
		})
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(err)
	}
	days, err := archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{9000}, days)
}

// TestArchiver_DropBeforeDropsTheOldestDays pins the purge: it drops every day before the one given and none after,
// refuses a day after today, and a day it dropped is never made again - not even by a later archive migration, which
// reaches only the days the catalog lists.
func TestArchiver_DropBeforeDropsTheOldestDays(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	archiver.SetClock(a, atHour(9003, 12))
	for day := 9000; day <= 9003; day++ {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}
	n, err := a.DropBefore(ctx, 9002)
	assert.NoError(err)
	assert.Equal(2, n)
	days, err := archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{9002, 9003}, days)

	n, err = a.DropBefore(ctx, 9003)
	assert.NoError(err)
	assert.Equal(1, n)
	_, err = a.DropBefore(ctx, 9004)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

	later := archiver.New(db, archiver.DefaultDayLength, withArchiveMigration(t, "3.sql",
		"-- DRIVER: mssql\nALTER TABLE dwarf_steps_archive ADD extra INT NOT NULL DEFAULT 0;\n\n"+
			"-- DRIVER: pgx mysql mariadb sqlite\nALTER TABLE dwarf_steps_archive ADD COLUMN extra INT NOT NULL DEFAULT 0;\n"))
	archiver.SetClock(later, atHour(9003, 12))
	assert.NoError(later.EnsureToday(ctx))
	assert.NoError(later.MigrateDays(ctx, 30))
	days, err = archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{9003, 9004, 9005}, days)
	var n9003 int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(extra) FROM "+migrations.StepsTable(9003)).Scan(&n9003))

	// Its records outlive the drop, so a dropped day is never made again: Ensure makes nothing and says so, naming
	// the day.
	err = archiver.Ensure(ctx, a, 9001)
	if assert.Error(err) {
		assert.Contains(err.Error(), "day 9001")
	}
	days, err = archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{9003, 9004, 9005}, days)
	assert.Error(archiver.Ensure(ctx, later, 9001))
}

// TestArchiver_MaintainKeepsTheRetention pins retention by days: Maintain makes the tables of today and the two days
// after, and drops a day only once it ended at least retention days ago. A retention of 0 still makes today's tables
// and those ahead, and drops every day before today.
func TestArchiver_MaintainKeepsTheRetention(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	const today = 9010
	for day := today - 4; day < today; day++ {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}
	archiver.SetClock(a, atHour(today, 12))

	dropped, err := a.Maintain(ctx, 2)
	assert.NoError(err)
	assert.Equal(2, dropped)
	days, err := archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{today - 2, today - 1, today, today + 1, today + 2}, days)

	// A retention as long as an int is the documented way to keep everything: it drops nothing, and no error.
	dropped, err = a.Maintain(ctx, math.MaxInt)
	assert.NoError(err)
	assert.Zero(dropped)
	days, err = archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Len(days, 5)

	dropped, err = a.Maintain(ctx, 0)
	assert.NoError(err)
	assert.Equal(2, dropped)
	days, err = archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{today, today + 1, today + 2}, days)
}

// TestArchiver_TheDaysAheadComeWithToday pins that making today makes the two days after it as well, so a day exists
// a whole day before it begins - and that once they are whole, making sure of them again reads only the clock until
// the day changes, from EnsureToday and Maintain alike.
func TestArchiver_TheDaysAheadComeWithToday(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	hour := 9000 * 24
	archiver.SetClock(a, func() int64 { return 946684800 + int64(hour)*3600 })
	assert.NoError(a.EnsureToday(ctx))
	days, err := archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{9000, 9001, 9002}, days)

	catalogs := archiver.Catalogs(a)
	assert.NoError(a.EnsureToday(ctx))
	_, err = a.Maintain(ctx, 30)
	assert.NoError(err)
	assert.Equal(catalogs+1, archiver.Catalogs(a), "only the drops read the catalog")

	hour += 24
	assert.NoError(a.EnsureToday(ctx))
	days, err = archiver.Days(ctx, a)
	assert.NoError(err)
	assert.Equal([]int{9000, 9001, 9002, 9003}, days, "the next day ahead, once the day changes")
}

// TestArchiver_ShortDaysAreMadeThirtySecondsAhead pins how many days are made ahead of today: two, or as many as cover
// 30 seconds of days shorter than 15 seconds, so a maintenance pass that falls a few short days behind still finds
// tomorrow made. MigrateDays accepts the last day made ahead and refuses the one after it.
func TestArchiver_ShortDaysAreMadeThirtySecondsAhead(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	for _, c := range []struct {
		dayLength time.Duration
		ahead     int
	}{
		{archiver.DefaultDayLength, 2},
		{time.Minute, 2},
		{15 * time.Second, 2},
		{14 * time.Second, 3},
		{7 * time.Second, 5},
		{2 * time.Second, 15},
		{time.Second, 30},
	} {
		assert.Equal(c.ahead, archiver.DaysAhead(archiver.New(nil, c.dayLength, migrations.Archive())), "%v", c.dayLength)
	}

	db := openShard(t)
	a := archiver.New(db, 2*time.Second, migrations.Archive())
	const today = 9000
	archiver.SetClock(a, func() int64 { return 946684800 + today*2 })
	assert.NoError(a.EnsureToday(ctx))
	days, err := archiver.Days(ctx, a)
	assert.NoError(err)
	want := []int{}
	for d := today; d <= today+15; d++ {
		want = append(want, d)
	}
	assert.Equal(want, days)
	assert.True(a.Whole(today + 15))
	assert.NoError(a.MigrateDays(ctx, 30), "the last day made ahead")
	assert.NoError(archiver.Ensure(ctx, a, today+16))
	err = a.MigrateDays(ctx, 30)
	if assert.Error(err) {
		assert.Contains(err.Error(), "another length")
	}
}

// TestArchiver_MigrateDaysAppliesALaterMigration pins how a schema change reaches the days already made: a later
// archive migration is applied to every existing day within the retention, once, and an older day is
// left for the retention to drop.
func TestArchiver_MigrateDaysAppliesALaterMigration(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	const today = 9002
	for _, day := range []int{today - 31, today - 30, today - 2, today - 1} {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}

	later := archiver.New(db, archiver.DefaultDayLength, withArchiveMigration(t, "3.sql",
		"-- DRIVER: mssql\nALTER TABLE dwarf_steps_archive ADD extra INT NOT NULL DEFAULT 0;\n\n"+
			"-- DRIVER: pgx mysql mariadb sqlite\nALTER TABLE dwarf_steps_archive ADD COLUMN extra INT NOT NULL DEFAULT 0;\n"))
	archiver.SetClock(later, atHour(today, 12))
	assert.NoError(later.MigrateDays(ctx, 30))
	// Up to date: a second run must run nothing, or the ADD COLUMN, which is not idempotent, would fail.
	assert.NoError(later.MigrateDays(ctx, 30))
	// The day that ended exactly the retention ago is kept, so it is migrated; the one before it is not.
	for _, day := range []int{today - 30, today - 2, today - 1} {
		var n int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(extra) FROM "+migrations.StepsTable(day)).Scan(&n), "day %d", day)
	}
	_, err := db.ExecContext(ctx, "SELECT COUNT(extra) FROM "+migrations.StepsTable(today-31))
	assert.Error(err, "a day past the retention was migrated")
}

// TestArchiver_TodayFollowsTheDayLength pins the day count: days since 2000-01-01 on the database clock, in days of
// the Archive's length.
func TestArchiver_TodayFollowsTheDayLength(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	for _, length := range []time.Duration{archiver.DefaultDayLength, 10 * time.Second} {
		today, err := archiver.New(db, length, migrations.Archive()).Today(ctx)
		assert.NoError(err)
		// The database clock may sit a little off this one.
		lo, hi := archiver.DayOf(time.Now().Add(-time.Minute), length), archiver.DayOf(time.Now().Add(time.Minute), length)
		assert.True(lo <= today && today <= hi, "%v: today %d is outside [%d, %d]", length, today, lo, hi)
	}
	clocked := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	archiver.SetClock(clocked, atHour(9000, 22))
	day, left, err := clocked.TodayLeft(ctx)
	assert.NoError(err)
	assert.Equal(9000, day)
	assert.Equal(2*time.Hour-time.Second, left, "rounded down: up to a second of the reading has passed")
	assert.Equal(0, archiver.DayOf(time.Date(2000, 1, 1, 23, 59, 59, 0, time.UTC), archiver.DefaultDayLength))
	assert.Equal(1, archiver.DayOf(time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC), archiver.DefaultDayLength))
	assert.Equal(-1, archiver.DayOf(time.Date(1999, 12, 31, 12, 0, 0, 0, time.UTC), archiver.DefaultDayLength))
}

// TestArchiver_AFailedDayMigrationFails pins that a day whose archive migration fails fails MigrateDays, and so
// Startup, naming the day - as a failed schema migration fails it - rather than leaving the day behind on an older
// schema. A day whose tables a peer dropped between the listing and its migration is skipped instead.
func TestArchiver_AFailedDayMigrationFails(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	assert.NoError(archiver.Ensure(ctx, a, 9000))
	assert.NoError(archiver.Ensure(ctx, a, 9001))
	brokenFS := withArchiveMigration(t, "3.sql", "ALTER TABLE dwarf_steps_archive ADD COLUMN step_id INT;\n")
	broken := archiver.New(db, archiver.DefaultDayLength, brokenFS)
	archiver.SetClock(broken, atHour(9002, 12))
	err := broken.MigrateDays(ctx, 30)
	if assert.Error(err) {
		assert.Contains(err.Error(), "day 9000")
	}

	archiver.SetClock(a, atHour(9001, 12))
	_, err = a.DropBefore(ctx, 9001)
	assert.NoError(err)
	assert.NoError(archiver.MigrateExisting(ctx, broken, 9000))
	assert.Error(archiver.MigrateExisting(ctx, broken, 9001))

	// A drop that stopped between the day's two tables is a drop in progress too.
	_, err = db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(9001))
	assert.NoError(err)
	assert.NoError(archiver.MigrateExisting(ctx, broken, 9001))
}

// TestArchiver_MigrateDaysRefusesAnotherDayLength pins the guard against two day lengths on one database: a day past
// the last day made ahead (the day after tomorrow, at a day's length) is numbered by another length, and Startup refuses rather than let a retention pass treat the
// other length's days as its own. The day after tomorrow itself, made ahead, is not.
func TestArchiver_MigrateDaysRefusesAnotherDayLength(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	assert.NoError(archiver.Ensure(ctx, a, 9002))
	archiver.SetClock(a, atHour(9000, 12))
	assert.NoError(a.MigrateDays(ctx, 30), "the day after tomorrow")
	assert.NoError(archiver.Ensure(ctx, a, 9003))
	err := a.MigrateDays(ctx, 30)
	if assert.Error(err) {
		assert.Contains(err.Error(), "another length")
	}
}

// TestArchiver_ADayThatCannotBeMadeIsCheckedAgain pins that the record of days seen whole is kept only when all three
// are: a day ahead that cannot be made fails every EnsureToday, rather than the first only.
func TestArchiver_ADayThatCannotBeMadeIsCheckedAgain(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	assert.NoError(archiver.Ensure(ctx, a, 9002))
	// Its migrations recorded, its tables gone: it cannot be made again.
	for _, table := range []string{migrations.FlowsTable(9002), migrations.StepsTable(9002)} {
		_, err := db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	archiver.SetClock(a, atHour(9000, 12))
	for range 2 {
		err := a.EnsureToday(ctx)
		if assert.Error(err) {
			assert.Contains(err.Error(), "day 9002")
		}
	}
}
