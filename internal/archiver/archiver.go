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

// Package archiver keeps one shard's archive tables: a pair per day, dwarf_flows_archive_<n> and
// dwarf_steps_archive_<n>, where n counts days since 2000-01-01 UTC on the database clock.
//
// A day's tables are made by the archive migrations (migrations.Archive) for that day, run as its own sequence, so a
// later archive migration reaches the existing days within the retention through MigrateDays, run at Startup right
// after the schema migrations. Today's tables and those of the two days after it are made ahead, so a day exists before
// it begins.
//
// One Archive serves one shard:
//
//	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
//	err := a.EnsureToday(ctx)             // at Startup: the tables of today and the two days after must exist
//	err = a.MigrateDays(ctx, retention)   // at Startup: the existing days within the retention brought up to date
//	n, err := a.Maintain(ctx, retention)  // periodically: the days ahead made, expired days dropped
//	n, err = a.DropBefore(ctx, day)       // an operator's purge of every day before one
//	err = a.Refresh(ctx)                  // periodically, where Maintain does not run: the days LocateFlow probes
//	day, err := a.LocateFlow(ctx, id, hour) // the day an archived flow is in, or a 404
//
// The Archive assumes every replica of the shard connects as the role that owns dwarf's tables.
//
// Every method is safe to call concurrently, from any number of replicas.
package archiver

import (
	"context"
	"io/fs"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// DefaultDayLength is how long a day is.
const DefaultDayLength = 24 * time.Hour

// epochUnix is 2000-01-01T00:00:00Z, the origin of the day count, in seconds since the Unix epoch.
const epochUnix = 946684800

// DayOf returns the day t falls in, for days of dayLength.
func DayOf(t time.Time, dayLength time.Duration) int {
	return floorDiv(t.Unix()-epochUnix, dayLengthSeconds(dayLength))
}

func dayLengthSeconds(dayLength time.Duration) int64 {
	return max(int64(dayLength/time.Second), 1)
}

func floorDiv(a, b int64) int {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return int(q)
}

// Archive keeps one shard's archive tables.
type Archive struct {
	db        *sequel.DB
	dayLength time.Duration
	fsys      fs.FS
	// clock overrides the database clock in tests; nil reads the database's.
	clock func(ctx context.Context) (int64, error)
	// mu guards ensured, the day whose tables, and those of the two days after it, this Archive last saw whole, and
	// known, the days with both tables at the last catalog read, less the days this Archive dropped since.
	mu      sync.Mutex
	ensured int
	known   []int
	// catalogs counts the catalog reads, for the tests.
	catalogs atomic.Int64
}

// New returns the Archive of the shard db is open on, for days of dayLength. fsys holds the archive migrations,
// migrations.Archive() outside tests. The Archive never opens or closes db.
func New(db *sequel.DB, dayLength time.Duration, fsys fs.FS) *Archive {
	return &Archive{db: db, dayLength: dayLength, fsys: fsys}
}

// Today returns the current day on the database clock.
func (a *Archive) Today(ctx context.Context) (day int, err error) {
	secs, err := a.now(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return floorDiv(secs-epochUnix, dayLengthSeconds(a.dayLength)), nil
}

// TodayLeft returns the current day on the database clock and how long it has left.
func (a *Archive) TodayLeft(ctx context.Context) (day int, left time.Duration, err error) {
	secs, err := a.now(ctx)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	length := dayLengthSeconds(a.dayLength)
	day = floorDiv(secs-epochUnix, length)
	// The clock reads whole seconds, so up to one has passed already: rounded down, the day is never taken to last
	// past its end.
	leftSecs := int64(day+1)*length - (secs - epochUnix) - 1
	return day, time.Duration(max(leftSecs, 0)) * time.Second, nil
}

// now returns the database clock in whole seconds since the Unix epoch.
func (a *Archive) now(ctx context.Context) (int64, error) {
	if a.clock != nil {
		return a.clock(ctx)
	}
	var stmt string
	switch a.db.DriverName() {
	case "pgx":
		stmt = "SELECT CAST(FLOOR(EXTRACT(EPOCH FROM NOW())) AS BIGINT)"
	case "mysql":
		stmt = "SELECT UNIX_TIMESTAMP()"
	case "mssql":
		stmt = "SELECT DATEDIFF_BIG(SECOND, '1970-01-01', SYSUTCDATETIME())"
	default:
		stmt = "SELECT CAST(STRFTIME('%s', 'now') AS INTEGER)"
	}
	var secs int64
	err := a.db.QueryRowContext(ctx, stmt).Scan(&secs)
	return secs, errors.Trace(err)
}

// ensure makes sure both of a day's tables exist and carry every archive migration. It is for days that were never
// dropped: on a dropped day it makes nothing, or fails on a later archive migration.
func (a *Archive) ensure(ctx context.Context, day int) error {
	if ctx.Err() != nil {
		return errors.Trace(ctx.Err())
	}
	dayFS, err := migrations.ArchiveDay(a.fsys, day)
	if err != nil {
		return errors.Trace(err)
	}
	// The migration runs to its end once started: it takes no context.
	err = a.db.Migrate(migrations.SequenceName(day), dayFS)
	if err != nil {
		return errors.Trace(err)
	}
	// The records can outlive the tables - a day dropped, a restore without the archive - and then the migration
	// makes nothing. Fail naming the day rather than report a day that is not there.
	_, whole, err := a.catalog(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if !slices.Contains(whole, day) {
		return errors.New("the archive tables of day %d are missing although its migrations are recorded"+
			" (dropped, or restored without them); delete the day's rows from sequel_migrations to make it again", day)
	}
	return nil
}

// EnsureToday makes sure the tables of today and of the two days after it exist. Once it has seen them whole, it reads
// only the clock until the day changes. It retries a failure a few times before returning it.
func (a *Archive) EnsureToday(ctx context.Context) error {
	backoff := 200 * time.Millisecond
	for attempt := 1; ; attempt++ {
		today, err := a.Today(ctx)
		if err == nil {
			err = a.ensureCurrent(ctx, today)
		}
		if err == nil {
			return nil
		}
		if attempt == 3 {
			return errors.Trace(err)
		}
		select {
		case <-ctx.Done():
			return errors.Trace(err)
		case <-time.After(backoff):
		}
		backoff *= 4
	}
}

// ensureCurrent makes sure the tables of today, as the caller read it, and of the two days after it exist - each only
// once the day before is whole, so days are always made in order. Days seen whole stay whole - nothing drops today or a
// day after it - so once they are, it does nothing until the day changes.
func (a *Archive) ensureCurrent(ctx context.Context, today int) error {
	a.mu.Lock()
	done := a.ensured == today
	a.mu.Unlock()
	if done {
		return nil
	}
	for day := today; day <= today+2; day++ {
		if err := a.ensure(ctx, day); err != nil {
			return errors.Trace(err)
		}
	}
	a.mu.Lock()
	a.ensured = today
	a.mu.Unlock()
	return nil
}

// MigrateDays brings every existing day before today, within the retention, up to the day's migrations,
// so a later archive migration reaches those days before anything reads them; run it after
// EnsureToday, which brings today and the days ahead. A day is existing when the catalog lists a table of it. An older day
// is left as it is, for the next Maintain to drop. A day whose migration fails fails the call, as a failed schema
// migration does, unless it is missing a table by then. A retention of 0 migrates nothing: every day before today is
// expired.
func (a *Archive) MigrateDays(ctx context.Context, retention int) error {
	if retention <= 0 {
		return nil
	}
	// The days are listed before today is read, so a day a peer made just after midnight, when this read of today
	// would still have been the day before, cannot read as past the day after tomorrow.
	days, _, err := a.catalog(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	today, err := a.Today(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	if len(days) > 0 && days[len(days)-1] > today+2 {
		return errors.New("archive day %d is past the day after tomorrow (%d): the database holds days of another length",
			days[len(days)-1], today+2)
	}
	for _, day := range days {
		// Written as a bound on day so that a retention near the largest int cannot overflow. Today and the days ahead
		// are EnsureToday's.
		if day < today-retention || day >= today {
			continue
		}
		if ctx.Err() != nil {
			return errors.Trace(ctx.Err())
		}
		err = a.migrateExisting(ctx, day)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// migrateExisting migrates a day the catalog listed, before today. A failure on a day missing either table is a drop
// in progress - a peer's landing between the listing and the migration, or one that stopped between its two tables -
// and not an error: a past day is never written, and the next drop finishes it.
func (a *Archive) migrateExisting(ctx context.Context, day int) error {
	dayFS, err := migrations.ArchiveDay(a.fsys, day)
	if err != nil {
		return errors.Trace(err)
	}
	err = a.db.Migrate(migrations.SequenceName(day), dayFS)
	if err == nil {
		return nil
	}
	_, whole, listErr := a.catalog(ctx)
	if listErr == nil && !slices.Contains(whole, day) {
		return nil
	}
	return errors.New("migrating the archive tables of day %d", day, err)
}

// Maintain makes sure the tables of today and of the two days after it exist, and drops every day that ended at least
// retention days ago. A retention of 0 keeps today's tables and those ahead, which nothing writes, and drops every day
// before today. It returns how many days it dropped, which a failure does not undo.
func (a *Archive) Maintain(ctx context.Context, retention int) (dropped int, err error) {
	// Negative would reach today, which is being written.
	retention = max(retention, 0)
	today, err := a.Today(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// The days ahead are made before the expired days go, from the same reading of today. A failure to make them does
	// not hold back the drops: the likeliest cause is a full database, which they free.
	err = a.ensureCurrent(ctx, today)
	// A day ends at the start of the next, so day d has been over for retention days once d+1+retention <= today.
	// Written as a bound on d so that a retention near the largest int cannot overflow.
	dropped, dropErr := a.dropDays(ctx, func(d int) bool { return d < today-retention })
	return dropped, errors.Join(err, dropErr, a.forgetDaysBefore(ctx, today-retention))
}

// DropBefore drops the tables of every day before day, and returns how many days it dropped. It returns a 400 for a
// day after today: today's tables are the ones being written, those ahead will be, and a day dropped is never made
// again.
// Dropping only the oldest days leaves no gap between the days kept.
func (a *Archive) DropBefore(ctx context.Context, day int) (dropped int, err error) {
	today, err := a.Today(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if day > today {
		return 0, errors.New("day %d is after today, %d", day, today, http.StatusBadRequest)
	}
	n, err := a.dropDays(ctx, func(d int) bool { return d < day })
	return n, errors.Join(err, a.forgetDaysBefore(ctx, day))
}

// dropDays drops every day that has tables and that drop selects, and returns how many it dropped.
func (a *Archive) dropDays(ctx context.Context, drop func(day int) bool) (int, error) {
	days, _, err := a.catalog(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	dropped := 0
	var errs error
	for _, day := range days {
		if !drop(day) {
			continue
		}
		if ctx.Err() != nil {
			return dropped, errors.Join(errs, ctx.Err())
		}
		// A day that cannot be dropped now, its lock held, does not hold back the days after it.
		err = a.dropDay(ctx, day)
		if err != nil {
			errs = errors.Join(errs, errors.New("dropping the archive tables of day %d", day, err))
			continue
		}
		a.forget(day)
		dropped++
	}
	return dropped, errs
}

// dropDay drops a day's two tables. Its migration records stay: they are sequel's, and a day dropped is never made
// again.
func (a *Archive) dropDay(ctx context.Context, day int) error {
	flows, steps := migrations.FlowsTable(day), migrations.StepsTable(day)
	var err error
	if a.db.DriverName() == "pgx" {
		err = a.db.Transact(ctx, func(tx *sequel.Tx) error {
			// A drop waits for every statement reading the table, and every statement after it queues behind the
			// wait. Bounded, a pass that cannot get the lock fails and the next one retries. MySQL and SQL Server
			// have no per-transaction bound, so there the drop waits for its lock.
			tx.ExecContext(ctx, "SET LOCAL lock_timeout = '2s'")
			tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+flows)
			tx.ExecContext(ctx, "DROP TABLE IF EXISTS "+steps)
			return nil
		})
	} else {
		_, err = a.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+flows)
		if err == nil {
			_, err = a.db.ExecContext(ctx, "DROP TABLE IF EXISTS "+steps)
		}
	}
	return errors.Trace(err)
}

// Whole reports whether the archive knows both of day's tables to exist: from its last catalog read, or from making
// the day since.
func (a *Archive) Whole(day int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := slices.BinarySearch(a.known, day)
	return ok
}

// DaysIn returns the days from through to, inclusive and ascending, whose tables the archive knows to exist: from its
// last catalog read, or from making the day since. A day dropped since by a peer is still listed until the next read
// (Refresh), and a read of it then fails on its missing table.
func (a *Archive) DaysIn(from, to int) []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	var days []int
	for _, d := range a.known {
		if d >= from && d <= to {
			days = append(days, d)
		}
	}
	return days
}

// DayLength returns how long the archive's days are.
func (a *Archive) DayLength() time.Duration {
	return a.dayLength
}

// DayEnds returns the instant day ends, which is the instant the next day begins.
func (a *Archive) DayEnds(day int) time.Time {
	return time.Unix(epochUnix+int64(day+1)*dayLengthSeconds(a.dayLength), 0).UTC()
}

// Refresh reads which days exist, for LocateFlow. Maintain and DropBefore read them too, so a replica that runs neither
// on the shard calls it periodically instead.
func (a *Archive) Refresh(ctx context.Context) error {
	_, _, err := a.catalog(ctx)
	return errors.Trace(err)
}

// catalog reads the database's catalog of tables for the archive's days, ascending: every day with a table, and those
// of them with both - the others are half made or half dropped.
func (a *Archive) catalog(ctx context.Context) (days []int, whole []int, err error) {
	a.catalogs.Add(1)
	// The pattern narrows what the catalog returns; DayOfTable decides. Its underscores match any character, harmlessly.
	var stmt string
	switch a.db.DriverName() {
	case "pgx":
		stmt = "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() AND tablename LIKE 'dwarf_%_archive_%'"
	case "mysql":
		stmt = "SELECT table_name FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name LIKE 'dwarf_%_archive_%'"
	case "mssql":
		// The default schema only, which the drops and the OBJECT_ID guards resolve to.
		stmt = "SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID() AND name LIKE 'dwarf_%_archive_%'"
	default:
		stmt = "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'dwarf_%_archive_%'"
	}
	rows, err := a.db.QueryContext(ctx, stmt)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer rows.Close()
	tables := map[int]int{}
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		day, ok := migrations.DayOfTable(name)
		if ok && (name == migrations.FlowsTable(day) || name == migrations.StepsTable(day)) {
			tables[day]++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, errors.Trace(err)
	}
	for day, n := range tables {
		days = append(days, day)
		if n == 2 {
			whole = append(whole, day)
		}
	}
	slices.Sort(days)
	slices.Sort(whole)
	a.mu.Lock()
	a.known = whole
	a.mu.Unlock()
	return days, whole, nil
}
