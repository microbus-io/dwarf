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
	"github.com/microbus-io/dwarf/internal/accessors"
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// dayStart is when a day of dayLength begins.
func dayStart(day int, dayLength time.Duration) time.Time {
	return time.Unix(946684800, 0).Add(time.Duration(day) * dayLength)
}

// hasArchiveDay reports whether shard 1 has both of a day's archive tables.
func hasArchiveDay(t *testing.T, e *Engine, day int) bool {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	for _, table := range []string{migrations.FlowsTable(day), migrations.StepsTable(day)} {
		rows, err := db.QueryContext(t.Context(), "SELECT 1 FROM "+table+" WHERE 1=0")
		if err != nil {
			return false
		}
		rows.Close()
	}
	return true
}

// makeArchiveDay makes a day's archive tables on shard 1, as the archive does.
func makeArchiveDay(t *testing.T, e *Engine, day int) error {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		return err
	}
	db := sh.DB()
	dayFS, err := migrations.ArchiveDay(migrations.Archive(), day)
	if err != nil {
		return err
	}
	return db.Migrate(migrations.SequenceName(day), dayFS)
}

// TestRetention_SetRetentionRules pins the setter: a negative count is a 400, 0 is allowed, and it cannot change once
// the engine runs.
func TestRetention_SetRetentionRules(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(hosts.NewMux())
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetRetention(-1)))
	assert.NoError(e.SetRetention(0))
	assert.NoError(e.SetRetention(30))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Equal(http.StatusConflict, errors.StatusCode(e.SetRetention(7)))
}

// TestRetention_StartupMakesTodaysArchive pins that a replica does not serve before today's archive tables exist.
func TestRetention_StartupMakesTodaysArchive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(hosts.NewMux())
	before := archiver.DayOf(time.Now(), archiver.DefaultDayLength)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	today, err := e.archiveOn(1).Today(t.Context())
	assert.NoError(err)
	if today != before {
		t.Skip("midnight passed during Startup")
	}
	assert.True(hasArchiveDay(t, e, today), "today %d missing", today)
	assert.True(hasArchiveDay(t, e, today+1), "tomorrow %d missing", today+1)
	assert.True(hasArchiveDay(t, e, today+2), "the day after tomorrow %d missing", today+2)
	assert.False(hasArchiveDay(t, e, today-1), "Startup made yesterday")
}

// TestRetention_ZeroStillMakesToday pins that a retention of 0 still makes today's archive tables, which nothing then
// writes, so a later retention finds the day in place.
func TestRetention_ZeroStillMakesToday(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(hosts.NewMux())
	assert.NoError(e.SetRetention(0))
	before := archiver.DayOf(time.Now(), archiver.DefaultDayLength)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	today, err := e.archiveOn(1).Today(t.Context())
	assert.NoError(err)
	if today != before {
		t.Skip("midnight passed during Startup")
	}
	assert.True(hasArchiveDay(t, e, today))
}

// TestRetention_PurgeBeforeDropsTheOlderDays pins PurgeBefore: every day before the one containing the time given is
// dropped, whatever the retention, and a time after today is a 400.
func TestRetention_PurgeBeforeDropsTheOlderDays(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	assert.Must(assert.NoError(e.Startup(ctx)))
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	for day := today - 3; day < today; day++ {
		assert.NoError(makeArchiveDay(t, e, day))
	}
	dayLength := e.dayLength()
	assert.NoError(e.PurgeBefore(ctx, dayStart(today-1, dayLength).Add(time.Hour)))
	assert.False(hasArchiveDay(t, e, today-3) || hasArchiveDay(t, e, today-2), "days before the one given are dropped")
	assert.True(hasArchiveDay(t, e, today-1) && hasArchiveDay(t, e, today), "the day given and after are kept")

	// Two days ahead, so a midnight passing during the test cannot make it today.
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.PurgeBefore(ctx, dayStart(today+2, dayLength))))
}

// TestRetention_TheLoopDropsExpiredDays pins archive retention running in the background on days of two seconds: a day
// past the retention is dropped, and the days ahead are made, without anyone asking.
func TestRetention_TheLoopDropsExpiredDays(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	e.archiveDayOverride = 2 * time.Second
	e.janitorManual = false // the janitor's jobs run in the background, archive retention every second at this length
	assert.NoError(e.SetRetention(1))
	assert.Must(assert.NoError(e.Startup(ctx)))
	// The day is made after archive retention's first pass, at its janitor's resume, so a later pass must drop it.
	enginetest.AwaitVisitCount(t, e.diags.Seams(), 1, 10*time.Second, diagnostics.CheckpointArchiveMaintained)
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	// Archive retention passes every second at this day length, so it may drop the day while it is being made, and the
	// migration then fails on a table already gone: a drop either way, so its error is not the test's.
	_ = makeArchiveDay(t, e, today-5)

	// Startup makes the 15 days ahead of its today at this length; the 16th after is made only once the day changes.
	ahead := today + 16
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for {
		if !hasArchiveDay(t, e, today-5) && hasArchiveDay(t, e, ahead) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("archive retention did not drop day %d and make day %d", today-5, ahead)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRetention_ReadersKeepNoArchive pins that archive retention, like every janitor job, runs only on shards this
// replica dispatches: a reader drops nothing, a dispatcher drops the expired day.
func TestRetention_ReadersKeepNoArchive(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	assert.NoError(e.SetRetention(1))
	assert.Must(assert.NoError(e.Startup(ctx)))
	// The archive loop's first pass, waited out so it cannot land in the middle of the test.
	enginetest.AwaitVisitCount(t, e.diags.Seams(), 1, 10*time.Second, diagnostics.CheckpointArchivePassDone)
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-5))

	setRole(e, 1, false)
	drainHoldingArchiving(ctx, e)
	e.refreshArchives(ctx)
	assert.True(hasArchiveDay(t, e, today-5), "a reader leaves the shard's archive to its dispatchers")
	setRole(e, 1, true)
	drainHoldingArchiving(ctx, e)
	assert.False(hasArchiveDay(t, e, today-5), "a dispatcher drops the expired day")
}

// TestRetention_TheDayLengthOverride pins the archive day a benchmark sets through accessors.SetArchiveDay: a value
// that is not a whole number of seconds of at least 1s is a 400, the day it sets is the run's, and it cannot change
// once the engine started.
func TestRetention_TheDayLengthOverride(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(hosts.NewMux())
	for _, bad := range []time.Duration{1500 * time.Millisecond, 500 * time.Millisecond, 0, -time.Second} {
		assert.Equal(http.StatusBadRequest, errors.StatusCode(accessors.SetArchiveDay(e, bad)), "%s", bad)
	}
	assert.NoError(accessors.SetArchiveDay(e, 10*time.Second))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Equal(10*time.Second, e.dayLength())
	assert.Equal(http.StatusConflict, errors.StatusCode(accessors.SetArchiveDay(e, 20*time.Second)))
}

// TestRetention_TheGraceOverride pins the archive grace a benchmark sets through accessors.SetArchiveGrace: a negative
// value is a 400, the grace it sets is the janitor's, and it cannot change once the engine started.
func TestRetention_TheGraceOverride(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(hosts.NewMux())
	assert.Equal(http.StatusBadRequest, errors.StatusCode(accessors.SetArchiveGrace(e, -time.Second)))
	assert.NoError(accessors.SetArchiveGrace(e, 5*time.Second))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Equal(5*time.Second, e.archiveGrace)
	assert.Equal(http.StatusConflict, errors.StatusCode(accessors.SetArchiveGrace(e, time.Second)))
}

// TestRetention_TheStagingOverrides pins the staging thresholds a benchmark sets through accessors.SetStageRows and
// SetStageBytes: a negative value is a 400, each sets only its own threshold, and neither can change once the engine
// started.
func TestRetention_TheStagingOverrides(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(hosts.NewMux())
	for _, set := range []func(any, int) error{accessors.SetStageRows, accessors.SetStageBytes} {
		assert.Equal(http.StatusBadRequest, errors.StatusCode(set(e, -1)))
	}
	bytes := e.reapStageBytes
	assert.NoError(accessors.SetStageRows(e, 131072))
	assert.Equal(bytes, e.reapStageBytes, "setting the rows left the bytes alone")
	assert.NoError(accessors.SetStageBytes(e, 67108864))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Equal(131072, e.reapStageRows)
	assert.Equal(67108864, e.reapStageBytes)
	for _, set := range []func(any, int) error{accessors.SetStageRows, accessors.SetStageBytes} {
		assert.Equal(http.StatusConflict, errors.StatusCode(set(e, 1)))
	}
}

// TestRetention_TheTestRunsStagingFromTheEnvironment pins what a staging-on run of the suite rests on: the test-mode
// constructor reads DWARF_TESTING_STAGE_ROWS and DWARF_TESTING_STAGE_BYTES, and a value that is not a whole number of
// at least 0 panics rather than run the suite unstaged. Not parallel: it sets environment variables.
func TestRetention_TheTestRunsStagingFromTheEnvironment(t *testing.T) {
	assert := testarossa.For(t)
	t.Setenv("DWARF_TESTING_STAGE_ROWS", "7")
	t.Setenv("DWARF_TESTING_STAGE_BYTES", "4096")
	e := NewEngineUnderTest(t.Name())
	assert.Equal(7, e.reapStageRows)
	assert.Equal(4096, e.reapStageBytes)
	for _, bad := range []string{"-1", "lots", "1.5"} {
		t.Setenv("DWARF_TESTING_STAGE_ROWS", bad)
		panicked := func() (p bool) {
			defer func() { p = recover() != nil }()
			NewEngineUnderTest(t.Name())
			return false
		}()
		assert.True(panicked, "%q did not panic", bad)
	}
}
