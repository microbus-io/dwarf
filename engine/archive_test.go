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
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/engineseams"
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
	today, err := e.archives.Load().byShard[1].Today(t.Context())
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
	today, err := e.archives.Load().byShard[1].Today(t.Context())
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
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	for day := today - 3; day < today; day++ {
		assert.NoError(makeArchiveDay(t, e, day))
	}
	dayLength := e.archives.Load().dayLength
	assert.NoError(e.PurgeBefore(ctx, dayStart(today-1, dayLength).Add(time.Hour)))
	assert.False(hasArchiveDay(t, e, today-3) || hasArchiveDay(t, e, today-2), "days before the one given are dropped")
	assert.True(hasArchiveDay(t, e, today-1) && hasArchiveDay(t, e, today), "the day given and after are kept")

	// Two days ahead, so a midnight passing during the test cannot make it today.
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.PurgeBefore(ctx, dayStart(today+2, dayLength))))
}

// TestRetention_TheLoopDropsExpiredDays pins the background pass on days of two seconds: a day past the retention is
// dropped, and the days ahead are made, without anyone asking.
func TestRetention_TheLoopDropsExpiredDays(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	e.testArchiveDay = 2 * time.Second
	assert.NoError(e.SetRetention(1))
	assert.Must(assert.NoError(e.Startup(ctx)))
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	// The loop passes every second at this day length, so it may drop the day while it is being made, and the
	// migration then fails on a table already gone: a drop either way, so its error is not the test's.
	_ = makeArchiveDay(t, e, today-5)

	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for {
		if !hasArchiveDay(t, e, today-5) && hasArchiveDay(t, e, today+3) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the archive loop did not drop day %d and make day %d", today-5, today+3)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRetention_ReadersKeepNoArchive pins that the archive pass, like every background sweep, runs only on shards
// this replica dispatches: a reader drops nothing, a dispatcher drops the expired day.
func TestRetention_ReadersKeepNoArchive(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	assert.NoError(e.SetRetention(1))
	assert.Must(assert.NoError(e.Startup(ctx)))
	// The loop passes once at its start, then an hour later; the day is made once that first pass is done.
	enginetest.AwaitVisitCount(t, e.seams, 1, 10*time.Second, engineseams.CheckpointArchivePassDone)
	set := e.archives.Load()
	today, err := set.byShard[1].Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-5))

	e.dispatchFlag(1).Store(false)
	e.maintainArchives(ctx, set)
	assert.True(hasArchiveDay(t, e, today-5), "a reader leaves the shard's archive to its dispatchers")
	e.dispatchFlag(1).Store(true)
	e.maintainArchives(ctx, set)
	assert.False(hasArchiveDay(t, e, today-5), "a dispatcher drops the expired day")
}

// TestRetention_ABadDayLengthFailsStartup pins the benchmark's day-length override: a value that is not a whole
// number of seconds fails Startup with a 400 before any shard opens, and the engine starts once it is fixed. Not
// parallel: it sets an environment variable.
func TestRetention_ABadDayLengthFailsStartup(t *testing.T) {
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(hosts.NewMux())
	for _, bad := range []string{"1.5s", "500ms", "soon"} {
		t.Setenv(archiveDayEnv, bad)
		err := e.Startup(t.Context())
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%q", bad)
		assert.Len(e.db.Indices(), 0, "%q left shards open", bad)
	}
	t.Setenv(archiveDayEnv, "10s")
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Equal(10*time.Second, e.archives.Load().dayLength)
}

// TestRetention_TheGraceFromTheEnvironment pins the benchmark's grace override: a value that is not a duration of at
// least 0 fails Startup with a 400 before any shard opens, and a good one is the grace the reaper uses. Not parallel:
// it sets an environment variable.
func TestRetention_TheGraceFromTheEnvironment(t *testing.T) {
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(hosts.NewMux())
	for _, bad := range []string{"-1s", "soon"} {
		t.Setenv(archiveGraceEnv, bad)
		err := e.Startup(t.Context())
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%q", bad)
		assert.Len(e.db.Indices(), 0, "%q left shards open", bad)
	}
	t.Setenv(archiveGraceEnv, "5s")
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Equal(5*time.Second, e.archiveGrace)
}

// TestRetention_TheStagingThresholdFromTheEnvironment pins the benchmark's staging overrides, in rows and in bytes: a
// value that is not a whole number of at least 0 fails Startup with a 400 before any shard opens, and good ones are
// the thresholds the reaper stages above. Not parallel: it sets an environment variable.
func TestRetention_TheStagingThresholdFromTheEnvironment(t *testing.T) {
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(hosts.NewMux())
	for _, env := range []string{stageRowsEnv, stageBytesEnv} {
		for _, bad := range []string{"-1", "lots", "1.5"} {
			t.Setenv(env, bad)
			err := e.Startup(t.Context())
			assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%s=%q", env, bad)
			assert.Len(e.db.Indices(), 0, "%s=%q left shards open", env, bad)
		}
		t.Setenv(env, "")
	}
	t.Setenv(stageRowsEnv, "131072")
	t.Setenv(stageBytesEnv, "67108864")
	assert.Must(assert.NoError(e.Startup(t.Context())))
	cfg := e.reaperConfig(t.Context(), e.archives.Load(), 0)
	assert.Equal(131072, cfg.StageRows)
	assert.Equal(67108864, cfg.StageBytes)
}
