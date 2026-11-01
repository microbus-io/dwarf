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
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// hourOfDay returns the creation hour of a row made at the given hour of a day of DefaultDayLength.
func hourOfDay(day, hour int) int {
	return day*24 + hour
}

// name writes the directory's row for a flow archived on day.
func name(t *testing.T, db *sequel.DB, flowID, day int) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) VALUES (?, ?)", flowID, day)
	if err != nil {
		t.Fatal(err)
	}
}

// archive writes a flow row into a day's flows table, as the reaper would.
func archive(t *testing.T, db *sequel.DB, flowID, day int) {
	t.Helper()
	_, err := db.ExecContext(t.Context(), "INSERT INTO "+migrations.FlowsTable(day)+
		" (flow_id, flow_token, workflow_url, workflow_name, graph, graph_hash, status) VALUES (?, 'tok', 'u', 'W', ?, 'h', 'completed')",
		flowID, []byte("{}")) // bound: graph is binary on SQL Server
	if err != nil {
		t.Fatal(err)
	}
}

// directoryDays returns the days the directory holds rows for, by flow.
func directoryDays(t *testing.T, db *sequel.DB) map[int]int {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT flow_id, day FROM "+archiver.DirectoryTable)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int]int{}
	for rows.Next() {
		var id, day int
		if err := rows.Scan(&id, &day); err != nil {
			t.Fatal(err)
		}
		out[id] = day
	}
	return out
}

// TestArchiver_LocateFindsTheCreationDayTheNextOrTheDirectorysDay pins where a locate finds a flow: in its creation day,
// in the day after, or in the day the directory names - and a 404 when none holds it. A directory day is returned
// whether or not its tables are still there, and a probed day is preferred to it. It does not check the token, and
// reads no catalog.
func TestArchiver_LocateFindsTheCreationDayTheNextOrTheDirectorysDay(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	for day := 9003; day <= 9007; day++ {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}
	archive(t, db, 101, 9003)
	archive(t, db, 102, 9004)
	archive(t, db, 103, 9006)
	name(t, db, 103, 9006)
	name(t, db, 104, 9001)
	archive(t, db, 106, 9003)
	name(t, db, 106, 9001)
	catalogs := archiver.Catalogs(a)

	day, err := a.LocateFlow(ctx, 101, hourOfDay(9003, 5))
	assert.NoError(err)
	assert.Equal(9003, day, "the creation day")
	day, err = a.LocateFlow(ctx, 102, hourOfDay(9003, 23))
	assert.NoError(err)
	assert.Equal(9004, day, "the day after")
	day, err = a.LocateFlow(ctx, 103, hourOfDay(9003, 5))
	assert.NoError(err)
	assert.Equal(9006, day, "the directory's day")
	day, err = a.LocateFlow(ctx, 104, hourOfDay(9003, 5))
	assert.NoError(err)
	assert.Equal(9001, day, "a directory day dropped is still returned: the caller's read of it fails")
	_, err = a.LocateFlow(ctx, 105, hourOfDay(9003, 5))
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "in no day")
	day, err = a.LocateFlow(ctx, 102, hourOfDay(9004, 1))
	assert.NoError(err)
	assert.Equal(9004, day, "a creation day is found whatever its hour")
	day, err = a.LocateFlow(ctx, 106, hourOfDay(9003, 5))
	assert.NoError(err)
	assert.Equal(9003, day, "a probed day is preferred to a directory row")
	assert.Equal(catalogs, archiver.Catalogs(a), "no catalog read")
}

// TestArchiver_LocatePastTheRetentionsEnd pins the locate of a key whose days have been dropped: the creation day gone,
// the day after is probed; both gone, the directory alone answers, finding a flow archived on a later day still kept;
// and with nothing kept, a 404. A key whose days never existed - one with a far-future hour - is a 404 too.
func TestArchiver_LocatePastTheRetentionsEnd(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	for day := 9003; day <= 9008; day++ {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}
	archive(t, db, 101, 9004) // created 9003, archived the day after
	archive(t, db, 102, 9008) // created 9003, archived five days later
	name(t, db, 102, 9008)
	archive(t, db, 103, 9003)
	_, err := a.DropBefore(ctx, 9004)
	assert.NoError(err)
	assert.Equal([]int{9004, 9005, 9006, 9007, 9008}, archiver.Known(a), "a day dropped is known gone at once")
	_, err = db.ExecContext(ctx, "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) VALUES (102, 9008)")
	assert.Error(err, "the directory row survived the drop") // DropBefore forgets only days before 9004

	day, err := a.LocateFlow(ctx, 101, hourOfDay(9003, 5))
	assert.NoError(err)
	assert.Equal(9004, day, "the creation day dropped: the day after")
	_, err = a.LocateFlow(ctx, 103, hourOfDay(9003, 5))
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "archived in a dropped day")

	_, err = a.DropBefore(ctx, 9005)
	assert.NoError(err)
	day, err = a.LocateFlow(ctx, 102, hourOfDay(9003, 5))
	assert.NoError(err)
	assert.Equal(9008, day, "both dropped: the directory alone")
	_, err = a.LocateFlow(ctx, 101, hourOfDay(9003, 5))
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "both dropped, no directory row")
	_, err = a.LocateFlow(ctx, 101, hourOfDay(9002, 5))
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "days that never existed")
}

// TestArchiver_LocateProbesTheDaysItKnows pins how a locate picks its days from what the archive last read of the
// catalog, which it never reads itself: a day it lacks below its newest is not probed - so an expired key costs the
// directory alone, and a day made there behind its back stays unseen - while a day later than every one it knows, made
// by a peer since, is.
func TestArchiver_LocateProbesTheDaysItKnows(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	peer := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	assert.NoError(archiver.Ensure(ctx, a, 9004))
	assert.NoError(archiver.Ensure(ctx, a, 9005))
	assert.Equal([]int{9004, 9005}, archiver.Known(a))
	catalogs := archiver.Catalogs(a)

	assert.NoError(archiver.Ensure(ctx, peer, 9003))
	archive(t, db, 101, 9003)
	_, err := a.LocateFlow(ctx, 101, hourOfDay(9003, 1))
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "a day below the newest known is not probed")

	assert.NoError(archiver.Ensure(ctx, peer, 9006))
	assert.NoError(archiver.Ensure(ctx, peer, 9007))
	archive(t, db, 102, 9007)
	day, err := a.LocateFlow(ctx, 102, hourOfDay(9006, 1))
	assert.NoError(err)
	assert.Equal(9007, day, "days later than every one known are probed")
	_, err = a.LocateFlow(ctx, 103, hourOfDay(9100, 1))
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "and when they do not exist, a 404")
	assert.Equal([]int{9004, 9005}, archiver.Known(a), "a day never known is not remembered missing")
	assert.Equal(catalogs, archiver.Catalogs(a), "no catalog read")
}

// TestArchiver_LocateFollowsTheCatalogReads pins that what a locate meets is not kept: a day a peer dropped is still
// probed, failing the statement and answering correctly, until the next catalog read, and a day made below the newest
// known - which only a hand can do - is seen only from then on.
func TestArchiver_LocateFollowsTheCatalogReads(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	peer := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	for day := 9003; day <= 9006; day++ {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}
	archive(t, db, 101, 9004)
	_, err := peer.DropBefore(ctx, 9004)
	assert.NoError(err)
	assert.NoError(archiver.Ensure(ctx, peer, 9001))
	archive(t, db, 102, 9001)

	for range 2 {
		day, err := a.LocateFlow(ctx, 101, hourOfDay(9003, 1))
		assert.NoError(err)
		assert.Equal(9004, day)
		_, err = a.LocateFlow(ctx, 102, hourOfDay(9001, 1))
		assert.Equal(http.StatusNotFound, errors.StatusCode(err), "a day made below the newest known is not probed")
	}
	assert.Equal([]int{9003, 9004, 9005, 9006}, archiver.Known(a), "nothing learned is kept")

	assert.NoError(a.Refresh(ctx))
	assert.Equal([]int{9001, 9004, 9005, 9006}, archiver.Known(a))
	day, err := a.LocateFlow(ctx, 102, hourOfDay(9001, 1))
	assert.NoError(err)
	assert.Equal(9001, day)
}

// TestArchiver_LocateBeforeAnyCatalogRead pins that an archive that has not read the catalog yet probes every day it is
// asked about rather than none.
func TestArchiver_LocateBeforeAnyCatalogRead(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	peer := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	assert.NoError(archiver.Ensure(ctx, peer, 9005))
	assert.NoError(archiver.Ensure(ctx, peer, 9006)) // a row goes into a day only once the day after it is whole
	archive(t, db, 101, 9005)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	day, err := a.LocateFlow(ctx, 101, hourOfDay(9005, 1))
	assert.NoError(err)
	assert.Equal(9005, day)
	assert.Zero(archiver.Catalogs(a))
}

// TestArchiver_LocateOfAHalfDroppedDay pins a day left with one table: with its flows table gone it is passed over as
// dropped; with only its steps table gone its flows table still answers.
func TestArchiver_LocateOfAHalfDroppedDay(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	for day := 9005; day <= 9007; day++ {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}
	archive(t, db, 101, 9006)
	archive(t, db, 102, 9007)
	_, err := db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(9005))
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "DROP TABLE "+migrations.StepsTable(9006))
	assert.NoError(err)

	day, err := a.LocateFlow(ctx, 101, hourOfDay(9005, 1))
	assert.NoError(err)
	assert.Equal(9006, day)
	day, err = a.LocateFlow(ctx, 102, hourOfDay(9006, 1))
	assert.NoError(err)
	assert.Equal(9007, day)
}

// TestArchiver_ALocateErrorIsNotANotFound pins that a failure other than a missing table fails the locate rather than
// read as a flow not archived: a 404 on a database error would tell a caller its flow is gone. The failure is one only
// a probe meets - a probed day's flows table without the column it reads - so leaving the day out would succeed.
func TestArchiver_ALocateErrorIsNotANotFound(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	assert.NoError(archiver.Ensure(ctx, a, 9005))
	assert.NoError(archiver.Ensure(ctx, a, 9006))
	_, err := db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(9005))
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "CREATE TABLE "+migrations.FlowsTable(9005)+" (other_id INT NOT NULL)")
	assert.NoError(err)
	_, err = a.LocateFlow(ctx, 101, hourOfDay(9005, 1))
	assert.Error(err)
	assert.NotEqual(http.StatusNotFound, errors.StatusCode(err), err)
	assert.False(sequel.IsUndefinedTableError(err), err)
}

// TestArchiver_DirectoryHours pins the bounds the reaper names flows by, for the production day and for days of
// seconds and of hours: a flow archived on day is named exactly when its creation day is before day-1 or after day.
func TestArchiver_DirectoryHours(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, length := range []time.Duration{archiver.DefaultDayLength, 2 * time.Second, 5 * time.Hour, 7 * time.Hour, 25 * time.Hour} {
		a := archiver.New(nil, length, migrations.Archive())
		for day := 9000; day < 9030; day++ {
			before, from := a.DirectoryHours(day)
			// Every hour around the bounds, named exactly when the locate would not find it on its own.
			for hour := before - 30; hour < from+30; hour++ {
				created := archiver.CreationDay(a, hour)
				named := hour < before || hour >= from
				assert.Equal(created < day-1 || created > day, named, "length %v day %d hour %d (created %d)", length, day,
					hour, created)
			}
		}
	}
}

// TestArchiver_RetentionForgetsTheDirectorysExpiredDays pins that the directory's rows go with their days: a pass
// deletes those of every expired day, whether or not the day's tables were there to drop, and so does DropBefore.
func TestArchiver_RetentionForgetsTheDirectorysExpiredDays(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	archiver.SetClock(a, atHour(9010, 12))
	name(t, db, 101, 9001)
	name(t, db, 102, 9004)
	name(t, db, 103, 9008)
	name(t, db, 104, 9010)
	name(t, db, 105, 9005)

	assert.NoError(a.Maintain(ctx, 5)) // keeps 9005 on
	assert.Equal(map[int]int{103: 9008, 104: 9010, 105: 9005}, directoryDays(t, db))
	_, err := a.DropBefore(ctx, 9009)
	assert.NoError(err)
	assert.Equal(map[int]int{104: 9010}, directoryDays(t, db))
}

// TestArchiver_RetentionForgetsManyRowsInBatches pins that a day naming more flows than one batch deletes loses all
// of them in a pass, and the day kept none.
func TestArchiver_RetentionForgetsManyRowsInBatches(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	archiver.SetClock(a, atHour(9010, 12))
	expired := 2*archiver.ForgetBatch + 1
	for start := 0; start < expired; start += 500 {
		var values []string
		for id := start; id < min(start+500, expired); id++ {
			values = append(values, fmt.Sprintf("(%d, 9001)", 1000+id))
		}
		_, err := db.ExecContext(ctx, "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) VALUES "+strings.Join(values, ","))
		assert.NoError(err)
	}
	name(t, db, 99, 9009)

	assert.NoError(a.Maintain(ctx, 5))
	assert.Equal(map[int]int{99: 9009}, directoryDays(t, db))
}

// TestArchiver_AKeyFromTheFutureIsNotLocated pins that a key whose creation day is more than a day past the replica's
// clock - which the database cannot have minted - is a 404 without a statement, and that one within a day of it is
// still probed.
func TestArchiver_AKeyFromTheFutureIsNotLocated(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	assert := testarossa.For(t)
	db := openShard(t)
	a := archiver.New(db, archiver.DefaultDayLength, migrations.Archive())
	today := archiver.DayOf(time.Now(), archiver.DefaultDayLength)
	for day := today; day <= today+2; day++ {
		assert.NoError(archiver.Ensure(ctx, a, day))
	}
	archive(t, db, 101, today+1)
	day, err := a.LocateFlow(ctx, 101, hourOfDay(today+1, 1))
	assert.NoError(err)
	assert.Equal(today+1, day, "a day ahead is within the tolerance")

	_, err = db.ExecContext(ctx, "DROP TABLE "+archiver.DirectoryTable)
	assert.NoError(err)
	if archiver.DayOf(time.Now(), archiver.DefaultDayLength) != today {
		t.Skip("midnight passed during the test")
	}
	_, err = a.LocateFlow(ctx, 102, hourOfDay(today+2, 1))
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "no statement: the directory is gone, and would fail it")
	_, err = a.LocateFlow(ctx, 102, hourOfDay(today+1, 1))
	assert.Error(err, "a key within the tolerance runs the statement")
	assert.NotEqual(http.StatusNotFound, errors.StatusCode(err))
}
