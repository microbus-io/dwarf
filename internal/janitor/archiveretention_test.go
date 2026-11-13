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

package janitor

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// retentionJanitor returns a Manual, active janitor on the rig's shard holding only an archive retention job over
// archive keeping retention days, metered by reader, and the job's round.
func retentionJanitor(t *testing.T, r *rig, archive *archiver.Archive, retention int) (round func(ctx context.Context) bool, reader *sdkmetric.ManualReader) {
	t.Helper()
	reader = sdkmetric.NewManualReader()
	d := diagnostics.New()
	d.SetLogger(slog.New(r.logs))
	d.SetSeams(r.seams)
	d.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	j := New(r.shard, d)
	j.AddJob(JobArchiveRetention, NewArchiveRetention(archive, retention))
	j.SetManual(true)
	j.Start(t.Context())
	j.Resume()
	t.Cleanup(j.Stop)
	return func(ctx context.Context) bool { return j.round(ctx, j.jobs[0]) }, reader
}

// unmadeArchive returns an archive on the rig's database with days of 48 hours, none of which the rig made: its
// days are numbered half as high as the rig's, so its today and the days ahead do not exist, and it never drops one
// of the rig's days, numbered after its own.
func unmadeArchive(r *rig) *archiver.Archive {
	return archiver.New(r.db, 48*time.Hour, migrations.Archive())
}

// TestArchiveRetention_KeepsTheDays pins a pass: today and the days ahead made, the days past the retention dropped,
// and progress reported; the next pass finds neither and reports none, so the idle wait paces the one after.
func TestArchiveRetention_KeepsTheDays(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 1)
	archive := unmadeArchive(r)
	today, err := archive.Today(ctx)
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(makeArchiveDay(t, r, today-5)))
	round, _ := retentionJanitor(t, r, archive, 1)
	assert.True(round(ctx), "the days made and the day dropped are progress")
	assert.False(round(ctx), "the next pass has nothing to do")
	if now, _ := archive.Today(ctx); now != today {
		t.Skip("the day changed during the passes")
	}
	assert.NoError(archive.Refresh(ctx))
	assert.False(archive.Whole(today-5), "the expired day dropped")
	for day := today; day <= today+2; day++ {
		assert.True(archive.Whole(day), "day %d made", day)
	}
}

// TestArchiveRetention_AFailedPassIsCountedAndAStopIsNot pins dwarf_archive_maintain_failed: a pass that fails counts
// and logs once, and a pass cut short by a stop does neither.
func TestArchiveRetention_AFailedPassIsCountedAndAStopIsNot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	round, reader := retentionJanitor(t, r, r.archive, 30)
	failed := func() int64 {
		var rm metricdata.ResourceMetrics
		assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
		return counterSum(rm, "dwarf_archive_maintain_failed")
	}
	const logged = "Janitor: maintaining the archive tables"
	r.seams.InjectN(diagnostics.FaultArchiveMaintainErr, 1)
	round(ctx)
	assert.Equal(int64(1), failed())
	assert.Equal(1, r.logs.count(logged))

	stopped, cancel := context.WithCancel(ctx)
	cancel()
	round(stopped)
	assert.Equal(int64(1), failed(), "a stop is no failure")
	assert.Equal(1, r.logs.count(logged), "a stop is not logged")
}

// TestArchiveRetention_WaitsForATurn pins that a pass runs under a turn on the shard: with the shard's one turn held,
// it waits for it, and runs once the turn is returned.
func TestArchiveRetention_WaitsForATurn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	round, _ := retentionJanitor(t, r, unmadeArchive(r), 30)
	r.shard.SetTurnLimit(1)
	_, held := r.shard.WaitTurn(ctx)
	done := make(chan bool)
	go func() { done <- round(ctx) }()
	deadline := time.Now().Add(10 * time.Second * timeoutScale)
	for {
		if _, waiting := r.shard.Turns(); waiting == 1 {
			break
		}
		if time.Now().After(deadline) {
			held.Return()
			<-done
			t.Fatal("the pass did not wait for a turn")
		}
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case <-done:
		t.Fatal("the pass ran while the turn was held")
	case <-time.After(50 * time.Millisecond):
	}
	held.Return()
	assert.True(<-done, "the pass ran once the turn was returned")
}

// TestArchiveRetention_TakesNoSecondConnection pins that a pass holds no connection of its own while the archiver
// migrates and drops on the shard's DB: on a pool of one connection, a pass that makes its days completes.
func TestArchiveRetention_TakesNoSecondConnection(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	round, _ := retentionJanitor(t, r, unmadeArchive(r), 30)
	r.db.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second*timeoutScale)
	defer cancel()
	assert.True(round(ctx), "the days made")
	assert.NoError(ctx.Err(), "the pass did not wait for a second connection")
}

// TestArchiveRetention_ThePassInterval pins the job's pace: hourly for a day of a day or longer, a twenty-fourth of a
// shorter day, and at least a second.
func TestArchiveRetention_ThePassInterval(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	idle := func(dayLength time.Duration) time.Duration {
		return NewArchiveRetention(archiver.New(nil, dayLength, migrations.Archive()), 30).idleWait()
	}
	assert.Equal(time.Hour, idle(24*time.Hour))
	assert.Equal(time.Hour, idle(48*time.Hour))
	assert.Equal(2*time.Second, idle(48*time.Second))
	assert.Equal(time.Second, idle(2*time.Second))
}
