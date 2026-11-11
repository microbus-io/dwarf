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

package reaper

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/testarossa"
)

// Concurrent Delete calls must serialize their decision and count the mark only once, even during staging.
func TestDeleteLive_ConcurrentCallsCountOneRemoval(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	root := seedTrees(t, r.db, 1, callShape)[0]
	_, err := r.db.ExecContext(t.Context(), "UPDATE dwarf_flows SET archive_attempt_at=NOW_UTC() WHERE flow_id=?", root)
	assert.Must(assert.NoError(err))
	type answer struct {
		result Deletion
		err    error
	}
	answers := make(chan answer, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			result, err := r.reaper.DeleteLive(t.Context(), r.db, archiver.DefaultDayLength, root)
			answers <- answer{result, err}
		}()
	}
	close(start)
	count := 0
	for range 2 {
		a := <-answers
		assert.NoError(a.err)
		assert.True(a.result.Found)
		assert.Zero(a.result.Archive)
		count += a.result.Live
	}
	assert.Equal(1, count)
	attempted, args := AttemptedSQL(r.db.DriverName())
	var active int
	assert.NoError(r.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND "+attempted,
		append([]any{root}, args...)...).Scan(&active))
	assert.Zero(active, "the stamp abandons the staged copy")
}

// A held root must not be mistaken for a tree that moved to the archive.
func TestDeleteLive_WaitsForTheRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	root := seedTrees(t, r.db, 1, callShape)[0]
	release := holdRows(t, r, "dwarf_flows", root)
	defer release()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := r.reaper.DeleteLive(ctx, r.db, archiver.DefaultDayLength, root)
	assert.Error(err, "a held row waits until cancellation; it does not report absent")
	assert.Error(ctx.Err())
}

// TestDeleteLive_TakesPrecedenceOverTheGrace pins Delete of a root in DeleteOnCompletion's grace: it is made due at once
// and counted, rather than left readable until the grace ends.
func TestDeleteLive_TakesPrecedenceOverTheGrace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedTrees(t, r.db, 1, callShape)[0]
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=60000, updated_at=NOW_UTC() WHERE flow_id=?", root)
	assert.Must(assert.NoError(err))
	result, err := r.reaper.DeleteLive(ctx, r.db, archiver.DefaultDayLength, root)
	assert.NoError(err)
	assert.Equal(1, result.Live)
	var stamp int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp))
	assert.Equal(1, stamp, "due at once")
	r.pass(ctx)
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "deleted on the next pass")
	result, err = r.reaper.DeleteLive(ctx, r.db, archiver.DefaultDayLength, root)
	assert.NoError(err)
	assert.False(result.Found, "gone")
}
