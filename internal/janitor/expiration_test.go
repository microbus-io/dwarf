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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/testarossa"
)

// expirationJanitor returns a Manual, active janitor on the rig's shard holding only an expiration job running pass.
func expirationJanitor(t *testing.T, r *rig, pass ExpirePass) *Janitor {
	t.Helper()
	d := diagnostics.New()
	d.SetSeams(r.seams)
	j := New(r.shard, d)
	j.AddJob(JobExpiration, NewExpiration(time.Hour, pass))
	j.SetManual(true)
	j.Start(t.Context())
	j.Resume()
	t.Cleanup(j.Stop)
	return j
}

// TestExpiration_ThePositionCarriesAcrossRounds pins the position: each pass goes on after the flow_id the last one
// returned, a pass that swept the shard leaves the next sweep to start from the oldest, and a resume forgets it.
func TestExpiration_ThePositionCarriesAcrossRounds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	var seen []int
	nexts := []int{7, 12, 0, 3}
	j := expirationJanitor(t, r, func(ctx context.Context, after int) (int, bool) {
		seen = append(seen, after)
		next := nexts[0]
		nexts = nexts[1:]
		return next, false
	})
	for range 3 {
		j.round(ctx, j.jobs[0])
	}
	assert.Equal([]int{0, 7, 12}, seen, "each pass after the last one's position")
	j.round(ctx, j.jobs[0])
	assert.Len(seen, 3, "the shard swept, the next sweep waits")
	j.Pause()
	j.Resume()
	j.round(ctx, j.jobs[0])
	assert.Equal([]int{0, 7, 12, 0}, seen, "a resume sweeps from the oldest at once")
}

// TestExpiration_TheRoundReportsThePassProgress pins the round's progress: the pass's own, so a pass that stopped at
// its budget is followed at once by the next; but a sweep from the oldest starts only an idle wait past the last one's
// end, whatever that one ended, so a drain - which repeats while any round makes progress - returns.
func TestExpiration_TheRoundReportsThePassProgress(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	var seen []int
	j := expirationJanitor(t, r, func(ctx context.Context, after int) (int, bool) {
		seen = append(seen, after)
		if after == 0 {
			return 9, true // stopped at its budget
		}
		return 0, true // swept, having ended roots
	})
	j.Drain(ctx)
	assert.Equal([]int{0, 9}, seen, "the sweep's two passes, and no sweep after it")

	x := j.jobs[0].job.(*Expiration)
	x.swept = time.Now().Add(-x.idleWait())
	assert.True(j.round(ctx, j.jobs[0]), "an idle wait past the last sweep, the next starts")
	assert.True(j.round(ctx, j.jobs[0]), "and goes on past its budget at once")
	assert.Equal([]int{0, 9, 0, 9}, seen)
	assert.False(j.round(ctx, j.jobs[0]), "and the one after waits an idle wait")
	j.Pause()
	j.Resume()
	assert.True(j.round(ctx, j.jobs[0]), "a resume sweeps at once")
}

// TestExpiration_ThePassRunsUnderATurn pins that the pass runs under a common turn on the job's shard.
func TestExpiration_ThePassRunsUnderATurn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.shard.SetTurnLimit(1)
	held := -1
	j := expirationJanitor(t, r, func(ctx context.Context, after int) (int, bool) {
		available, _ := r.shard.Turns()
		held = available
		return 0, false
	})
	j.round(ctx, j.jobs[0])
	assert.Equal(0, held, "the shard's one turn is the pass's")
	available, _ := r.shard.Turns()
	assert.Equal(1, available, "and returned after it")
}

// TestExpiration_ThePace pins the idle wait: an hour, or half the expiration when that is shorter, at least a minute.
func TestExpiration_ThePace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	idle := func(expiration time.Duration) time.Duration { return NewExpiration(expiration, nil).idleWait() }
	assert.Equal(time.Hour, idle(30*24*time.Hour))
	assert.Equal(time.Hour, idle(2*time.Hour))
	assert.Equal(30*time.Minute, idle(time.Hour))
	assert.Equal(5*time.Minute, idle(10*time.Minute))
	assert.Equal(time.Minute, idle(90*time.Second))
	assert.Equal(time.Minute, idle(time.Millisecond))
}
