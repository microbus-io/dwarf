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
	"errors"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/testarossa"
)

// recoveryJanitor returns a Manual, active janitor on the rig's shard holding only a recovery job running pass every
// interval.
func recoveryJanitor(t *testing.T, r *rig, every time.Duration, pass RecoveryPass) *Janitor {
	t.Helper()
	d := diagnostics.New()
	d.SetSeams(r.seams)
	j := New(r.shard, d)
	j.AddJob(JobLeaseRecovery, NewRecovery(every, pass))
	j.SetManual(true)
	j.Start(t.Context())
	j.Resume()
	t.Cleanup(j.Stop)
	return j
}

// TestRecovery_ThePace pins the pace: a pass at once, none in the round after it, the next an interval after the last
// one ended, and one at once after a resume.
func TestRecovery_ThePace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	passes := 0
	j := recoveryJanitor(t, r, time.Hour, func(ctx context.Context) error {
		passes++
		return nil
	})
	assert.True(j.round(ctx, j.jobs[0]), "the first pass runs at once and is progress")
	assert.False(j.round(ctx, j.jobs[0]), "the round after it waits for the interval")
	assert.Equal(1, passes)

	rec := j.jobs[0].job.(*Recovery)
	rec.ended = time.Now().Add(-time.Hour)
	assert.True(j.round(ctx, j.jobs[0]), "an interval past the last pass, the next runs")
	assert.Equal(2, passes)
	assert.False(j.round(ctx, j.jobs[0]))

	j.Pause()
	j.Resume()
	assert.True(j.round(ctx, j.jobs[0]), "a resume runs a pass at once")
	assert.Equal(3, passes)
	assert.Equal(time.Hour, rec.idleWait(), "the idle wait is the interval")
}

// TestRecovery_AFailedPassIsNoProgress pins that only a pass that ran to its end without an error is progress, and
// that a failed pass is followed by the next an interval after it, as a completed one is.
func TestRecovery_AFailedPassIsNoProgress(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	fail := true
	passes := 0
	j := recoveryJanitor(t, r, time.Hour, func(ctx context.Context) error {
		passes++
		if fail {
			return errors.New("a statement failed")
		}
		return nil
	})
	assert.False(j.round(ctx, j.jobs[0]), "a failed pass is no progress")
	assert.False(j.round(ctx, j.jobs[0]), "and waits the interval")
	assert.Equal(1, passes)

	fail = false
	j.jobs[0].job.(*Recovery).ended = time.Now().Add(-time.Hour)
	assert.True(j.round(ctx, j.jobs[0]), "the next pass, completed, is progress")
	assert.Equal(2, passes)
}

// TestRecovery_ADrainRunsOnePass pins that a drain runs the pass once: the round after it waits for the interval.
func TestRecovery_ADrainRunsOnePass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	passes := 0
	j := recoveryJanitor(t, r, time.Hour, func(ctx context.Context) error {
		passes++
		return nil
	})
	j.Drain(ctx)
	j.Drain(ctx)
	assert.Equal(1, passes)
}

// TestRecovery_ThePassRunsUnderATurn pins that the pass runs under a common turn on the job's shard.
func TestRecovery_ThePassRunsUnderATurn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.shard.SetTurnLimit(1)
	held := -1
	j := recoveryJanitor(t, r, time.Hour, func(ctx context.Context) error {
		available, _ := r.shard.Turns()
		held = available
		return nil
	})
	j.round(ctx, j.jobs[0])
	assert.Equal(0, held, "the shard's one turn is the pass's")
	available, _ := r.shard.Turns()
	assert.Equal(1, available, "and returned after it")
}
