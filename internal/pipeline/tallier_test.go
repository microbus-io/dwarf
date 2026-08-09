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

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestTallier_PublishesTheBand pins the whole of a healthy tally cycle: the shard's own band is reported
// back to the caller and to the planner, where a peer planning off the same planner can see it.
func TestTallier_PublishesTheBand(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{
		{Key: "a", Weight: 1, AgeMs: 100, Count: 3},
		{Key: "b", Weight: 1, AgeMs: 100, Count: 3},
	}, 5)

	res := r.tallier.Cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(5, res.Band)
	assert.Equal(1, r.band.scans)
	assert.True(res.Total > 0)
	assert.True(res.Tallying >= 0)
	assert.Equal(time.Duration(0), res.Slept, "the first cycle never waits")
	assert.Equal(5, r.planner.Plan(2, 8).GlobalBand, "a peer sees this shard's band")
}

// TestTallier_NothingDueStillReports pins that a shard with nothing due REPORTS rather than staying quiet.
// Silence would mean "my previous tally still stands", so a shard that quietly stopped reporting would hold
// a band claim forever and wedge every peer. Nothing due may also RAISE the global band and release a peer,
// which only a report can do.
func TestTallier_NothingDueStillReports(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, nil, NoBand)

	res := r.tallier.Cycle(ctx)
	assert.NoError(res.Err)
	assert.Equal(NoBand, res.Band)
	assert.True(r.planner.Plan(1, 8).Tallied, "nothing due is a REPORT, not an absence")
	assert.Equal(NoBand, r.planner.Plan(1, 8).GlobalBand)
}

// TestTallier_ScanFailureClearsTheShard pins the first half of the error policy. A shard that could not look
// must leave planning - a stale claim on the best band makes every peer find no keys of its own there and
// dispatch nothing - and it must come back on the next good cycle with no cooldown.
func TestTallier_ScanFailureClearsTheShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 1)

	assert.NoError(r.tallier.Cycle(ctx).Err)
	assert.Equal(1, r.planner.Plan(2, 8).GlobalBand, "a peer sees this shard's band")

	r.band.scanErr = errors.New("database is down")
	res := r.tallier.Cycle(ctx)
	assert.Error(res.Err)
	assert.Contains(res.Err.Error(), "tallying", "the error names the phase that failed")
	assert.Equal(NoBand, res.Band)
	assert.Equal(NoBand, r.planner.Plan(2, 8).GlobalBand, "the failed shard is cleared from planning")
	assert.False(r.planner.Plan(1, 8).Tallied, "and reads as untallied, which is UNKNOWN, not 'nothing due'")

	r.band.scanErr = nil
	assert.NoError(r.tallier.Cycle(ctx).Err)
	assert.Equal(1, r.planner.Plan(2, 8).GlobalBand, "recovery costs exactly one cycle")
}

// TestTallier_ScanFailureNeverTouchesTheCache pins the other half of the same policy, which the split turned
// from a rule into a structural fact: the Tallier has no cache at all. The Supplier is what spares it (see
// TestSupplier_UntalliedShardSparesTheCache), and neither can clear a healthy partition on a scan blip.
func TestTallier_ScanFailureNeverTouchesTheCache(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 1)
	seed(r.cache, 11, 12)

	r.band.scanErr = errors.New("database is down")
	assert.Error(r.tallier.Cycle(ctx).Err)
	assert.Equal(2, r.cache.Len(), "a failed scan must leave the partition intact")
	assert.Equal([]int{11, 12}, stepIDs(drain(r.cache)), "and intact means the same candidates")
}

// TestTallier_CancelDuringSleepEndsTheCycle pins that a cancelled context cuts the wait short and ends the
// cycle before it scans - which is what lets a caller stop promptly with no second signal, since the query
// is read-only and safe to abandon.
func TestTallier_CancelDuringSleepEndsTheCycle(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	r := newRig(t, []planner.Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}}, 5)
	r.tallier.SetInterval(30 * time.Second) // long enough that the test would hang if cancellation were ignored

	ctx, cancel := context.WithCancel(context.Background())
	r.tallier.Cycle(ctx) // anchor the cadence
	scansBefore := r.band.scans

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	res := r.tallier.Cycle(ctx)
	assert.True(time.Since(start) < 5*time.Second, "cancellation must cut the wait short")
	assert.Error(res.Err)
	assert.Equal(scansBefore, r.band.scans, "a cancelled cycle never scans")
}

// TestTallier_NewValidates pins that a wiring mistake is caught at construction - which is the whole reason
// Cycle has no error return to spend on one - and that a fresh Tallier is paced rather than running flat out
// until someone remembers to configure it.
func TestTallier_NewValidates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	src, pl := &fakeBandSource{}, planner.New()

	tl, err := NewTallier(1, src, pl)
	assert.NoError(err)
	assert.Equal(1, tl.Shard())
	assert.Equal(DefaultInterval, tl.Interval(), "a fresh Tallier is paced by default")
	assert.Equal(DefaultMinGap, tl.MinGap())

	_, err = NewTallier(0, src, pl)
	assert.Error(err, "shard must be positive")
	_, err = NewTallier(-1, src, pl)
	assert.Error(err)
	_, err = NewTallier(1, nil, pl)
	assert.Error(err, "source is required")
	_, err = NewTallier(1, src, nil)
	assert.Error(err, "planner is required")

	// The Supplier's own construction guards, asserted here so both live beside each other.
	cache := &candidates.Cache{}
	cache.Init(4)
	t.Cleanup(cache.Close)
	_, err = NewSupplier(1, &fakeStepSource{}, pl, nil)
	assert.Error(err, "cache is required")
}
