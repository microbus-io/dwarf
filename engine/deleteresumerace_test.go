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
	"database/sql"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestDeleteResumeRace pins that the orphan state "a `running` flow with zero step rows" is
// unreachable however a Delete or Purge races a Resume on an interrupted flow. Deferred deletion closes the
// window by construction - Delete/Purge no longer delete steps inline (they mark `delete_after_ms` and flip
// `interrupted -> cancelled` under the flow-row lock, mutually exclusive with Resume's `WHERE
// status='interrupted'`), and the reaper only removes provably-terminal trees. So the invariant holds: every
// `running`/`interrupted` flow has >= 1 step, and no `running` flow ever has zero steps.
//
// The test hammers the race ~50 times per variant. Both operations may error (404/409) or succeed on either
// side - all outcomes are legitimate; only the forbidden state is asserted against.
func TestDeleteResumeRace(t *testing.T) {
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("DRR")
	g.SetEndpoint("Gate", "drr/gate")
	g.SetEndpoint("Done", "drr/done")
	g.AddTransition("Gate", "Done")
	g.AddTransition("Done", workflow.END)
	testarossa.For(t).NoError(g.Validate())
	proxy.HandleGraph("drr/g", g)
	// Gate interrupts on its first dispatch (yield=true); a Resume re-dispatches it (yield=false) and it
	// proceeds to Done -> END.
	proxy.HandleTask("drr/gate", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("drr/done", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.RunInTest(t)

	// assertNoOrphan proves the forbidden state is absent for one flow: if the flow row still exists and is
	// non-terminal (`running`/`interrupted`), it must carry at least one step row.
	assertNoOrphan := func(t *testing.T, flowKey string) {
		assert := testarossa.For(t)
		shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
		if !assert.NoError(err) {
			return
		}
		db, err := e.db.Shard(shardNum)
		if !assert.NoError(err) {
			return
		}
		var status string
		err = db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status)
		if err == sql.ErrNoRows {
			return // fully gone - fine (nothing to strand)
		}
		if !assert.NoError(err) {
			return
		}
		status = strings.TrimSpace(status)
		if status != workflow.StatusRunning && status != workflow.StatusInterrupted {
			return // terminal - fine
		}
		var steps int
		if !assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&steps)) {
			return
		}
		// The forbidden orphan: a non-terminal flow with no steps.
		assert.True(steps >= 1)
	}

	// assertResumeHonest pins the Resume-vs-Delete race: Resume must not falsely report success for a resume the racing
	// Delete/Cancel preempted. The two operations serialize on the root-flow row (Resume's gate write and
	// Delete's interrupted->cancelled flip both key on `WHERE status='interrupted'`), so exactly one wins. If
	// Resume returned nil it genuinely took effect - the flow moved to `running` (and on from there), so a
	// Delete that flipped it to `cancelled` must have 409'd, and the flow is never `cancelled` here. A false
	// success (the pre-fix bug) would show up as a `cancelled` flow after a nil Resume.
	assertResumeHonest := func(t *testing.T, flowKey string, resumeErr error) {
		if resumeErr != nil {
			return // Resume reported a conflict/not-found - honest, nothing to prove
		}
		assert := testarossa.For(t)
		shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
		if !assert.NoError(err) {
			return
		}
		db, err := e.db.Shard(shardNum)
		if !assert.NoError(err) {
			return
		}
		var status string
		err = db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&status)
		if err == sql.ErrNoRows {
			return // reaped after a genuine resume+complete then delete - not a false success
		}
		if !assert.NoError(err) {
			return
		}
		assert.NotEqual(workflow.StatusCancelled, strings.TrimSpace(status))
	}

	// createInterrupted creates a flow and blocks until it rests `interrupted`.
	createInterrupted := func(t *testing.T) string {
		assert := testarossa.For(t)
		flowKey, err := e.Create(ctx, "drr/g", nil, nil)
		if !assert.NoError(err) {
			return ""
		}
		awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		out, err := e.Await(awaitCtx, flowKey)
		if !assert.NoError(err) || !assert.NotNil(out) {
			return ""
		}
		assert.Equal(workflow.StatusInterrupted, out.Status)
		return flowKey
	}

	// race fires opA and opB as simultaneously as possible (both park on a shared start channel) and waits for
	// both to return.
	race := func(opA, opB func()) {
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; opA() }()
		go func() { defer wg.Done(); <-start; opB() }()
		close(start)
		wg.Wait()
	}

	t.Run("delete_vs_resume", func(t *testing.T) {
		for range 50 {
			flowKey := createInterrupted(t)
			if flowKey == "" {
				return
			}
			var resumeErr error
			race(
				func() { _ = e.Delete(ctx, flowKey) },
				func() { resumeErr = e.Resume(ctx, flowKey, nil) },
			)
			assertNoOrphan(t, flowKey)
			assertResumeHonest(t, flowKey, resumeErr)
		}
	})

	t.Run("purge_vs_resume", func(t *testing.T) {
		for range 50 {
			flowKey := createInterrupted(t)
			if flowKey == "" {
				return
			}
			var resumeErr error
			race(
				func() { _, _ = e.Purge(ctx, workflow.Query{Status: workflow.StatusInterrupted}) },
				func() { resumeErr = e.Resume(ctx, flowKey, nil) },
			)
			assertNoOrphan(t, flowKey)
			assertResumeHonest(t, flowKey, resumeErr)
		}
	})
}
