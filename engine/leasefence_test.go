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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestLeaseFence_FailStep pins that failStep's post-execution write is fenced on the dispatch's lease
// generation (lease_seq): a zombie worker holding a stale generation cannot terminalize a flow the current
// owner is healthily re-executing (the "late error → healthy-flow kill" race), while the current owner
// still fails it normally.
func TestLeaseFence_FailStep(t *testing.T) {
	ctx := context.Background()

	// setup inserts one running root flow (flow_id=1) whose only step (step_id=1) is running under lease
	// generation `ownerSeq` - simulating the worker that legitimately re-claimed after a lease loss.
	setup := func(t *testing.T, e *Engine, ownerSeq int) {
		db, err := e.db.Shard(1)
		testarossa.For(t).NoError(err)
		// flow_id is auto-increment; the first insert on a fresh per-test DB is flow_id=1 on every driver.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			"ftok", "u", "W", "{}", workflow.StatusRunning, 1, 1, 1000,
		)
		testarossa.For(t).NoError(err)
		// lease_expires is set well into the future so the engine's own lease-recovery poll (running under
		// RunInTest) does not reset this deliberately-running step to pending mid-test.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, lease_seq, lease_expires) VALUES (?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 60000))",
			1, 1, "stok", "T", "u", workflow.StatusRunning, 1000, ownerSeq,
		)
		testarossa.For(t).NoError(err)
	}

	statuses := func(t *testing.T, e *Engine) (flowStatus, stepStatus string) {
		db, err := e.db.Shard(1)
		testarossa.For(t).NoError(err)
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=1").Scan(&flowStatus)
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE step_id=1").Scan(&stepStatus)
		return strings.TrimSpace(flowStatus), strings.TrimSpace(stepStatus)
	}

	t.Run("stale_lease_is_noop", func(t *testing.T) {
		assert := testarossa.For(t)
		e := NewEngine()
		e.SetHost(NewTestProxy())
		e.RunInTest(t)
		setup(t, e, 5) // the owner holds generation 5

		// A zombie holding the prior generation 4 returns an error late. It must write nothing.
		fenced, err := e.failStep(ctx, 1, 1, 4, 1, "ftok", errors.New("boom"), "T")
		assert.NoError(err)
		assert.True(fenced)

		flowStatus, stepStatus := statuses(t, e)
		assert.Equal(workflow.StatusRunning, flowStatus) // healthy flow not terminalized
		assert.Equal(workflow.StatusRunning, stepStatus) // owner's step untouched
	})

	t.Run("current_lease_fails", func(t *testing.T) {
		assert := testarossa.For(t)
		e := NewEngine()
		e.SetHost(NewTestProxy())
		e.RunInTest(t)
		setup(t, e, 5)

		// The current owner (generation 5) fails the step normally.
		fenced, err := e.failStep(ctx, 1, 1, 5, 1, "ftok", errors.New("boom"), "T")
		assert.NoError(err)
		assert.False(fenced)

		flowStatus, stepStatus := statuses(t, e)
		assert.Equal(workflow.StatusFailed, flowStatus)
		assert.Equal(workflow.StatusFailed, stepStatus)
	})
}

// zombieDispatch drives the real dispatch path into the late-error healthy-flow-kill lease-loss race and returns once a peer
// worker has re-claimed and completed the flow while the first ("zombie") dispatch is still blocked. It:
//   - blocks the named task's FIRST dispatch on a release channel (so it sits running under generation N),
//   - force-expires that step's lease and runs pollPendingSteps so a free worker re-claims it (generation
//     N+1) and re-runs it to completion, driving the flow to a terminal outcome,
//   - awaits that terminal outcome, then returns a release func the caller invokes to unblock the zombie.
//
// The zombie's late writes then carry the stale generation N and must be fenced. taskCalls counts dispatches
// of the blocking task (ends at 2: zombie + owner).
func zombieDispatch(t *testing.T, eng *Engine, flowURL, blockTaskName string, taskCalls *atomic.Int64,
	started chan struct{}, release chan struct{}) (flowKey string, outcome *workflow.FlowOutcome) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	var err error
	flowKey, err = eng.Create(ctx, flowURL, nil, nil)
	if !assert.NoError(err) {
		return "", nil
	}

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatalf("%s's first dispatch never started", blockTaskName)
	}

	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return flowKey, nil
	}
	// Simulate lease loss on the blocked step only (DB-clock-step / overrun): expire its lease, then run
	// recovery so a free worker re-claims it (bumping lease_seq) and re-runs the task to completion.
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE status='"+workflow.StatusRunning+"' AND task_name=?",
		blockTaskName)
	assert.NoError(err)
	eng.pollPendingSteps(ctx)

	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err = eng.Await(awaitCtx, flowKey)
	if !assert.NoError(err) {
		return flowKey, nil
	}
	return flowKey, outcome
}

// TestLeaseFence_CompletionNoDuplicateSuccessor pins the "late success → duplicate successors"
// variant: a zombie whose completion UPDATE carries a stale lease_seq must not re-complete its step nor
// insert a second successor for the flow a peer already advanced. A -> B -> END; A's first dispatch is the
// zombie, a peer re-runs A to completion (creating exactly one B), then the zombie is released and its
// fenced completion must be a no-op.
func TestLeaseFence_CompletionNoDuplicateSuccessor(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls, bCalls atomic.Int64
	aStarted := make(chan struct{}, 1)
	aRelease := make(chan struct{})

	proxy := NewTestProxy()
	g := workflow.NewGraph("LF-B")
	g.SetEndpoint("A", "lfb/a")
	g.SetEndpoint("B", "lfb/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("lfb/g", g)
	proxy.HandleTask("lfb/a", func(ctx context.Context, f *workflow.Flow) error {
		if aCalls.Add(1) == 1 {
			aStarted <- struct{}{}
			<-aRelease // the first (zombie) dispatch blocks until the peer has finished the flow
		}
		return nil
	})
	proxy.HandleTask("lfb/b", func(ctx context.Context, f *workflow.Flow) error {
		bCalls.Add(1)
		return nil
	})

	eng := NewEngine()
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(proxy)
	eng.RunInTest(t)

	flowKey, outcome := zombieDispatch(t, eng, "lfb/g", "A", &aCalls, aStarted, aRelease)
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie; its late completion UPDATE carries the stale lease_seq and must write nothing.
	close(aRelease)

	_, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	// Give the released zombie ample time to attempt (and, if the fence were broken, complete) its write.
	// A broken fence inserts a second B within milliseconds; the settle window makes the absence meaningful.
	time.Sleep(1 * time.Second)
	var bSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='B'", flowID).Scan(&bSteps))
	assert.Equal(1, bSteps)               // exactly one successor, not a zombie-inserted duplicate
	assert.Equal(int64(1), bCalls.Load()) // B executed exactly once
	assert.Equal(int64(2), aCalls.Load()) // A ran twice by design (zombie + owner re-dispatch)
}

// TestLeaseFence_CohortNoDoubleArrival pins the fan-out variant: a zombie branch whose completion
// is fenced must not bump cohort_arrivals a second time (which would overshoot cohort_size and re-fire the
// fan-in). A -> {X, Y} -> J (fan-in) -> END; X's first dispatch is the zombie, a peer re-runs X so the
// cohort resolves and J fires exactly once, then the released zombie's fenced arrival must be a no-op.
func TestLeaseFence_CohortNoDoubleArrival(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	var xCalls, jCalls atomic.Int64
	xStarted := make(chan struct{}, 1)
	xRelease := make(chan struct{})

	proxy := NewTestProxy()
	g := workflow.NewGraph("LF-C")
	g.SetEndpoint("A", "lfc/a")
	g.SetEndpoint("X", "lfc/x")
	g.SetEndpoint("Y", "lfc/y")
	g.SetEndpoint("J", "lfc/j")
	g.SetFanIn("J")
	g.AddTransition("A", "X")
	g.AddTransition("A", "Y")
	g.AddTransition("X", "J")
	g.AddTransition("Y", "J")
	g.AddTransition("J", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("lfc/g", g)
	proxy.HandleTask("lfc/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("lfc/x", func(ctx context.Context, f *workflow.Flow) error {
		if xCalls.Add(1) == 1 {
			xStarted <- struct{}{}
			<-xRelease // the first (zombie) X dispatch blocks until the peer has resolved the cohort
		}
		return nil
	})
	proxy.HandleTask("lfc/y", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("lfc/j", func(ctx context.Context, f *workflow.Flow) error {
		jCalls.Add(1)
		return nil
	})

	eng := NewEngine()
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(proxy)
	eng.RunInTest(t)

	flowKey, outcome := zombieDispatch(t, eng, "lfc/g", "X", &xCalls, xStarted, xRelease)
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie X; its late arrival must be fenced (no second cohort_arrivals bump, no second J).
	close(xRelease)

	_, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	time.Sleep(1 * time.Second) // let the released zombie attempt its fenced write
	var arrivals, size, jSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT cohort_arrivals, cohort_size FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&arrivals, &size))
	assert.Equal(2, size)     // two branches spawned
	assert.Equal(2, arrivals) // both arrived exactly once; the zombie did not add a third
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='J'", flowID).Scan(&jSteps))
	assert.Equal(1, jSteps)               // fan-in fired exactly once
	assert.Equal(int64(1), jCalls.Load()) // J executed exactly once
	assert.Equal(int64(2), xCalls.Load()) // X ran twice by design (zombie + owner re-dispatch)
}
