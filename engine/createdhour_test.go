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
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// hourEpoch is the epoch created_hour counts from.
var hourEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// createdHourGraphs registers a parent that parks at an interrupt first, so a test can move its hour before anything
// else of it is written, and then takes every step-creating path - a fan-out and its fan-in, a subgraph spawn, and an
// empty forEach that fires its fan-in directly - and the child it calls.
func createdHourGraphs(mux *hosts.Mux) {
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Gate", "hour/gate")
	parent.SetURL("Seed", "hour/seed")
	parent.SetURL("Branch", "hour/branch")
	parent.SetURL("Join", "hour/join")
	parent.SetURL("Call", "hour/call")
	parent.SetURL("Empty", "hour/empty")
	parent.SetURL("EBranch", "hour/ebranch")
	parent.SetURL("EJoin", "hour/ejoin")
	parent.AddTransition("Gate", "Seed")
	parent.AddTransitionForEach("Seed", "Branch", "items", "item")
	parent.AddTransition("Branch", "Join")
	parent.SetFanIn("Join")
	parent.AddTransitionChain("Join", "Call", "Empty")
	parent.AddTransitionForEach("Empty", "EBranch", "none", "n")
	parent.AddTransition("EBranch", "EJoin")
	parent.SetFanIn("EJoin")
	parent.AddTransition("EJoin", workflow.End)
	mux.HandleGraph("hour/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Leaf", "hour/leaf")
	child.AddTransition("Leaf", workflow.End)
	mux.HandleGraph("hour/child", child)

	noop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	mux.HandleTask("hour/gate", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"q": "go?"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("hour/seed", func(ctx context.Context, f *workflow.Flow) error {
		if err := f.Set("items", []int{1, 2, 3}); err != nil {
			return err
		}
		return f.Set("none", []int{})
	})
	mux.HandleTask("hour/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("hour/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	for _, url := range []string{"hour/branch", "hour/join", "hour/empty", "hour/ebranch", "hour/ejoin", "hour/leaf"} {
		mux.HandleTask(url, noop)
	}
}

// TestCreatedHour_EveryRowCarriesItsFlowsHour pins created_hour on every creation path. A flow's hour is the
// database clock's hour at its insert, and every step of it carries the FLOW's hour, never one of its own: the flow
// parks at an interrupt, its hour is moved 100 hours back, and every step written after the Resume - successors,
// fan-out branches, the fan-in, a directly fired fan-in - must carry the moved hour, which no clock reading gives.
// The new flows - the subgraph child, a Continue turn, Fork's clones - carry the current hour, not the hour of the
// flow they came from. The keys handed out carry the hour of the row they name.
func TestCreatedHour_EveryRowCarriesItsFlowsHour(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	createdHourGraphs(mux)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)

	parkedKey, outcome, err := e.Run(ctx, "hour/parent", nil, nil)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status, outcome.Error) {
		return
	}
	shard, hour, rootID, rootToken, err := keys.ParseFlowKey(parkedKey)
	assert.NoError(err)
	const back = 100
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET created_hour=created_hour-? WHERE flow_id=?", back, rootID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET flow_created_hour=flow_created_hour-? WHERE flow_id=?", back, rootID)
	assert.NoError(err)
	movedHour := hour - back
	rootKey := keys.NewFlowKey(shard, movedHour, rootID, rootToken)

	if !assert.NoError(e.Resume(ctx, rootKey, nil)) {
		return
	}
	outcome, err = e.Await(ctx, rootKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error) {
		return
	}
	turnKey, err := e.Continue(ctx, rootKey, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err = e.Await(ctx, turnKey) // the turn parks at its Gate
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status, outcome.Error) {
		return
	}
	// Fork inside the child, so the clone takes both the root and the subgraph flow on the rewind chain.
	steps, err := e.History(ctx, rootKey)
	if !assert.NoError(err) {
		return
	}
	var leafKey, childKey string
	for _, s := range steps {
		if s.TaskName == "Call" {
			childKey = s.SubgraphKey
			for _, sub := range s.SubHistory {
				if sub.TaskName == "Leaf" {
					leafKey = sub.StepKey
				}
			}
		}
	}
	if !assert.NotEqual("", leafKey) {
		return
	}
	forkKey, err := e.Fork(ctx, leafKey, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err = e.Await(ctx, forkKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error) {
		return
	}

	// Every flow but the moved root was created now, so its hour is its created_at's, both on the database clock.
	// SQL Server rounds created_at to the millisecond, up past an hour boundary at worst, so that one case may read
	// an hour ahead.
	hourByFlow := map[int]int{}
	rows, err := db.QueryContext(ctx, "SELECT flow_id, created_hour, created_at FROM dwarf_flows")
	if !assert.NoError(err) {
		return
	}
	for rows.Next() {
		var flowID, h int
		var createdAt time.Time
		assert.NoError(rows.Scan(&flowID, &h, &createdAt))
		hourByFlow[flowID] = h
		if flowID == rootID {
			continue
		}
		atHour := int(createdAt.Sub(hourEpoch) / time.Hour)
		if atHour != h {
			sinceBoundary := createdAt.Sub(hourEpoch) % time.Hour
			assert.True(atHour == h+1 && sinceBoundary < time.Millisecond,
				"flow %d: created_hour %d, created_at %v (hour %d)", flowID, h, createdAt, atHour)
		}
	}
	assert.NoError(rows.Err())
	rows.Close()
	// The root and its child, the Continue turn, the fork and the clone of the root's child.
	assert.Len(hourByFlow, 5)
	assert.Equal(movedHour, hourByFlow[rootID])
	nowHour := int(time.Since(hourEpoch) / time.Hour)
	for flowID, h := range hourByFlow {
		if flowID != rootID {
			assert.True(h >= nowHour-1 && h <= nowHour+1, "flow %d: created_hour %d, now is hour %d", flowID, h, nowHour)
		}
	}

	var strays, rootSteps int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.flow_created_hour<>f.created_hour").Scan(&strays))
	assert.Zero(strays, "every step carries its own flow's hour")
	// Gate, Seed, three branches, Join, Call, Empty and EJoin, all but Gate written after the hour moved.
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND flow_created_hour=?", rootID, movedHour).Scan(&rootSteps))
	assert.Equal(9, rootSteps)

	// The keys handed out name their row's hour.
	keyHour := func(flowKey string) (int, int) {
		_, h, flowID, _, err := keys.ParseFlowKey(flowKey)
		assert.NoError(err)
		return flowID, h
	}
	for _, k := range []string{turnKey, forkKey, childKey} {
		flowID, h := keyHour(k)
		assert.Equal(hourByFlow[flowID], h, "key %s", k)
		assert.NotEqual(movedHour, h, "key %s is a new flow's", k)
	}
	stepHours := map[int]int{}
	srows, err := db.QueryContext(ctx, "SELECT step_id, flow_created_hour FROM dwarf_steps")
	if !assert.NoError(err) {
		return
	}
	for srows.Next() {
		var stepID, h int
		assert.NoError(srows.Scan(&stepID, &h))
		stepHours[stepID] = h
	}
	assert.NoError(srows.Err())
	srows.Close()
	for _, s := range steps {
		_, h, _, stepID, _, err := keys.ParseStepKey(s.StepKey)
		assert.NoError(err)
		assert.Equal(stepHours[stepID], h, "step key %s", s.StepKey)
		assert.Equal(movedHour, h, "step key %s is the root's", s.StepKey)
	}
	summaries, _, err := e.List(ctx, workflow.Query{})
	assert.NoError(err)
	assert.Len(summaries, 3) // the roots: the original, its turn and the fork
	for _, s := range summaries {
		flowID, h := keyHour(s.FlowKey)
		assert.Equal(hourByFlow[flowID], h, "flow key %s", s.FlowKey)
		// Every root here is on the original's thread, so every thread key is the moved root's.
		threadID, threadHour := keyHour(s.ThreadKey)
		assert.Equal(rootID, threadID, "thread key %s", s.ThreadKey)
		assert.Equal(movedHour, threadHour, "thread key %s", s.ThreadKey)
	}
}

// TestCreatedHour_WrongHourIsNotFound pins that the hour is part of the address: a key whose hour is not its row's
// is not found by any operation, exactly as a key with the wrong token is not, and an Await on one settles at
// once rather than waiting out its budget. Accepting it would give a flow a second spelling, and an Await parked on
// that spelling would never be released by a stop signalled under the row's own.
func TestCreatedHour_WrongHourIsNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	done := workflow.NewGraph("Done")
	done.SetURL("Only", "wronghour/only")
	done.AddTransition("Only", workflow.End)
	mux.HandleGraph("wronghour/done", done)
	mux.HandleTask("wronghour/only", func(ctx context.Context, f *workflow.Flow) error { return nil })
	parked := workflow.NewGraph("Parked")
	parked.SetURL("Ask", "wronghour/ask")
	parked.AddTransition("Ask", workflow.End)
	mux.HandleGraph("wronghour/parked", parked)
	mux.HandleTask("wronghour/ask", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"q": "?"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	doneKey, outcome, err := e.Run(ctx, "wronghour/done", nil, nil)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error) {
		return
	}
	parkedKey, outcome, err := e.Run(ctx, "wronghour/parked", nil, nil)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status, outcome.Error) {
		return
	}
	steps, err := e.History(ctx, doneKey)
	if !assert.NoError(err) || !assert.Len(steps, 1) {
		return
	}

	// Each key respelled with the hour after its own.
	respell := func(key string) string {
		shard, hour, id, token, err := keys.ParseFlowKey(key)
		assert.NoError(err)
		return keys.NewFlowKey(shard, hour+1, id, token)
	}
	respellStep := func(key string) string {
		shard, hour, flowID, stepID, token, err := keys.ParseStepKey(key)
		assert.NoError(err)
		return keys.NewStepKey(shard, hour+1, flowID, stepID, token)
	}
	wrongDone, wrongParked, wrongStep := respell(doneKey), respell(parkedKey), respellStep(steps[0].StepKey)
	notFound := func(op string, err error) {
		if assert.Error(err, op) {
			assert.Equal(http.StatusNotFound, errors.StatusCode(err), "%s: %v", op, err)
		}
	}

	_, err = e.Snapshot(ctx, wrongDone)
	notFound("Snapshot", err)
	_, _, err = e.Fingerprint(ctx, wrongDone)
	notFound("Fingerprint", err)
	_, err = e.History(ctx, wrongDone)
	notFound("History", err)
	_, err = e.Step(ctx, wrongStep)
	notFound("Step", err)
	_, err = e.Fork(ctx, wrongStep, nil)
	notFound("Fork", err)
	_, err = e.Continue(ctx, wrongDone, nil)
	notFound("Continue", err)
	_, err = e.Create(ctx, "wronghour/done", nil, &workflow.FlowOptions{ThreadKey: wrongDone})
	notFound("Create with a ThreadKey", err)
	_, _, err = e.List(ctx, workflow.Query{ThreadKey: wrongDone})
	notFound("List by ThreadKey", err)
	err = e.Delete(ctx, wrongDone)
	notFound("Delete", err)
	err = e.Resume(ctx, wrongParked, nil)
	notFound("Resume", err)
	err = e.Cancel(ctx, wrongParked, "")
	notFound("Cancel", err)
	err = e.Terminate(ctx, wrongParked, "")
	notFound("Terminate", err)

	// Await's first read finds no row, so it answers at once. Had it parked, the latch's resolver settles a
	// wrong-hour key as naming no row, which wakes it into the same not-found.
	actx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	t0 := time.Now()
	_, err = e.Await(actx, wrongParked)
	notFound("Await", err)
	assert.True(time.Since(t0) < 5*time.Second, "Await settled after %v", time.Since(t0))
	resolved, err := e.resolveStoppedFlows(ctx, []string{parkedKey, wrongParked, doneKey, wrongDone})
	if assert.NoError(err) {
		assert.Equal(workflow.StatusInterrupted, resolved[parkedKey])
		assert.Equal(workflow.StatusCompleted, resolved[doneKey])
		assert.Equal(flowUnresolved, resolved[wrongParked])
		assert.Equal(flowUnresolved, resolved[wrongDone])
	}

	// The right keys still work, and the parked flow was not touched by any of the above.
	outcome, err = e.Snapshot(ctx, parkedKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusInterrupted, outcome.Status)
	}
	_, err = e.Snapshot(ctx, doneKey)
	assert.NoError(err)
}
