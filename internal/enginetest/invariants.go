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

package enginetest

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"

	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

const (
	terminalSQL    = "(" + database.TerminalStatusesSQL + ")"
	nonTerminalSQL = "('" + workflow.StatusCreated + "', '" + workflow.StatusPending + "', '" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')"
	openFlowSQL    = "('" + workflow.StatusCreated + "', '" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')"

	// notDoomedStep and notDoomedFlow exclude the rows of a tree whose root is scheduled for deletion. Delete of an
	// interrupted flow cancels it and leaves its interrupted leaf and parked callers for the janitor to remove with
	// the whole tree, so a doomed tree legitimately holds shapes a live one may not.
	notDoomedStep = " AND NOT EXISTS (SELECT 1 FROM dwarf_flows df JOIN dwarf_flows dr ON dr.flow_id=df.root_flow_id" +
		" WHERE df.flow_id=s.flow_id AND dr.delete_after_ms>0)"
	notDoomedFlow = " AND NOT EXISTS (SELECT 1 FROM dwarf_flows dr WHERE dr.flow_id=f.root_flow_id AND dr.delete_after_ms>0)"
)

// invariantCheck is one durable invariant, stated as a query counting the rows that violate it.
type invariantCheck struct {
	label string
	query string
}

// invariantChecks are the safety invariants of a quiesced shard. Each counts its violating rows; every one must count
// zero. The checks hold only at rest: several are transiently violated while work is in flight (a cohort mid-resolve,
// a child completed and its caller not yet revived), which is why AssertInvariants must run after the workload has
// quiesced.
var invariantChecks = []invariantCheck{
	// A closed flow retains no non-terminal step. A doomed tree is excluded: its interrupted leaf waits for the janitor.
	{"closed flow with a non-terminal step",
		"SELECT COUNT(*) FROM dwarf_steps s JOIN dwarf_flows f ON s.flow_id=f.flow_id" +
			" WHERE f.status IN " + terminalSQL + " AND f.delete_after_ms=0 AND s.status IN " + nonTerminalSQL},

	{"terminal step with parked<>0",
		"SELECT COUNT(*) FROM dwarf_steps WHERE status IN " + terminalSQL + " AND parked<>0"},

	// A running flow with every step terminal is the orphan shape: nothing will ever advance it.
	{"running flow with zero non-terminal steps",
		"SELECT COUNT(*) FROM dwarf_flows f WHERE f.status='" + workflow.StatusRunning + "'" +
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN " + nonTerminalSQL + ")"},

	// created is transient: Fork's clone holds its rows created until its mapping is complete, in one transaction.
	{"flow at rest in created",
		"SELECT COUNT(*) FROM dwarf_flows WHERE status='" + workflow.StatusCreated + "'"},
	{"step at rest in created",
		"SELECT COUNT(*) FROM dwarf_steps WHERE status='" + workflow.StatusCreated + "'"},

	// --- Tree links ---

	{"dangling surgraph link",
		"SELECT COUNT(*) FROM dwarf_flows f WHERE f.surgraph_step_id>0 AND (" +
			"NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.step_id=f.surgraph_step_id)" +
			" OR NOT EXISTS (SELECT 1 FROM dwarf_flows p WHERE p.flow_id=f.surgraph_flow_id AND p.root_flow_id=f.root_flow_id))"},
	{"root_flow_id not resolving to a self-root",
		"SELECT COUNT(*) FROM dwarf_flows f WHERE NOT EXISTS (" +
			"SELECT 1 FROM dwarf_flows r WHERE r.flow_id=f.root_flow_id AND r.root_flow_id=r.flow_id)"},
	{"cross-flow DAG edge",
		"SELECT COUNT(*) FROM dwarf_steps s WHERE" +
			" (s.successor_id<>0 AND EXISTS (SELECT 1 FROM dwarf_steps t WHERE t.step_id=s.successor_id AND t.flow_id<>s.flow_id))" +
			" OR (s.predecessor_id<>0 AND EXISTS (SELECT 1 FROM dwarf_steps t WHERE t.step_id=s.predecessor_id AND t.flow_id<>s.flow_id))"},

	// --- One durable advancement per obligation ---

	// Every transition that commits while the flow stays open clears advancing, so a survivor is a transition that
	// never committed - one the orphan sweep would replay. A closed flow keeps the flag on its tail, never read.
	{"completed step still advancing on an open flow",
		"SELECT COUNT(*) FROM dwarf_steps s JOIN dwarf_flows f ON s.flow_id=f.flow_id" +
			" WHERE f.status IN ('" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')" +
			" AND s.status='" + workflow.StatusCompleted + "' AND s.advancing=1"},

	// A step's transition inserts each successor once: a linear successor, one row per static fan-out target, one per
	// forEach element (told apart by fan_out_ordinal). A Retry, a Resume and a subgraph re-dispatch reuse the step's
	// own row, and a Goto loop's next iteration has the previous one as its predecessor. So two rows naming the same
	// predecessor, task and ordinal are one transition committed twice.
	{"successor inserted twice by one transition",
		"SELECT COUNT(*) FROM (SELECT flow_id FROM dwarf_steps WHERE predecessor_id<>0" +
			" GROUP BY flow_id, predecessor_id, task_name, fan_out_ordinal HAVING COUNT(*)>1) d"},

	// A fan-in step's predecessor is the member of the cohort that resolved it, and it is in the cohort's enclosing
	// frame - the spawn's own lineage. A cohort converges once.
	{"cohort converged more than once",
		"SELECT COUNT(*) FROM (SELECT m.lineage_id FROM dwarf_steps z" +
			" JOIN dwarf_steps m ON z.predecessor_id=m.step_id JOIN dwarf_steps sp ON sp.step_id=m.lineage_id" +
			" WHERE m.lineage_id<>0 AND z.flow_id=m.flow_id AND z.lineage_id=sp.lineage_id" +
			" GROUP BY m.lineage_id HAVING COUNT(*)>1) d"},

	// --- Cohort accounting ---

	{"cohort counter overshoot",
		"SELECT COUNT(*) FROM dwarf_steps WHERE cohort_size>0 AND (cohort_arrivals>cohort_size" +
			" OR cohort_failures>cohort_arrivals OR cohort_cancellations>cohort_failures)"},

	// A cohort converges only when every branch arrived and none was lost; one with a loss fails the flow instead.
	{"cohort converged before it resolved, or with a loss",
		"SELECT COUNT(*) FROM dwarf_steps sp WHERE sp.cohort_size>0" +
			" AND (sp.cohort_arrivals<sp.cohort_size OR sp.cohort_failures>0) AND EXISTS (" +
			"SELECT 1 FROM dwarf_steps z JOIN dwarf_steps m ON z.predecessor_id=m.step_id" +
			" WHERE m.lineage_id=sp.step_id AND z.flow_id=sp.flow_id AND z.lineage_id=sp.lineage_id)"},

	// --- Child-owned park settlement ---
	//
	// A caller waiting on its child is either parked (running, parked=1) or, while the child's interrupt holds the
	// chain, interrupted with parked=0; either way it names the child that owns its park, which Resume re-parks.

	// A waiting caller's child is still open: a closed child's delivery settles its caller, and the wedge sweep
	// exists for the park whose delivery was lost.
	{"waiting caller without an open child owning its park",
		"SELECT COUNT(*) FROM dwarf_steps s WHERE (s.parked<>0 AND s.status IN " + nonTerminalSQL + "" +
			" OR s.parked_child_flow_id<>0 AND s.status='" + workflow.StatusInterrupted + "')" +
			" AND NOT EXISTS (SELECT 1 FROM dwarf_flows c WHERE c.flow_id=s.parked_child_flow_id" +
			" AND c.surgraph_step_id=s.step_id AND c.status IN " + openFlowSQL + ")" + notDoomedStep},

	// An open child has a caller waiting on it, by its own id: the orphaned child the mirror sweep exists for (a
	// caller terminal, or settled by another child) fails this, as does a second open child of one caller.
	{"open child whose caller does not hold its park",
		"SELECT COUNT(*) FROM dwarf_flows f WHERE f.surgraph_step_id<>0 AND f.status IN " + openFlowSQL +
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.step_id=f.surgraph_step_id AND s.parked_child_flow_id=f.flow_id" +
			" AND (s.parked<>0 OR s.status='" + workflow.StatusInterrupted + "'))" + notDoomedFlow},

	// The delivery that consumes a park clears its owner, as does the Retry rewind, so a step that is neither waiting
	// nor terminal names no child: a stale owner would let a late delivery from that child settle a later park.
	{"step neither waiting nor terminal still naming a park owner",
		"SELECT COUNT(*) FROM dwarf_steps WHERE parked=0 AND parked_child_flow_id<>0" +
			" AND status IN ('" + workflow.StatusCreated + "', '" + workflow.StatusPending + "', '" + workflow.StatusRunning + "')"},
}

// AssertInvariants is the reusable end-of-test sweep that proves a workload never left a shape the recovery
// machinery exists to catch, nor one it could not: it runs the invariant checks by direct SQL against every shard of
// the engine and fails the test, naming the check and the shard, for each that counts a violating row.
//
// Call it only after the workload has quiesced - every Await returned, every repair the test induced driven - because
// several checks are transiently violated while work is in flight. It asserts once, against the state as it stands.
// Do not retry it for a few seconds on the theory that a violation is work still in flight: a settle can only ever
// hide a real violation, and a check that waits for an invariant to come true is not asserting one.
func AssertInvariants(t *testing.T, e Engine) {
	t.Helper()
	assert := testarossa.For(t)
	shards := map[int]*sequel.DB{}
	for _, shard := range accessors.DB(e).Indices() {
		sh, err := accessors.DB(e).Shard(shard)
		if assert.NoError(err) {
			shards[shard] = sh.DB()
		}
	}
	AssertInvariantsIn(t, shards)
}

// AssertInvariantsIn is AssertInvariants against the given shard databases, keyed by shard index: for a database
// no engine in the test has open, such as one a benchmark run left behind.
func AssertInvariantsIn(t *testing.T, shards map[int]*sequel.DB) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()
	for shard, db := range shards {
		for _, c := range invariantChecks {
			var n int
			if !assert.NoError(db.QueryRowContext(ctx, c.query).Scan(&n), "shard %d: %s", shard, c.label) {
				continue
			}
			assert.Equal(0, n, "shard %d: invariant violated: %s (%d offending rows)", shard, c.label, n)
		}
	}
}

// Outcomes is a record of every closed outcome on an engine's shards at one moment - each closed flow's status,
// final state, error and cancel reason, and each terminal step's status, changes and error - taken by CaptureOutcomes
// and compared against a later moment by AssertOutcomesKept.
type Outcomes struct {
	flows map[string]flowOutcome // by flow key
	steps map[string]stepOutcome // by shard and step id
	live  map[string]bool        // every flow key in the live tables, closed or not
}

type flowOutcome struct {
	status, err, cancelReason string
	finalState                []byte
}

type stepOutcome struct {
	flowKey     string
	status, err string
	changes     []byte
}

// CaptureOutcomes records every closed outcome on the engine's shards: every closed flow, and every terminal step of
// a closed flow or completed with its transition committed. Those are the outcomes nothing may change; a Delete, a
// Purge or the janitor may only remove them, which AssertOutcomesKept reports.
func CaptureOutcomes(t *testing.T, e Engine) Outcomes {
	t.Helper()
	assert := testarossa.For(t)
	shards := map[int]*sequel.DB{}
	for _, shard := range accessors.DB(e).Indices() {
		sh, err := accessors.DB(e).Shard(shard)
		if assert.NoError(err) {
			shards[shard] = sh.DB()
		}
	}
	return CaptureOutcomesIn(t, shards)
}

// CaptureOutcomesIn records the closed outcomes CaptureOutcomes does, read from the given databases by shard index
// rather than through an engine: for a test that records them while no engine runs there, as after a process that
// held the shard was killed and before its replacement starts.
func CaptureOutcomesIn(t *testing.T, shards map[int]*sequel.DB) Outcomes {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()
	out := Outcomes{flows: map[string]flowOutcome{}, steps: map[string]stepOutcome{}, live: map[string]bool{}}
	for shard, db := range shards {
		rows, err := db.QueryContext(ctx,
			"SELECT flow_id, created_hour, flow_token, status, error, cancel_reason, final_state FROM dwarf_flows")
		if !assert.NoError(err) {
			continue
		}
		for rows.Next() {
			var flowID, hour int
			var token string
			var o flowOutcome
			if assert.NoError(rows.Scan(&flowID, &hour, &token, &o.status, &o.err, &o.cancelReason, &o.finalState)) {
				flowKey := keys.NewFlowKey(shard, hour, flowID, token)
				out.live[flowKey] = true
				if workflow.IsTerminalStatus(o.status) {
					out.flows[flowKey] = o
				}
			}
		}
		assert.NoError(rows.Err())
		rows.Close()

		rows, err = db.QueryContext(ctx,
			"SELECT s.step_id, f.flow_id, f.created_hour, f.flow_token, s.status, s.error, s.changes"+
				" FROM dwarf_steps s JOIN dwarf_flows f ON s.flow_id=f.flow_id WHERE s.status IN "+terminalSQL+
				" AND (f.status IN "+terminalSQL+" OR (s.status='"+workflow.StatusCompleted+"' AND s.advancing=0))")
		if !assert.NoError(err) {
			continue
		}
		for rows.Next() {
			var stepID, flowID, hour int
			var token string
			var o stepOutcome
			if assert.NoError(rows.Scan(&stepID, &flowID, &hour, &token, &o.status, &o.err, &o.changes)) {
				o.flowKey = keys.NewFlowKey(shard, hour, flowID, token)
				out.steps[strconv.Itoa(shard)+":"+strconv.Itoa(stepID)] = o
			}
		}
		assert.NoError(rows.Err())
		rows.Close()
	}
	return out
}

// AssertOutcomesKept fails the test for every outcome in before that the engine's shards now hold differently: a
// closed flow whose status, final state, error or cancel reason changed, or a terminal step whose status, changes or
// error did, or that is gone while its flow is not. JSON is compared by value, not by its bytes. A flow no longer in
// the live tables is not compared; its key is returned, for the test to account for - a Delete, a Purge, or a move
// to the archive that a read by key should still answer.
func AssertOutcomesKept(t *testing.T, e Engine, before Outcomes) (removed []string) {
	t.Helper()
	assert := testarossa.For(t)
	now := CaptureOutcomes(t, e)
	for flowKey, was := range before.flows {
		if !now.live[flowKey] {
			removed = append(removed, flowKey)
			continue
		}
		is, ok := now.flows[flowKey]
		if !assert.True(ok, "closed flow %s reopened", flowKey) {
			continue
		}
		assert.Equal(was.status, is.status, "closed flow %s changed status", flowKey)
		assert.Equal(was.err, is.err, "closed flow %s changed error", flowKey)
		assert.Equal(was.cancelReason, is.cancelReason, "closed flow %s changed cancel reason", flowKey)
		assert.True(sameJSON(was.finalState, is.finalState), "closed flow %s changed final state: %s, now %s", flowKey, was.finalState, is.finalState)
	}
	for stepRef, was := range before.steps {
		if !now.live[was.flowKey] {
			continue // removed with its flow
		}
		is, ok := now.steps[stepRef]
		if !assert.True(ok, "terminal step %s of flow %s is gone or no longer terminal", stepRef, was.flowKey) {
			continue
		}
		assert.Equal(was.status, is.status, "terminal step %s changed status", stepRef)
		assert.Equal(was.err, is.err, "terminal step %s changed error", stepRef)
		assert.True(sameJSON(was.changes, is.changes), "terminal step %s changed its changes: %s, now %s", stepRef, was.changes, is.changes)
	}
	return removed
}

// sameJSON reports whether two stored JSON values are equal by value. Stored bytes are not canonical - PostgreSQL and
// MySQL re-spell what they store - so they are decoded first, numbers as json.Number to stay exact.
func sameJSON(a, b []byte) bool {
	if bytes.Equal(a, b) {
		return true
	}
	decode := func(raw []byte) (any, bool) {
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, false
		}
		return v, true
	}
	va, okA := decode(a)
	vb, okB := decode(b)
	return okA && okB && reflect.DeepEqual(va, vb)
}
