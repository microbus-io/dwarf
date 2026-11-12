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
	"fmt"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// payloadBytesColumns are the payload columns of each table that carry a byte count, the count named <column>_bytes.
var payloadBytesColumns = map[string][]string{
	"dwarf_flows": {"graph", "baggage", "final_state"},
	"dwarf_steps": {"state", "changes", "interrupt_payload", "resume_data", "subgraph_result"},
}

// TestPayloadBytes_EveryWriterCountsWhatItWrites drives every writer of a payload column - Create, a successor, a
// retry, an interrupt and its Resume, a subgraph's park and result, a fan-in with branches and one without, a
// completion, a failure, a Terminate, and a Fork inside a subgraph (the clone, the leaf, the re-parked caller) - and checks each row's byte counts. On SQLite and SQL
// Server, which store the bytes as written, each count equals the stored length; on PostgreSQL and MySQL, which
// re-spell JSON, each count of a value that was written is not zero.
func TestPayloadBytes_EveryWriterCountsWhatItWrites(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()

	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Start", "bytes/start")
	parent.SetURL("Ask", "bytes/ask")
	parent.SetURL("Call", "bytes/call")
	parent.AddTransition("Start", "Ask")
	parent.AddTransition("Ask", "Call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("bytes/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Work", "bytes/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("bytes/child", child)
	failing := workflow.NewGraph("Failing")
	failing.SetURL("Fail", "bytes/fail")
	failing.AddTransition("Fail", workflow.End)
	mux.HandleGraph("bytes/failing", failing)
	fanOut := workflow.NewGraph("FanOut")
	fanOut.SetURL("Spread", "bytes/spread")
	fanOut.SetURL("Each", "bytes/each")
	fanOut.SetURL("Join", "bytes/join")
	fanOut.SetFanIn("Join")
	fanOut.AddTransitionForEach("Spread", "Each", "items", "item")
	fanOut.AddTransitionChain("Each", "Join", workflow.End)
	mux.HandleGraph("bytes/fanout", fanOut)
	for _, task := range []string{"bytes/spread", "bytes/each", "bytes/join"} {
		mux.HandleTask(task, func(ctx context.Context, f *workflow.Flow) error {
			f.Set("visited", task)
			return nil
		})
	}

	mux.HandleTask("bytes/start", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("attempt", f.Attempt())
		if f.Attempt() == 0 && f.Retry(time.Millisecond, 1, time.Millisecond, 0) {
			return nil
		}
		return nil
	})
	mux.HandleTask("bytes/ask", func(ctx context.Context, f *workflow.Flow) error {
		var answer map[string]any
		yield, err := f.Interrupt(map[string]any{"question": "proceed?"}, &answer)
		if yield || err != nil {
			return err
		}
		f.Set("answer", answer["reply"])
		return nil
	})
	mux.HandleTask("bytes/call", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("bytes/child", map[string]any{"n": 1}, &out)
		if yield || err != nil {
			return err
		}
		f.Set("got", out["done"])
		return nil
	})
	mux.HandleTask("bytes/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})
	mux.HandleTask("bytes/fail", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("partial", "written before the failure")
		return errors.New("the task failed")
	})

	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(ctx)))
	timeout := time.Minute

	// A flow run through every step writer: a retry, an interrupt resumed, a subgraph, the completion.
	done, err := e.Create(ctx, "bytes/parent", map[string]any{"input": "the initial state"},
		&workflow.FlowOptions{Baggage: map[string]any{"tenant": "t1"}})
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, done, workflow.StatusInterrupted, timeout)
	assert.Must(assert.NoError(e.Resume(ctx, done, map[string]any{"reply": "yes"})))
	enginetest.AwaitFlowStatus(t, e, done, workflow.StatusCompleted, timeout)

	// A fan-out with branches, and one with none, whose fan-in is inserted straight from the source.
	for _, items := range [][]int{{1, 2}, {}} {
		fanned, err := e.Create(ctx, "bytes/fanout", map[string]any{"items": items}, nil)
		assert.Must(assert.NoError(err))
		enginetest.AwaitFlowStatus(t, e, fanned, workflow.StatusCompleted, timeout)
	}

	// A failure, and a Terminate of an interrupted flow.
	failed, err := e.Create(ctx, "bytes/failing", nil, nil)
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, failed, workflow.StatusFailed, timeout)
	stopped, err := e.Create(ctx, "bytes/parent", nil, nil)
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, stopped, workflow.StatusInterrupted, timeout)
	assert.Must(assert.NoError(e.Terminate(ctx, stopped, "stopped by the test")))
	enginetest.AwaitFlowStatus(t, e, stopped, workflow.StatusTerminated, timeout)

	// A Fork at the child's step: the tree is cloned, the leaf reset and the caller re-parked.
	history, err := e.History(ctx, done)
	assert.Must(assert.NoError(err))
	var workKey string
	for _, step := range history {
		for _, sub := range step.SubHistory {
			if sub.TaskName == "Work" {
				workKey = sub.StepKey
			}
		}
	}
	assert.Must(assert.NotEqual("", workKey, "the child's step"))
	forked, err := e.Fork(ctx, workKey, map[string]any{"n": 2})
	assert.Must(assert.NoError(err))
	enginetest.AwaitFlowStatus(t, e, forked, workflow.StatusCompleted, timeout)

	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	driver := db.DriverName()
	exact := ""
	switch driver {
	case "sqlite":
		exact = "LENGTH(CAST(%s AS BLOB))"
	case "mssql":
		exact = "DATALENGTH(%s)"
	}
	for table, columns := range payloadBytesColumns {
		for _, col := range columns {
			var rows, unwritten, mismatched int
			check := "0"
			if exact != "" {
				// A count of 0 with the column's default {} is a value never written.
				length := fmt.Sprintf(exact, col)
				check = "SUM(CASE WHEN " + col + "_bytes=" + length + " OR (" + col + "_bytes=0 AND " + length + "=2) THEN 0 ELSE 1 END)"
			}
			// A count of 0 is a value never written; the columns every row has written must not hold one.
			err := db.QueryRowContext(ctx, "SELECT COUNT(*), SUM(CASE WHEN "+col+"_bytes=0 THEN 1 ELSE 0 END), "+check+
				" FROM "+table).Scan(&rows, &unwritten, &mismatched)
			assert.Must(assert.NoError(err))
			assert.True(rows > 0, "%s has rows", table)
			assert.Zero(mismatched, "%s.%s_bytes is the stored length on %s", table, col, driver)
			switch col {
			case "graph", "baggage", "state":
				assert.Zero(unwritten, "%s.%s_bytes is written on every row", table, col)
			}
		}
	}
	// The values written past creation: each table holds rows that counted them.
	for _, c := range []struct{ table, col string }{
		{"dwarf_flows", "final_state"},
		{"dwarf_steps", "changes"},
		{"dwarf_steps", "interrupt_payload"},
		{"dwarf_steps", "resume_data"},
		{"dwarf_steps", "subgraph_result"},
	} {
		var counted int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table+" WHERE "+c.col+"_bytes>2").Scan(&counted)
		assert.Must(assert.NoError(err))
		assert.True(counted > 0, "%s.%s_bytes counts a value written", c.table, c.col)
	}
}
