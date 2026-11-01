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
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestFork_TransactionOpensWithAWrite pins that every attempt of Fork's transaction opens with a write. On SQLite a
// transaction that reads first cannot upgrade its lock against a concurrent writer, and fails at once on every
// retry; fixtures/forkconcurrentflow_test.go shows that under load, but only now and then. This reads the order
// of the statements off the database spans instead, so it fails every time the opening write is dropped, moved
// after a read, or replaced by a plain read. On PostgreSQL and MySQL the lock is a locking read, which a span does
// not tell apart from a plain one; TestArchivedReads_ForkHoldsTheReaperOffItsSource pins that it is taken.
func TestFork_TransactionOpensWithAWrite(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	mux := hosts.NewMux()
	g := workflow.NewGraph("ForkWriteFirst")
	g.SetURL("A", "forkwritefirst/step")
	g.SetURL("B", "forkwritefirst/step")
	g.AddTransitionChain("A", "B", workflow.End)
	mux.HandleGraph("forkwritefirst/g", g)
	mux.HandleTask("forkwritefirst/step", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.SetTracerProvider(tp)
	assert.NoError(e.Startup(t.Context()))
	db, err := e.db.Shard(1)
	assert.NoError(err)
	if locksByReading(db.DriverName()) {
		t.Skip("the lock is a locking read on " + db.DriverName())
	}

	fk, outcome, err := e.Run(ctx, "forkwritefirst/g", nil, nil)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
		return
	}
	steps, err := e.History(ctx, fk)
	if !assert.NoError(err) || !assert.Len(steps, 2) {
		return
	}

	forkCtx, forkSpan := tp.Tracer("test").Start(ctx, "fork")
	_, err = e.Fork(forkCtx, steps[1].StepKey, nil)
	forkSpan.End()
	if !assert.NoError(err) {
		return
	}

	// Started() is in start order, and Fork runs its statements one after another on this goroutine. BEGIN and
	// COMMIT nest under the transact span; the statements under the ctx Fork passes them, which is its caller's.
	fork := forkSpan.SpanContext().SpanID()
	transacts := map[[8]byte]bool{}
	attempts := 0
	opening := false // an attempt has begun and its first statement is still to come
	for _, s := range sr.Started() {
		parent := s.Parent().SpanID()
		switch {
		case s.Name() == "transact" && parent == fork:
			transacts[s.SpanContext().SpanID()] = true
		case s.Name() == "BEGIN" && transacts[parent]:
			attempts++
			opening = true
		case opening && parent == fork:
			opening = false
			switch operationName(s) {
			case "UPDATE", "INSERT", "DELETE":
			default:
				t.Errorf("attempt %d of Fork's transaction opens with %q, not a write", attempts, s.Name())
			}
		}
	}
	assert.NotZero(attempts, "no transaction attempt under Fork")
	assert.False(opening, "Fork's transaction ran no statement")
}

// operationName is a database span's db.operation.name.
func operationName(s sdktrace.ReadOnlySpan) string {
	for _, kv := range s.Attributes() {
		if kv.Key == attribute.Key("db.operation.name") {
			return kv.Value.AsString()
		}
	}
	return ""
}
