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

/*
Flow state round-trips through JSON, where a number is carried as a float64 - exact only for integers
up to 2^53. A larger integer (a 64-bit database key, a Snowflake id, time.Now().UnixNano()) would come
back ROUNDED from the next step, and the engine would then persist the rounded value onward into
final_state, a fork, a continuation - silently charging the wrong order. So it is rejected where it is
written, never stored: a task's write panics into a clean step failure, and a host-supplied payload is
a 400. This pins both halves end to end, plus the workaround (carry the id as a string) actually
working across a step boundary.
*/
package fixtures

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

const snowflakeID = 1234567890123456789 // 1.2e18: rounds to ...768 through a float64

func TestPrecisionflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Writer -> Reader, with an onError handler off Writer, so a rejected write is observable as a
	// routed failure rather than only as a failed flow.
	g := workflow.NewGraph("Precision")
	g.SetEndpoint("Writer", "precisionflow.verify:428/writer")
	g.SetEndpoint("Reader", "precisionflow.verify:428/reader")
	g.SetEndpoint("Handler", "precisionflow.verify:428/handler")
	g.AddTransition("Writer", "Reader")
	g.AddTransition("Reader", workflow.END)
	g.AddTransitionOnError("Writer", "Handler")
	g.AddTransition("Handler", workflow.END)
	proxy.HandleGraph("precisionflow.verify:428/g", g)

	proxy.HandleTask("precisionflow.verify:428/writer", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetBool("nul") {
			// Panics: a NUL is legal JSON but PostgreSQL's JSONB rejects it, and the write that carries it
			// is the step's own completion UPDATE - so unguarded it leaves the step running with no error
			// recorded, re-executing this task every few minutes forever.
			f.SetString("payload", "a"+string(rune(0))+"b")
			return errors.New("unreachable: SetString should have panicked")
		}
		if f.GetBool("safe") {
			// The workaround: the id crosses the step boundary as a string, byte for byte.
			f.SetString("orderID", "1234567890123456789")
			f.SetInt("qty", 3)
			return nil
		}
		// Panics: state cannot carry this integer, and rounding it would corrupt the workflow.
		f.SetInt("orderID", snowflakeID)
		return errors.New("unreachable: SetInt should have panicked")
	})
	proxy.HandleTask("precisionflow.verify:428/reader", func(ctx context.Context, f *workflow.Flow) error {
		if f.Has("payload") {
			f.SetString("readBack", f.GetString("payload")) // read back AFTER the database round trip
			return nil
		}
		f.SetString("readBack", f.GetString("orderID"))
		return nil
	})
	proxy.HandleTask("precisionflow.verify:428/handler", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("handled", true)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("a task writing an oversized integer fails the step, routed to onError", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "precisionflow.verify:428/g", map[string]any{}, nil)
		if !assert.NoError(err) {
			return
		}
		// The panic became a normal step failure: onError routed it, so the flow completes via Handler
		// rather than wedging or storing a rounded id.
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["handled"])
		// The rounded value was never persisted, under any name.
		assert.Nil(outcome.State["orderID"])
	})

	t.Run("the string workaround survives the step boundary exactly", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "precisionflow.verify:428/g", map[string]any{"safe": true}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		// Read back in the NEXT step - i.e. after the value went through the database - digit for digit.
		assert.Equal("1234567890123456789", outcome.State["readBack"])
		assert.Equal(float64(3), outcome.State["qty"]) // an ordinary integer still reads as float64
	})

	t.Run("a task writing a NUL fails the step, routed to onError", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "precisionflow.verify:428/g", map[string]any{"nul": true}, nil)
		if !assert.NoError(err) {
			return
		}
		// The panic became a normal step failure and routed to the handler. Unguarded, this flow would sit
		// `running` forever on Postgres (SQLSTATE 22P05 on the completion UPDATE) while the task re-ran on
		// every lease expiry - so reaching a terminal status here IS the assertion.
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["handled"])
		assert.Nil(outcome.State["payload"])
	})

	t.Run("base64 is the workaround for binary payloads", func(t *testing.T) {
		assert := testarossa.For(t)

		raw := []byte{0x00, 0x01, 0xff} // a NUL and invalid UTF-8: unstorable as a string
		proxy.HandleTask("precisionflow.verify:428/writer", func(ctx context.Context, f *workflow.Flow) error {
			f.SetString("payload", base64.StdEncoding.EncodeToString(raw))
			return nil
		})
		defer proxy.HandleTask("precisionflow.verify:428/writer", func(ctx context.Context, f *workflow.Flow) error {
			return nil
		})

		_, outcome, err := eng.Run(ctx, "precisionflow.verify:428/g", map[string]any{}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		// Survived the database and came back byte-identical.
		decoded, derr := base64.StdEncoding.DecodeString(outcome.State["readBack"].(string))
		assert.NoError(derr)
		assert.Equal(raw, decoded)
	})

	t.Run("host-supplied initial state is rejected with a 400", func(t *testing.T) {
		assert := testarossa.For(t)

		_, err := eng.Create(ctx, "precisionflow.verify:428/g",
			map[string]any{"orderID": int64(snowflakeID)}, nil)
		if !assert.Error(err) {
			return
		}
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
		assert.Contains(err.Error(), "orderID")
	})

	t.Run("host-supplied baggage is rejected with a 400", func(t *testing.T) {
		assert := testarossa.For(t)

		_, err := eng.Create(ctx, "precisionflow.verify:428/g", map[string]any{},
			&workflow.FlowOptions{Baggage: map[string]any{"tenantID": int64(snowflakeID)}})
		if !assert.Error(err) {
			return
		}
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
		assert.Contains(err.Error(), "tenantID")
	})
}
