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
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestRetry_NegativeDelayFailsTheStep pins that a retry whose delay is negative fails the step. Retry refuses a
// negative initialDelay, so only a carrier decoded off the wire can carry one, and rewound with a not_before in
// the past the step would be re-dispatched at once on every attempt.
func TestRetry_NegativeDelayFailsTheStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := hosts.NewMux()
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	g := workflow.NewGraph("NegativeRetry")
	g.SetURL("Only", "negativeretry/only")
	g.AddTransition("Only", workflow.End)
	mux.HandleGraph("negativeretry/wf", g)
	var calls atomic.Int32
	mux.HandleTask("negativeretry/only", func(ctx context.Context, f *workflow.Flow) error {
		calls.Add(1)
		return overWire(f, func(wire map[string]json.RawMessage) {
			wire["retry"] = json.RawMessage(`true`)
			wire["backoffInitialDelay"] = json.RawMessage(`-1000000000`)
		})
	})

	_, outcome, err := e.Run(ctx, "negativeretry/wf", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "negative delay")
	}
	assert.Equal(int32(1), calls.Load())
}
