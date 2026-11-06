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
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSubgraphError_ReachesTheCallerWhole pins the subgraph_error column: a failed child's error reaches the
// caller's flow.Subgraph with its status code and properties, its message made storable (a NUL in it would
// otherwise be refused by a JSONB column inside the child's terminating transaction), and a caller retried after
// it sees no error on the re-run child. When the properties make the JSON too big to store, the caller still
// gets the message and status code.
func TestSubgraphError_ReachesTheCallerWhole(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	run := func(t *testing.T, detail string) error {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		parent := workflow.NewGraph("Parent")
		parent.SetURL("Call", "sge/call")
		parent.AddTransition("Call", workflow.End)
		assert.NoError(parent.Validate())
		mux.HandleGraph("sge/parent", parent)
		child := workflow.NewGraph("Child")
		child.SetURL("Fail", "sge/fail")
		child.AddTransition("Fail", workflow.End)
		assert.NoError(child.Validate())
		mux.HandleGraph("sge/child", child)

		var got error
		mux.HandleTask("sge/call", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph("sge/child", nil, nil)
			if yield {
				return nil
			}
			if err != nil && f.Attempt() == 0 {
				got = err
				// The retry's re-run child succeeds, and the caller must see no error from the cleared column.
				if f.Retry(time.Millisecond, 1, 0, 0) {
					return nil
				}
			}
			return err
		})
		mux.HandleTask("sge/fail", func(ctx context.Context, f *workflow.Flow) error {
			if f.Attempt() > 0 || got != nil {
				return nil
			}
			return errors.New("bad\x00thing", http.StatusTeapot, "detail", detail)
		})

		e := NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		e.SetHost(mux)
		e.maxPayloadBytes = 1000
		assert.Must(assert.NoError(e.Startup(t.Context())))

		runCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
		defer cancel()
		_, outcome, err := e.Run(runCtx, "sge/parent", nil, nil)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
		}
		return got
	}

	t.Run("properties_kept", func(t *testing.T) {
		assert := testarossa.For(t)
		got := run(t, "small")
		if assert.Error(got) {
			assert.Equal("bad�thing", got.Error())
			assert.Equal(http.StatusTeapot, errors.StatusCode(got))
			assert.Equal("small", errors.Convert(got).Properties["detail"])
		}
	})

	t.Run("unstorable_properties_dropped", func(t *testing.T) {
		assert := testarossa.For(t)
		got := run(t, strings.Repeat("d", 2000))
		if assert.Error(got) {
			assert.Equal("bad�thing", got.Error())
			assert.Equal(http.StatusTeapot, errors.StatusCode(got))
			_, has := errors.Convert(got).Properties["detail"]
			assert.False(has, "properties too big to store are dropped")
		}
	})
}
