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

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestReason_TerminateAndCancelRefuseAnUnstorableReason pins the reason check: a reason over maxReasonBytes, or
// one that is not valid UTF-8 or holds a NUL, is a 400 from both Terminate and Cancel before anything is written,
// and a reason of exactly maxReasonBytes is recorded whole.
func TestReason_TerminateAndCancelRefuseAnUnstorableReason(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("Reason")
	g.SetURL("Wait", "reason/wait")
	g.AddTransition("Wait", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("reason/wf", g)
	mux.HandleTask("reason/wait", func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(nil, nil)
		return err
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	bad := map[string]string{
		"too_long":     strings.Repeat("r", maxReasonBytes+1),
		"nul":          "stop\x00now",
		"invalid_utf8": "stop\xffnow",
	}
	for _, op := range []string{"terminate", "cancel"} {
		fk, out, err := e.Run(ctx, "reason/wf", nil, nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, out.Status) {
			return
		}
		call := func(reason string) error {
			if op == "terminate" {
				return e.Terminate(ctx, fk, reason)
			}
			return e.Cancel(ctx, fk, reason)
		}
		for name, reason := range bad {
			err := call(reason)
			assert.Error(err, op, name)
			assert.Equal(http.StatusBadRequest, errors.StatusCode(err), op, name)
		}
		out, err = e.Snapshot(ctx, fk)
		assert.NoError(err)
		assert.Equal(workflow.StatusInterrupted, out.Status, "a refused reason must stop nothing", op)

		longest := strings.Repeat("é", maxReasonBytes/2)
		assert.NoError(call(longest), op)
		if op == "terminate" {
			out, err = e.Snapshot(ctx, fk)
			assert.NoError(err)
			assert.Equal(workflow.StatusTerminated, out.Status)
			assert.Equal(longest, out.TerminateReason, "a reason at the limit is recorded whole")
		} else {
			assert.NoError(e.Terminate(ctx, fk, ""))
		}
	}
}
