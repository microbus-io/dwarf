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
	"unicode/utf8"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestSanitizeErrorMessage pins what makes a message storable: control bytes and invalid UTF-8 replaced, and a
// message over maxErrorBytes cut on a rune boundary to exactly the limit or less, ending with the mark.
func TestSanitizeErrorMessage(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	assert.Equal("plain\nmessage\twith tabs", sanitizeErrorMessage("plain\nmessage\twith tabs"))
	assert.Equal("nul�here", sanitizeErrorMessage("nul\x00here"))
	assert.Equal("bad�byte", sanitizeErrorMessage("bad\xffbyte"))

	exact := strings.Repeat("x", maxErrorBytes)
	assert.Equal(exact, sanitizeErrorMessage(exact), "a message at the limit is kept whole")

	long := sanitizeErrorMessage(strings.Repeat("x", 100_000))
	assert.Len(long, maxErrorBytes)
	assert.True(strings.HasSuffix(long, truncatedMark))

	// Two-byte runes put the cut point mid-rune half the time; it must back off to a boundary.
	for _, prefix := range []string{"", "x"} {
		wide := sanitizeErrorMessage(prefix + strings.Repeat("é", 50_000))
		assert.True(len(wide) <= maxErrorBytes)
		assert.True(utf8.ValidString(wide), "a cut must not split a rune")
		assert.True(strings.HasSuffix(wide, truncatedMark))
	}
}

// TestErrorText_OversizedTaskErrorFailsTheStepOnce pins the end to end: a task error far over every text column's
// size fails its flow with the message cut to maxErrorBytes, and the task runs once. On MySQL, whose TEXT holds
// 65,535 bytes, an uncut message is refused by failStep itself, which the classifier reads as the database being
// down - the step is left for lease recovery and the task re-runs.
func TestErrorText_OversizedTaskErrorFailsTheStepOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("ErrorText")
	g.SetURL("Fail", "errortext/fail")
	g.AddTransition("Fail", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("errortext/wf", g)
	mux.HandleTask("errortext/fail", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		return errors.New("%s", strings.Repeat("e", 100_000))
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	assert.NoError(e.Startup(t.Context()))

	// Bounded, so a step left for lease recovery fails the test rather than hanging it.
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	fk, outcome, err := e.Run(runCtx, "errortext/wf", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.Len(outcome.Error, maxErrorBytes)
	assert.True(strings.HasSuffix(outcome.Error, truncatedMark))
	assert.Equal(int32(1), runs.Load(), "the task must run exactly once")

	steps, err := e.History(ctx, fk)
	if assert.NoError(err) && assert.Len(steps, 1) {
		assert.Equal(outcome.Error, steps[0].Error, "the step and the flow carry the same cut message")
	}
}
