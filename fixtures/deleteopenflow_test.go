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

package fixtures

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestDeleteOpenflow pins that Delete and Purge remove only closed flows. Delete of a running or an interrupted
// flow is a 409 and leaves it as it was - the interrupted one still resumes to completion - while Purge over a mix
// marks only the closed flow, and a Purge whose status filter names an open status is a 400. Nothing is
// terminated on the caller's behalf: an open flow is the caller's to Terminate first.
func TestDeleteOpenflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	graph := workflow.NewGraph("Open")
	graph.SetURL("Work", "deleteopenflow.verify:428/work")
	graph.AddTransition("Work", workflow.End)
	mux.HandleGraph("deleteopenflow.verify:428/g", graph)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux.HandleTask("deleteopenflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		switch f.GetString("mode") {
		case "block":
			started <- struct{}{}
			<-release
		case "park":
			if yield, err := f.Interrupt(nil, nil); yield || err != nil {
				return err
			}
		}
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(release) }) }
	defer releaseOnce() // declared after the Shutdown defer, so it unwinds first and the drain is not left waiting
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	parked, outcome, err := eng.Run(ctx, "deleteopenflow.verify:428/g", map[string]any{"mode": "park"}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusInterrupted, outcome.Status)
	running, err := eng.Create(ctx, "deleteopenflow.verify:428/g", map[string]any{"mode": "block"}, nil)
	assert.NoError(err)
	select {
	case <-started:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "task never started")
		return
	}
	// The closed flow is created last, after the open ones, so a Purge reaching the live tables would meet them first.
	ended, outcome, err := eng.Run(ctx, "deleteopenflow.verify:428/g", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	statusIs := func(op string, code int, err error) {
		if assert.Error(err, op) {
			assert.Equal(code, errors.StatusCode(err), "%s: %v", op, err)
		}
	}
	statusIs("Delete of a running flow", http.StatusConflict, eng.Delete(ctx, running))
	statusIs("Delete of an interrupted flow", http.StatusConflict, eng.Delete(ctx, parked))
	for _, status := range []string{workflow.StatusRunning, workflow.StatusInterrupted} {
		_, _, err := eng.Purge(ctx, enginetest.Recent(workflow.Query{Status: status}))
		statusIs("Purge by status "+status, http.StatusBadRequest, err)
	}

	// A Purge matching all three removes only the closed flow, once archived, and only closed flows count against its
	// limit.
	enginetest.ArchiveNow(ctx, eng)
	marked, _, err := eng.Purge(ctx, enginetest.Recent(workflow.Query{WorkflowURL: "deleteopenflow.verify:428/g", Limit: 1}))
	assert.NoError(err)
	assert.Equal(1, marked)
	_, err = eng.History(ctx, ended)
	statusIs("History of the purged flow", http.StatusNotFound, err)

	// Neither open flow was touched: the running one finishes, the interrupted one resumes to completion.
	releaseOnce()
	outcome, err = eng.Await(ctx, running)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
	assert.NoError(eng.Resume(ctx, parked, nil))
	outcome, err = eng.Await(ctx, parked)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}

	// Once ended, Delete takes it.
	assert.NoError(eng.Delete(ctx, parked))
	_, err = eng.History(ctx, parked)
	statusIs("History of the deleted flow", http.StatusNotFound, err)
}
