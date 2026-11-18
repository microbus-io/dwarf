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

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// heldShutdown starts e, holds a Create in a LoadGraph that ignores its ctx, and calls Shutdown, which then waits for
// that call. It returns once the Shutdown is under way, with the channels the Shutdown and the Create report on; the
// host's Release lets both finish.
func heldShutdown(t *testing.T, e *Engine, host *heldGraphHost) (stopped chan error, created chan error) {
	assert := testarossa.For(t)
	ctx := context.Background()
	e.SetHost(host)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	created = make(chan error, 1)
	go func() {
		_, err := e.Create(ctx, host.url, nil, nil)
		created <- err
	}()
	select {
	case <-host.entered:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Create never reached LoadGraph"))
	}
	stopped = make(chan error, 1)
	go func() {
		stopped <- e.Shutdown(ctx)
	}()
	select {
	case <-host.cancelled:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Shutdown never cancelled the call it admitted"))
	}
	return stopped, created
}

// receive waits for ch, failing the test if it does not report within the hang ceiling.
func receive(t *testing.T, ch chan error, what string) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert := testarossa.For(t)
		assert.Must(assert.True(false, "%s never returned", what))
		return nil
	}
}

// TestLifecycle_StartupDuringShutdownWaitsForIt pins that a Startup called while a Shutdown is under way waits for the
// Shutdown to finish rather than re-initializing the runtime the Shutdown is stopping, and then starts the engine.
func TestLifecycle_StartupDuringShutdownWaitsForIt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	host := newHeldGraphHost(ingressSoloMux("lifecycle.restart"), "lifecycle.restart/held", false)
	e := NewEngineUnderTest(t.Name())
	// Not once the test has failed: a Startup run inside the Shutdown leaves that Shutdown's drain waiting forever on
	// the runtime it re-initialized, and a Shutdown here would wait behind it, turning the failure into a hang.
	defer func() {
		if !t.Failed() {
			e.Shutdown(ctx)
		}
	}()
	defer host.Release() // below the Shutdown defer, so it unwinds first: a held LoadGraph would hold the drain
	stopped, created := heldShutdown(t, e, host)

	restarted := make(chan error, 1)
	go func() {
		restarted <- e.Startup(t.Context())
	}()
	// A Startup that did not wait would run at once, against the runtime the Shutdown has yet to drain.
	select {
	case err := <-restarted:
		assert.Must(assert.True(false, "Startup ran while a Shutdown was under way: %v", err))
	case <-time.After(200 * time.Millisecond):
	}

	host.Release()
	assert.NoError(receive(t, stopped, "Shutdown"))
	assert.Must(assert.NoError(receive(t, restarted, "Startup")))
	assert.True(e.started.Load())
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	_, outcome, err := e.Run(runCtx, "lifecycle.restart/g", nil, nil)
	if assert.NoError(err) {
		assert.Equal("completed", outcome.Status)
	}
	assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(receive(t, created, "Create")))
}

// TestLifecycle_ShutdownDuringShutdownWaitsForIt pins that a Shutdown called while another is under way returns only
// once the engine has stopped, not at once because the first has already flipped started.
func TestLifecycle_ShutdownDuringShutdownWaitsForIt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	host := newHeldGraphHost(ingressSoloMux("lifecycle.twice"), "lifecycle.twice/held", false)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer host.Release() // below the Shutdown defer, so it unwinds first: a held LoadGraph would hold the drain
	stopped, created := heldShutdown(t, e, host)

	stoppedAgain := make(chan error, 1)
	go func() {
		stoppedAgain <- e.Shutdown(ctx)
	}()
	select {
	case <-stoppedAgain:
		assert.Must(assert.True(false, "a second Shutdown returned while the first was still under way"))
	case <-time.After(200 * time.Millisecond):
	}

	host.Release()
	assert.NoError(receive(t, stopped, "the first Shutdown"))
	assert.NoError(receive(t, stoppedAgain, "the second Shutdown"))
	assert.False(e.started.Load())
	assert.Equal(0, e.db.NumShards(), "the shards are still open")
	assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(receive(t, created, "Create")))
}

// TestLifecycle_ShutdownDuringAFailingStartupWaitsForIt pins that a Shutdown called while Startup is under way waits
// for it. Held just after the engine reports itself started - before the crew is built, and with initRuntime set to
// fail there - a Shutdown that did not wait would take the engine over from the Startup and drain a runtime that was
// never finished; waiting, it finds the failed Startup's engine stopped, and the engine starts again cleanly.
func TestLifecycle_ShutdownDuringAFailingStartupWaitsForIt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(ingressSoloMux("lifecycle.failing"))
	seams := e.diags.Seams()
	seams.Break(diagnostics.CheckpointRuntimeStarted)
	defer seams.Resume(diagnostics.CheckpointRuntimeStarted)
	held := seams.Waiter(diagnostics.CheckpointRuntimeStarted)
	seams.Inject(diagnostics.FaultInitRuntime)

	started := make(chan error, 1)
	go func() {
		started <- e.Startup(t.Context())
	}()
	select {
	case <-held:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.Must(assert.True(false, "Startup never reached its runtime"))
	}
	stopped := make(chan error, 1)
	go func() {
		stopped <- e.Shutdown(ctx)
	}()
	select {
	case err := <-stopped:
		assert.Must(assert.True(false, "Shutdown ran while Startup was under way: %v", err))
	case <-time.After(200 * time.Millisecond):
	}

	seams.Resume(diagnostics.CheckpointRuntimeStarted)
	assert.Error(receive(t, started, "Startup"), "the injected fault")
	assert.NoError(receive(t, stopped, "Shutdown"))
	assert.False(e.started.Load())
	assert.Equal(0, e.db.NumShards(), "the shards are still open")
	assertRestartServes(t, e, "lifecycle.failing/g")
}
