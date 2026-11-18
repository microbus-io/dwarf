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

	"github.com/microbus-io/errors"
)

// errIngressShutdown is the cause closeIngress cancels an admitted call's context with, which is how release tells
// that cancellation from the caller's own.
var errIngressShutdown = errors.New("engine is shutting down")

// admitIngress admits one call to an exported operation, or refuses it with a 503 when the engine is not running -
// never started, or shut down. The call runs on ingressCtx, which Shutdown cancels, and defers release over its named
// error result. op names the call in dwarf_ingress_operations; an empty op records nothing.
//
// The check and the count-in share ingressMu with closeIngress. Checked outside it, a call could pass the check while
// closeIngress finds nothing in flight, and run on into the next Startup's state.
func (e *Engine) admitIngress(ctx context.Context, op string) (ingressCtx context.Context, release func(opErr *error), err error) {
	ingressCtx, cancel := context.WithCancelCause(ctx)
	e.ingressMu.Lock()
	if !e.started.Load() {
		e.ingressMu.Unlock()
		cancel(nil)
		return nil, nil, errors.New("engine is not started", http.StatusServiceUnavailable)
	}
	id := e.ingressNext
	e.ingressNext++
	e.ingress[id] = cancel
	e.ingressMu.Unlock()
	release = func(opErr *error) {
		// Counted out in a defer, so a panic in the host's meter cannot leave Shutdown waiting for the call forever.
		defer func() {
			e.ingressMu.Lock()
			delete(e.ingress, id)
			if len(e.ingress) == 0 {
				e.ingressDrained.Broadcast()
			}
			e.ingressMu.Unlock()
			cancel(nil)
		}()
		if *opErr != nil {
			if shutErr := shutdownErr(ingressCtx); shutErr != nil {
				*opErr = shutErr
			}
		}
		// Before the call counts out: past that, a Startup may be assigning e.metrics.
		if op != "" {
			e.metricIngressOperation(ctx, op, opErr)
		}
	}
	return ingressCtx, release, nil
}

// closeIngress stops admitting calls, cancels every call admitted, and waits for each to return. It reports whether it
// stopped a running engine.
func (e *Engine) closeIngress() bool {
	e.ingressMu.Lock()
	if !e.started.CompareAndSwap(true, false) {
		e.ingressMu.Unlock()
		return false
	}
	cancels := make([]context.CancelCauseFunc, 0, len(e.ingress))
	for _, cancel := range e.ingress {
		cancels = append(cancels, cancel)
	}
	e.ingressMu.Unlock()
	// Outside the lock: a cancel detaches the call's context from the caller's, which runs the caller's own context
	// code when it is not a standard one - code that may call the engine.
	for _, cancel := range cancels {
		cancel(errIngressShutdown)
	}
	e.ingressMu.Lock()
	for len(e.ingress) > 0 {
		e.ingressDrained.Wait()
	}
	e.ingressMu.Unlock()
	return true
}

// shutdownErr is the 503 a call answers when Shutdown ended it, and nil when ctx was not ended by Shutdown. It reads
// the context's cause, not the call's error: what a cancelled statement or wait returns varies.
func shutdownErr(ctx context.Context) error {
	if !errors.Is(context.Cause(ctx), errIngressShutdown) {
		return nil
	}
	return errors.New("engine is shutting down", http.StatusServiceUnavailable)
}
