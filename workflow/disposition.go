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

package workflow

import "errors"

// The engine has exactly two adaptive responses to a downstream that cannot take more work, and a host
// signals which one by wrapping the error its transport returned:
//
//   - ErrBackpressure  → the valve: the step is bounced back to pending and the task's adaptive dispatch
//     rate is cut (CUBIC recovery). For "you're going too fast" signals (e.g. HTTP 429).
//   - ErrBreakerTrip   → the breaker: the task's pending backlog is parked and probed on an exponential
//     schedule. For "I cannot serve right now" signals (downstream unreachable / unavailable / overloaded,
//     e.g. an ack timeout, HTTP 503, HTTP 529).
//
// An undecorated error is an ordinary task failure: the engine takes the task's onError transition if one
// matches, else fails the flow. The engine never inspects transport status codes or error text itself -
// the host owns that mapping and expresses the outcome through these wrappers, keeping the engine
// transport-agnostic. The wrapped error is preserved (Unwrap), so errors.Is/As and any status-code
// extraction still see through to it.

// dispositionKind is the engine's behavioral response. Unexported: the API surface is the two
// constructors and the two accessors; the kind never crosses the boundary.
type dispositionKind int

const (
	dispositionBackpressure dispositionKind = iota + 1
	dispositionBreakerTrip
)

// dispositionError wraps a transport error with an engine disposition and an opaque cause label.
type dispositionError struct {
	err   error
	kind  dispositionKind
	cause string
}

func (d *dispositionError) Error() string {
	if d.err != nil {
		return d.err.Error()
	}
	if d.kind == dispositionBackpressure {
		return "backpressure"
	}
	return "breaker trip"
}

func (d *dispositionError) Unwrap() error { return d.err }

// ErrBackpressure marks err as a backpressure signal: the downstream is rate-limiting this task. The
// engine bounces the step back to pending and cuts the task's adaptive dispatch rate rather than failing
// the flow. cause is an opaque label forwarded to logging/metrics (pass "" if none). A nil err is allowed
// (e.g. a host self-throttling with no underlying transport error).
func ErrBackpressure(err error, cause string) error {
	return &dispositionError{err: err, kind: dispositionBackpressure, cause: cause}
}

// ErrBreakerTrip marks err as an "I cannot serve right now" signal: the downstream is unreachable,
// unavailable, or overloaded. The engine trips the task's circuit breaker - parking its backlog and
// probing on an exponential schedule - rather than failing the flow. cause is an opaque label forwarded
// to the breaker trip/probe metrics (pass "" if none). A nil err is allowed.
func ErrBreakerTrip(err error, cause string) error {
	return &dispositionError{err: err, kind: dispositionBreakerTrip, cause: cause}
}

// IsBackpressure reports whether err (or anything it wraps) was marked by ErrBackpressure, returning the
// cause label.
func IsBackpressure(err error) (cause string, ok bool) {
	var d *dispositionError
	if errors.As(err, &d) && d.kind == dispositionBackpressure {
		return d.cause, true
	}
	return "", false
}

// IsBreakerTrip reports whether err (or anything it wraps) was marked by ErrBreakerTrip, returning the
// cause label.
func IsBreakerTrip(err error) (cause string, ok bool) {
	var d *dispositionError
	if errors.As(err, &d) && d.kind == dispositionBreakerTrip {
		return d.cause, true
	}
	return "", false
}
