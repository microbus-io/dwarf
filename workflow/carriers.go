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

import (
	"encoding/json"
	"math"
	"net/http"
	"time"

	"github.com/microbus-io/errors"
)

// CarrierVersion is the newest version of the Flow's JSON format that this build reads and writes. Every
// encoding of a Flow carries its version in the "v" field. A Flow encodes in the version it was decoded with,
// or in CarrierVersion if it was never decoded, so a task server replies in the version its request came in
// and an older host can read the reply.
//
// Decoding fails with status 400 when "v" is missing (the sender predates the versioned format) or is not a
// positive integer, and with status 501, naming both versions, when it is newer than CarrierVersion. An older
// version this build still reads is accepted.
const CarrierVersion = 1

// Carrier is one direction of a Flow's JSON format, for carrying a task across a process boundary. It
// encodes only that direction's fields and, when decoding, applies only those fields to its Flow and ignores
// the rest of the body. Pass it to the json package wherever a value goes:
//
//	body, err := json.Marshal(workflow.RequestCarrier(flow))             // host: send the request
//	err = json.NewDecoder(r.Body).Decode(workflow.RequestCarrier(&flow)) // task server: read it
//	err = json.NewEncoder(w).Encode(workflow.ResultCarrier(&flow))       // task server: send the result
//	err = json.Unmarshal(respBody, workflow.ResultCarrier(flow))         // host: read it onto the flow
//
// Both are optional: a Flow's own MarshalJSON and UnmarshalJSON carry every field in both directions, and the
// engine reads back only the task's changes and control signals either way. The carriers leave the request's
// state and reducers out of the result, which can halve what crosses the wire for a large state, and keep a
// reply from writing anything onto the host's Flow but the task's changes and control signals.
type Carrier interface {
	json.Marshaler
	json.Unmarshaler
}

// RequestCarrier returns the request direction of f's JSON format, from host to task server: the state, the
// graph's reducers, the changes of the step's earlier runs, the flow and step keys, the timestamps, the attempt,
// whether the step runs in a fan-out branch, and how an earlier Interrupt or Subgraph was resolved. It carries
// no control signals. Decoding sets those fields on f and clears its control signals, so a request cannot arm
// one.
func RequestCarrier(f *Flow) Carrier {
	return &requestCarrier{f: f}
}

// ResultCarrier returns the result direction of f's JSON format, from task server to host: the step's changes
// and its control signals (Goto, Retry and its backoff, Sleep, Interrupt and its payload, Subgraph and its
// input). Decoding sets those fields on f and leaves everything else it holds as it was.
func ResultCarrier(f *Flow) Carrier {
	return &resultCarrier{f: f}
}

// flowJSON is the wire format for Flow. Every field but V is omitted when zero, so a direction's encoding is
// this struct with the other direction's fields left zero. Durations go out as whole milliseconds under names
// that say so: the engine schedules at millisecond resolution, and a reader in any language can take an
// integer at its word. A sleep is truncated, as the engine truncates it; a backoff delay is rounded up, so a
// positive one never arrives as 0.
type flowJSON struct {
	V                         *int               `json:"v,omitempty"`
	FlowKey                   string             `json:"flowKey,omitzero"`
	StepKey                   string             `json:"stepKey,omitzero"`
	State                     State              `json:"state,omitzero"`
	Reducers                  map[string]Reducer `json:"reducers,omitzero"`
	Changes                   Changes            `json:"changes,omitzero"`
	Goto                      string             `json:"goto,omitzero"`
	Retry                     bool               `json:"retry,omitzero"`
	SleepMillis               int64              `json:"sleepMillis,omitzero"`
	Interrupt                 bool               `json:"interrupt,omitzero"`
	InterruptPayload          State              `json:"interruptPayload,omitzero"`
	SubgraphURL               string             `json:"subgraphURL,omitzero"`
	SubgraphInput             State              `json:"subgraphInput,omitzero"`
	InterruptDone             bool               `json:"interruptDone,omitzero"`
	ResumeData                State              `json:"resumeData,omitzero"`
	SubgraphDone              bool               `json:"subgraphDone,omitzero"`
	SubgraphResult            State              `json:"subgraphResult,omitzero"`
	SubgraphError             string             `json:"subgraphError,omitzero"`
	Attempt                   int                `json:"attempt,omitzero"`
	BackoffInitialDelayMillis int64              `json:"backoffInitialDelayMillis,omitzero"`
	BackoffDelayMultiplier    float64            `json:"backoffDelayMultiplier,omitzero"`
	BackoffMaxDelayMillis     int64              `json:"backoffMaxDelayMillis,omitzero"`
	CreatedAt                 time.Time          `json:"createdAt,omitzero"`
	UpdatedAt                 time.Time          `json:"updatedAt,omitzero"`
	StepCreatedAt             time.Time          `json:"stepCreatedAt,omitzero"`
	InBranch                  bool               `json:"inBranch,omitzero"`
}

// MarshalJSON encodes every field of the Flow, both directions, in the version it was decoded with (or
// CarrierVersion). A host or task server may use it as the wire format; RequestCarrier and ResultCarrier
// encode one direction each.
func (f *Flow) MarshalJSON() ([]byte, error) {
	w := f.versioned()
	f.putRequest(&w)
	f.putResult(&w)
	return json.Marshal(w)
}

// UnmarshalJSON decodes every field of the Flow, both directions, replacing what it held. It fails on a body
// whose "v" is missing, malformed or newer than CarrierVersion; see CarrierVersion.
func (f *Flow) UnmarshalJSON(data []byte) error {
	w, v, err := decodeCarrier(data)
	if err != nil {
		return err
	}
	f.takeRequest(&w)
	f.takeResult(&w)
	f.carrierVersion = v
	return nil
}

type requestCarrier struct{ f *Flow }

func (c *requestCarrier) MarshalJSON() ([]byte, error) {
	w := c.f.versioned()
	c.f.putRequest(&w)
	return json.Marshal(w)
}

func (c *requestCarrier) UnmarshalJSON(data []byte) error {
	w, v, err := decodeCarrier(data)
	if err != nil {
		return err
	}
	c.f.takeRequest(&w)
	c.f.takeResult(&flowJSON{Changes: c.f.changes}) // no control signals: clear any the Flow held
	c.f.carrierVersion = v
	return nil
}

type resultCarrier struct{ f *Flow }

func (c *resultCarrier) MarshalJSON() ([]byte, error) {
	w := c.f.versioned()
	c.f.putResult(&w)
	return json.Marshal(w)
}

// UnmarshalJSON applies the result onto the Flow the host sent. Its version is not recorded: the host's Flow
// was made by the engine, and stays in the newest format.
func (c *resultCarrier) UnmarshalJSON(data []byte) error {
	w, _, err := decodeCarrier(data)
	if err != nil {
		return err
	}
	c.f.takeResult(&w)
	return nil
}

// versioned starts a wire value at the version f encodes in.
func (f *Flow) versioned() flowJSON {
	v := f.carrierVersion
	if v == 0 {
		v = CarrierVersion
	}
	return flowJSON{V: &v}
}

// putRequest copies the request direction's fields into w.
func (f *Flow) putRequest(w *flowJSON) {
	w.FlowKey = f.flowKey
	w.StepKey = f.stepKey
	w.State = f.state
	w.Reducers = f.reducers
	w.Changes = f.changes
	w.InterruptDone = f.interruptDone
	w.ResumeData = f.resumeData
	w.SubgraphDone = f.subgraphDone
	w.SubgraphResult = f.subgraphResult
	w.SubgraphError = f.subgraphError
	w.Attempt = f.attempt
	w.CreatedAt = f.createdAt
	w.UpdatedAt = f.updatedAt
	w.StepCreatedAt = f.stepCreatedAt
	w.InBranch = f.inBranch
}

// putResult copies the result direction's fields into w: the changes and the control signals.
func (f *Flow) putResult(w *flowJSON) {
	w.Changes = f.changes
	w.Goto = f.gotoNext
	w.Retry = f.retry
	w.SleepMillis = f.sleepDuration.Milliseconds()
	w.Interrupt = f.interrupt
	w.InterruptPayload = f.interruptPayload
	w.SubgraphURL = f.subgraphURL
	w.SubgraphInput = f.subgraphInput
	w.BackoffInitialDelayMillis = millisRoundedUp(f.backoffInitialDelay)
	w.BackoffDelayMultiplier = f.backoffDelayMultiplier
	w.BackoffMaxDelayMillis = millisRoundedUp(f.backoffMaxDelay)
}

// takeRequest sets the request direction's fields from w.
func (f *Flow) takeRequest(w *flowJSON) {
	f.flowKey = w.FlowKey
	f.stepKey = w.StepKey
	f.state = w.State
	f.changes = w.Changes
	f.ready() // either may be absent from the wire
	f.reducers = w.Reducers
	f.interruptDone = w.InterruptDone
	f.resumeData = w.ResumeData
	f.subgraphDone = w.SubgraphDone
	f.subgraphResult = w.SubgraphResult
	f.subgraphError = w.SubgraphError
	f.attempt = w.Attempt
	f.createdAt = w.CreatedAt
	f.updatedAt = w.UpdatedAt
	f.stepCreatedAt = w.StepCreatedAt
	f.inBranch = w.InBranch
}

// takeResult sets the changes and the control signals from w.
func (f *Flow) takeResult(w *flowJSON) {
	f.changes = w.Changes
	f.ready()
	f.gotoNext = w.Goto
	f.retry = w.Retry
	f.sleepDuration = durationOfMillis(w.SleepMillis)
	f.interrupt = w.Interrupt
	f.interruptPayload = w.InterruptPayload
	f.subgraphURL = w.SubgraphURL
	f.subgraphInput = w.SubgraphInput
	f.backoffInitialDelay = durationOfMillis(w.BackoffInitialDelayMillis)
	f.backoffDelayMultiplier = w.BackoffDelayMultiplier
	f.backoffMaxDelay = durationOfMillis(w.BackoffMaxDelayMillis)
}

// decodeCarrier decodes a body of either direction and checks its version, which it returns.
func decodeCarrier(data []byte) (w flowJSON, v int, err error) {
	if err := json.Unmarshal(data, &w); err != nil {
		return w, 0, errors.New("malformed flow carrier: %v", err, http.StatusBadRequest)
	}
	switch {
	case w.V == nil:
		return w, 0, errors.New("flow carrier has no version (v): its sender predates carrier v1 and must be upgraded", http.StatusBadRequest)
	case *w.V < 1:
		return w, 0, errors.New("flow carrier version %d is not a positive integer", *w.V, http.StatusBadRequest)
	case *w.V > CarrierVersion:
		return w, 0, errors.New("flow carrier v%d is newer than this build reads (v%d): upgrade dwarf on this side", *w.V, CarrierVersion, http.StatusNotImplemented)
	}
	return w, *w.V, nil
}

// millisRoundedUp converts a duration to whole milliseconds, rounding a positive one up, for a value whose zero
// means something else: a backoff base (the engine truncates the delay it computes for an attempt, not the base, and
// a base truncated to 0 is grown by no multiplier, so the retry would run back to back) and FlowOptions.TimeBudget
// (whose zero is the engine's default).
func millisRoundedUp(d time.Duration) int64 {
	ms := d.Milliseconds()
	if d > 0 && d%time.Millisecond != 0 {
		ms++
	}
	return ms
}

// durationOfMillis converts whole milliseconds off the wire to a Duration, saturating at the Duration's range
// rather than overflowing.
func durationOfMillis(ms int64) time.Duration {
	const limit = math.MaxInt64 / int64(time.Millisecond)
	switch {
	case ms > limit:
		return math.MaxInt64
	case ms < -limit:
		return math.MinInt64
	}
	return time.Duration(ms) * time.Millisecond
}
