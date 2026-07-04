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

// Package keys encodes and decodes the engine's composite flow and step keys. A key is
// "{shard}-{id}-{token}" with a 1-based shard: the shard routes the request, the id is the per-shard
// sequential row id, and the random token is an unguessable write capability. CorrelationID derives the
// token-free, capability-free identifier that observability sinks must use.
package keys

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	"github.com/microbus-io/errors"
)

// ParseFlowKey extracts the shard, numeric flow ID and flow token from a composite flow key.
// Format: "{shard}-{flowID}-{token}" with a 1-based shard.
func ParseFlowKey(flowKey string) (shardNum int, flowID int, flowToken string, err error) {
	parts := strings.SplitN(flowKey, "-", 3)
	if len(parts) != 3 {
		return 0, 0, "", errors.New("invalid flow ID", http.StatusBadRequest)
	}
	shardNum64, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || shardNum64 < 1 {
		return 0, 0, "", errors.New("invalid flow ID", http.StatusBadRequest)
	}
	flowID64, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, "", errors.New("invalid flow ID", http.StatusBadRequest)
	}
	return int(shardNum64), int(flowID64), parts[2], nil
}

// ParseStepKey extracts the shard, numeric step ID and step token from a composite step key.
// Format: "{shard}-{stepID}-{token}" with a 1-based shard.
func ParseStepKey(stepKey string) (shardNum int, stepID int, stepToken string, err error) {
	parts := strings.SplitN(stepKey, "-", 3)
	if len(parts) != 3 {
		return 0, 0, "", errors.New("invalid step key", http.StatusBadRequest)
	}
	shardNum64, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || shardNum64 < 1 {
		return 0, 0, "", errors.New("invalid step key", http.StatusBadRequest)
	}
	stepID64, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, "", errors.New("invalid step key", http.StatusBadRequest)
	}
	return int(shardNum64), int(stepID64), parts[2], nil
}

// CorrelationID is the non-secret, capability-free flow identifier for telemetry (spans, logs,
// metrics): the flowKey with its random token segment omitted. It uniquely identifies the flow for
// correlation - {shard} disambiguates the per-shard sequential flow_id - but grants nothing. It is
// deliberately NOT a valid engine key: no operation accepts it, and the engine offers no
// correlationID->key lookup, so a trace/log reader cannot escalate it into the flow's write capability.
// Every place a flow identifier crosses into an observability sink must use this, never the token-bearing
// key (which belongs only on the task carrier and in-memory waiter matching).
func CorrelationID(shardNum, flowID int) string {
	return strconv.Itoa(shardNum) + "-" + strconv.Itoa(flowID)
}

// RandomIdentifier generates a random hex string of the given byte length. It mints the unguessable
// token segment of flow and step keys.
func RandomIdentifier(n int) string {
	b := make([]byte, n/2+1)
	rand.Read(b)
	return hex.EncodeToString(b)[:n]
}
