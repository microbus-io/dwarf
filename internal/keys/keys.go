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
// "{shard}-{hour}-{id}-{token}" with a 1-based shard: the shard routes the request, the hour is the flow's creation
// hour (hours since 2000-01-01 00:00 UTC, shared by the flow and every one of its steps), the id is the per-shard
// sequential row id, and the random token is an unguessable write capability. CorrelationID derives the
// token-free, capability-free identifier that observability sinks must use.
package keys

import (
	"crypto/rand"
	"encoding/hex"
	"math"
	"net/http"
	"strconv"
	"strings"

	"github.com/microbus-io/errors"
)

// New composes a flow or step key from its parts: "{shard}-{hour}-{id}-{token}" with a 1-based shard. Flow and
// step keys share one format, so one constructor serves both, and it is the inverse of ParseFlowKey and
// ParseStepKey - a key it builds parses back to the parts it was given.
func New(shardNum, hour, id int, token string) string {
	return strconv.Itoa(shardNum) + "-" + strconv.Itoa(hour) + "-" + strconv.Itoa(id) + "-" + token
}

// ParseFlowKey extracts the shard, creation hour, numeric flow ID and flow token from a composite flow key.
// Format: "{shard}-{hour}-{flowID}-{token}" with a 1-based shard.
func ParseFlowKey(flowKey string) (shardNum int, hour int, flowID int, flowToken string, err error) {
	return parseKey(flowKey, "invalid flow key")
}

// ParseStepKey extracts the shard, creation hour, numeric step ID and step token from a composite step key.
// Format: "{shard}-{hour}-{stepID}-{token}" with a 1-based shard.
func ParseStepKey(stepKey string) (shardNum int, hour int, stepID int, stepToken string, err error) {
	return parseKey(stepKey, "invalid step key")
}

// parseKey splits a "{shard}-{hour}-{id}-{token}" key; invalid names the key kind in the 400 it returns. It accepts
// only the canonical spelling New produces: ParseInt also takes a '+' or leading zeros, and a key spelled that way
// names the same row under a different string, which anything keying on the key - a parked Await - never matches.
func parseKey(key, invalid string) (shardNum int, hour int, id int, token string, err error) {
	parts := strings.SplitN(key, "-", 4)
	if len(parts) != 4 {
		return 0, 0, 0, "", errors.New("%s", invalid, http.StatusBadRequest)
	}
	shardNum64, ok := canonicalInt(parts[0])
	if !ok || shardNum64 < 1 {
		return 0, 0, 0, "", errors.New("%s", invalid, http.StatusBadRequest)
	}
	hour64, ok := canonicalInt(parts[1])
	if !ok || hour64 > math.MaxInt32 { // the hour columns are INT: a larger value names no row, and PostgreSQL refuses to bind it
		return 0, 0, 0, "", errors.New("%s", invalid, http.StatusBadRequest)
	}
	id64, ok := canonicalInt(parts[2])
	if !ok {
		return 0, 0, 0, "", errors.New("%s", invalid, http.StatusBadRequest)
	}
	if !canonicalToken(parts[3]) {
		return 0, 0, 0, "", errors.New("%s", invalid, http.StatusBadRequest)
	}
	return int(shardNum64), int(hour64), int(id64), parts[3], nil
}

// canonicalToken reports whether s is spelled as RandomIdentifier(16) spells a token: 16 lowercase hex digits. A
// database comparing case-insensitively, or ignoring trailing spaces (MySQL's and SQL Server's default collations),
// matches the token's row under another spelling too, which anything keying on the key - a parked Await - never
// matches.
func canonicalToken(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// canonicalInt parses s as a base-10 integer spelled exactly as strconv formats it. A segment never holds a sign:
// '-' is the separator, and a '+' is a second spelling.
func canonicalInt(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || strconv.FormatInt(n, 10) != s {
		return 0, false
	}
	return n, true
}

// CorrelationID is the non-secret, capability-free flow identifier for telemetry (spans, logs,
// metrics): the flowKey with its creation hour and its random token segment omitted. It uniquely identifies the flow for
// correlation - {shard} disambiguates the per-shard sequential flow_id - but grants nothing. It is
// deliberately NOT a valid engine key: no operation accepts it, and the engine offers no
// correlationID->key lookup, so a trace/log reader cannot escalate it into the flow's write capability.
// Every place a flow identifier crosses into an observability sink must use this, never the token-bearing
// key (which belongs only on the task carrier and in-memory waiter matching).
func CorrelationID(shardNum, flowID int) string {
	return strconv.Itoa(shardNum) + "-" + strconv.Itoa(flowID)
}

// RandomIdentifier generates a random hex string of n characters (4n bits of entropy). It mints the unguessable
// token segment of flow and step keys.
func RandomIdentifier(n int) string {
	b := make([]byte, n/2+1)
	rand.Read(b)
	return hex.EncodeToString(b)[:n]
}
