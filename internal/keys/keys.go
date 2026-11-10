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

// Package keys encodes and decodes the engine's composite flow and step keys. A flow key is
// "{shard}-{hour}-{flowID}-{token}" and a step key "{shard}-{hour}-{flowID}-{stepID}-{token}", with a 1-based
// shard: the shard routes the request, the hour is the flow's creation hour (hours since 2000-01-01 00:00 UTC,
// shared by the flow and every one of its steps), the ids are the per-shard sequential row ids - a step key names
// its flow too - and the random token is an unguessable write capability. CorrelationID derives the token-free,
// capability-free identifier that observability sinks must use.
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

// NewFlowKey composes a flow key from its parts: "{shard}-{hour}-{flowID}-{token}" with a 1-based shard. It is the
// inverse of ParseFlowKey - a key it builds parses back to the parts it was given.
func NewFlowKey(shardNum, hour, flowID int, token string) string {
	return strconv.Itoa(shardNum) + "-" + strconv.Itoa(hour) + "-" + strconv.Itoa(flowID) + "-" + token
}

// NewStepKey composes a step key from its parts: "{shard}-{hour}-{flowID}-{stepID}-{token}" with a 1-based shard,
// the hour and flow its flow's. It is the inverse of ParseStepKey.
func NewStepKey(shardNum, hour, flowID, stepID int, token string) string {
	return strconv.Itoa(shardNum) + "-" + strconv.Itoa(hour) + "-" + strconv.Itoa(flowID) + "-" + strconv.Itoa(stepID) +
		"-" + token
}

// ParseFlowKey extracts the shard, creation hour, numeric flow ID and flow token from a composite flow key.
// Format: "{shard}-{hour}-{flowID}-{token}" with a 1-based shard.
func ParseFlowKey(flowKey string) (shardNum int, hour int, flowID int, flowToken string, err error) {
	ids, token, err := parseKey(flowKey, 1, "invalid flow key")
	if err != nil {
		return 0, 0, 0, "", errors.Trace(err)
	}
	return ids[0], ids[1], ids[2], token, nil
}

// ParseStepKey extracts the shard, its flow's creation hour, the numeric flow and step IDs and the step token from a
// composite step key. Format: "{shard}-{hour}-{flowID}-{stepID}-{token}" with a 1-based shard.
func ParseStepKey(stepKey string) (shardNum int, hour int, flowID int, stepID int, stepToken string, err error) {
	ids, token, err := parseKey(stepKey, 2, "invalid step key")
	if err != nil {
		return 0, 0, 0, 0, "", errors.Trace(err)
	}
	return ids[0], ids[1], ids[2], ids[3], token, nil
}

// parseKey splits a "{shard}-{hour}-{id}...-{token}" key with idCount ids, and returns the shard, the hour and the ids
// in that order, then the token; invalid names the key kind in the 400 it returns. It accepts only the canonical
// spelling the constructors produce: ParseInt also takes a '+' or leading zeros, and a key spelled that way names the
// same row under a different string, which anything keying on the key - a parked Await - never matches.
func parseKey(key string, idCount int, invalid string) ([]int, string, error) {
	parts := strings.SplitN(key, "-", idCount+3)
	if len(parts) != idCount+3 {
		return nil, "", errors.New("%s", invalid, http.StatusBadRequest)
	}
	nums := make([]int, idCount+2)
	for i, part := range parts[:idCount+2] {
		n, ok := canonicalInt(part)
		switch {
		case !ok:
			return nil, "", errors.New("%s", invalid, http.StatusBadRequest)
		case i == 0 && n < 1:
			return nil, "", errors.New("%s", invalid, http.StatusBadRequest)
		case i == 1 && n > math.MaxInt32: // the hour columns are INT: a larger value names no row, and PostgreSQL refuses to bind it
			return nil, "", errors.New("%s", invalid, http.StatusBadRequest)
		}
		nums[i] = int(n)
	}
	token := parts[idCount+2]
	if !canonicalToken(token) {
		return nil, "", errors.New("%s", invalid, http.StatusBadRequest)
	}
	return nums, token, nil
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
// metrics): "{shard}-{hour}-{flowID}", the flow key with its random token segment omitted. It uniquely identifies
// the flow for correlation - {shard} disambiguates the per-shard sequential flow_id - but grants nothing. It is
// deliberately NOT a valid engine key: no operation accepts it, so a trace/log reader cannot escalate it into the
// flow's write capability. It is a prefix of the flow key, so a caller that can already list flows - and so already
// receives their keys - finds the flow by searching for it followed by "-" within its shard.
// Every place a flow identifier crosses into an observability sink must use this, never the token-bearing
// key (which belongs only on the task carrier and in-memory waiter matching).
func CorrelationID(shardNum, hour, flowID int) string {
	return strconv.Itoa(shardNum) + "-" + strconv.Itoa(hour) + "-" + strconv.Itoa(flowID)
}

// RandomIdentifier generates a random hex string of n characters (4n bits of entropy). It mints the unguessable
// token segment of flow and step keys.
func RandomIdentifier(n int) string {
	b := make([]byte, n/2+1)
	rand.Read(b)
	return hex.EncodeToString(b)[:n]
}
