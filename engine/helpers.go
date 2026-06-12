/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/microbus-io/errors"
)

// parseFlowKey extracts the shard, numeric flow ID and flow token from a composite flow key.
// Format: "{shard}-{flowID}-{token}" with a 1-based shard.
func parseFlowKey(flowKey string) (shardNum int, flowID int, flowToken string, err error) {
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

// parseStepKey extracts the shard, numeric step ID and step token from a composite step key.
// Format: "{shard}-{stepID}-{token}" with a 1-based shard.
func parseStepKey(stepKey string) (shardNum int, stepID int, stepToken string, err error) {
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

// unmarshalJSONMap parses a JSON string into a map. Empty or "{}" input yields a nil map.
func unmarshalJSONMap(jsonStr string, out *map[string]any) {
	if jsonStr == "" || jsonStr == "{}" {
		return
	}
	json.Unmarshal([]byte(jsonStr), out)
}

// randomIdentifier generates a random hex string of the given byte length.
func randomIdentifier(n int) string {
	b := make([]byte, n/2+1)
	rand.Read(b)
	return hex.EncodeToString(b)[:n]
}
