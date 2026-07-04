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

package keys

import (
	"fmt"
	"strings"
	"testing"
)

// FuzzParseFlowKey asserts ParseFlowKey never panics on arbitrary input, and that any accepted
// key satisfies the format invariants and round-trips: re-encoding the parsed parts reproduces
// the original string. The parser is the first thing every public engine operation runs on
// caller-controlled input, so it must be total.
func FuzzParseFlowKey(f *testing.F) {
	f.Add("1-42-a1b2c3d4e5f60718")
	f.Add("2-0-")
	f.Add("0-1-token")
	f.Add("---")
	f.Add("1-42")
	f.Add("999999999999999999999999-1-t")
	f.Add("1-42-tok-with-dashes")
	f.Add("")
	f.Fuzz(func(t *testing.T, raw string) {
		shard, id, token, err := ParseFlowKey(raw)
		if err != nil {
			return
		}
		if shard < 1 {
			t.Fatalf("accepted key %q with shard %d < 1", raw, shard)
		}
		// The token is the verbatim third segment, so re-encoding must reproduce the input.
		if re := fmt.Sprintf("%d-%d-%s", shard, id, token); re != raw {
			// Leading zeros / plus signs in the numeric segments are the only way re-encoding can
			// differ; those are accepted by ParseInt but harmless (they re-parse identically).
			s2, i2, t2, err2 := ParseFlowKey(re)
			if err2 != nil || s2 != shard || i2 != id || t2 != token {
				t.Fatalf("round-trip mismatch: %q -> (%d,%d,%q) -> %q", raw, shard, id, token, re)
			}
		}
		// A key never contains fewer than two dashes if it parsed.
		if strings.Count(raw, "-") < 2 {
			t.Fatalf("accepted key %q with fewer than 2 dashes", raw)
		}
	})
}

// FuzzParseStepKey mirrors FuzzParseFlowKey for step keys.
func FuzzParseStepKey(f *testing.F) {
	f.Add("1-7-deadbeefdeadbeef")
	f.Add("3-")
	f.Add("-1-x")
	f.Fuzz(func(t *testing.T, raw string) {
		shard, id, token, err := ParseStepKey(raw)
		if err != nil {
			return
		}
		if shard < 1 {
			t.Fatalf("accepted step key %q with shard %d < 1", raw, shard)
		}
		s2, i2, t2, err2 := ParseStepKey(fmt.Sprintf("%d-%d-%s", shard, id, token))
		if err2 != nil || s2 != shard || i2 != id || t2 != token {
			t.Fatalf("round-trip mismatch for %q", raw)
		}
	})
}
