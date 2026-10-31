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
	"strings"
	"testing"
)

// FuzzParseFlowKey asserts ParseFlowKey never panics on arbitrary input, and that any accepted
// key satisfies the format invariants and round-trips exactly: re-encoding the parsed parts reproduces
// the original string, so no second spelling of a key is accepted. The parser is the first thing every public engine operation runs on
// caller-controlled input, so it must be total.
func FuzzParseFlowKey(f *testing.F) {
	f.Add("1-42-a1b2c3d4e5f60718")
	f.Add("2-0-")
	f.Add("0-1-token")
	f.Add("---")
	f.Add("1-42")
	f.Add("999999999999999999999999-1-t")
	f.Add("1-42-tok-with-dashes")
	f.Add("1-+5-tok")
	f.Add("01-5-tok")
	f.Add("")
	f.Fuzz(func(t *testing.T, raw string) {
		shard, id, token, err := ParseFlowKey(raw)
		if err != nil {
			return
		}
		if shard < 1 {
			t.Fatalf("accepted key %q with shard %d < 1", raw, shard)
		}
		// New is ParseFlowKey's inverse, and the token is the verbatim third segment, so re-encoding a
		// parsed key must reproduce the input.
		if re := New(shard, id, token); re != raw {
			t.Fatalf("accepted a non-canonical key: %q -> (%d,%d,%q) -> %q", raw, shard, id, token, re)
		}
		if !canonicalToken(token) {
			t.Fatalf("accepted key %q with a token %q not spelled as RandomIdentifier spells one", raw, token)
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
		s2, i2, t2, err2 := ParseStepKey(New(shard, id, token))
		if err2 != nil || s2 != shard || i2 != id || t2 != token {
			t.Fatalf("round-trip mismatch for %q", raw)
		}
	})
}

// TestParseKey_RejectsNonCanonicalSpellings pins that a key is accepted only as New spells it. A '+' or leading
// zeros name the same row under a different string, and a parked Await keyed on that string is never released:
// it re-reads the flow on every latch sweep instead. So does a token in capitals or with trailing spaces, which a
// case-insensitive or space-padding collation matches to the row.
func TestParseKey_RejectsNonCanonicalSpellings(t *testing.T) {
	const tok = "a1b2c3d4e5f60718"
	for _, key := range []string{
		"1-+5-" + tok, "+1-5-" + tok, "01-5-" + tok, "1-05-" + tok, "1-00-" + tok, "1--0-" + tok, "1- 5-" + tok,
		"1-5-A1B2C3D4E5F60718",  // a token in capitals
		"1-5-" + tok + " ",      // a token padded with a space
		"1-5-a1b2c3d4",          // a token too short
		"1-5-",                  // no token
		"1-5-a1b2c3d4-e5f60718", // a token with a dash
		"1-5-a1b2c3d4e5f6071g",  // a token not hex
	} {
		if _, _, _, err := ParseFlowKey(key); err == nil {
			t.Errorf("flow key %q must be rejected", key)
		}
		if _, _, _, err := ParseStepKey(key); err == nil {
			t.Errorf("step key %q must be rejected", key)
		}
	}
	for _, key := range []string{"1-5-" + tok, "12-0-" + tok, "3-1234567890-" + tok} {
		if _, _, _, err := ParseFlowKey(key); err != nil {
			t.Errorf("flow key %q must parse: %v", key, err)
		}
	}
}
