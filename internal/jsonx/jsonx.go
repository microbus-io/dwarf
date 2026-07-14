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

// Package jsonx enforces one invariant on everything dwarf stores: a value written to flow state must
// come back unchanged, on every supported dialect. CheckStorable rejects, at the point of writing, the
// two values that cannot - and both are silent, not loud, if allowed through.
//
// # Integers beyond 2^53
//
// State, baggage, and payloads are carried as map[string]any across a JSON round trip through the
// database, and encoding/json decodes every JSON number into a float64 when the target is an any. A
// float64 holds integers exactly only up to 2^53, so an integer written beyond that comes back
// rounded - 1234567890123456789 decodes as 1234567890123456768 - and the engine then re-marshals the
// rounded value into the next step's state, the flow's final_state, a fork, a continuation. Nothing
// errors; the workflow simply charges the wrong order.
//
// Rather than change what a decoded number means (every number would have to become an int64 or a
// json.Number, which every reader of a state map would then have to expect), dwarf keeps float64 and
// makes the unrepresentable value impossible to store. The resulting invariant - every number in dwarf
// state round-trips exactly through float64 - is what lets the rest of the engine read state as float64
// without qualification.
//
// Only INTEGER-shaped literals are constrained. A fractional or exponent-notation number is
// float64-domain by construction and round-trips exactly whatever its magnitude, so 1e300 is fine and
// 9007199254740993 is not. An identifier too large to carry (a Snowflake id, a 64-bit database key,
// time.Now().UnixNano()) must be carried as a string - the same reason APIs that mint 64-bit ids
// publish an id_str alongside them.
//
// # The NUL character (U+0000)
//
// A NUL in a Go string is valid UTF-8 and marshals to a perfectly legal JSON escape, but PostgreSQL's
// JSONB cannot store it: the INSERT fails with SQLSTATE 22P05, "unsupported Unicode escape sequence".
// The other three dialects accept it, so an unguarded NUL is a value that works in tests (SQLite) and
// dies in production (Postgres) - and dies in the worst way, since the write that carries it is the
// step's own completion UPDATE. The step is left running with no error recorded, and lease recovery
// re-executes the task, side effects and all, every few minutes forever.
//
// So it is rejected on write, uniformly on every dialect: a value dwarf cannot store on the recommended
// production database has no business entering state anywhere. Binary data belongs in state base64-encoded.
// (Invalid UTF-8 needs no guard - encoding/json coerces it to U+FFFD on marshal, so it can never reach
// the database.)
//
// Rejecting on write does NOT make the engine safe against a permanent write failure in general; it
// closes the one instance that is reachable through the public API. The structural hazard - any
// non-transient database error re-executing a task forever - is a separate, open concern.
package jsonx

import (
	"bytes"
	"encoding/json"
	"io"
	"strconv"
	"strings"

	"github.com/microbus-io/errors"
)

// MaxSafeInteger is the largest integer a float64 represents exactly (2^53). Integers in [-2^53, 2^53]
// survive dwarf's JSON round trip; beyond it, consecutive integers share a float64 and the value is
// silently rounded.
const MaxSafeInteger = 1 << 53

// CheckStorable reports an error if the JSON document holds a value dwarf cannot store and read back
// unchanged: an integer a float64 cannot carry exactly (beyond ±2^53), or a NUL character, which
// PostgreSQL's JSONB rejects outright. It names the field (dotted path, with array indices) so the
// author knows which value to fix. A malformed document is reported as-is - the caller is about to
// marshal or store it either way. See the package doc for why each is a silent corruption rather than a
// clean failure if it slips through.
func CheckStorable(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	// One frame per open container, so a number's error can name its full path. An object frame tracks
	// the key it is currently reading a value for; an array frame tracks the index.
	type frame struct {
		object  bool
		wantKey bool
		key     string
		index   int
	}
	var stack []frame

	path := func() string {
		var b strings.Builder
		for _, f := range stack {
			if !f.object {
				b.WriteByte('[')
				b.WriteString(strconv.Itoa(f.index))
				b.WriteByte(']')
				continue
			}
			if b.Len() > 0 {
				b.WriteByte('.')
			}
			b.WriteString(f.key)
		}
		return b.String()
	}
	// valueDone advances the enclosing container past the value just read: an object goes back to
	// expecting a key, an array to the next index.
	valueDone := func() {
		if n := len(stack); n > 0 {
			if stack[n-1].object {
				stack[n-1].wantKey = true
			} else {
				stack[n-1].index++
			}
		}
	}

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			// Token() streams; it reports EOF without complaining about unclosed containers. An open frame
			// here means the document was truncated.
			if len(stack) > 0 {
				return errors.New("unexpected end of JSON input")
			}
			return nil
		}
		if err != nil {
			return errors.Trace(err)
		}
		if delim, ok := tok.(json.Delim); ok {
			switch delim {
			case '{':
				stack = append(stack, frame{object: true, wantKey: true})
			case '[':
				stack = append(stack, frame{})
			case '}', ']':
				stack = stack[:len(stack)-1]
				valueDone()
			}
			continue
		}
		if n := len(stack); n > 0 && stack[n-1].object && stack[n-1].wantKey {
			key, _ := tok.(string)
			stack[n-1].key = key
			stack[n-1].wantKey = false
			// A NUL is as unstorable in a key as in a value. Checked after the key is on the stack, so the
			// path names the offending field itself.
			err = checkNUL(key, path())
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		switch v := tok.(type) {
		case json.Number:
			err = checkNumber(v, path())
		case string:
			err = checkNUL(v, path())
		}
		if err != nil {
			return errors.Trace(err)
		}
		valueDone()
	}
}

// checkNUL rejects a string carrying U+0000. It marshals to a legal JSON escape and every dialect but
// Postgres stores it, so it is a value that passes the tests (SQLite) and kills the flow in production.
func checkNUL(s string, path string) error {
	if !strings.ContainsRune(s, 0) {
		return nil
	}
	where := "at the top level"
	if path != "" {
		where = "in field '" + path + "'"
	}
	return errors.New(
		"the string %s contains a NUL character (U+0000), which workflow state cannot store: PostgreSQL's "+
			"JSONB rejects it (SQLSTATE 22P05) - encode binary data as base64",
		where,
	)
}

func checkNumber(num json.Number, path string) error {
	literal := num.String()
	// A fractional or exponent literal is float64-domain: it decodes to the float64 it was written from
	// and re-marshals identically, at any magnitude. Only an integer literal can promise more precision
	// than a float64 keeps.
	if strings.ContainsAny(literal, ".eE") {
		return nil
	}
	i, err := strconv.ParseInt(literal, 10, 64)
	if err == nil && i >= -MaxSafeInteger && i <= MaxSafeInteger {
		return nil
	}
	where := "at the top level"
	if path != "" {
		where = "in field '" + path + "'"
	}
	return errors.New(
		"integer %s %s exceeds the precision of workflow state (±2^53): state round-trips through JSON, "+
			"where a value this large is silently rounded - carry it as a string",
		literal, where,
	)
}
