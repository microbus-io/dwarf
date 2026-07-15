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

package jsonx

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/microbus-io/testarossa"
)

func TestCheckStorable_AcceptsWhatFloat64CarriesExactly(t *testing.T) {
	assert := testarossa.For(t)

	for _, doc := range []string{
		`{"n":0}`,
		`{"n":-1}`,
		`{"n":9007199254740992}`,       // exactly 2^53: the last exactly-representable integer
		`{"n":-9007199254740992}`,      // and its negative
		`{"n":1.5}`,                    // fractional: float64-domain, exact round trip by construction
		`{"n":9007199254740993.5}`,     // fractional and huge: still float64-domain
		`{"n":1e300}`,                  // exponent notation: float64-domain
		`{"n":9.007199254740993e15}`,   // a float64 that a reducer's sum might produce
		`{"id":"1234567890123456789"}`, // the prescribed workaround: carry it as a string
		`{"a":{"b":[1,2,{"c":9007199254740992}]}}`,
		`{}`,
		`null`,
	} {
		assert.NoError(CheckStorable([]byte(doc)), "should accept %s", doc)
	}
}

func TestCheckStorable_RejectsIntegersFloat64WouldRound(t *testing.T) {
	assert := testarossa.For(t)

	// The failure this exists to prevent: the standard decode rounds the value and reports nothing.
	var round map[string]any
	assert.NoError(json.Unmarshal([]byte(`{"orderID":1234567890123456789}`), &round))
	assert.Equal(int64(1234567890123456768), int64(round["orderID"].(float64)))

	err := CheckStorable([]byte(`{"orderID":1234567890123456789}`))
	if !assert.Error(err) {
		return
	}
	assert.Contains(err.Error(), "orderID")              // names the field
	assert.Contains(err.Error(), "1234567890123456789")  // and the value
	assert.Contains(err.Error(), "carry it as a string") // and the fix

	// 2^53+1 is the first integer float64 cannot hold: it shares a float64 with 2^53.
	assert.Error(CheckStorable([]byte(`{"n":9007199254740993}`)))
	assert.Error(CheckStorable([]byte(`{"n":-9007199254740993}`)))
	// time.Now().UnixNano() is ~1.7e18 - the most likely way a task trips this.
	assert.Error(CheckStorable([]byte(`{"ts":1752459999999000000}`)))
	// Beyond int64 entirely.
	assert.Error(CheckStorable([]byte(`{"n":123456789012345678901234567890}`)))
	// A bare top-level number, with no field to name.
	err = CheckStorable([]byte(`9007199254740993`))
	if assert.Error(err) {
		assert.Contains(err.Error(), "top level")
	}
}

// TestCheckStorable_RejectsExactlyRepresentableIntegersPast2to53 pins the deliberately-CONSERVATIVE half of the
// guard: an integer past 2^53 that float64 holds EXACTLY - the kind a ReducerAdd sum
// produces - is still rejected on sight, because checkNumber sees only "integer literal > 2^53" and cannot tell an
// exact derived float64 from a lossy external integer. The documented consequence: such a sum
// stores and round-trips fine but is rejected the moment a task RE-AUTHORS it (Set / flow.Subgraph(flow.Snapshot)).
// If this ever relaxes to accept round-trippable integers, that doc and the "carry it as a string" workaround change.
func TestCheckStorable_RejectsExactlyRepresentableIntegersPast2to53(t *testing.T) {
	assert := testarossa.For(t)

	// 2^53+2 is even, so float64 holds it EXACTLY and json.Marshal of that float64 re-emits this very literal -
	// it round-trips perfectly, yet is rejected by design (it is not float-shaped, so it looks like a lossy int).
	const sum = `9007199254740994`
	var v any
	assert.NoError(json.Unmarshal([]byte(sum), &v))
	assert.Equal(int64(9007199254740994), int64(v.(float64))) // round-trips exactly through float64
	b, _ := json.Marshal(v.(float64))
	assert.Equal(sum, string(b)) // and marshals back integer-shaped, NOT as 9.007199254740994e+15

	assert.Error(CheckStorable([]byte(`{"total":`+sum+`}`)), "an exact-but->2^53 integer is rejected on re-authoring, by design")
}

func TestCheckStorable_NamesTheNestedPath(t *testing.T) {
	assert := testarossa.For(t)

	err := CheckStorable([]byte(`{"order":{"lines":[{"sku":"a"},{"id":9007199254740993}]}}`))
	if !assert.Error(err) {
		return
	}
	// The path pinpoints the offending element, not just the top-level field.
	assert.Contains(err.Error(), "order.lines[1].id")
}

func TestCheckStorable_ArrayIndicesAdvanceCorrectly(t *testing.T) {
	assert := testarossa.For(t)

	// A number in the third element must report [2] - the index tracking is the fiddly part of the walk,
	// and an off-by-one here would name the wrong element in every error.
	err := CheckStorable([]byte(`{"ids":[1,2,9007199254740993]}`))
	if !assert.Error(err) {
		return
	}
	assert.Contains(err.Error(), "ids[2]")

	// Nested containers must not corrupt the enclosing array's index.
	err = CheckStorable([]byte(`{"rows":[{"a":1},[7],{"b":9007199254740993}]}`))
	if !assert.Error(err) {
		return
	}
	assert.Contains(err.Error(), "rows[2].b")
}

func TestCheckStorable_MalformedJSON(t *testing.T) {
	assert := testarossa.For(t)
	err := CheckStorable([]byte(`{"n":`))
	if !assert.Error(err) {
		return
	}
	assert.False(strings.Contains(err.Error(), "2^53"), "a syntax error should not be reported as a precision error")
}

// A NUL is valid UTF-8 and marshals to legal JSON, but PostgreSQL's JSONB rejects it (SQLSTATE 22P05) -
// so it is a value that passes on SQLite and kills the flow on the recommended production database.
// Rejected on write, uniformly on every dialect.
func TestCheckStorable_RejectsNUL(t *testing.T) {
	assert := testarossa.For(t)

	nul := string(rune(0))

	// The shape a task produces: f.SetString("payload", string(rawBytes)) with a 0x00 among the bytes.
	doc, err := json.Marshal(map[string]any{"payload": "a" + nul + "b"})
	if !assert.NoError(err) {
		return
	}
	// It marshals to a perfectly legal JSON escape - which is exactly why nothing catches it downstream.
	assert.Equal(`{"payload":"a`+`\u0000`+`b"}`, string(doc))

	err = CheckStorable(doc)
	if !assert.Error(err) {
		return
	}
	assert.Contains(err.Error(), "payload") // names the field
	assert.Contains(err.Error(), "U+0000")  // and the offender
	assert.Contains(err.Error(), "22P05")   // and the failure it prevents
	assert.Contains(err.Error(), "base64")  // and the fix

	// Nested inside an array element: the path must pinpoint it.
	nested, _ := json.Marshal(map[string]any{"order": map[string]any{"lines": []any{"ok", nul}}})
	err = CheckStorable(nested)
	if assert.Error(err) {
		assert.Contains(err.Error(), "order.lines[1]")
	}

	// A NUL in a KEY is just as unstorable as one in a value.
	inKey, _ := json.Marshal(map[string]any{"bad" + nul + "key": "v"})
	assert.Error(CheckStorable(inKey))
}

// The guard is on the NUL specifically, not on control characters or exotic text as a class: everything
// else round-trips through every dialect and must keep working.
func TestCheckStorable_AcceptsEveryOtherString(t *testing.T) {
	assert := testarossa.For(t)

	ok, err := json.Marshal(map[string]any{
		"tab":     "a\tb",
		"newline": "a\nb",
		"ctrl":    "\x01\x1f", // other C0 controls are storable
		"emoji":   "🙂",
		"empty":   "",
		"escapes": `quote " backslash \ slash /`,
	})
	if !assert.NoError(err) {
		return
	}
	assert.NoError(CheckStorable(ok))

	// Invalid UTF-8 needs no guard at all: encoding/json coerces it to U+FFFD on marshal, so it can never
	// reach the database. This is why the write-side guard needs to cover only the NUL.
	coerced, err := json.Marshal(map[string]any{"s": string([]byte{0xff, 0xfe})})
	if !assert.NoError(err) {
		return
	}
	assert.NoError(CheckStorable(coerced))
	assert.Equal(`{"s":"`+`\ufffd\ufffd`+`"}`, string(coerced)) // both bad bytes became U+FFFD, escaped
}
