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

func TestCheckPrecision_AcceptsWhatFloat64CarriesExactly(t *testing.T) {
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
		assert.NoError(CheckPrecision([]byte(doc)), "should accept %s", doc)
	}
}

func TestCheckPrecision_RejectsIntegersFloat64WouldRound(t *testing.T) {
	assert := testarossa.For(t)

	// The failure this exists to prevent: the standard decode rounds the value and reports nothing.
	var round map[string]any
	assert.NoError(json.Unmarshal([]byte(`{"orderID":1234567890123456789}`), &round))
	assert.Equal(int64(1234567890123456768), int64(round["orderID"].(float64)))

	err := CheckPrecision([]byte(`{"orderID":1234567890123456789}`))
	if !assert.Error(err) {
		return
	}
	assert.Contains(err.Error(), "orderID")              // names the field
	assert.Contains(err.Error(), "1234567890123456789")  // and the value
	assert.Contains(err.Error(), "carry it as a string") // and the fix

	// 2^53+1 is the first integer float64 cannot hold: it shares a float64 with 2^53.
	assert.Error(CheckPrecision([]byte(`{"n":9007199254740993}`)))
	assert.Error(CheckPrecision([]byte(`{"n":-9007199254740993}`)))
	// time.Now().UnixNano() is ~1.7e18 - the most likely way a task trips this.
	assert.Error(CheckPrecision([]byte(`{"ts":1752459999999000000}`)))
	// Beyond int64 entirely.
	assert.Error(CheckPrecision([]byte(`{"n":123456789012345678901234567890}`)))
	// A bare top-level number, with no field to name.
	err = CheckPrecision([]byte(`9007199254740993`))
	if assert.Error(err) {
		assert.Contains(err.Error(), "top level")
	}
}

func TestCheckPrecision_NamesTheNestedPath(t *testing.T) {
	assert := testarossa.For(t)

	err := CheckPrecision([]byte(`{"order":{"lines":[{"sku":"a"},{"id":9007199254740993}]}}`))
	if !assert.Error(err) {
		return
	}
	// The path pinpoints the offending element, not just the top-level field.
	assert.Contains(err.Error(), "order.lines[1].id")
}

func TestCheckPrecision_ArrayIndicesAdvanceCorrectly(t *testing.T) {
	assert := testarossa.For(t)

	// A number in the third element must report [2] - the index tracking is the fiddly part of the walk,
	// and an off-by-one here would name the wrong element in every error.
	err := CheckPrecision([]byte(`{"ids":[1,2,9007199254740993]}`))
	if !assert.Error(err) {
		return
	}
	assert.Contains(err.Error(), "ids[2]")

	// Nested containers must not corrupt the enclosing array's index.
	err = CheckPrecision([]byte(`{"rows":[{"a":1},[7],{"b":9007199254740993}]}`))
	if !assert.Error(err) {
		return
	}
	assert.Contains(err.Error(), "rows[2].b")
}

func TestCheckPrecision_MalformedJSON(t *testing.T) {
	assert := testarossa.For(t)
	err := CheckPrecision([]byte(`{"n":`))
	if !assert.Error(err) {
		return
	}
	assert.False(strings.Contains(err.Error(), "2^53"), "a syntax error should not be reported as a precision error")
}
