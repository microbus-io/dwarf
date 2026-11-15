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

package engine

import (
	"bytes"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

/*
maxPayloadBytes is the largest JSON value the engine writes into one payload column, one figure for every
dialect. PostgreSQL's JSONB holds at most 268,435,455 bytes, fixed. MySQL and MariaDB cap a whole statement, and
a whole row sent back, at max_allowed_packet, which the deployment doc asks to be set to 512M: a step row holds
its state and its changes side by side, the claim and the fan-in read them together, and each can be this large.
SQL Server's VARBINARY(MAX) and SQLite's default SQLITE_MAX_LENGTH are several times larger. The ~12 MB between
this and JSONB's bound is room for its binary form running a little over the text.

What is measured is the JSON text. JSONB stores strings unescaped, so for string-heavy values it over-counts and
rejects slightly early. For an array of many small numbers JSONB's binary form is several times the text, so such
a value can still be refused by PostgreSQL below this; it then fails at the write, as before the check existed.
*/
const maxPayloadBytes = 256_000_000

// checkPayload refuses a payload the engine must not write into a payload column, naming what it is: one over the
// size limit, with its size, its largest field and the limit (a 413), or one holding text unstorableText names, with
// the field that holds it (a 400). The error is a permanentError: either verdict is a pure function of the data, so
// persist must not retry it and settlePersist fails the step without counting a write the database refused.
func (e *Engine) checkPayload(what string, payload []byte) error {
	// what can name a task, and a task name may hold a '%', so it is an argument, never part of the pattern.
	if len(payload) > e.maxPayloadBytes {
		msg := "%s is %d bytes, over the %d-byte limit"
		args := []any{what, len(payload), e.maxPayloadBytes}
		if name, size := largestField(payload); name != "" {
			msg += "; its largest field, %q, is %d bytes"
			args = append(args, name, size)
		}
		return errors.Trace(&permanentError{errors.New(msg, args...)}, http.StatusRequestEntityTooLarge)
	}
	if problem := unstorableText(payload); problem != "" {
		msg := "%s holds %s"
		args := []any{what, problem}
		if name := fieldHolding(payload, func(v []byte) bool { return unstorableText(v) != "" }); name != "" {
			msg += " in field %q"
			args = append(args, name)
		}
		msg += ", which cannot be stored; carry binary data base64-encoded"
		return errors.Trace(&permanentError{errors.New(msg, args...)}, http.StatusBadRequest)
	}
	return nil
}

// unstorableText names what in a JSON text PostgreSQL's JSONB refuses, or returns "" if nothing is: invalid UTF-8,
// a NUL (a raw byte or a \u0000 escape), and a \u escape of a surrogate that is not half of a pair. Every other
// dialect stores some of them, so refusing them on every dialect is what keeps a flow that runs on one from failing
// on another. A Go string marshals its NUL as \u0000; the other two arrive only in bytes passed through as given (a
// json.RawMessage, a remote host's changes), because encoding/json replaces invalid UTF-8 with U+FFFD and never
// escapes a surrogate.
func unstorableText(payload []byte) string {
	if !utf8.Valid(payload) {
		return "invalid UTF-8"
	}
	if bytes.IndexByte(payload, 0) >= 0 {
		return "a NUL character"
	}
	// A backslash appears only inside a JSON string, where it begins an escape. Consuming each escape whole keeps an
	// escaped backslash from being read as the start of the next one: `\\u0000` is the text \u0000, not a NUL.
	const unpaired = "an unpaired surrogate escape"
	for i := 0; ; {
		j := bytes.IndexByte(payload[i:], '\\')
		if j < 0 || i+j+1 >= len(payload) {
			return ""
		}
		i += j + 1
		if payload[i] != 'u' {
			i++
			continue
		}
		r, ok := hexRune(payload[i+1:])
		if !ok {
			// Not JSON; the database's parser, not this check, refuses it.
			i++
			continue
		}
		i += 5
		switch {
		case r == 0:
			return "a NUL character"
		case 0xD800 <= r && r < 0xDC00:
			if i+6 > len(payload) || payload[i] != '\\' || payload[i+1] != 'u' {
				return unpaired
			}
			lo, ok := hexRune(payload[i+2:])
			if !ok || lo < 0xDC00 || lo >= 0xE000 {
				return unpaired
			}
			i += 6
		case 0xDC00 <= r && r < 0xE000:
			return unpaired
		}
	}
}

// hexRune decodes the four hex digits a \u escape carries.
func hexRune(b []byte) (r rune, ok bool) {
	if len(b) < 4 {
		return 0, false
	}
	for _, c := range b[:4] {
		switch {
		case '0' <= c && c <= '9':
			r = r<<4 | rune(c-'0')
		case 'a' <= c && c <= 'f':
			r = r<<4 | rune(c-'a'+10)
		case 'A' <= c && c <= 'F':
			r = r<<4 | rune(c-'A'+10)
		default:
			return 0, false
		}
	}
	return r, true
}

// largestField returns the name and size of the largest top-level field of a JSON object, or "" if payload is
// not one. It decodes the whole payload, so it runs only on the way to an error.
func largestField(payload []byte) (name string, size int) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return "", 0
	}
	for k, v := range fields {
		if len(v) > size {
			name, size = k, len(v)
		}
	}
	return fieldOfKey(name), size
}

// fieldHolding returns the name of the first top-level field of a JSON object, in key order, whose value satisfies
// bad, or "" if there is none or payload is not an object. It decodes the whole payload, so it runs only on the way
// to an error.
func fieldHolding(payload []byte, bad func(value []byte) bool) string {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return ""
	}
	for _, k := range slices.Sorted(maps.Keys(fields)) {
		if bad(fields[k]) {
			return fieldOfKey(k)
		}
	}
	return ""
}

// fieldOfKey is the field a payload's top-level key names: an overwrite or an addition in a changes column is
// reported under its field's name.
func fieldOfKey(key string) string {
	return strings.TrimSuffix(strings.TrimSuffix(key, workflow.OverwriteSuffix), workflow.AdditionSuffix)
}

// emptyJSON is the empty JSON object, as a BIND value rather than an inline SQL literal.
//
// Every reset of a payload column (`state`/`changes`/`state_refs`/`interrupt_payload`/`resume_data`/
// `subgraph_result`) must bind this instead of writing `SET col='{}'`. Those columns are `VARBINARY(MAX)` on
// SQL Server (see the migrations), and a T-SQL `'{}'` literal is a *varchar*, so an inline assignment fails
// outright: `Implicit conversion from data type varchar to varbinary(max) is not allowed`. Binding a []byte
// sends the matching type and needs no per-driver branch at all - MySQL `JSON`, Postgres `JSONB`, SQLite
// `TEXT`, and SQL Server `VARBINARY` all accept it. (A *comparison* against a payload column is the one place
// that still needs a per-driver form - see the `interrupt_payload` empty-guard in execution.go.)
var emptyJSON = []byte("{}")
