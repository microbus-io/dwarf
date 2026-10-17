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
	"encoding/json"
	"net/http"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

/*
maxPayloadBytes is the largest JSON value the engine writes into one payload column, one figure for every
dialect. PostgreSQL's JSONB holds at most 268,435,455 bytes, fixed. MySQL and MariaDB cap a whole statement at
max_allowed_packet, which the deployment doc asks to be set to 256M (268,435,456). SQL Server's VARBINARY(MAX)
and SQLite's default SQLITE_MAX_LENGTH are several times larger. The ~12 MB between this and the first two is
room for the rest of a MySQL statement and for JSONB's binary form running a little over the text.

What is measured is the JSON text. JSONB stores strings unescaped, so for string-heavy values it over-counts and
rejects slightly early. For an array of many small numbers JSONB's binary form is several times the text, so such
a value can still be refused by PostgreSQL below this; it then fails at the write, as before the check existed.
*/
const maxPayloadBytes = 256_000_000

// checkPayloadSize refuses a payload too large to store in one column, naming what it is, its size, its largest
// field and the limit. The error is a permanentError: the size is a pure function of the data, so persist must not
// retry it and settlePersist fails the step without counting a write the database refused. It carries a 413 for
// the operations that return it to a caller.
func (e *Engine) checkPayloadSize(what string, payload []byte) error {
	if len(payload) <= e.maxPayloadBytes {
		return nil
	}
	msg := what + " is %d bytes, over the %d-byte limit"
	args := []any{len(payload), e.maxPayloadBytes}
	if name, size := largestField(payload); name != "" {
		msg += "; its largest field, %q, is %d bytes"
		args = append(args, name, size)
	}
	return errors.Trace(&permanentError{errors.New(msg, args...)}, http.StatusRequestEntityTooLarge)
}

// largestField returns the name and size of the largest top-level field of a JSON object, or "" if payload is
// not one. An overwrite in a changes column is reported under its field's name. It decodes the whole payload, so
// it runs only on the way to an error.
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
	return strings.TrimSuffix(name, workflow.OverwriteSuffix), size
}
