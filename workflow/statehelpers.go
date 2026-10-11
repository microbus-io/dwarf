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

package workflow

import (
	"bytes"
	"encoding/json"
)

// isCleared reports whether a stored field holds a null - what Set(name, nil) stores, and the form a JSON null
// takes coming from a column or off the wire. A null means "nothing": it deletes a field with no reducer and is
// a reducer's identity (see State.Apply).
//
// A null has two spellings and both must match: a Go nil json.RawMessage and the bytes `null`. encoding/json
// marshals a nil json.RawMessage as `null`, so the two are one value; a check that missed either would make a
// null read back from a column stop meaning "nothing".
func isCleared(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) == 0 || bytes.Equal(raw, []byte("null"))
}
