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
	"github.com/microbus-io/dwarf/internal/graphcache"
	"github.com/microbus-io/errors"
)

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

// internGraph is graphcache's Intern with its parse failure made a permanentError: the stored bytes will not parse
// on a retry either, and a flow whose graph cannot be read must still be stoppable (terminalFinalState takes a
// permanentError as an empty final state). Every intern goes through here.
func (e *Engine) internGraph(graphJSON []byte, workflowURL, graphHash string) (*graphcache.Entry, error) {
	cg, err := e.graphs.Intern(graphJSON, workflowURL, graphHash)
	if err != nil {
		return nil, errors.Trace(&permanentError{err})
	}
	return cg, nil
}
