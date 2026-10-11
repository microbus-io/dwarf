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
	"net/http"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// stampReducers records every field the graph declares a reducer for in the state's metadata, including a
// field with no value yet, and drops whatever metadata the state held before. The engine is the only author
// of a state's metadata, so a stamp is also what discards an entry a task or a caller forged.
func stampReducers(s workflow.State, reducers map[string]workflow.Reducer) {
	var meta map[string]workflow.FieldMeta
	for k, r := range reducers {
		if r == "" || r == workflow.ReducerReplace {
			continue
		}
		if meta == nil {
			meta = make(map[string]workflow.FieldMeta, len(reducers))
		}
		meta[k] = workflow.FieldMeta{Reducer: r}
	}
	s.SetMeta(meta)
}

// rejectMeta fails a caller-supplied state that carries metadata. Only the engine writes it.
func rejectMeta(s workflow.State, what string) error {
	if s.MetaJSON() != nil {
		return errors.New("%s must not contain the reserved field %q", what, workflow.MetaKey, http.StatusBadRequest)
	}
	return nil
}

// redactMeta strips the metadata from a state the engine hands outside itself. The stored columns keep it.
func redactMeta(s workflow.State) workflow.State {
	if !s.IsZero() {
		s.SetMeta(nil)
	}
	return s
}
