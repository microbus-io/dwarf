/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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
	"encoding/json"
	"maps"

	"github.com/microbus-io/errors"
)

// Reducer defines how concurrent state modifications from parallel tasks are merged during fan-in.
type Reducer string

const (
	ReducerReplace Reducer = "replace" // Last write wins (default)
	ReducerAppend  Reducer = "append"  // Concatenate arrays
	ReducerAdd     Reducer = "add"     // Sum numeric values
	ReducerMin     Reducer = "min"     // Smaller of two numeric values
	ReducerMax     Reducer = "max"     // Larger of two numeric values
	ReducerUnion   Reducer = "union"   // Merge arrays, deduplicate
	ReducerMerge   Reducer = "merge"   // Merge objects, new key wins
	ReducerAnd     Reducer = "and"     // Logical AND of booleans
	ReducerOr      Reducer = "or"      // Logical OR of booleans
	ReducerConcat  Reducer = "concat"  // Concatenate strings
)

// Reduce applies the reducer to merge an incoming value into an existing value.
func (r Reducer) Reduce(existing, incoming any) (any, error) {
	switch r {
	case ReducerReplace, "":
		return incoming, nil
	case ReducerAppend:
		return reduceAppend(existing, incoming)
	case ReducerAdd:
		return reduceAdd(existing, incoming)
	case ReducerMin:
		return reduceMin(existing, incoming)
	case ReducerMax:
		return reduceMax(existing, incoming)
	case ReducerUnion:
		return reduceUnion(existing, incoming)
	case ReducerMerge:
		return reduceMerge(existing, incoming)
	case ReducerAnd:
		return reduceAnd(existing, incoming)
	case ReducerOr:
		return reduceOr(existing, incoming)
	case ReducerConcat:
		return reduceConcat(existing, incoming)
	default:
		return nil, errors.New("unknown reducer: %s", string(r))
	}
}

// MergeState applies changes on top of state, using the provided reducers for
// fields that have one. Fields without a registered reducer use replace
// semantics (last write wins).
func MergeState(state any, changes any, reducers map[string]Reducer) (map[string]any, error) {
	stateMap, err := toAnyMap(state)
	if err != nil {
		return nil, errors.Trace(err)
	}
	changesMap, err := toAnyMap(changes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	merged := make(map[string]any, len(stateMap)+len(changesMap))
	maps.Copy(merged, stateMap)
	for k, v := range changesMap {
		existing, exists := merged[k]
		reducer := reducers[k]
		if !exists || reducer == "" || reducer == ReducerReplace {
			merged[k] = v
			continue
		}
		merged[k], err = reducer.Reduce(existing, v)
		if err != nil {
			return nil, errors.New("reducer '%s' failed on field '%s': %w", string(reducer), k, err)
		}
	}
	return merged, nil
}

func toAnyMap(v any) (map[string]any, error) {
	if v == nil {
		return nil, nil
	}
	switch m := v.(type) {
	case map[string]any:
		return m, nil
	case map[string]json.RawMessage:
		result := make(map[string]any, len(m))
		for k, val := range m {
			result[k] = val
		}
		return result, nil
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var result map[string]any
		err = json.Unmarshal(data, &result)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return result, nil
	}
}

func jsonKind(v any) string {
	if v == nil {
		return "null"
	}
	raw, err := marshalAny(v)
	if err != nil {
		return ""
	}
	for _, b := range raw {
		switch b {
		case ' ', '\t', '\n', '\r':
			continue
		case '{':
			return "object"
		case '[':
			return "array"
		case '"':
			return "string"
		case 't', 'f':
			return "bool"
		case 'n':
			return "null"
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			return "number"
		default:
			return ""
		}
	}
	return ""
}

func reduceAppend(existing, incoming any) (any, error) {
	a, err := unmarshalArray(existing, "append")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalArray(incoming, "append")
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := json.Marshal(append(a, b...))
	return json.RawMessage(result), errors.Trace(err)
}

func reduceAdd(existing, incoming any) (any, error) {
	a, err := unmarshalNumber(existing, "add")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalNumber(incoming, "add")
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := json.Marshal(a + b)
	return json.RawMessage(result), errors.Trace(err)
}

func reduceMin(existing, incoming any) (any, error) {
	if isCleared(existing) {
		return incoming, nil
	}
	if isCleared(incoming) {
		return existing, nil
	}
	a, err := unmarshalNumber(existing, "min")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalNumber(incoming, "min")
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := a
	if b < a {
		m = b
	}
	result, err := json.Marshal(m)
	return json.RawMessage(result), errors.Trace(err)
}

func reduceMax(existing, incoming any) (any, error) {
	if isCleared(existing) {
		return incoming, nil
	}
	if isCleared(incoming) {
		return existing, nil
	}
	a, err := unmarshalNumber(existing, "max")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalNumber(incoming, "max")
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := a
	if b > a {
		m = b
	}
	result, err := json.Marshal(m)
	return json.RawMessage(result), errors.Trace(err)
}

func reduceUnion(existing, incoming any) (any, error) {
	a, err := unmarshalArray(existing, "union")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalArray(incoming, "union")
	if err != nil {
		return nil, errors.Trace(err)
	}
	seen := make(map[string]bool, len(a))
	for _, v := range a {
		seen[string(v)] = true
	}
	for _, v := range b {
		if !seen[string(v)] {
			a = append(a, v)
			seen[string(v)] = true
		}
	}
	result, err := json.Marshal(a)
	return json.RawMessage(result), errors.Trace(err)
}

func reduceAnd(existing, incoming any) (any, error) {
	a, err := unmarshalBool(existing, "and")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalBool(incoming, "and")
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := json.Marshal(a && b)
	return json.RawMessage(result), errors.Trace(err)
}

func reduceOr(existing, incoming any) (any, error) {
	a, err := unmarshalBool(existing, "or")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalBool(incoming, "or")
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := json.Marshal(a || b)
	return json.RawMessage(result), errors.Trace(err)
}

func reduceConcat(existing, incoming any) (any, error) {
	a, err := unmarshalString(existing, "concat")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalString(incoming, "concat")
	if err != nil {
		return nil, errors.Trace(err)
	}
	result, err := json.Marshal(a + b)
	return json.RawMessage(result), errors.Trace(err)
}

func reduceMerge(existing, incoming any) (any, error) {
	a, err := unmarshalObject(existing, "merge")
	if err != nil {
		return nil, errors.Trace(err)
	}
	b, err := unmarshalObject(incoming, "merge")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if a == nil {
		a = make(map[string]json.RawMessage, len(b))
	}
	for k, v := range b {
		a[k] = v
	}
	result, err := json.Marshal(a)
	return json.RawMessage(result), errors.Trace(err)
}

func unmarshalArray(v any, reducerName string) ([]json.RawMessage, error) {
	if isCleared(v) {
		return nil, nil
	}
	raw, err := marshalAny(v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, errors.New("%s reducer requires array, got %s", reducerName, jsonKind(v))
	}
	return arr, nil
}

func unmarshalObject(v any, reducerName string) (map[string]json.RawMessage, error) {
	if isCleared(v) {
		return nil, nil
	}
	raw, err := marshalAny(v)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, errors.New("%s reducer requires object, got %s", reducerName, jsonKind(v))
	}
	return obj, nil
}

func unmarshalNumber(v any, reducerName string) (float64, error) {
	if isCleared(v) {
		return 0, nil
	}
	raw, err := marshalAny(v)
	if err != nil {
		return 0, errors.Trace(err)
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err != nil {
		return 0, errors.New("%s reducer requires number, got %s", reducerName, jsonKind(v))
	}
	return n, nil
}

func unmarshalBool(v any, reducerName string) (bool, error) {
	if isCleared(v) {
		return false, nil
	}
	raw, err := marshalAny(v)
	if err != nil {
		return false, errors.Trace(err)
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err != nil {
		return false, errors.New("%s reducer requires bool, got %s", reducerName, jsonKind(v))
	}
	return b, nil
}

func unmarshalString(v any, reducerName string) (string, error) {
	if isCleared(v) {
		return "", nil
	}
	raw, err := marshalAny(v)
	if err != nil {
		return "", errors.Trace(err)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", errors.New("%s reducer requires string, got %s", reducerName, jsonKind(v))
	}
	return s, nil
}
