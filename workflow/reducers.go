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
	"reflect"
	"time"

	"github.com/microbus-io/errors"
)

// Reducer names how concurrent state modifications from parallel branches are merged during fan-in. A
// field is bound to one with graph.SetReducer; the engine consults it via State.MergeReduce. The fold
// implementations live in reducers.go (Reducer.Reduce dispatches to them).
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

// The reducer functions below are the Go-native fold implementations selected by the Reducer string type
// (the vocabulary the graph produces and State.MergeReduce consumes). They are unexported: a caller names
// a reducer with a Reducer constant, never by calling the function directly. Each operates on the Go
// values (no JSON round trip) and treats a nil base - an absent field on the accumulating State - as
// "seed from incoming", letting the field appear for the first time. None mutates its arguments; results
// are freshly allocated.
//
// Replace (and the empty Reducer) is handled by MergeReduce itself, not here: it needs no base and its
// cleared-incoming case is a delete tombstone, which Reduce has no way to express.

// Reduce folds incoming onto base using the strategy named by r, and is how State.MergeReduce dispatches a
// field to its reducer. The empty and Replace cases are resolved by MergeReduce itself (a delete tombstone,
// which this cannot express) and reach here only defensively.
func (r Reducer) Reduce(base any, incoming any) (any, error) {
	switch r {
	case ReducerAppend:
		return doAppend(base, incoming)
	case ReducerUnion:
		return doUnion(base, incoming)
	case ReducerAdd:
		return doAdd(base, incoming)
	case ReducerMin:
		return doMin(base, incoming)
	case ReducerMax:
		return doMax(base, incoming)
	case ReducerAnd:
		return doAnd(base, incoming)
	case ReducerOr:
		return doOr(base, incoming)
	case ReducerConcat:
		return doConcat(base, incoming)
	case ReducerMerge:
		return doMerge(base, incoming)
	case ReducerReplace, "":
		return incoming, nil
	default:
		return nil, errors.New("unknown reducer: %s", string(r))
	}
}

// doAppend appends incoming onto base's slice and returns the combined slice.
//
// A nil base seeds from incoming: a slice/array incoming becomes a fresh copy of itself, and a
// single-value incoming becomes a one-element slice of its type.
//
// Otherwise base must be a slice or array, and incoming may be either:
//   - a slice/array whose elements are appended individually (a spread), or
//   - a single value assignable to base's element type, appended as one element.
//
// When incoming could be read either way - it is a slice/array of assignable elements yet also a valid
// single element of base (e.g. a []int into a []any base) - it is appended as a single element, since a
// list-typed field of base is more specific than a spread. The result is a newly allocated slice; base
// is never modified.
func doAppend(base any, incoming any) (any, error) {
	return appendInto("append", base, incoming, false)
}

// doUnion appends incoming onto base's slice like doAppend, but skips any element already present
// (compared by reflect.DeepEqual), yielding a set-like slice. base's own elements are preserved as-is;
// only newly added elements are deduplicated, against everything accumulated so far.
func doUnion(base any, incoming any) (any, error) {
	return appendInto("union", base, incoming, true)
}

// appendInto is the shared core of doAppend and doUnion. It builds a fresh slice from base (or, for a nil
// base, an empty slice typed from incoming), then adds incoming's element(s) - spread if incoming is a
// list of base's elements, otherwise as a single element. With dedupe set, an element equal to one
// already in the result is skipped.
func appendInto(name string, base any, incoming any, dedupe bool) (any, error) {
	if incoming == nil {
		return nil, errors.New("%s reducer: incoming must not be nil", name)
	}
	incVal := reflect.ValueOf(incoming)
	incIsList := incVal.Kind() == reflect.Slice || incVal.Kind() == reflect.Array

	var elemType reflect.Type
	var baseVal reflect.Value
	hasBase := base != nil
	if hasBase {
		baseVal = reflect.ValueOf(base)
		if k := baseVal.Kind(); k != reflect.Slice && k != reflect.Array {
			return nil, errors.New("%s reducer: base must be a slice or array, got %T", name, base)
		}
		elemType = baseVal.Type().Elem()
	} else if incIsList {
		elemType = incVal.Type().Elem()
	} else {
		elemType = incVal.Type()
	}

	capHint := 1
	if incIsList {
		capHint = incVal.Len()
	}
	if hasBase {
		capHint += baseVal.Len()
	}
	out := reflect.MakeSlice(reflect.SliceOf(elemType), 0, capHint)
	if hasBase {
		for i := range baseVal.Len() {
			out = reflect.Append(out, baseVal.Index(i))
		}
	}

	add := func(v reflect.Value) {
		if dedupe {
			for i := range out.Len() {
				if reflect.DeepEqual(out.Index(i).Interface(), v.Interface()) {
					return
				}
			}
		}
		out = reflect.Append(out, v)
	}

	// Spread when incoming is a list of base's elements: its type is exactly base's slice type, or (for a
	// differently-shaped list) its elements are assignable to base's element type while incoming is not
	// itself a valid single element of base - the single-element reading wins that tie. With no base, a
	// list incoming always spreads (there is nothing to disambiguate against).
	spread := incIsList
	if incIsList && hasBase {
		incType := incVal.Type()
		spread = incType == reflect.SliceOf(elemType) ||
			(incType.Elem().AssignableTo(elemType) && !incType.AssignableTo(elemType))
	}
	if spread {
		for i := range incVal.Len() {
			add(incVal.Index(i))
		}
		return out.Interface(), nil
	}
	if incVal.Type().AssignableTo(elemType) {
		add(incVal)
		return out.Interface(), nil
	}
	return nil, errors.New("%s reducer: incoming %T is neither a slice of %s nor a single %s", name, incoming, elemType, elemType)
}

// doAdd returns the numeric sum base + incoming. A nil base seeds from incoming.
func doAdd(base any, incoming any) (any, error) {
	return combineNumeric("add", opAdd, base, incoming)
}

// doMin returns the smaller of base and incoming. A nil base seeds from incoming.
func doMin(base any, incoming any) (any, error) {
	return combineNumeric("min", opMin, base, incoming)
}

// doMax returns the larger of base and incoming. A nil base seeds from incoming.
func doMax(base any, incoming any) (any, error) {
	return combineNumeric("max", opMax, base, incoming)
}

type numericOp int

const (
	opAdd numericOp = iota
	opMin
	opMax
)

// realNumber is every Go integer and floating-point type (and named types over them, e.g. time.Duration),
// used to fold two same-typed numbers natively.
type realNumber interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64
}

func numericApply[T realNumber](op numericOp, a, b T) T {
	switch op {
	case opAdd:
		return a + b
	case opMin:
		return min(a, b)
	case opMax:
		return max(a, b)
	}
	return a // unreachable
}

// combineNumeric folds two numbers with a native operation in their own type - no float64 detour, so an
// int64 past 2^53 stays exact. base and incoming must be the same Go type (a fold across types would have
// to promote to float64 and lose integer precision); a mismatch is an error. A nil base seeds from
// incoming.
func combineNumeric(name string, op numericOp, base any, incoming any) (any, error) {
	if incoming == nil {
		return nil, errors.New("%s reducer: incoming must not be nil", name)
	}
	if base == nil {
		if !isNumeric(incoming) {
			return nil, errors.New("%s reducer: incoming must be numeric, got %T", name, incoming)
		}
		return incoming, nil
	}
	if reflect.TypeOf(base) != reflect.TypeOf(incoming) {
		return nil, errors.New("%s reducer: base (%T) and incoming (%T) must be the same type", name, base, incoming)
	}
	switch b := base.(type) {
	case time.Duration:
		return numericApply(op, b, incoming.(time.Duration)), nil
	case int:
		return numericApply(op, b, incoming.(int)), nil
	case int8:
		return numericApply(op, b, incoming.(int8)), nil
	case int16:
		return numericApply(op, b, incoming.(int16)), nil
	case int32:
		return numericApply(op, b, incoming.(int32)), nil
	case int64:
		return numericApply(op, b, incoming.(int64)), nil
	case uint:
		return numericApply(op, b, incoming.(uint)), nil
	case uint8:
		return numericApply(op, b, incoming.(uint8)), nil
	case uint16:
		return numericApply(op, b, incoming.(uint16)), nil
	case uint32:
		return numericApply(op, b, incoming.(uint32)), nil
	case uint64:
		return numericApply(op, b, incoming.(uint64)), nil
	case float32:
		return numericApply(op, b, incoming.(float32)), nil
	case float64:
		return numericApply(op, b, incoming.(float64)), nil
	default:
		return nil, errors.New("%s reducer: unsupported numeric type %T", name, base)
	}
}

// isNumeric reports whether v is any Go integer or floating-point kind.
func isNumeric(v any) bool {
	switch reflect.ValueOf(v).Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

// doAnd returns the logical AND of two booleans. A nil base seeds from incoming.
func doAnd(base any, incoming any) (any, error) {
	return combineBool("and", base, incoming, func(a, b bool) bool { return a && b })
}

// doOr returns the logical OR of two booleans. A nil base seeds from incoming.
func doOr(base any, incoming any) (any, error) {
	return combineBool("or", base, incoming, func(a, b bool) bool { return a || b })
}

func combineBool(name string, base any, incoming any, op func(a, b bool) bool) (any, error) {
	if incoming == nil {
		return nil, errors.New("%s reducer: incoming must not be nil", name)
	}
	ib, ok := incoming.(bool)
	if !ok {
		return nil, errors.New("%s reducer: incoming must be a bool, got %T", name, incoming)
	}
	if base == nil {
		return incoming, nil
	}
	bb, ok := base.(bool)
	if !ok {
		return nil, errors.New("%s reducer: base must be a bool, got %T", name, base)
	}
	return op(bb, ib), nil
}

// doConcat returns the string base + incoming. A nil base seeds from incoming.
func doConcat(base any, incoming any) (any, error) {
	if incoming == nil {
		return nil, errors.New("concat reducer: incoming must not be nil")
	}
	is, ok := incoming.(string)
	if !ok {
		return nil, errors.New("concat reducer: incoming must be a string, got %T", incoming)
	}
	if base == nil {
		return incoming, nil
	}
	bs, ok := base.(string)
	if !ok {
		return nil, errors.New("concat reducer: base must be a string, got %T", base)
	}
	return bs + is, nil
}

// doMerge combines two maps and returns the result, with incoming's entries winning on key collision. A
// nil base seeds from a fresh copy of incoming. Both must be maps; incoming's key and element types must
// be assignable to base's. The result is a newly allocated map; neither argument is modified.
func doMerge(base any, incoming any) (any, error) {
	if incoming == nil {
		return nil, errors.New("merge reducer: incoming must not be nil")
	}
	iv := reflect.ValueOf(incoming)
	if iv.Kind() != reflect.Map {
		return nil, errors.New("merge reducer: incoming must be a map, got %T", incoming)
	}
	if base == nil {
		out := reflect.MakeMapWithSize(iv.Type(), iv.Len())
		for _, k := range iv.MapKeys() {
			out.SetMapIndex(k, iv.MapIndex(k))
		}
		return out.Interface(), nil
	}
	bv := reflect.ValueOf(base)
	if bv.Kind() != reflect.Map {
		return nil, errors.New("merge reducer: base must be a map, got %T", base)
	}
	if !iv.Type().Key().AssignableTo(bv.Type().Key()) || !iv.Type().Elem().AssignableTo(bv.Type().Elem()) {
		return nil, errors.New("merge reducer: incoming %s is not assignable to base %s", iv.Type(), bv.Type())
	}
	out := reflect.MakeMapWithSize(bv.Type(), bv.Len()+iv.Len())
	for _, k := range bv.MapKeys() {
		out.SetMapIndex(k, bv.MapIndex(k))
	}
	for _, k := range iv.MapKeys() {
		out.SetMapIndex(k, iv.MapIndex(k))
	}
	return out.Interface(), nil
}
