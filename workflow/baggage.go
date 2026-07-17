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

import "context"

// baggageKeyType is an unexported context-key type so the baggage value cannot collide with any other
// package's context keys.
type baggageKeyType struct{}

var baggageKey = baggageKeyType{}

// ContextWithBaggage returns a copy of ctx carrying the flow's opaque baggage as a State. The engine calls
// this when dispatching to the host's LoadGraph/ExecuteTask (and at the create-time LoadGraph call); hosts
// read the value back with BaggageFrom. Set the baggage itself via FlowOptions.Baggage at Create, not here.
func ContextWithBaggage(ctx context.Context, baggage State) context.Context {
	return context.WithValue(ctx, baggageKey, baggage)
}

// BaggageFrom returns the flow's opaque baggage carried on ctx as a State, or a nil State if none was set.
// It is the JSON-decoded form of what the host set in FlowOptions.Baggage at Create (numbers as float64,
// etc.); a nil State indexes safely (state["k"] yields the zero value). It lives in the workflow package so
// task code can read it without importing the engine.
func BaggageFrom(ctx context.Context) State {
	baggage, _ := ctx.Value(baggageKey).(State)
	return baggage
}
