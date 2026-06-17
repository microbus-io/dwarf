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

// Package workflow holds the pure data types of the dwarf workflow engine: the building blocks a host
// uses to define workflows and the carriers it reads and writes when running tasks.
//
// It has no heavy dependencies and no knowledge of the database or the engine's runtime, so code that
// defines tasks and graphs imports only this package, never the engine.
//
// # Defining a workflow
//
// A Graph is a directed graph of tasks and transitions. Build one with NewGraph and the Add* methods:
//
//	g := workflow.NewGraph("Checkout")
//	g.SetEndpoint("Reserve", "inventory.reserve")
//	g.SetEndpoint("Charge", "billing.charge")
//	g.AddTransition("Reserve", "Charge")
//	g.AddTransition("Charge", workflow.END)
//
// Transitions can be unconditional, conditional (AddTransitionWhen / AddTransitionSwitch), dynamic
// fan-out over an array (AddTransitionForEach), an error handler (AddTransitionOnError), or an explicit
// jump target (AddTransitionGoto). When parallel branches converge at a fan-in, per-field reducers
// (SetReducer, see the Reducer constants) merge their changes.
//
// # Running a task
//
// A task receives a *Flow: the engine pre-populates it with the step's input state, the task reads
// inputs and writes outputs with the typed accessors (Get/Set and friends), and may emit control signals
// - Retry, Sleep, Goto, Interrupt (human-in-the-loop), or Subgraph (call another workflow). Writes to
// reducer-managed fields are deltas, not accumulated values.
//
//	func charge(ctx context.Context, f *workflow.Flow) error {
//		amount := f.GetFloat("amount")
//		if amount <= 0 {
//			return errors.New("nothing to charge")
//		}
//		f.SetString("receipt", chargeCard(amount))
//		return nil
//	}
//
// # Signaling backpressure and breakers
//
// To engage the engine's adaptive mechanisms from a host's ExecuteTask, wrap the returned error with
// ErrRateLimited (rate-limit the task) or ErrUnavailable (trip the task's circuit breaker). The engine
// classifies via IsRateLimited / IsUnavailable; it never inspects status codes itself.
//
// FlowOutcome, FlowStep, FlowSummary, and Query are the read-side result types returned by the engine's
// inspection operations.
package workflow
