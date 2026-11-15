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

package main

import (
	"os"
	"testing"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/sequel"
)

// TestRig_Invariants runs the engine's invariant checks against the database a rig run left behind, named by
// DWARF_TESTING_RIG_DSN (shard 1), once the run has drained. A rig ships it as a test binary (go test -c) beside
// the bench. Without the variable it is skipped.
func TestRig_Invariants(t *testing.T) {
	dsn := os.Getenv("DWARF_TESTING_RIG_DSN")
	if dsn == "" {
		t.Skip("DWARF_TESTING_RIG_DSN is not set")
	}
	db, err := sequel.Open("", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	enginetest.AssertInvariantsIn(t, map[int]*sequel.DB{1: db})
}
