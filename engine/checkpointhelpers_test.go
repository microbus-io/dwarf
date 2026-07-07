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
	"testing"
	"time"
)

// cpWaitFor blocks until the engine reaches (or is already frozen at) the named checkpoint, failing the
// test on timeout rather than hanging to the suite deadline. Shared by every checkpoint-driven test.
func cpWaitFor(t *testing.T, e *Engine, name string, timeout time.Duration) {
	t.Helper()
	reached := make(chan struct{})
	go func() { e.seams.Wait(name); close(reached) }()
	select {
	case <-reached:
	case <-time.After(timeout):
		t.Fatalf("engine never reached checkpoint %q", name)
	}
}
