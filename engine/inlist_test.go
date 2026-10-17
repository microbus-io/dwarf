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

	"github.com/microbus-io/testarossa"
)

// TestLockOrder_AscendingCopy pins the two halves of lockOrder: the chunked lock passes see the ids ascending, and
// the caller's list keeps its order, since Terminate keeps it aligned with each flow's key for the post-commit
// signals.
func TestLockOrder_AscendingCopy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	ids := []any{7, 3, 1025, 512, 3, 1}
	assert.Equal([]any{1, 3, 3, 7, 512, 1025}, lockOrder(ids))
	assert.Equal([]any{7, 3, 1025, 512, 3, 1}, ids)
}
