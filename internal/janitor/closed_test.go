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

package janitor

import (
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// TestClosed_AScannedAttemptIsAHint pins filtering from the scan and the transaction's recheck when another copier
// starts an attempt after the scan. A retention of zero still removes trees with attempts.
func TestClosed_AScannedAttemptIsAHint(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	roots := seedTrees(t, r.db, 2, callShape)
	setAttempt(t, r, roots[0], time.Now().Add(-time.Minute))
	candidates := closedCandidates(t, r, roots...)
	assert.True(candidates[0].attempted)
	assert.False(candidates[1].attempted)
	setAttempt(t, r, roots[1], time.Now().Add(-time.Minute))
	s := r.settings
	cut, via, err := cutClosed(ctx, r.db, r.seams, candidates, true, s.batchRows, s.batchBytes, s.stageRows, s.stageBytes)
	assert.Must(assert.NoError(err))
	assert.Equal(roots[1:], cut, "the scan's unattempted root is sized, without rereading its attempt")
	assert.Equal(routeMove, via)
	removed, changed, err := removeClosed(ctx, r.db, r.seams, cut, via, 0, filing(t, r))
	assert.NoError(err)
	assert.False(changed, "the lock finds the new attempt and leaves the tree to its copier")
	assert.Zero(removed.flows)
	cut, via, err = cutClosed(ctx, r.db, r.seams, candidates, false, s.batchRows, s.batchBytes, s.stageRows, s.stageBytes)
	assert.Must(assert.NoError(err))
	assert.Equal(roots, cut, "retention zero includes attempted trees")
	removed, changed, err = removeClosed(ctx, r.db, r.seams, cut, via, 0, nil)
	assert.NoError(err)
	assert.True(changed)
	assert.Equal(4, removed.flows)
}
