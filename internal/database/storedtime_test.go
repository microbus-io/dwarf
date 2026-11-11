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

package database

import (
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// TestStoredTime_KeepsTheWallClock pins StoredTime on a time a driver placed in a location of its own, as MySQL's
// does with the DSN's loc: the stored wall clock is the UTC instant, where UTC() would shift it by the offset.
func TestStoredTime_KeepsTheWallClock(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	stored := time.Date(2026, 11, 10, 23, 30, 0, 5_000_000, time.UTC)
	for _, loc := range []*time.Location{time.UTC, time.FixedZone("east", 5*3600), time.FixedZone("west", -8*3600)} {
		scanned := time.Date(2026, 11, 10, 23, 30, 0, 5_000_000, loc)
		assert.True(StoredTime(scanned).Equal(stored), "%v", loc)
		assert.Equal(time.UTC, StoredTime(scanned).Location())
	}
}
