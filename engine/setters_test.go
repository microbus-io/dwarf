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

	"github.com/microbus-io/testarossa"
)

// TestSetters_ConstructionTimeOnly asserts the construction-time-only setters reject a call on a running
// engine, while the live setters succeed after Startup. The split is the contract that lets a host change
// safe knobs (time budget, pool size, shard count) hot while refusing the unsafe ones (DSN, workers,
// host, providers) rather than silently no-op'ing.
func TestSetters_ConstructionTimeOnly(t *testing.T) {
	assert := testarossa.For(t)

	e := NewEngine()
	e.SetHost(noopHost{})
	e.RunInTest(t)

	// Construction-time-only: rejected after Startup.
	assert.Error(e.SetDSN("file:other.sqlite"))
	assert.Error(e.SetWorkers(8))
	assert.Error(e.SetHost(noopHost{}))
	assert.Error(e.SetLogger(nil))
	assert.Error(e.SetDebugLogger())
	assert.Error(e.SetMeterProvider(nil))
	assert.Error(e.SetTracerProvider(nil))

	// Live: succeed after Startup.
	assert.NoError(e.SetTimeBudget(30 * time.Second))
	assert.NoError(e.SetDefaultPriority(5))
	assert.NoError(e.SetMaxOpenConns(4))
	assert.NoError(e.SetNumShards(1)) // <= current: no-op, no error
}
