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

package fixtures

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/microbus-io/dwarf/engine"
)

// commonEngine is a single 1-shard engine, with commonProxy as its host, shared by the many fixtures that
// only inspect their OWN flows (by key) and do not depend on the engine being otherwise idle. Sharing one
// engine - and thus one connection pool - across the parallel suite is what keeps it from opening an engine
// (and up to maxOpenConns per shard) PER TEST: dozens of per-test pools otherwise sum past a server's
// connection cap (PostgreSQL defaults to 100) under -parallel load and the suite wedges on connection
// exhaustion. TestProxy guards its handler maps with a RWMutex, so parallel tests register concurrently;
// fixture task/graph URLs are namespaced per file, so the shared registry never collides.
//
// A test must keep its OWN engine (engine.NewEngine()+RunInTest) when it needs isolation rather than
// shared use:
//   - deterministic exclusive scheduling - asserts cross-flow ordering, priority, fairness, throughput,
//     or a timing race that a concurrently-loaded shared engine would perturb (the SetWorkers tests);
//   - a non-default topology - SetNumShards, SetTimeBudget, or a specific SetWorkers count;
//   - host singletons - OnFlowStopped (one callback per proxy) or multi-replica AddPeer/peers;
//   - a clean database - List/Purge/ShardInfo or any assertion over the full flow set.
var (
	commonEngine *engine.Engine
	commonProxy  *engine.TestProxy
)

// TestMain builds the shared engine(s) once for the whole package, runs the suite, then tears them down.
// StartupInTest (the *testing.T-free entry point) opens an isolated throwaway database keyed by the given
// id; Shutdown drops it. Additional common engines with other topologies (e.g. a multi-shard variant) can
// be added here as fixtures need them.
func TestMain(m *testing.M) {
	ctx := context.Background()

	commonProxy = engine.NewTestProxy()
	commonEngine = engine.NewEngine()
	mustSetup(commonEngine.SetHost(commonProxy))
	mustSetup(commonEngine.StartupInTest(ctx, "fixtures/common-1shard"))

	code := m.Run()

	_ = commonEngine.Shutdown(ctx)
	os.Exit(code)
}

func mustSetup(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "fixtures TestMain setup failed:", err)
		os.Exit(1)
	}
}
