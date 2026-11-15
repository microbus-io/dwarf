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

// Package accessors reaches into a running engine for code in this module outside package engine - its tests and
// benchmarks: DB and Seams, the engine's shard set and seam registry; ListLive, ArchiveNow and ArchiveDays, which
// list the live tables in every status, archive what just closed and read the archive's days; FlowsClosed, the
// count of flows an engine closed; SetRefillInterval, the benchmark override of the refill cycle period; and
// ShardBudget, the pool the engine derives for a shard. The seams' names are internal/diagnostics':
//
//	accessors.Seams(eng).Inject(diagnostics.Join(diagnostics.FaultExecuteTask, "Charge"))
//
// The accessors take the engine as an any because this package cannot import package engine, which imports
// it; they panic when given anything but an *engine.Engine, and are nil until package engine is linked in.
package accessors

import (
	"context"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
)

// DB returns the engine's shard set, for a test to inspect flow and step rows directly.
var DB func(eng any) *database.ShardSet

// Seams returns the engine's seam registry, for a test to arm faults and checkpoints and wait on them.
var Seams func(eng any) *seamster.Seamster

// ListLive lists the flows in the engine's live tables in every status, open and closed - what ListOpen lists plus the
// closed flows not archived yet - with ListOpen's rules otherwise, for a test that must see a flow it just closed.
var ListLive func(eng any, ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error)

// ArchiveNow runs the janitors' jobs until they make no progress, with no archive grace: every closed root is archived,
// however recently it closed, and the roots scheduled for deletion whose time has come are removed - but for one whose
// stamp came due behind fast deletion's position, which waits for sweep deletion's window - for a test that lists or
// purges what it just closed.
var ArchiveNow func(eng any, ctx context.Context)

// ArchiveDays reads the archive days whose tables exist on a shard, ascending, for a benchmark that counts or sizes
// the archive.
var ArchiveDays func(eng any, ctx context.Context, shard int) ([]int, error)

// FlowsClosed counts the flows this engine has closed - completed, failed, terminated or cancelled - with an atomic
// read that needs no meter, for a benchmark to sum over the engines it runs in process. A flow is closed by whichever
// replica dispatches its last step, so one engine's count means nothing alone.
var FlowsClosed func(eng any) int64

// SetRefillInterval pins every shard's refill cycle period to d on a running or configured engine, for a
// benchmark sweep to hold the period at fixed values, or a fixture whose backlog makes the derived period too
// short under -race; d <= 0 restores the derived period.
var SetRefillInterval func(eng any, d time.Duration) error

// SetArchiveDay replaces the length of the archive's day, a whole number of seconds of at least 1s, before Startup:
// for a benchmark that shows day rollover and retention under load within one run.
var SetArchiveDay func(eng any, d time.Duration) error

// SetArchiveGrace sets how long a closed root waits in the live tables before it is archived, at least 0, before
// Startup: for a benchmark that needs every closed flow archived within its window, or held longer.
var SetArchiveGrace func(eng any, d time.Duration) error

// SetStageRows sets the tree size in rows above which a closed tree is archived in stages (0, never), at least 0,
// before Startup.
var SetStageRows func(eng any, n int) error

// SetStageBytes sets the tree payload in bytes above which a closed tree is archived in stages (0, no byte
// threshold), at least 0, before Startup.
var SetStageBytes func(eng any, n int) error

// ShardBudget is the connection budget the engine derives for one PostgreSQL shard with no MaxConnections
// declared, at rttMs of round-trip time against a server whose max_connections is serverMax (0 = unknown) -
// the pool a lone replica opens. A benchmark computes it ahead of a run to pin exactly the pool that ships,
// since the engine's own Startup probe varies run to run.
var ShardBudget func(rttMs float64, serverMax int) int
