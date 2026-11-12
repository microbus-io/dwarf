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

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/errors"
)

func TestPublished_ContinueRacingLiveCleanup(t *testing.T) {
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	root, _, _, _ := treeKeys(t, e)
	publish(t, e, root, time.Now())
	e.diags.Seams().Break(diagnostics.CheckpointContinuePublished)
	defer e.diags.Seams().Resume(diagnostics.CheckpointContinuePublished)
	done := make(chan error, 1)
	go func() { _, err := e.Continue(ctx, root, nil); done <- err }()
	if !e.diags.Seams().WaitTimeout(ctx, diagnostics.CheckpointContinuePublished, 10*time.Second) {
		t.Fatal("checkpoint not reached")
	}
	e.reapDueFlows(ctx)
	_, _, id, _, _ := keys.ParseFlowKey(root)
	n, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
	if n != 0 {
		t.Fatalf("live cleanup incomplete: %d", n)
	}
	e.diags.Seams().Resume(diagnostics.CheckpointContinuePublished)
	err := <-done
	if err != nil {
		t.Fatalf("Continue rejected published source after live cleanup: status=%d err=%v", errors.StatusCode(err), err)
	}
}

func TestPublished_ForkRacingLiveCleanup(t *testing.T) {
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	sh, _ := e.db.Shard(1)
	db := sh.DB()
	if db.DriverName() != "pgx" {
		t.Skip("requires PostgreSQL nonlocking refusal read")
	}
	root, _, step, _ := treeKeys(t, e)
	publish(t, e, root, time.Now())
	e.diags.Seams().Break(diagnostics.CheckpointForkPublished)
	defer e.diags.Seams().Resume(diagnostics.CheckpointForkPublished)
	done := make(chan error, 1)
	go func() { _, err := e.Fork(ctx, step, nil); done <- err }()
	if !e.diags.Seams().WaitTimeout(ctx, diagnostics.CheckpointForkPublished, 10*time.Second) {
		t.Fatal("checkpoint not reached")
	}
	e.reapDueFlows(ctx)
	e.diags.Seams().Resume(diagnostics.CheckpointForkPublished)
	if err := <-done; err != nil {
		t.Fatalf("Fork failed during published source cleanup: status=%d err=%v", errors.StatusCode(err), err)
	}
}
