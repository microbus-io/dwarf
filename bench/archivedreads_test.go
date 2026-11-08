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
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestArchivedReaders_ReportListErrors pins that a ListClosed refresh that fails is counted, not swallowed. With no
// keys the readers idle, so a run whose listing always failed reported zero errors and stayed valid having applied
// none of the read load it asked for.
func TestArchivedReaders_ReportListErrors(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	eng := engine.NewEngineUnderTest(t.Name()) // never started, so every ListClosed is a 503
	stop := startArchivedReaders(context.Background(), eng, 2)
	r := stop()
	assert.True(r.ListErrors >= 1, "list errors %d", r.ListErrors)
	assert.Zero(r.Reads)
}

// TestArchivedListWindow_FitsTheArchiveLimit pins that the readers' window stays inside the archive days one
// ListClosed reads. Under a 600s DWARF_ARCHIVE_DAY and a retention of 100 days, a fixed day-long window reaches 101
// days and is refused on every refresh.
func TestArchivedListWindow_FitsTheArchiveLimit(t *testing.T) {
	assert := testarossa.For(t)
	t.Setenv("DWARF_ARCHIVE_DAY", "")
	assert.Equal(24*time.Hour, archivedListWindow())

	t.Setenv("DWARF_ARCHIVE_DAY", "600s")
	assert.Equal(5*time.Hour, archivedListWindow())

	ctx := context.Background()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetHost(hosts.NewMux()))
	assert.NoError(eng.SetRetention(100))
	assert.Must(assert.NoError(eng.Startup(t.Context())))
	now := time.Now()
	_, err := eng.ListClosed(ctx, workflow.Query{FromTime: now.Add(-archivedListWindow()), ToTime: now, Limit: 10})
	assert.NoError(err)
	_, err = eng.ListClosed(ctx, workflow.Query{FromTime: now.Add(-24 * time.Hour), ToTime: now, Limit: 10})
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "a day-long window over 600s days is refused")
}

func TestArchivedListWindow_CapsLargeDays(t *testing.T) {
	for _, day := range []string{"48m", "24h", "87600h", "2562047h47m16s"} {
		t.Run(day, func(t *testing.T) {
			t.Setenv("DWARF_ARCHIVE_DAY", day)
			if got := archivedListWindow(); got != 24*time.Hour {
				t.Fatalf("archive day %s: window = %s, want 24h", day, got)
			}
		})
	}
}
