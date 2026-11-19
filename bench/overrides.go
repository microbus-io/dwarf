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
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/errors"
)

// archiveOverrides sets eng's archive day, archive grace and staging thresholds from the environment, before
// Startup, and returns what each setter answered. The rig sets them per arm (RUN_ENV in bench/gcp); the engine
// reads no environment itself. Each one set is logged, since it changes how every closed flow leaves the live tables.
//
//	DWARF_TESTING_ARCHIVE_DAY    the archive day's length, a whole number of seconds as a Go duration (600s)
//	DWARF_TESTING_ARCHIVE_GRACE  how long a closed root waits before it is archived, a Go duration of at least 0
//	DWARF_TESTING_STAGE_ROWS     the tree size in rows above which archival is staged, at least 0 (0, never)
//	DWARF_TESTING_STAGE_BYTES    the tree payload in bytes above which archival is staged, at least 0 (0, none)
func archiveOverrides(eng *engine.Engine, logger *slog.Logger) []error {
	var errs []error
	duration := func(name string, set func(any, time.Duration) error) {
		s := os.Getenv(name)
		if s == "" {
			return
		}
		d, err := time.ParseDuration(s)
		if err != nil {
			errs = append(errs, errors.New("%s must be a Go duration, not %q", name, s))
			return
		}
		logger.Warn("Set from the environment, for benchmarks only", "variable", name, "value", d)
		errs = append(errs, set(eng, d))
	}
	whole := func(name string, set func(any, int) error) {
		s := os.Getenv(name)
		if s == "" {
			return
		}
		n, err := strconv.Atoi(s)
		if err != nil {
			errs = append(errs, errors.New("%s must be a whole number, not %q", name, s))
			return
		}
		logger.Warn("Set from the environment, for benchmarks only", "variable", name, "value", n)
		errs = append(errs, set(eng, n))
	}
	duration("DWARF_TESTING_ARCHIVE_DAY", accessors.SetArchiveDay)
	duration("DWARF_TESTING_ARCHIVE_GRACE", accessors.SetArchiveGrace)
	whole("DWARF_TESTING_STAGE_ROWS", accessors.SetStageRows)
	whole("DWARF_TESTING_STAGE_BYTES", accessors.SetStageBytes)
	return errs
}
