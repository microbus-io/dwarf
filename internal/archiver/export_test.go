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

package archiver

import (
	"context"
	"slices"
)

// The tests drive the Archive through its exported API; these expose the few internals they pin.

// SetClock makes a read the given seconds since the Unix epoch instead of the database clock.
func SetClock(a *Archive, secs func() int64) {
	a.clock = func(context.Context) (int64, error) { return secs(), nil }
}

// Ensure exposes ensure to the tests.
func Ensure(ctx context.Context, a *Archive, day int) error {
	return a.ensure(ctx, day)
}

// Days returns the days that have a table of flows or steps, ascending.
func Days(ctx context.Context, a *Archive) ([]int, error) {
	days, _, err := a.catalog(ctx)
	return days, err
}

// Known returns the days a knows have both tables.
func Known(a *Archive) []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.known)
}

// OnCatalogRead makes f run after the query of each of a's catalog reads, before the read updates the days a knows.
func OnCatalogRead(a *Archive, f func()) {
	a.catalogRead = f
}

// Catalogs returns how many times a has read the catalog.
func Catalogs(a *Archive) int {
	return int(a.catalogs.Load())
}

// MigrateExisting exposes migrateExisting to the tests.
func MigrateExisting(ctx context.Context, a *Archive, day int) error {
	return a.migrateExisting(ctx, day)
}

// CreationDay exposes creationDay to the tests.
func CreationDay(a *Archive, hour int) int {
	return a.creationDay(hour)
}

// DaysAhead exposes daysAhead to the tests.
func DaysAhead(a *Archive) int {
	return daysAhead(a.dayLength)
}

// ForgetBatch is how many expired directory rows one statement deletes.
const ForgetBatch = forgetBatch
