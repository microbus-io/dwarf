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

import "strings"

// seamsJoin MUST join a targeted seam name the way the engine's own seamsJoin does, so a fixture waits on the
// string the engine actually fires. It is spelled here rather than exported from engine, which would put a
// name-building helper on the engine's public surface for the benefit of tests alone; a drift makes a wait
// here target a name nothing fires, so it times out and fails loudly rather than going quietly wrong.
func seamsJoin(parts ...string) string {
	return strings.Join(parts, ":")
}
