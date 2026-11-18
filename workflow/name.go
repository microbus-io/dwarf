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

package workflow

// nameRule describes a valid graph name, task name, task URL or workflow URL, for error messages.
const nameRule = "names and URLs are 1 to 512 printable ASCII characters, neither beginning nor ending with a space"

// maxNameLen is the width of the columns a name or a URL is stored in (workflow_name, workflow_url, task_name,
// task_url, advance_goto). A longer one fails as a column overflow on its write, which for a task other than the
// entry point comes in the middle of the flow, at the transition that inserts its step.
const maxNameLen = 512

// IsValidName reports whether name is a valid graph name or task name: 1 to 512 printable ASCII characters (a space
// through '~'), neither beginning nor ending with a space. "Charge", "Pay Now" and "billing.charge" are valid; "",
// " Charge", "Café" and "Line\nBreak" are not.
func IsValidName(name string) bool {
	// Every rule here is about the name as a key matched byte for byte - by transitions, Goto, fan-in, the stored
	// graph and the lists' filters:
	//   - Empty means "no task" throughout the engine (no successor, no Goto, no entry point yet), so a task by that
	//     name would be skipped, not run.
	//   - A padded name differs from its trimmed spelling in Go but not in a SQL comparison on MySQL or SQL Server,
	//     which ignores trailing spaces.
	//   - ASCII leaves no two spellings that look alike and differ: no Unicode normalization forms, homoglyphs or
	//     direction overrides. Nor invalid UTF-8, which encoding/json replaces with U+FFFD, so two names that differ
	//     only in invalid bytes would be one name in the stored graph and the engine would dispatch by a graph other
	//     than the one Validate passed; nor a NUL, which PostgreSQL refuses; nor a control character, which the
	//     renderers' escaping does not cover.
	if name == "" || len(name) > maxNameLen || name[0] == ' ' || name[len(name)-1] == ' ' {
		return false
	}
	for i := 0; i < len(name); i++ {
		if name[i] < ' ' || name[i] > '~' {
			return false
		}
	}
	return true
}

// IsValidURL reports whether url is a valid task URL or workflow URL. The rule is IsValidName's: 1 to 512 printable
// ASCII characters, neither beginning nor ending with a space. A host percent-encodes any other character.
func IsValidURL(url string) bool {
	return IsValidName(url)
}
