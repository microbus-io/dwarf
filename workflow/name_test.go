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

import (
	"strings"
	"testing"

	"github.com/microbus-io/testarossa"
)

func TestIsValidName(t *testing.T) {
	assert := testarossa.For(t)

	for _, name := range []string{
		"x", "~", "Charge", "Pay Now", "Order (v2)", "billing.charge", "https://svc/pay?x=1&y=%C3%A9", "a  b",
		strings.Repeat("n", maxNameLen),
	} {
		assert.True(IsValidName(name), "%q", name)
		assert.True(IsValidURL(name), "%q", name)
	}
	for _, name := range []string{
		"", " ", "  ", " Charge", "Charge ", " Charge ", "\tCharge", "Charge\n", strings.Repeat("n", maxNameLen+1),
		"Jo\xffin", "Jo\x00in", "Jo\rin", "Jo\x01in", "Jo\x7fin", "Jo\u0085in", "Jo\u00e9in", "Joe\u0301in",
		"Jo\u202ein", "Jo\ufeffin", "Jo\u200din", "Jo\U0001F4E6in",
	} {
		assert.False(IsValidName(name), "%q", name)
		assert.False(IsValidURL(name), "%q", name)
	}
}
