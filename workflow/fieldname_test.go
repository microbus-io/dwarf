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
	"testing"

	"github.com/microbus-io/testarossa"
)

func TestIsValidFieldName(t *testing.T) {
	assert := testarossa.For(t)

	for _, name := range []string{
		"x", "_", "_x", "item", "orderID", "order_id", "x1", "X_9_y",
		"kebab-case-123", "camelCase_123", "a-", "_-", "x--y",
	} {
		assert.True(IsValidFieldName(name), "%s", name)
	}
	for _, name := range []string{
		"", "1x", "9", "-x", "-", "order id", "order.items", "é", "naïve", "x$", " x", "x ", "x\n",
		"total!", "total+", "x!!", "a[0]", "a=b", "a'b", `a"b`, "a\x00b",
	} {
		assert.False(IsValidFieldName(name), "%q", name)
	}
}
