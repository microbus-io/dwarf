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

package memlimit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/microbus-io/testarossa"
)

// writeFile creates root/rel with content, making its directories.
func writeFile(t *testing.T, root, rel, content string) {
	t.Helper()
	file := filepath.Join(root, rel)
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestMemlimit_ReadLimit(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	dir := t.TempDir()
	writeFile(t, dir, "v2", "2147483648\n")
	writeFile(t, dir, "max", "max\n")
	writeFile(t, dir, "v1unlimited", "9223372036854771712\n")
	assert.Equal(int64(2147483648), readLimit(filepath.Join(dir, "v2")))
	assert.Equal(int64(0), readLimit(filepath.Join(dir, "max")))
	assert.Equal(int64(0), readLimit(filepath.Join(dir, "v1unlimited")), "v1's unlimited value is no limit")
	assert.Equal(int64(0), readLimit(filepath.Join(dir, "absent")))
}

// TestMemlimit_CgroupV2WalksAncestors pins that a limit on an ancestor counts, and the tightest one wins: a
// systemd service's limit sits on its own cgroup, a pod's on a parent of its container's.
func TestMemlimit_CgroupV2WalksAncestors(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	root := t.TempDir()
	writeFile(t, root, "kubepods/pod1/memory.max", "8589934592\n")
	writeFile(t, root, "kubepods/pod1/ctr/memory.max", "max\n")
	writeFile(t, root, "kubepods/memory.max", "17179869184\n")
	assert.Equal(int64(8589934592), cgroupLimit(root, []byte("0::/kubepods/pod1/ctr\n")))

	// In its own cgroup namespace a container sees its cgroup as the root.
	writeFile(t, root, "memory.max", "4294967296\n")
	assert.Equal(int64(4294967296), cgroupLimit(root, []byte("0::/\n")))

	// Nothing set anywhere.
	assert.Equal(int64(0), cgroupLimit(t.TempDir(), []byte("0::/system.slice/app.service\n")))
}

// TestMemlimit_CgroupV1 pins the v1 layout: the memory controller's own hierarchy, found by name in a
// comma-separated controller list, and a host path that does not exist in the container's mount walked up
// to the mount's root.
func TestMemlimit_CgroupV1(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	root := t.TempDir()
	writeFile(t, root, "memory/memory.limit_in_bytes", "2147483648\n")
	self := strings.Join([]string{
		"12:cpu,cpuacct:/docker/abc",
		"11:memory:/docker/abc",
		"1:name=systemd:/docker/abc",
	}, "\n")
	assert.Equal(int64(2147483648), cgroupLimit(root, []byte(self)))
}

func TestMemlimit_ParseMemTotal(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	meminfo := "MemFree:         1000 kB\nMemTotal:       65536 kB\n"
	assert.Equal(int64(65536*1024), parseMemTotal(strings.NewReader(meminfo)))
	assert.Equal(int64(0), parseMemTotal(strings.NewReader("MemFree: 1 kB\n")))
}
