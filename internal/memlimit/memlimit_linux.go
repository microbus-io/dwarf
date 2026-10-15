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
	"bufio"
	"bytes"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	// cgroupRoot is where the cgroup filesystem is mounted: the unified (v2) hierarchy itself, or the parent
	// of the v1 controller hierarchies.
	cgroupRoot = "/sys/fs/cgroup"
	// unlimited is the bar above which a cgroup limit means "none": v1 reports an unlimited cgroup as a page-
	// aligned value just under 2^63.
	unlimited = 1 << 62
)

// detectSystem returns the smaller of the cgroup memory limit and physical memory.
func detectSystem() (int64, string) {
	limit, source := memTotal(), SourcePhysical
	if self, err := os.ReadFile("/proc/self/cgroup"); err == nil {
		if c := cgroupLimit(cgroupRoot, self); c > 0 && (limit == 0 || c < limit) {
			limit, source = c, SourceCgroup
		}
	}
	if limit == 0 {
		return 0, SourceNone
	}
	return limit, source
}

// cgroupLimit returns the tightest memory limit on this process's cgroup and its ancestors, found under root
// from the contents of /proc/self/cgroup, or 0 when none is set. A limit can sit on any ancestor (a systemd
// slice, a pod above its container), so the walk takes the minimum all the way up. A path in
// /proc/self/cgroup that does not exist under root - a container's own cgroup namespace shows its cgroup as
// "/", while a v1 mount may show the host's path - is walked up until it does; the cgroup the process sees as
// its root is checked last.
func cgroupLimit(root string, self []byte) int64 {
	best := int64(0)
	take := func(file string) {
		if c := readLimit(file); c > 0 && (best == 0 || c < best) {
			best = c
		}
	}
	for _, line := range strings.Split(string(self), "\n") {
		// hierarchy-ID:controllers:path
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		var dir, file string
		switch {
		case parts[0] == "0" && parts[1] == "":
			dir, file = root, "memory.max" // v2
		case hasController(parts[1], "memory"):
			dir, file = path.Join(root, "memory"), "memory.limit_in_bytes" // v1
		default:
			continue
		}
		for p := path.Clean("/" + parts[2]); ; p = path.Dir(p) {
			take(path.Join(dir, p, file))
			if p == "/" {
				break
			}
		}
	}
	return best
}

// hasController reports whether a /proc/self/cgroup controller list names the controller.
func hasController(list, name string) bool {
	for _, c := range strings.Split(list, ",") {
		if c == name {
			return true
		}
	}
	return false
}

// readLimit reads a cgroup memory limit file, returning 0 when it is absent, unlimited or unreadable.
func readLimit(file string) int64 {
	data, err := os.ReadFile(file)
	if err != nil {
		return 0
	}
	n, err := strconv.ParseInt(string(bytes.TrimSpace(data)), 10, 64)
	if err != nil || n <= 0 || n >= unlimited {
		return 0
	}
	return n
}

// memTotal returns physical memory from /proc/meminfo, or 0 when it cannot be read.
func memTotal() int64 {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}
	defer f.Close()
	return parseMemTotal(f)
}

// parseMemTotal reads the MemTotal line of /proc/meminfo's format, in bytes, or 0 when there is none.
func parseMemTotal(meminfo io.Reader) int64 {
	scanner := bufio.NewScanner(meminfo)
	for scanner.Scan() {
		rest, ok := strings.CutPrefix(scanner.Text(), "MemTotal:")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return 0
		}
		kb, err := strconv.ParseInt(fields[0], 10, 64)
		if err != nil || kb <= 0 {
			return 0
		}
		return kb * 1024
	}
	return 0
}
