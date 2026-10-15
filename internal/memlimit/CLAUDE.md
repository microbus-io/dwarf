# `internal/memlimit` - how much memory this process may use

> Load when: changing how the engine sizes a memory-bound default (the state-ref resolve cache), or porting
> detection to another OS.

`Detect` answers one question for sizing caches as a share of memory. The order and the walk are deliberate:

- **`GOMEMLIMIT` first.** It is the operator's own statement of the budget, it works the same in and out of
  containers, and the Go runtime enforces it - near it the collector works harder rather than letting the heap
  reach twice its live size. Read it with `debug.SetMemoryLimit(-1)`; `math.MaxInt64` means unset.
- **Then, on Linux, the tightest cgroup limit on the process's own cgroup AND every ancestor.** Do not read only
  `/sys/fs/cgroup/memory.max`: that file is the limit only when the process's cgroup is the namespace root (a
  container in its own cgroup namespace). A systemd service's `MemoryMax=`, a pod's limit on a parent of its
  container, and a container sharing the host's cgroup namespace all put the limit elsewhere, and missing it
  sizes the cache from physical memory - a 64 GB host limited to 8 GB would get a 16 GB cache. The path comes
  from `/proc/self/cgroup`; a path that does not exist under the mount (v1 shows the host's path inside a
  container) is walked up until it does.
- **Values of 2^62 and up are "unlimited".** cgroup v1 reports an unlimited cgroup as a page-aligned number just
  under 2^63; v2 writes `max`.
- **Physical memory caps the cgroup answer** (`/proc/meminfo` `MemTotal`), so an unreadable or absent cgroup
  still yields a number.
- **Assumed mount point: `/sys/fs/cgroup`.** Parsing `/proc/self/mountinfo` for a relocated mount is not done; a
  host that mounts cgroups elsewhere falls back to physical memory.
- **Nothing outside Linux.** There is no portable physical-memory read without a dependency, and dwarf takes
  none; the caller falls back to a fixed size (the engine: 1 GiB for its state cache).

Tests: the cgroup walk and parsing take a root directory and the `/proc/self/cgroup` bytes, so they run against
a temp tree (`memlimit_linux_test.go`, Linux only). On macOS they compile with `GOOS=linux go test -c`; run them
in any arm64/amd64 Linux container by copying the test binary in.
