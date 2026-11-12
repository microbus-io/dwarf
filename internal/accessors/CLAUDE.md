# Dwarf `internal/accessors` — how a test reaches a running engine's internals

> Load when: adding or changing an accessor, or `engineundertest.go`'s `init` that fills them.
> Coupled with: `internal/diagnostics/CLAUDE.md` (the seam names a test arms through `Seams`) and
> `fixtures/CLAUDE.md` (the black-box tests that use them).

A black-box test (one outside package engine, e.g. `fixtures`) or a benchmark sometimes needs an internal: the shard
set to inspect rows (`DB`), the seam registry to arm faults and wait on checkpoints (`Seams`), the live tables in
every status (`ListLive`), an archiving pass with no grace (`ArchiveNow`), the archive's days (`ArchiveDays`), the
closed-flow count (`FlowsClosed`), the refill-period override (`SetRefillInterval`) or the derived budget
(`ShardBudget`). Each is a **function variable taking the engine as an `any`**, filled by `engineundertest.go`'s
`init`, because this package cannot import `engine`, which imports it. They panic when given anything but an
`*engine.Engine`, and are nil until package engine is linked in. None of it is on the engine's exported surface.

**It must stay production-safe:** `engineundertest.go` is production code, so this package must not import
`testing`, `testarossa` or anything test-only.

A test that needs an unexported method or forges internal columns (failStep, the refiller, pool sizing) stays a
white-box test in package engine rather than growing an accessor.
