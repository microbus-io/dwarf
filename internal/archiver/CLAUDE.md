# Dwarf `internal/archiver` — one shard's archive day tables

> Load when: changing how archive day tables are made, migrated, dropped or named.
> Coupled with: `internal/migrations/CLAUDE.md` ("The archive day migrations": the day tables' SQL, their indexes and
> the rules a migration touching them follows), `engine/CLAUDE.md` ("The archive": the engine's wiring, retention and
> the maintenance loop) and `internal/database/CLAUDE.md` (the owner check at shard open).

Each shard holds, beside the live `dwarf_flows`/`dwarf_steps`, one pair of tables per day: `dwarf_flows_archive_<n>`
and `dwarf_steps_archive_<n>`, `n` the days since 2000-01-01 UTC (the origin the creation hour also counts from).
They are plain tables, not partitions: a lookup names its day's table, and retention is a `DROP TABLE`, which reclaims
the space without writing WAL in proportion to the data.

**A day's tables are made by running the archive migrations for that day, not by copying another table's definition.**
The Archive is given the archive migrations (`migrations.Archive()`, `internal/migrations/archive/`),
`migrations.ArchiveDay` substitutes the day's table names into them, and the Archive runs the result as the day's own
migration sequence (`migrations.SequenceName(day)`) through sequel's runner, which does all the parsing. No template
table exists: `live/` makes none, so nothing but a day is ever named `dwarf_*_archive*`. An archive migration changes a
day's tables and nothing else: a live table in it would be changed once per day. Three things follow:
- **One source of truth on every dialect.** There is no portable "create like": SQLite and SQL Server have none, and
  SQL Server's `SELECT ... INTO` copies the identity property, which an archive id must not have (it keeps its live
  value, which state refs point at).
- **The SQL is exercised by sequel**, the same runner and splitting rules as the schema.
- **A later archive migration reaches the existing days**: the engine's Startup runs `EnsureToday` (today and tomorrow)
  and then `MigrateDays` on each shard, before anything reads a day. Not inside the shard's open: the
  database layer knows connections and the schema, not this package's tables. `MigrateDays` migrates every day before
  today that the catalog lists, back to today minus the retention, one `Migrate` each - an up-to-date day costs sequel's
  one read of its progress, since sequel makes sure of its own table once per process. **A day whose migration fails
  fails Startup**, as a failed schema migration does: left behind, the day would keep an older schema that a later read
  trips over long after a log line went by. An operator repairs or drops the day's tables by hand. An older listed day -
  expired, not dropped yet - is left for the next `Maintain` to drop, and nothing may read past the retention, since
  such a day can be behind. **An archive migration runs over every retained day at the next Startup, on every replica at
  once**, so keep one cheap on a full table (on PostgreSQL an `ADD COLUMN` with a constant default rewrites nothing; a
  type change rewrites every day). In a rolling upgrade a day an old build makes after the new replicas started is
  brought up when a new-build dispatcher ensures it as today or tomorrow, and otherwise at the next Startup.

**The catalog says which days exist; nothing here reads or writes `sequel_migrations`.** The table is sequel's own,
its schema differs by dialect, and a sequel release may change it. Two consequences:
- **A dropped day keeps its migration records**, one row per archive migration per day (about 730 a year with the two
  archive migrations), so it can never be made again: its records say its migrations so far are done, so its migration
  makes nothing; `ensure` then looks for both tables in the catalog and fails naming the day, rather than report a day
  that is not there (`EnsureToday`, and so Startup, for today). The same holds for tables dropped by hand, or a restore
  that left them out but kept `sequel_migrations`: the fix is to delete the day's rows from `sequel_migrations`, which
  the engine never does. So nothing drops a day still being written. `Maintain` drops only days before today minus the
  retention, which is never negative, and `DropBefore` refuses a day after today. Dropping only the oldest days leaves
  no gap between the days kept. Gaps come only from days no pass made: the engine down, or every pass of the day
  failing. So whether a day exists is read from the listing, never assumed from a range of numbers.
- **A drop can be half done when a day is migrated.** A peer's drop lands between the listing and the migration, or
  a drop off PostgreSQL (two statements there, one transaction on PostgreSQL) stops between the day's two tables.
  It matters only when a migration is pending: its `ALTER` then fails on a missing table. `migrateExisting`
  re-lists the tables after a failure and skips a day missing either one - it is past, so never written, and the
  next drop finishes it. Without that a half-dropped day inside the retention would fail every replica's Startup.
- **The listing reads the connection's own schema** (`current_schema()`, `DATABASE()`, `SCHEMA_ID()`), the one the
  drops and the `OBJECT_ID` guards resolve to. Listed across schemas, as a bare `sys.tables` does on SQL Server, a
  second deployment's tables would be migrated - made, here - and dropped as if they were this one's.

**Today and tomorrow must exist; nothing else is made.** Rows are archived into the day they are archived, so no past
day is written - but for the batch a pass has in flight when the day turns, which lands in the day that just ended (the
reaper re-reads the day before its next batch). At a retention of 1 or more that day is kept through the whole of the
next; at 0 nothing is archived. `EnsureToday` (Startup) and `Maintain` (every pass) make both today's and tomorrow's
tables, so each of a day's 24 passes is an attempt at tomorrow's: a day is missing at its start only after a whole
day of failed passes. Do not make tomorrow only near its start to save the empty tables - a window of a pass or two
turns a short outage into a missing today. `Maintain` makes them before it drops the expired days, both from one
reading of today, and drops even when making them failed: the likeliest cause is a full database, which the drops
free, and a drop never reaches a day being written.

**A day that cannot be dropped does not hold back the next.** `dropDays` goes on past a failed day - its lock held
past PostgreSQL's 2-second timeout - and returns the failures joined, so the next pass retries it.

**Racing replicas converge**, because the archive statements are idempotent (`IF NOT EXISTS`, or the `OBJECT_ID` /
`sys.indexes` guards on SQL Server) and sequel's runner locks each migration number per sequence. Idempotence is
load-bearing beyond the race: the lock is a lease the runner renews while it works, so a runner that dies part-way,
or whose renewal fails, leaves the migration to a peer that runs it again over whatever the first made.
`TestArchiver_EveryArchiveStatementIsIdempotent` replays a day's whole migration over its tables.

**sequel's `Migrate` takes no context**, so an `ensure` in flight runs its migration to the end however its caller
is cancelled; `MigrateDays` and the drops check the context between days, and that is the granularity of a stop.

**Today comes from the database clock**, read as whole seconds since the Unix epoch and divided in Go. A Go clock
skewed across midnight from the database would ensure the wrong days, and doing the division in Go avoids each
dialect's rounding rules (a `CAST` rounds on PostgreSQL and MySQL). Tests replace the clock (`SetClock`).

**`EnsureToday` retries a few times, not until it works.** A transient failure clears within its retries; anything
else - a missing privilege, a full disk, a broken migration - fails Startup with the error rather than hanging it
silently.

**Every replica of a shard connects as the role that owns dwarf's tables**, and the Archive assumes it: on PostgreSQL a
table belongs to the role that made it, so a day made by another role could not be dropped by its peers, and nothing
would show it until retention failed. The check is not here but at shard open (`internal/database`, `checkOwner`),
since every later migration that alters a table needs the owner too. Do not hand a day table to the live tables' owner
with `ALTER TABLE ... OWNER TO`: it needs `SET` on the owner role and `CREATE` on the schema, and failing over it helps
nobody. On MySQL and SQL Server the one role matters to the listing as well: the catalog shows a user only the tables
it holds a privilege on.

**A drop takes a 2-second lock timeout on PostgreSQL** (`SET LOCAL lock_timeout`). `DROP TABLE` takes an access
exclusive lock, which waits behind every statement reading the table while every later statement queues behind the
wait; bounded, a pass that cannot get the lock fails and the next one retries. MySQL and SQL Server have no
per-transaction form (their lock timeouts are session settings, which would stay on the pooled connection), so there
a drop waits for its lock unbounded.

**Retention is in days, and a day is kept until it ENDED `retention` days ago** (`Maintain` drops `d < today -
retention`, written as a bound on `d` so a retention near the largest int cannot overflow; `MigrateDays` uses the
same bound). So nothing is dropped less than `retention` days after it was archived, and a retention of 1 keeps
yesterday through the whole of today.

**The day length is a parameter, for tests and the benchmark only.** Production days are `DefaultDayLength`; a test
passes seconds, and the engine reads `DWARF_ARCHIVE_DAY` for the rig. Day numbers of different lengths are different
counts, so a database must only ever see one length. `MigrateDays` refuses a listed day past tomorrow - a shorter
length's numbering, seen by a longer one - so Startup fails rather than keep both. The other way round is not caught:
a shorter length reads a longer one's days as long expired, and its retention drops them.

**`TodayLeft` rounds down.** The clock reads whole seconds, so up to one has passed by the reading; rounded down, the
reaper never takes a day to last past its end.

Pinned by `archiver_test.go` (the substitution itself by `internal/migrations`' tests): a day made with the live tables'
columns and their flows indexes (all but the deletion one), a written id and PostgreSQL's freeze settings, a whole migration replayed over existing tables, eight racing
ensures converging, the oldest days dropped and never made again (an `ensure` of one makes nothing, and fails on a later
migration), retention by days up to the largest int, tomorrow made with today, a later migration
reaching the retained days once - the day exactly the retention old included - and leaving the one before, the day
count, a failed day migration failing the call unless the day is missing a table by then, and a listed day past
tomorrow refused.
