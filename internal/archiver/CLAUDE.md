# Dwarf `internal/archiver` — one shard's archive day tables

> Load when: changing how archive day tables are made, migrated, dropped, named or located, or the directory.
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
- **A later archive migration reaches the existing days**: the engine's Startup runs `EnsureToday` (today and the two days
  after) and then `MigrateDays` on each shard, before anything reads a day. Not inside the shard's open: the
  database layer knows connections and the schema, not this package's tables. `MigrateDays` migrates every day before
  today that the catalog lists, back to today minus the retention, one `Migrate` each - an up-to-date day costs sequel's
  one read of its progress, since sequel makes sure of its own table once per process. **A day whose migration fails
  fails Startup**, as a failed schema migration does: left behind, the day would keep an older schema that a later read
  trips over long after a log line went by. An operator repairs or drops the day's tables by hand. An older listed day -
  expired, not dropped yet - is left for the next `Maintain` to drop, and nothing may read past the retention, since
  such a day can be behind. **An archive migration runs over every retained day at the next Startup, on every replica at
  once**, so keep one cheap on a full table (on PostgreSQL an `ADD COLUMN` with a constant default rewrites nothing; a
  type change rewrites every day). In a rolling upgrade a day an old build makes after the new replicas started is
  brought up when a new-build dispatcher ensures it as today or a day ahead, and otherwise at the next Startup.

**The catalog says which days exist; nothing here reads or writes `sequel_migrations`.** One query reads it
(`catalog`), returning the days with both tables and those with one; maintenance alone calls it - `ensure`'s check,
`MigrateDays`, the drops - and never a read path (below). The table is sequel's own,
its schema differs by dialect, and a sequel release may change it. Two consequences:
- **A dropped day keeps its migration records**, one row per archive migration per day (about 730 a year with the two
  archive migrations), so it can never be made again: its records say its migrations so far are done, so its migration
  makes nothing; `ensure` then looks for both tables in the catalog and fails naming the day, rather than report a day
  that is not there (`EnsureToday`, and so Startup, for today). The same holds for tables dropped by hand, or a restore
  that left them out but kept `sequel_migrations`: the fix is to delete the day's rows from `sequel_migrations`, which
  the engine never does. So nothing drops a day still being written. `Maintain` drops only days before today minus the
  retention, which is never negative, and `DropBefore` refuses a day after today. Dropping only the oldest days leaves
  no gap between the days kept. Gaps come only from days no pass made: the engine down, or every pass of the day
  failing. So maintenance reads which days exist from the catalog, never assumes them from a range of numbers: a
  drop walking down from the cutoff until the first missing day would strand everything older than a gap.
- **A drop can be half done when a day is migrated.** A peer's drop lands between the listing and the migration, or
  a drop off PostgreSQL (two statements there, one transaction on PostgreSQL) stops between the day's two tables.
  It matters only when a migration is pending: its `ALTER` then fails on a missing table. `migrateExisting`
  reads the catalog again after a failure and skips a day missing either one - it is past, so never written, and the
  next drop finishes it. Without that a half-dropped day inside the retention would fail every replica's Startup.
- **The catalog read is of the connection's own schema** (`current_schema()`, `DATABASE()`, `SCHEMA_ID()`), the one the
  drops and the `OBJECT_ID` guards resolve to. Listed across schemas, as a bare `sys.tables` does on SQL Server, a
  second deployment's tables would be migrated - made, here - and dropped as if they were this one's.

**Today and the two days after it must exist; nothing else is made.** Rows are archived into the day they are
archived, so no past day is written - but for the batch a pass has in flight when the day turns, which lands in the day
that just ended, its `archived_at` clamped to that day's last millisecond (`DayEnds`; the engine's reaper also stops a
margin before midnight, so this is rare). At a retention of 1 or more that day is kept through the whole of the next;
at 0 nothing is archived. The reaper makes no table, and archives only while the archive's record holds tomorrow
whole (`Whole`). `EnsureToday` (Startup) and `Maintain` (every maintenance pass) make the three days, so a day exists a whole day before it begins, and is missing at its start
only after two days of failed passes. **The day after tomorrow keeps a locate's `created+1` in place**: for a flow
created just after midnight, `created+1` is the new tomorrow, made the day before as the day after tomorrow. Making
only tomorrow leaves it missing from midnight until the first pass after it, and every locate of such a key would
first fail on it. Do not make a day only near its start to save the empty
tables - a window of a pass or two turns a short outage into a missing today. `Maintain` makes them before it drops
the expired days, both from one reading of today, and drops even when making them failed: the likeliest cause is a
full database, which the drops free, and a drop never reaches a day being written.

**A day seen whole is not checked again** (`ensured`, the today of the last `ensureCurrent` that saw all three whole).
Nothing drops today or a day after it - `Maintain` drops days before today minus the retention, `DropBefore` refuses a
day after today - so a maintenance pass reads only the clock until the day changes, rather than a `Migrate` and a
catalog read per day. The record is per Archive, so each replica, and each Startup, checks once. What it misses is a
current day's tables dropped by hand; the reaper's move then fails, and the failure grace deletes the closed trees
unarchived until the day changes.

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
counts, so a database must only ever see one length. `MigrateDays` refuses a day the catalog lists past the day after
tomorrow - a shorter length's numbering, seen by a longer one - so Startup fails rather than keep both. The other way round is not caught:
a shorter length reads a longer one's days as long expired, and its retention drops them.

**`TodayLeft` rounds down.** The clock reads whole seconds, so up to one has passed by the reading; rounded down, the
reaper never takes a day to last past its end.

### Locating an archived row

**A locate is one statement, and reads no catalog of its own** (`LocateFlow`): a `UNION ALL` of the directory's row for the flow
and primary-key probes of the flows tables of its creation day and the day after, each branch selecting its day. It
does not check the token - the caller's read of the day, with the flow's token and hour, says whether the flow is the
one asked for - and a step is found by its flow (its key names it), so the directory holds no steps. One round trip, at
most three relations.

- **Do not walk days, cache a flow-to-day map, or `UNION ALL` many days.** A walk over the retained days in one
  statement locks a relation per day on PostgreSQL until the statement ends: about 160 at 30 days, and about 40% of
  connections doing it exhausts the shared lock table. Three relations are not many.
- **The directory (`dwarf_flow_days`, a live table: `flow_id` to `day`) names only what the probes cannot find**: a
  flow archived two or more days after its creation day, or on a day before it (a straggler created while a batch
  begun before midnight ran, reachable only when a day is shorter than an hour). `DirectoryHours(day)` gives the
  creation hours outside which a flow archived on `day` is named, and the reaper writes the rows in the move's own
  transaction from the same rows it moves. Keeping both halves of the rule here - the probes and the hours - is what
  keeps them agreeing. So the directory holds the flows that ran about a day or longer (long approvals, long sleeps),
  none of the minute- or hour-long flows that make up the volume. **Do not make it name every archived flow** to drop
  the probes: at a realistic rate that is about 26 M rows a day, 780 M at a 30-day retention, and a 26 M-row delete a
  day - the mass-delete vacuum the day tables' `DROP` exists to avoid.
- **The creation day agrees with the archive day only because both clocks are the database's.** The hour and the day
  count from the same origin and both round down, so `hour x 3600 / dayLength` is the day the hour falls in, for any
  day length. The hour comes from the column default, which `TestCreatedHour_EveryRowCarriesItsFlowsHour` (engine)
  pins, and the day from the database clock too. An hour computed in Go would let a replica with a skewed clock write
  a flow into a day its key does not name, with no directory row to say so. With days shorter than an hour (tests,
  `DWARF_ARCHIVE_DAY`), the hour's day is the day the hour began, so every move but one within about two days of that
  is named.
- **It probes the days the archive knows** (`known`: the whole days at the last catalog read - Startup, every `ensure`,
  every maintenance pass, and `Refresh` on a replica that maintains nothing there - less the days this archive dropped
  since), **and any day later than every one it knows.** A day it lacks below its newest was dropped or never made, and
  nothing makes it later, so it is skipped: an expired key costs the directory alone, one statement and no error. A
  later day may have been made since by a peer, so it is probed. **The record is stale in both directions for up to a
  pass interval (an hour)**: a day made since is probed as later than every known one, and a known day a peer dropped
  fails the statement, and so costs a server-side error, on every locate that probes it until the next read.
- **On a missing table** (`sequel.IsUndefinedTableError`) the locate leaves out the oldest day it probed and runs the
  statement again - the day after, then the directory alone, which a drop cannot reach. The statement does not say
  which table was missing, and need not. Days drop oldest first, so a missing creation day is the usual case; and **a
  row goes into a day only once the day after it is whole** (`ensureCurrent` makes the three days in order and records
  them only together, and the reaper archives only after it), so when the day after the creation day is missing, the
  creation day holds nothing of the flow either. Keep that order: an archive into a day whose next day may be missing
  would make this leave out a day that holds the flow. A flow created on day 1 and archived on day 5 is still found
  once days 1 and 2 are gone.
  - **Do not let a locate change the record.** Only a catalog read sets it, and a drop by this archive narrows it. What
    a failed statement proves is unsound to keep: a day can be made between a locate's two statements, so "the
    statement without it succeeded" does not prove a day missing, and a forgotten day that holds flows is then
    skipped until the next read; and forgetting the newest known day makes it "later than every known day", probed
    again on every locate. The price of not learning is the stale window above: each failed probe is a server-side
    error PostgreSQL logs with its text (`log_min_error_statement` defaults to `ERROR`). A key whose days never
    existed (a forged future hour) costs two each time.
  - **Do not read the catalog here.** A locate runs at the API's cadence, not the engine's, so a cached listing must be
    re-read on a miss - and a burst of misses (the new tomorrow not yet made at midnight, forged keys) then reads it
    once per locate (measured 31 reads for 32 concurrent locates), which takes a shared, timed read to bound.
  - **Any other error fails the locate**, never reads as a 404: a 404 on a lost connection tells the caller the flow
    is gone. The cancelled caller's error included.
  - **`IsUndefinedTableError` needs sequel v1.15.4 or later**: PostgreSQL raises `42P01` for a bad `FROM`-clause
    qualifier as well as a missing table, and earlier versions read that SQL mistake as a missing table - a 404 on
    PostgreSQL alone.
- **A day the directory names is returned as written.** It may have been dropped before retention deleted its rows,
  and the caller's read of it then fails on the missing table; the read paths take `IsUndefinedTableError` as not
  found. On MySQL a probe of a table being dropped waits behind the `DROP`'s metadata lock, which can itself wait on a
  long statement; only a locate of a day being dropped meets it, and the caller's deadline bounds it.
- **A day with one table** - half made, or off PostgreSQL half dropped - holds no row a locate may find (the reaper
  writes only into a day `ensure` saw whole, and a drop is of an expired day). With its flows table gone it reads as
  dropped; with its steps table gone its flows table answers, empty.
- **Retention deletes a day's directory rows** (`forgetDaysBefore`, every `Maintain` pass and `DropBefore`), by a bound
  rather than per day dropped, so rows a failed pass left behind go with the next, `forgetBatch` (4,096) rows at a time -
  a select of their ids by the day index, then a delete by those ids - so a day of long-running flows is not one long
  delete.

Pinned by `locate_test.go`: the creation day, the day after and the directory's day each found with no catalog read, a
day the archive lacks below its newest not probed and one later than its newest probed, a peer's drop still probed and
a day made below the newest unseen until the next catalog read, a day dropped by the archive itself forgotten at once, a probed day
preferred to a directory row, a dropped directory day returned as written, the creation day dropped and
then both (the directory alone finding a later day), a day with either table missing, a failure other than a missing
table (one only a probe meets) not read as a 404, `DirectoryHours` against the creation day for several day lengths, and retention deleting the
directory's expired rows across batches. The engine's `directory_test.go` pins the reaper's half - which moves it names - and a reader's `Refresh` on every archive pass.

Pinned by `archiver_test.go` (the substitution itself by `internal/migrations`' tests): a day made with the live tables'
columns and their flows indexes (all but the deletion one), a written id and PostgreSQL's freeze settings, a whole migration replayed over existing tables, eight racing
ensures converging, the oldest days dropped and never made again (an `ensure` of one makes nothing, and fails on a later
migration), retention by days up to the largest int, the two days ahead made with today and not checked again until the day
changes - unless one could not be made - a later migration
reaching the retained days once - the day exactly the retention old included - and leaving the one before, the day
count, a failed day migration failing the call unless the day is missing a table by then, and a day past the day
after tomorrow refused.
