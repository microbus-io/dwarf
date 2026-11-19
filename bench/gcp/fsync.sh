#!/usr/bin/env bash
# Usage: ./fsync.sh <dsn>...  Per DSN, one line: the drawn hardware's WAL flush floor, measured from SQL. On ONE psql
# session, 40 SELECT 1 and 40 single-row INSERTs into a scratch table, each its own transaction: an INSERT's commit
# waits for its WAL to reach the disk and a SELECT's does not, so their difference is one small, uncontended WAL
# flush - on Cloud SQL, the network round trip to its Persistent Disk plus the write. Prints
#   "rtt <min> <median>  commit <min> <median>  flush <median commit - median rtt>" in ms (first sample of each dropped).
# Run it on an idle instance: under load a flush also queues and carries more bytes. Cloud SQL on PostgreSQL 16 does
# not allow track_wal_io_timing, so the server cannot time its own flushes; this is the substitute. The scratch table
# (fsync_probe) is created and dropped in the DSN's database.
for d in "$@"; do
  { echo 'CREATE TABLE IF NOT EXISTS fsync_probe (x int);'
    echo '\timing on'
    for i in $(seq 1 41); do echo 'SELECT 1;'; done
    for i in $(seq 1 41); do echo 'INSERT INTO fsync_probe VALUES (1);'; done
    echo '\timing off'
    echo 'DROP TABLE fsync_probe;'; } | psql "$d" -X -At 2>&1 |
    awk '/^Time:/ { n++; v = $2 + 0; if (n == 1 || n == 42) next; if (n <= 41) s[++ns] = v; else c[++nc] = v }
      function med(a, k,   i, j, t) { for (i = 1; i <= k; i++) for (j = i + 1; j <= k; j++) if (a[j] < a[i]) { t = a[i]; a[i] = a[j]; a[j] = t }
        return k ? a[int((k + 1) / 2)] : -1 }
      END { if (!ns || !nc) { print "?"; exit } ms = med(s, ns); mc = med(c, nc)
        printf "rtt %.3f %.3f  commit %.3f %.3f  flush %.3f\n", s[1], ms, c[1], mc, mc - ms }'
done
