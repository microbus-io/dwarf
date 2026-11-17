#!/usr/bin/env bash
# Usage: ./ff-set.sh <dsn> <fillfactor> <out> <sample-at-s>...   Sets dwarf_flows' fillfactor as soon as the bench's
# migrations have made the table: an insert leaves the reserve free on whatever page it lands, so the few rows written
# before the setting take effect do not matter once the archive has turned the table over. Then, at each
# <sample-at-s> after the setting, one line to <out>: epoch, live rows, their mean size in bytes, and pgstattuple's
# table_len tuple_count tuple_len dead_tuple_count dead_tuple_len free_space free_percent (an exact scan of the
# table, which the statistics' n_live_tup and relation size only estimate).
dsn=$1 ff=$2 out=$3; shift 3
q() { psql "$dsn" -X -q -At -F' ' -c "$1" 2>&1 | tr '\n' ' '; }
for i in $(seq 1 1200); do
  psql "$dsn" -X -q -At -c "ALTER TABLE dwarf_flows SET (fillfactor = $ff)" >/dev/null 2>&1 && break
  sleep 0.25
done
t0=$(date +%s)
echo "$t0 set $(q "SELECT COALESCE(reloptions::text, 'default') FROM pg_class WHERE relname = 'dwarf_flows'")" >> "$out"
q "CREATE EXTENSION IF NOT EXISTS pgstattuple" >/dev/null
for at in "$@"; do
  now=$(date +%s); (( t0 + at > now )) && sleep $(( t0 + at - now ))
  echo "$(date +%s) $(q "SELECT count(*), round(avg(pg_column_size(f.*))) FROM dwarf_flows f") $(q "SELECT table_len, tuple_count, tuple_len, dead_tuple_count, dead_tuple_len, free_space, round(free_percent::numeric, 1) FROM pgstattuple('dwarf_flows')")" >> "$out"
done
