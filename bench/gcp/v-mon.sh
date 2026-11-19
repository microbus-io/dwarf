#!/usr/bin/env bash
# Usage: ./v-mon.sh <seconds> <out> <dsn>   Every 60 s: epoch db_bytes steps_bytes steps_idx_bytes flows_bytes
# steps_live steps_dead steps_autovac flows_dead cache_hit_pct
end=$(( $(date +%s) + $1 )); out=$2; dsn=$3
q="SELECT pg_database_size(current_database()), pg_total_relation_size('dwarf_steps'), pg_indexes_size('dwarf_steps'), pg_total_relation_size('dwarf_flows'), (SELECT n_live_tup FROM pg_stat_user_tables WHERE relname='dwarf_steps'), (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='dwarf_steps'), (SELECT autovacuum_count FROM pg_stat_user_tables WHERE relname='dwarf_steps'), (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='dwarf_flows'), (SELECT round(100.0*blks_hit/nullif(blks_hit+blks_read,0),2) FROM pg_stat_database WHERE datname=current_database())"
while (( $(date +%s) < end )); do
  echo "$(date +%s) $(psql "$dsn" -X -q -At -F' ' -c "$q" 2>&1 | tr '\n' ' ')" >> "$out"; sleep 60
done
