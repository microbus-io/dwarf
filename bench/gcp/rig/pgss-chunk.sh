#!/usr/bin/env bash
# Usage: ./pgss-chunk.sh <delay-seconds> <out> <dsn>  After the delay, the janitor's chunk step reads from
# pg_stat_statements - calls, rows, mean and max ms, buffers a call - and the statements over 100 ms on average: what
# a large tree's chunks cost under load, read before the arm's database is dropped.
sleep "$1"; out=$2; dsn=$3
psql "$dsn" -X -q -At -F' | ' -c "SELECT calls, rows, round(mean_exec_time::numeric,2), round(max_exec_time::numeric,1), round(((shared_blks_hit+shared_blks_read)::numeric/GREATEST(calls,1)),0), left(regexp_replace(query,'\s+',' ','g'),200) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND query LIKE '%s.step_id%' ORDER BY total_exec_time DESC LIMIT 20" > "$out" 2>&1
echo "--- statements over 100 ms on average" >> "$out"
psql "$dsn" -X -q -At -F' | ' -c "SELECT calls, rows, round(mean_exec_time::numeric,2), round(max_exec_time::numeric,1), left(regexp_replace(query,'\s+',' ','g'),200) FROM pg_stat_statements WHERE dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND mean_exec_time > 100 ORDER BY total_exec_time DESC LIMIT 30" >> "$out" 2>&1
