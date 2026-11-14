#!/usr/bin/env bash
# Usage: ./slow-mon.sh <seconds> <out> <dsn>  Every 250 ms, each statement active for over 500 ms: epoch, pid, its age
# in ms, its wait event, its first 200 characters - one line each (_BUGS.md #11: which statement a slow janitor round
# runs). One psql a sample: its startup costs the sampling rate, not the reading.
end=$(( $(date +%s) + $1 )); out=$2; dsn=$3
q="SELECT pid, (EXTRACT(EPOCH FROM clock_timestamp()-query_start)*1000)::int, COALESCE(wait_event_type,'-')||'/'||COALESCE(wait_event,'-'), left(regexp_replace(query,'\s+',' ','g'),200) FROM pg_stat_activity WHERE datname=current_database() AND state='active' AND pid<>pg_backend_pid() AND clock_timestamp()-query_start > interval '500 milliseconds'"
while (( $(date +%s) < end )); do
  psql "$dsn" -X -q -At -F' ' -c "$q" 2>/dev/null | sed "s/^/$(date +%s.%N | cut -c1-14) /" >> "$out"
  sleep 0.25
done
