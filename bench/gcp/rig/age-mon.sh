#!/usr/bin/env bash
# Usage: ./age-mon.sh <seconds> <out> <dsn>  Every 10 s: epoch, then for each closed status (completed, failed,
# terminated, cancelled) the age in seconds of the oldest closed root not archived yet, 0 for none - how far archiving
# is behind, from the database rather than the engine's gauge. Each is one seek into idx_dwarf_flows_status.
end=$(( $(date +%s) + $1 )); out=$2; dsn=$3
q=""
for s in completed failed terminated cancelled; do
  q+="${q:+, }COALESCE(EXTRACT(EPOCH FROM (now() AT TIME ZONE 'UTC') - (SELECT MIN(updated_at) FROM dwarf_flows"
  q+=" WHERE status='$s' AND surgraph_flow_id=0 AND delete_after_ms=0))::int, 0)"
done
while (( $(date +%s) < end )); do
  r=$(psql "$dsn" -X -q -At -F' ' -c "SELECT $q" 2>&1 | tr '\n' ' ')
  echo "$(date +%s) $r" >> "$out"
  sleep 10
done
