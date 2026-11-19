#!/usr/bin/env bash
# Usage: ./clk-mon.sh <seconds> <out> <dsn>   Every 5 s: this host's epoch, the database's clock_timestamp() and the
# database clock's offset from this host (from the midpoint of the round trip, so good to the psql startup's few ms).
# The record that a step happened when the log says it did, and that nothing stepped it back.
end=$(( $(date +%s) + $1 )); out=$2 dsn=$3
while (( $(date +%s) < end )); do
  a=$(date +%s.%N); d=$(psql "$dsn" -X -At -c 'SELECT extract(epoch FROM clock_timestamp())' 2>/dev/null); b=$(date +%s.%N)
  [ -n "$d" ] && awk -v a="$a" -v b="$b" -v d="$d" 'BEGIN { m = (a + b) / 2; printf "%.3f %.3f %+.3f\n", m, d, d - m }' >> "$out"
  sleep 5
done
