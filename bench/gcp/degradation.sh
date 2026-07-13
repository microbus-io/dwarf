#!/usr/bin/env bash
#
# Fill-and-probe driver for the degradation-vs-volume runs. Runs ON the bench VM, next to the
# deployed dwarf-bench binary. Fills a database with the axis workload, pausing at each checkpoint
# to (1) run the standard 60s linear probe and (2) snapshot database-side stats (table/index sizes,
# live/dead tuples, vacuum counts, cache hit ratios). Throughput-vs-volume comes from the probe
# artifacts; the mechanism (index depth vs bloat vs cache) from the snapshots.
#
# Axis A - rows: empty-state linear fill; checkpoints by dwarf_steps row count. Rows stay tiny in
#   bytes (a few GB at 100M rows, far under RAM), so degradation on this curve is row-count-caused.
# Axis B - bytes: 64KB-payload state fill; checkpoints by total database size. Row count stays
#   small (<4M, a range axis A covers), so degradation beyond A's at the same row count is
#   bytes-caused. Checkpoints are dense around the two memory cliffs: ~11GB (Cloud SQL sets
#   shared_buffers to ~RAM/3 on the 32GB tier) and 32GB (total RAM / OS page cache).
#
# Each axis wants a FRESH database. Run A, recreate the database, then run B.
#
# Usage:
#   DSN='postgres://postgres:PASS@IP:5432/dwarf?sslmode=disable' ./degradation.sh A
#   DSN='...' ./degradation.sh B
# Knobs (env): BENCH (binary, default ./dwarf-bench), OUT (default ./degradation-results)
set -euo pipefail

AXIS="${1:?usage: degradation.sh A|B}"
DSN="${DSN:?set DSN}"
BENCH="${BENCH:-./dwarf-bench}"
OUT="${OUT:-./degradation-results}"
mkdir -p "$OUT"

# The probe is identical for both axes and deliberately CONNECTION-BOUND (M=8), far under the
# database's CPU ceiling: per-step DB time is then read directly as M / throughput, and the
# measurement is not confounded by saturation dynamics. Volume is the only variable across every
# data point of both curves.
PROBE_ARGS=(-workload linear -workers 512 -max-open-conns 8 -concurrency 512 -window 60s -warmup 15s)

case "$AXIS" in
A)
  CHECKPOINTS=(1 5 10 20 30 40 50 60 70 80 90 100)   # millions of dwarf_steps rows
  UNIT="Mrows"
  FILL_ARGS=(-workload linear -workers 512 -max-open-conns 48 -concurrency 512 -warmup 0s)
  RATE=4500   # initial fill-rate guess (rows/s at the measured ~4,600 steps/s ceiling); re-measured per leg
  ;;
B)
  CHECKPOINTS=(8 16 32 48 64 100 150 200 250 300 350 400)   # GB of pg_database_size
  UNIT="GB"
  FILL_ARGS=(-workload state -payload 65536 -workers 512 -max-open-conns 48 -concurrency 512 -warmup 0s)
  RATE=0   # measured after the first leg
  ;;
*) echo "axis must be A or B" >&2; exit 1 ;;
esac

psq() { psql "$DSN" -X -q -At -c "$1"; }

# measure prints the axis variable in base units: rows for A, bytes for B.
measure() {
  if [[ "$AXIS" == A ]]; then
    psq "ANALYZE dwarf_steps" >/dev/null
    psq "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname='dwarf_steps'"
  else
    psq "SELECT pg_database_size(current_database())"
  fi
}

# target converts a checkpoint value to base units.
target() {
  if [[ "$AXIS" == A ]]; then echo $(( $1 * 1000000 )); else echo $(( $1 * 1000000000 )); fi
}

snapshot() { # $1 = label
  psq "SELECT json_build_object(
    'at', now(),
    'db_size', pg_database_size(current_database()),
    'tables', (SELECT json_agg(json_build_object(
        'rel', relname, 'live', n_live_tup, 'dead', n_dead_tup,
        'total_size', pg_total_relation_size(relid), 'heap_size', pg_relation_size(relid),
        'vacuums', vacuum_count + autovacuum_count, 'analyzes', analyze_count + autoanalyze_count,
        'seq_scan', seq_scan, 'idx_scan', idx_scan))
      FROM pg_stat_user_tables),
    'indexes', (SELECT json_agg(json_build_object(
        'idx', indexrelname, 'size', pg_relation_size(indexrelid), 'scans', idx_scan))
      FROM pg_stat_user_indexes),
    'cache', (SELECT json_build_object(
        'heap_read', COALESCE(sum(heap_blks_read),0), 'heap_hit', COALESCE(sum(heap_blks_hit),0),
        'idx_read', COALESCE(sum(idx_blks_read),0), 'idx_hit', COALESCE(sum(idx_blks_hit),0),
        'toast_read', COALESCE(sum(toast_blks_read),0), 'toast_hit', COALESCE(sum(toast_blks_hit),0))
      FROM pg_statio_user_tables))" > "$OUT/stats-$1.json"
}

checkpoint() { # $1 = label
  local vol; vol="$(measure)"
  echo "== checkpoint $1 (measured: $vol) =="
  "$BENCH" -dsn "$DSN" "${PROBE_ARGS[@]}" -label "deg$AXIS-$1" -out "$OUT/probe-$1.json"
  snapshot "$1"
  # One summary line per checkpoint: volume at probe start + the probe's steps/s.
  local sps; sps="$(python3 -c "import json;print(json.load(open('$OUT/probe-$1.json'))['results'][0]['stepsPerSec'])")"
  echo "$1,$vol,$sps" >> "$OUT/summary-$AXIS.csv"
}

echo "label,volume,stepsPerSec" > "$OUT/summary-$AXIS.csv"
checkpoint "0"   # the empty-database y-intercept

leg=0
for cp in "${CHECKPOINTS[@]}"; do
  tgt="$(target "$cp")"
  while :; do
    now="$(measure)"
    if (( now >= tgt )); then break; fi
    # Fill-leg window sized from the observed rate: long legs mid-fill, short legs near a
    # checkpoint. Clamped to [60s, 600s]; first leg of axis B runs blind at the floor.
    window=60
    if (( RATE > 0 )); then
      window="$(( (tgt - now) / RATE ))"
      if (( window < 60 )); then window=60; fi
      if (( window > 600 )); then window=600; fi
    fi
    leg=$(( leg + 1 ))
    echo "-- fill leg $leg: at $now / target $tgt, window ${window}s (rate $RATE/s)"
    "$BENCH" -dsn "$DSN" "${FILL_ARGS[@]}" -window "${window}s" \
      -label "fill$AXIS-leg$leg" -out "$OUT/fill-leg$leg.json"
    after="$(measure)"
    if (( after > now )); then RATE=$(( (after - now) / window )); fi
  done
  checkpoint "${cp}${UNIT}"
done

echo "done. summary:"
column -s, -t "$OUT/summary-$AXIS.csv" 2>/dev/null || cat "$OUT/summary-$AXIS.csv"
