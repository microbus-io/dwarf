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
# Axis F - rows, fan-out shape: same row axis as A, but both the fill and the probe are wide
#   forEach cohorts. A linear flow holds exactly one pending step per in-flight flow, so axis A
#   pins the pending backlog flat at the submitter concurrency and keeps the refiller's row
#   estimates honest - which is precisely why A cannot see a backlog-proportional scan cost. Width
#   64 decouples the two (backlog ~= concurrency x width), so F is the arm that puts the band scan
#   under deep-backlog pressure at every volume point. It runs with many fairness keys ON PURPOSE:
#   a single key packs same-flow siblings into one batch and puts the run in the bistable
#   flow-row-convoy regime (healthy ~2,400 steps/s or collapsed ~550, never in between, collapsing
#   2 of 3 runs), which on a volume curve is indistinguishable from deterioration. Many keys
#   measured 0 collapses in 12 runs, so the curve reads as volume-vs-throughput and nothing else.
#
# Each axis wants a FRESH database, and axes run in parallel want a fresh INSTANCE, not just a
# fresh database: throughput on this rig fell ~2.6x uniformly once databases accumulated on one
# Cloud SQL instance, which silently invalidates every cross-run comparison.
#
# Usage:
#   DSN='postgres://postgres:PASS@IP:5432/dwarf?sslmode=disable' ./degradation.sh A
#   DSN='...' ./degradation.sh B
#   DSN='...' CHECKPOINTS='1 2 4 6 8 10' ./degradation.sh F
# Knobs (env): BENCH (binary, default ./dwarf-bench), OUT (default ./degradation-results),
#   CHECKPOINTS (space-separated, overrides the axis ladder), WIDTH/KEYS (axis F shape).
set -euo pipefail

AXIS="${1:?usage: degradation.sh A|B|F}"
DSN="${DSN:?set DSN}"
BENCH="${BENCH:-./dwarf-bench}"
OUT="${OUT:-./degradation-results}"
# FILL_CONNS sizes the fill's connection pool. Default 48 = 6x an 8-vCPU DB; on a larger DB set it to
# 6x its vCPUs (e.g. 192 for 32-vCPU) or the fill throttles on connections and the bigger box is wasted.
# The PROBES stay at M=8 deliberately (connection-bound methodology); only the fill scales.
FILL_CONNS="${FILL_CONNS:-48}"
mkdir -p "$OUT"

# The probe is deliberately CONNECTION-BOUND (M=8), far under the database's CPU ceiling: per-step
# DB time is then read directly as M / throughput, and the measurement is not confounded by
# saturation dynamics. Volume is the only variable across every data point of a curve. Axis F
# overrides the workload but keeps M=8, so its points stay comparable along its own axis.
PROBE_ARGS=(-workload linear -workers 512 -max-open-conns 8 -concurrency 512 -window 60s -warmup 15s)

# An optional SECOND probe at each checkpoint, of a different workload shape against the SAME
# database. It is what separates the two explanations for a declining curve that the main probe
# alone cannot tell apart: a degraded TABLE (the rows this fill built) versus a degraded WORKLOAD
# SHAPE (what this probe asks of them). On axis F the cross probe is `linear`, identical to axis A's
# probe, so a fan-out-built table can be read with a linear workload and compared directly against
# the linear arm's own curve at the same row count. Empty = no cross probe.
CROSS_PROBE_ARGS=()

WIDTH="${WIDTH:-64}"
KEYS="${KEYS:-256}"

case "$AXIS" in
A)
  DEFAULT_CHECKPOINTS="1 5 10 20 30 40 50 60 70 80 90 100"   # millions of dwarf_steps rows
  UNIT="Mrows"
  FILL_ARGS=(-workload linear -workers 512 -max-open-conns "$FILL_CONNS" -concurrency 512 -warmup 0s)
  RATE=4500   # initial fill-rate guess (rows/s at the measured ~4,600 steps/s ceiling); re-measured per leg
  ;;
B)
  DEFAULT_CHECKPOINTS="8 16 32 48 64 100 150 200 250 300 350 400"   # GB of pg_database_size
  UNIT="GB"
  FILL_ARGS=(-workload state -payload 65536 -workers 512 -max-open-conns "$FILL_CONNS" -concurrency 512 -warmup 0s)
  RATE=0   # measured after the first leg
  ;;
F)
  DEFAULT_CHECKPOINTS="1 5 10 20 30 40 50 60 70 80 90 100"   # millions of dwarf_steps rows, same axis as A
  UNIT="Mrows"
  FILL_ARGS=(-workload fanout -fanout-width "$WIDTH" -fairness-keys "$KEYS"
             -workers 512 -max-open-conns "$FILL_CONNS" -concurrency 512 -warmup 0s)
  PROBE_ARGS=(-workload fanout -fanout-width "$WIDTH" -fairness-keys "$KEYS"
              -workers 512 -max-open-conns 8 -concurrency 512 -window 60s -warmup 15s)
  # Same flags as axis A's probe, so the two arms' linear numbers are directly comparable.
  CROSS_PROBE_ARGS=(-workload linear -workers 512 -max-open-conns 8 -concurrency 512
                    -window 60s -warmup 15s)
  RATE=7000   # measured fan-out fill rate on a 16-vCPU DB with fix1+fix2 (was 2400 pre-fix); re-measured per leg
  ;;
*) echo "axis must be A, B or F" >&2; exit 1 ;;
esac

# shellcheck disable=SC2206  # word splitting is the intended parse of the space-separated ladder
CHECKPOINTS=(${CHECKPOINTS:-$DEFAULT_CHECKPOINTS})

psq() { psql "$DSN" -X -q -At -c "$1"; }

# measure prints the axis variable in base units: rows for A and F, bytes for B.
#
# Axis F must NOT reach for A's `ANALYZE` + reltuples: refreshing statistics is not a neutral
# reading here, it is an intervention on the exact variable under test. The refiller's band scan
# flips between an index scan and a seq scan purely on statistics freshness (measured 0.29 ms vs
# 99.8 ms on the same data minutes apart), and a churning queue table is structurally unable to
# keep those estimates honest. ANALYZE-ing immediately before each probe would hand every F point
# the good plan and hide the thing F exists to catch. An exact count costs seconds once per fill
# leg and perturbs nothing.
measure() {
  if [[ "$AXIS" == A ]]; then
    psq "ANALYZE dwarf_steps" >/dev/null
    psq "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname='dwarf_steps'"
  elif [[ "$AXIS" == F ]]; then
    # The y-intercept checkpoint measures BEFORE any probe has run, so the engine has not migrated
    # yet and the table does not exist. The existence test must be a SEPARATE statement: PostgreSQL
    # resolves table references at PARSE time, so wrapping the count in CASE/COALESCE still fails to
    # parse - the untaken branch is resolved anyway.
    if [[ -n "$(psq "SELECT 1 FROM pg_class WHERE relname='dwarf_steps'")" ]]; then
      psq "SELECT count(*) FROM dwarf_steps"
    else
      echo 0
    fi
  else
    psq "SELECT pg_database_size(current_database())"
  fi
}

# target converts a checkpoint value to base units: millions of rows on the row axes, GB on B.
target() {
  if [[ "$AXIS" == B ]]; then echo $(( $1 * 1000000000 )); else echo $(( $1 * 1000000 )); fi
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

sps_of() { # $1 = artifact path
  python3 -c "import json;print(json.load(open('$1'))['results'][0]['stepsPerSec'])"
}

checkpoint() { # $1 = label
  local vol; vol="$(measure)"
  echo "== checkpoint $1 (measured: $vol) =="
  "$BENCH" -dsn "$DSN" "${PROBE_ARGS[@]}" -label "deg$AXIS-$1" -out "$OUT/probe-$1.json"
  local xps=""
  if (( ${#CROSS_PROBE_ARGS[@]} )); then
    # Runs back-to-back with the main probe, never concurrently: two probes sharing the database
    # would contend and neither number would mean anything. The fill is paused for both.
    "$BENCH" -dsn "$DSN" "${CROSS_PROBE_ARGS[@]}" -label "deg$AXIS-$1-cross" -out "$OUT/cross-$1.json"
    xps="$(sps_of "$OUT/cross-$1.json")"
  fi
  snapshot "$1"
  # One summary line per checkpoint: volume at probe start, the probe's steps/s, the cross probe's.
  local sps; sps="$(sps_of "$OUT/probe-$1.json")"
  echo "$1,$vol,$sps,$xps" >> "$OUT/summary-$AXIS.csv"
}

echo "label,volume,stepsPerSec,crossStepsPerSec" > "$OUT/summary-$AXIS.csv"
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
