#!/usr/bin/env bash
#
# Covering-index A/B: does carrying not_before, lease_expires and fairness_weight in
# idx_dwarf_steps_selection pay under real load, on both a shallow (linear) and a deep (fanout)
# backlog? Runs ON the bench VM, next to the deployed dwarf-bench binary.
#
# WHY A DDL SWAP RATHER THAN TWO BINARIES. The covering columns live in 2.sql, so there is no build
# flag to toggle. Building a second binary with a patched migration would change the artifact under
# test; instead every arm runs the SAME binary against a fresh database, and the "plain" arm replaces
# the index with its pre-covering shape once the engine has migrated. One variable, nothing else.
#
# THE SWAP IS VERIFIED THREE TIMES, and that is not paranoia. A swap that silently failed - or that
# something recreated mid-run - would produce a NULL RESULT that looks exactly like "the index does
# not matter", which is the single most expensive way this campaign could lie. So: assert the shape
# after the swap, assert it again when the run ends, and abort the arm on any mismatch.
#
# WHAT TO EXPECT. Under `linear` a flow holds exactly one in-flight step, so the pending backlog
# equals the submitter count and the band scan is cheap in BOTH arms - the linear arms are the
# CONTROL (the wider index must not cost anything), not the place the effect lives. Under `fanout`
# the backlog is concurrency x width, deep enough to reproduce the regime where the local test
# measured 370ms -> 158ms.
#
# Usage:
#   DSN=postgres://postgres:...@10.x.x.x:5432/dwarf?sslmode=disable ./coveringindex.sh
# Knobs (env): BENCH (default ./dwarf-bench), OUT (default ./covering-results), REPS (default 3),
#   WINDOW/WARMUP, VCPUS (default 8), LIN_CONC, FAN_CONC, FAN_WIDTH, KEYS, TASK_DELAY,
#   RUN_ID (database namespace; defaults to a timestamp).
set -euo pipefail


# PREFLIGHT: psql must exist. Every campaign here drops and creates its own database and gates on an idle
# RTT probe, and BOTH go through psql - but the probe pipes stderr to /dev/null (it has to; a failed probe
# is an ordinary outcome the gate retries). So on a host without psql the gate reads "no measurement" as
# "RTT too high" and cools down forever, printing nothing that names the cause. Measured: a bare Debian
# bench VM sat in that loop for 40 minutes on the first arm. Fail here instead, where the message is the
# actual problem. provision.sh installs it; this is the guard for a host provisioned some other way.
command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found. Install it (Debian: sudo apt-get install -y postgresql-client)." >&2
  exit 1
}

DSN="${DSN:?set DSN (the Cloud SQL instance, private IP)}"
BENCH="${BENCH:-./dwarf-bench}"
OUT="${OUT:-./covering-results}"
REPS="${REPS:-3}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-20s}"
VCPUS="${VCPUS:-8}"

# Linear: backlog == submitter count, so concurrency is the only backlog knob. Fanout: backlog is
# ~FAN_CONC x FAN_WIDTH, which is how backlog moves independently of client cost (16k submitter
# goroutines depress throughput on their own, so the deep arm uses FEWER submitters, not more).
LIN_CONC="${LIN_CONC:-4096}"
FAN_CONC="${FAN_CONC:-1024}"
FAN_WIDTH="${FAN_WIDTH:-128}"
# Many keys, non-zero delay: a single fairness key is the degenerate single-partition case for the
# refiller and puts fanout into a bistable regime (healthy ~2,400 steps/s or collapsed ~550), and
# zero task delay is the least production-like point on the whole rig.
KEYS="${KEYS:-256}"
TASK_DELAY="${TASK_DELAY:-5ms}"
# Seconds after the bench starts at which to capture the plan - late in the measurement window, when
# the backlog is deepest. Default assumes the default 20s warmup + 60s window.
EXPLAIN_AT="${EXPLAIN_AT:-65}"

RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"

mkdir -p "$OUT"
echo "run id: ${RUN_ID}  (databases: dwarf_cov_${RUN_ID}_<arm>_r<rep>)"

dsn_with_db() { # $1 = dsn, $2 = dbname
  local dsn="$1" db="$2" base query=""
  base="${dsn%%\?*}"
  [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"
  echo "${base%/*}/${db}${query}"
}
admin_dsn() { dsn_with_db "$1" postgres; }
psq() { psql "$1" -X -q -At -v ON_ERROR_STOP=1 -c "$2"; }

ADMIN="$(admin_dsn "$DSN")"

# The pre-covering shape, taken verbatim from the parent of the commit that added the INCLUDE.
PLAIN_INDEX="CREATE INDEX idx_dwarf_steps_selection ON dwarf_steps (status, parked, priority, fairness_key, created_at, step_id) WHERE status IN ('pending', 'running')"
# The shipped shape, recreated verbatim by the covering arm's sham rebuild (see run_arm).
COVERING_INDEX="CREATE INDEX idx_dwarf_steps_selection ON dwarf_steps (status, parked, priority, fairness_key, created_at, step_id) INCLUDE (not_before, lease_expires, fairness_weight) WHERE status IN ('pending', 'running')"

# Phase 1 of the refill (scanBandKeys), with sequel's NOW_UTC()/DATE_DIFF_MILLIS macros expanded to
# their pgx forms. Kept byte-identical to the engine's query apart from that expansion, because an
# EXPLAIN of a query the engine does not actually run proves nothing.
read -r -d '' BANDSCAN <<'SQL' || true
SELECT fairness_key, cnt, age_ms, weight, priority FROM (
  SELECT fairness_key, priority,
   COUNT(*) OVER (PARTITION BY fairness_key) AS cnt,
   (EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - created_at)) * 1000.0) AS age_ms,
   fairness_weight AS weight,
   ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn
  FROM dwarf_steps
  WHERE status='pending' AND parked=0 AND not_before<=(NOW() AT TIME ZONE 'UTC') AND lease_expires<=(NOW() AT TIME ZONE 'UTC')
  AND priority=(SELECT MIN(priority) FROM dwarf_steps
    WHERE status='pending' AND parked=0 AND not_before<=(NOW() AT TIME ZONE 'UTC') AND lease_expires<=(NOW() AT TIME ZONE 'UTC'))
) t WHERE rn=1
SQL

index_def() { psq "$1" "SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND indexname='idx_dwarf_steps_selection'"; }

# assert_shape aborts the arm rather than recording a run whose index shape is not the one the arm
# claims to be measuring. See the header: a failed swap is indistinguishable from a null result.
assert_shape() { # $1 = dsn, $2 = variant, $3 = when
  local def; def="$(index_def "$1")"
  if [[ -z "$def" ]]; then
    echo "ABORT: idx_dwarf_steps_selection missing ($3)" >&2; exit 1
  fi
  case "$2" in
    covering) [[ "$def" == *INCLUDE* ]] || { echo "ABORT: expected covering index $3, got: $def" >&2; exit 1; } ;;
    plain)    [[ "$def" != *INCLUDE* ]] || { echo "ABORT: expected plain index $3, got: $def" >&2; exit 1; } ;;
  esac
  echo "   [$3] $2 ok"
}

run_arm() { # $1 = variant (covering|plain), $2 = workload, $3 = rep
  local variant="$1" workload="$2" rep="$3"
  local tag="${variant}-${workload}-r${rep}"
  local db="dwarf_cov_${RUN_ID}_${variant}_${workload}_r${rep}"
  local rdsn; rdsn="$(dsn_with_db "$DSN" "$db")"

  echo "-- ${tag}"
  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
  psq "$ADMIN" "CREATE DATABASE ${db}" >/dev/null

  local args=(-dsn "$rdsn" -workload "$workload" -vcpus "$VCPUS"
    -window "$WINDOW" -warmup "$WARMUP"
    -fairness-keys "$KEYS" -task-delay "$TASK_DELAY"
    -label "covering-index ${variant} ${workload} rep ${rep}"
    -out "${OUT}/r-${tag}.json")
  if [[ "$workload" == fanout ]]; then
    args+=(-concurrency "$FAN_CONC" -fanout-width "$FAN_WIDTH")
  else
    args+=(-concurrency "$LIN_CONC")
  fi

  "$BENCH" "${args[@]}" >"${OUT}/log-${tag}.txt" 2>&1 &
  local pid=$!

  # Wait for the engine's migration to land the index, then swap it before any backlog accumulates,
  # so both arms see an identical table history. The table is empty here, so the DDL is instant and
  # its ACCESS EXCLUSIVE lock costs nothing - and it all happens inside the discarded warmup.
  local waited=0
  until [[ -n "$(index_def "$rdsn" 2>/dev/null)" ]]; do
    kill -0 "$pid" 2>/dev/null || { echo "ABORT: bench exited before migrating; see ${OUT}/log-${tag}.txt" >&2; exit 1; }
    sleep 0.2; waited=$((waited + 1))
    [[ $waited -lt 300 ]] || { echo "ABORT: index never appeared" >&2; kill "$pid" 2>/dev/null; exit 1; }
  done
  # BOTH arms drop and recreate the index, differing ONLY in its shape. The covering arm's rebuild is
  # a SHAM: it recreates the identical index it just dropped, and exists purely so the two arms pay
  # the same DDL cost. Without it the plain arm alone pays a non-concurrent CREATE INDEX - which takes
  # a write-blocking lock - and this rig's fanout workload is bistable, so a warmup write stall is a
  # plausible way to knock one arm into the collapsed basin. That would show up as a throughput win
  # for the covering arm that has nothing to do with the index shape.
  psq "$rdsn" "DROP INDEX idx_dwarf_steps_selection" >/dev/null
  if [[ "$variant" == plain ]]; then
    psq "$rdsn" "$PLAIN_INDEX" >/dev/null
  else
    psq "$rdsn" "$COVERING_INDEX" >/dev/null
  fi
  assert_shape "$rdsn" "$variant" "after-swap"

  # Capture the plan late in the measurement window, when the backlog is at its deepest. One
  # execution of a ~0.2s query against a 60s window is not a measurable perturbation, and it is the
  # only way to confirm the covering columns are actually producing an Index Only Scan with few
  # Heap Fetches rather than merely existing.
  ( sleep "$EXPLAIN_AT"
    {
      echo "== ${tag} =="
      echo "-- index"; index_def "$rdsn"
      echo "-- backlog"; psq "$rdsn" "SELECT count(*) FROM dwarf_steps WHERE status='pending' AND parked=0"
      echo "-- autovacuum / visibility"
      psq "$rdsn" "SELECT n_live_tup, n_dead_tup, autovacuum_count, vacuum_count, COALESCE(last_autovacuum::text,'never') FROM pg_stat_user_tables WHERE relname='dwarf_steps'"
      echo "-- explain"
      psql "$rdsn" -X -q -v ON_ERROR_STOP=1 -c "EXPLAIN (ANALYZE, BUFFERS) ${BANDSCAN}"
    } >>"${OUT}/explain-${tag}.txt" 2>&1
  ) &
  local exp=$!

  wait "$pid" || { echo "ABORT: bench failed; see ${OUT}/log-${tag}.txt" >&2; kill "$exp" 2>/dev/null; exit 1; }
  wait "$exp" 2>/dev/null || true
  assert_shape "$rdsn" "$variant" "after-run"

  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
}

# Rep-major interleaving: any drift over the session hits every arm equally instead of loading onto
# whichever arm happened to run last. With only one instance in play this is the main control left,
# and the rig's deep-backlog runs are bimodal enough that it matters.
echo
echo "== ${REPS} reps x {covering,plain} x {linear,fanout}, ${WINDOW} window =="
for rep in $(seq 1 "$REPS"); do
  for workload in linear fanout; do
    for variant in covering plain; do
      run_arm "$variant" "$workload" "$rep"
    done
  done
done

echo
echo "== summary =="
python3 - "$OUT" <<'PY'
import glob, json, os, re, statistics, sys

out = sys.argv[1]
arms = {}
for path in sorted(glob.glob(os.path.join(out, "r-*.json"))):
    m = re.match(r"r-(covering|plain)-(linear|fanout)-r(\d+)\.json", os.path.basename(path))
    if not m:
        continue
    doc = json.load(open(path))
    for r in doc.get("results", []):
        arms.setdefault((m.group(2), m.group(1)), []).append(r)

def band_ms(r):
    for h in r.get("engineHistograms", []):
        if h["name"] == "dwarf_refill_query_duration_seconds" and h["count"]:
            if h.get("attrs", {}).get("phase") == "band_keys":
                return h["sumSeconds"] / h["count"] * 1000
    return None

print(f"{'workload':>9} {'index':>9} {'n':>2} {'steps/s':>9} {'spread':>14} {'band ms':>8} {'p99ms':>7}")
agg = {}
for key in sorted(arms):
    rs = arms[key]
    sp = [r["stepsPerSec"] for r in rs]
    bm = [b for b in (band_ms(r) for r in rs) if b is not None]
    agg[key] = (statistics.mean(sp), statistics.mean(bm) if bm else None)
    spread = f"{min(sp):.0f}-{max(sp):.0f}"
    band = f"{agg[key][1]:.1f}" if agg[key][1] else "n/a"
    p99 = statistics.mean(r["p99Ms"] for r in rs)
    print(f"{key[0]:>9} {key[1]:>9} {len(sp):>2} {statistics.mean(sp):>9.0f} "
          f"{spread:>14} {band:>8} {p99:>7.0f}")

print("\n-- covering vs plain --")
for wl in ("linear", "fanout"):
    c, p = agg.get((wl, "covering")), agg.get((wl, "plain"))
    if not c or not p:
        continue
    print(f"  {wl:>7}: throughput x{c[0]/p[0]:.3f}", end="")
    if c[1] and p[1]:
        print(f"   band scan {p[1]:.1f} -> {c[1]:.1f} ms (x{c[1]/p[1]:.2f})")
    else:
        print()
    sp = [r["stepsPerSec"] for r in arms[(wl, "plain")]]
    if len(sp) > 1:
        noise = (max(sp) - min(sp)) / statistics.mean(sp)
        print(f"           plain-arm run-to-run spread is {noise*100:.0f}% of its mean - a "
              f"throughput delta smaller than this is NOT resolved")

print("""
Reading it:
  - linear is the CONTROL. Its backlog is the submitter count, so the band scan is cheap either way
    and the expected result is "no difference". A LOSS here is the finding that matters (the wider
    index costing something when it cannot help).
  - fanout is where the effect should live. Check band ms first: it is the low-variance phase-level
    metric, and this rig's throughput numbers are bimodal enough to hide a real effect.
  - then read explain-*.txt: covering arms must show `Index Only Scan` with Heap Fetches well under
    the row count. High Heap Fetches means autovacuum is NOT holding the backlog's pages
    all-visible at this write rate - which is the one thing the local test could not settle.""")
PY
