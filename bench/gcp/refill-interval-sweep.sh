#!/usr/bin/env bash
#
# Refill scan-interval sweep. Finds the interval that maximizes throughput for a given workload, and -
# by sweeping the shard COUNT alongside it - tests whether that optimum is configuration-independent
# (the derivation says bufferShare/c = 96/450 with vCPUs, R and N all cancelling, so T_opt should be a
# constant; if it drifts across shard counts the formula needs real exponents). Runs ON the bench VM.
#
# THE AXES: interval (via -refill-scan-floor-ms) x shard count. One workload per invocation (-workload).
# The interval is the axis under test, so it must be interleaved (control 2), rep-major within each
# shard count: for each rep, for each shard count, sweep all intervals back to back.
#
# CONTROLS (inherited from decouple-ab.sh / shardladder.sh, each already saved a campaign):
#   1. FRESH DATABASE PER RUN, dropped before AND after - so no run inherits another's bloat/stats/cache
#      (the ~2.6x accumulation hazard). This script's only destructive op is DROP DATABASE on an exact
#      RUN_ID-namespaced name, so it can only ever reap databases it created in this run.
#   2. INTERLEAVED interval arms.
#   3. RTT GATE up front (aborts rather than silently producing an ambiguous sweep).
#
# Usage:
#   DSN1=... ... DSN6=... WORKLOAD=linear ./refill-interval-sweep.sh
# Knobs (env): BENCH, OUT, REPS (3), SHARDS ("2 4 6"), INTERVALS ("50 80 110 150 220"), CONC (4096),
#   WINDOW (60s), WARMUP (15s), VCPUS (8), RTT_MAX_DELTA_MS (1.0), RUN_ID, EXTRA.
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

DSNS=()
while :; do
  varname="DSN$(( ${#DSNS[@]} + 1 ))"
  [[ -n "${!varname:-}" ]] || break
  DSNS+=("${!varname}")
done
BENCH="${BENCH:-./dwarf-bench-sweep}"
OUT="${OUT:-./refill-sweep-results}"
REPS="${REPS:-3}"
SHARDS="${SHARDS:-2 4 6}"
INTERVALS="${INTERVALS:-50 80 110 150 220}"
CONC="${CONC:-4096}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-15s}"
VCPUS="${VCPUS:-8}"
WORKLOAD="${WORKLOAD:-linear}"
RTT_MAX_DELTA_MS="${RTT_MAX_DELTA_MS:-1.0}"
# 256 fairness keys keeps the refiller off its degenerate single-partition case. 5ms task delay is the
# reference workload; fanout adds width via -fanout-width but keeps the delay so cohort siblings do not
# all complete at once (task-jitter would too, but the delay is what campaign 7 used).
EXTRA="${EXTRA:--fairness-keys 256 -task-delay 5ms}"

# OPEN-LOOP with a per-shard-scaled backlog cap. The interval is the lever only in the window where the
# backlog EXCEEDS the per-shard cache buffer (~768, so the refiller is the supply constraint) but the
# scan stays FASTER than the floor (phase 1 is per-due-row: ~46ms + 0.0085ms/row, so a too-deep backlog
# makes the scan itself outlast any interval and the floor stops binding). Target ~2500 due steps/shard,
# comfortably inside [768, ~7500]. Scaling the cap by shard count ALSO holds per-shard load constant
# across the 2/4/6 axis - which is what makes the config-independence check clean (fixed total concurrency
# would overload low shard counts and under-load high ones). PER_SHARD_FLOWS is set per workload from its
# steps-per-flow: linear ~1 active step/flow, fanout ~fanout-width (16) at cohort peak.
OPEN_LOOP="${OPEN_LOOP:-1}"
if [[ "$WORKLOAD" == fanout ]]; then
  PER_SHARD_FLOWS="${PER_SHARD_FLOWS:-160}"   # x16 branches ~= 2560 steps/shard
else
  PER_SHARD_FLOWS="${PER_SHARD_FLOWS:-2500}"  # ~1 step/flow ~= 2500 steps/shard
fi

MAXNEEDED=0; for s in $SHARDS; do [[ $s -gt $MAXNEEDED ]] && MAXNEEDED=$s; done
if [[ ${#DSNS[@]} -lt $MAXNEEDED ]]; then
  echo "need $MAXNEEDED DSNs for SHARDS='$SHARDS'; got ${#DSNS[@]}" >&2; exit 1
fi
[[ -x "$BENCH" ]] || { echo "not executable: $BENCH" >&2; exit 1; }

RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"; RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"
mkdir -p "$OUT"
echo "run id: ${RUN_ID}  workload=${WORKLOAD}  shards={${SHARDS}}  intervals={${INTERVALS}}ms"

dsn_with_db() { local dsn="$1" db="$2" base query=""; base="${dsn%%\?*}"; [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"; echo "${base%/*}/${db}${query}"; }
admin_dsn() { dsn_with_db "$1" postgres; }
psq() { psql "$1" -X -q -At -c "$2"; }

# --- Control 3: RTT gate (one psql session, min taken; see shardladder.sh for the full rationale) ----
probe_rtt_ms() { local dsn="$1" script i; script=$'\\timing on\n'; for i in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null | awk '/^Time:/ {v=$2+0; if (m=="" || v<m) m=v} END {if (m=="") m=0; printf "%.4f", m}'; }
echo "== RTT gate =="
RTTS=()
for i in $(seq 0 $((MAXNEEDED - 1))); do r=$(probe_rtt_ms "$(admin_dsn "${DSNS[$i]}")"); RTTS+=("$r"); printf "  shard %d  rtt %s ms\n" "$((i+1))" "$r"; done
RTT_MIN=$(printf '%s\n' "${RTTS[@]}" | sort -g | head -1); RTT_MAX=$(printf '%s\n' "${RTTS[@]}" | sort -g | tail -1)
RTT_DELTA=$(awk -v a="$RTT_MAX" -v b="$RTT_MIN" 'BEGIN{printf "%.3f", a-b}')
echo "  spread ${RTT_DELTA} ms absolute, tolerance ${RTT_MAX_DELTA_MS} ms"
if awk -v d="$RTT_DELTA" -v t="$RTT_MAX_DELTA_MS" 'BEGIN{exit !(d>t)}'; then
  echo "ABORT: shard RTTs differ by ${RTT_DELTA} ms (> ${RTT_MAX_DELTA_MS}) - not placement jitter." >&2; exit 1; fi

# --- The sweep ------------------------------------------------------------------------------------
echo
echo "== sweep: ${REPS} reps x shards{${SHARDS}} x intervals{${INTERVALS}}ms, ${WORKLOAD}, conc ${CONC}, ${WINDOW} =="
for rep in $(seq 1 "$REPS"); do
  for n in $SHARDS; do
    for ms in $INTERVALS; do
      tag="${WORKLOAD}-${n}s-i${ms}-r${rep}"
      db="dwarf_sw_${RUN_ID}_${WORKLOAD}_${n}s_i${ms}_r${rep}"
      maxout=$(( PER_SHARD_FLOWS * n ))   # per-shard-scaled outstanding cap (constant per-shard load)
      # shellcheck disable=SC2206
      args=(-workload "$WORKLOAD" -vcpus "$VCPUS" -concurrency "$CONC" -window "$WINDOW" -warmup "$WARMUP"
        -refill-scan-floor-ms "$ms" ${EXTRA:-}
        -label "refill sweep ${WORKLOAD} ${n}-shard ${ms}ms rep ${rep}" -out "${OUT}/r-${tag}.json")
      if [[ "$OPEN_LOOP" == 1 ]]; then
        args+=(-open-loop -max-outstanding "$maxout")
      fi
      # Control 1: DROP (before) + CREATE per run, on this arm's shards.
      for i in $(seq 0 $((n - 1))); do
        adm=$(admin_dsn "${DSNS[$i]}")
        psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
        psq "$adm" "CREATE DATABASE ${db}" >/dev/null
        args+=(-dsn "$((i + 1))=$(dsn_with_db "${DSNS[$i]}" "$db")")
      done
      echo "-- ${tag}"
      "$BENCH" "${args[@]}"
      # DROP (after): reclaim immediately so the next run starts clean and nothing accumulates.
      for i in $(seq 0 $((n - 1))); do psq "$(admin_dsn "${DSNS[$i]}")" "DROP DATABASE IF EXISTS ${db}" >/dev/null; done
    done
  done
done

# --- Summary --------------------------------------------------------------------------------------
echo
echo "== summary (${WORKLOAD}) =="
python3 - "$OUT" "$WORKLOAD" <<'PY'
import glob, json, os, statistics, sys
out, workload = sys.argv[1], sys.argv[2]
arms = {}  # (shards, ms) -> [result]
for path in sorted(glob.glob(os.path.join(out, f"r-{workload}-*.json"))):
    b = os.path.basename(path)[:-5].split("-")   # r, workload, Ns, iMS, rR
    n = int(b[2].rstrip("s")); ms = int(b[3].lstrip("i"))
    for r in json.load(open(path)).get("results", []):
        arms.setdefault((n, ms), []).append(r)

def col(vals, f): xs = [f(r) for r in vals]; return statistics.mean(xs), min(xs), max(xs)

shards = sorted({n for n, _ in arms}); intervals = sorted({ms for _, ms in arms})
print(f"{'shards':>6} {'floor':>6} {'n':>2} {'steps/s':>9} {'spread':>15} {'p99ms':>7} {'waste':>6} {'batch':>6} {'scan_ms':>7} {'backlog':>8}")
best = {}
for n in shards:
    for ms in intervals:
        if (n, ms) not in arms: continue
        v = arms[(n, ms)]
        m, lo, hi = col(v, lambda r: r["stepsPerSec"])
        p99, _, _ = col(v, lambda r: r["p99Ms"])
        sel = sum(r.get("engineCounters", {}).get("dwarf_refill_candidates_selected", 0) for r in v)
        dis = sum(r.get("engineCounters", {}).get("dwarf_refill_candidates_discarded", 0) for r in v)
        npass = sum(h["count"] for r in v for h in r.get("engineHistograms", []) if h["name"] == "dwarf_refill_duration_seconds")
        waste = f"{dis/sel*100:.0f}%" if sel else "n/a"
        batch = f"{sel/npass:.0f}" if npass else "n/a"
        peak = int(sum(r.get("maxOutstandingObserved",0) for r in v)/len(v))
        scanms = (sum(h["sumSeconds"] for r in v for h in r.get("engineHistograms",[]) if h["name"]=="dwarf_refill_query_duration_seconds" and (h.get("attrs") or {}).get("phase")=="band_keys")*1000
                  / max(1,sum(h["count"] for r in v for h in r.get("engineHistograms",[]) if h["name"]=="dwarf_refill_query_duration_seconds" and (h.get("attrs") or {}).get("phase")=="band_keys")))
        print(f"{n:>6} {ms:>5}m {len(v):>2} {m:>9.0f} {lo:>7.0f}-{hi:<7.0f} {p99:>7.0f} {waste:>6} {batch:>6} {scanms:>7.0f} {peak:>8d}")
        if n not in best or m > best[n][1]: best[n] = (ms, m, p99)
    print()

print("throughput-max interval per shard count (the config-independence check):")
for n in shards:
    ms, m, p99 = best[n]
    print(f"  {n} shard(s): {ms}ms  ({m:.0f} steps/s, p99 {p99:.0f})")
vals = [best[n][0] for n in shards]
if len(set(vals)) == 1:
    print(f"\n  -> T_opt is CONSTANT across shard counts ({vals[0]}ms): the cancellation holds, a")
    print("     configuration-derived constant is correct. Confirm the value against the derived ~107ms.")
else:
    print(f"\n  -> T_opt MOVES with shard count ({vals}): the formula needs a shard-count term - the")
    print("     cancellation argument (bufferShare and c both N-independent) is contradicted; investigate.")
print("""
Reading it:
  - throughput rises then falls across the interval    -> a real optimum (too-hot churns, too-cold starves)
  - waste high at short intervals, ~0 at long           -> the churn mechanism; the optimum sits where it clears
  - batch grows ~linearly with the interval             -> backlog self-balances (B = drain x T), the supply model
  - throughput-max interval flat across shard counts    -> ship a derived constant; else fit the exponents""")
PY
