#!/usr/bin/env bash
#
# Shard-scaling ladder: measures throughput at 1, 2 and 3 shards with enough control to support a
# causal claim. Runs ON the bench VM, next to the deployed dwarf-bench binary.
#
# WHY THIS SCRIPT EXISTS. The original ladder (docs/benchmark-cloud.md: x2.04 at two shards, x2.19 at
# three, "the third shard is slow") is the only comparison in bench/results/ with NO replicates - one
# 60s run per arm. Three things make that thin:
#   - The sibling n=2 experiment (r-ratio-*) shows run-to-run spread the size of the effect: one
#     1-shard replicate reads 2,571 steps/s and the other 1,399 at the same point.
#   - Two campaigns disagree ~1.5x at the identical (3 shards, conc 4096) point - 6,105 vs 9,392 -
#     and nothing reconciles them.
#   - The third shard's measured RTT was ~2x the other two (0.834ms vs 0.377/0.432). That is an
#     uncontrolled variable sitting directly on the arm that failed to scale.
# This script controls all three: n=3 interleaved replicates, a fresh database per run on SEPARATE
# instances, and an up-front RTT gate that ABORTS rather than silently producing another ambiguous
# ladder.
#
# THE THREE CONTROLS, and why each is not optional:
#   1. SEPARATE INSTANCES, not just separate databases. Throughput on this rig fell ~2.6x uniformly
#      once databases accumulated on one Cloud SQL instance. Sharing an instance across arms would
#      make the 3-shard arm - which touches the most databases - look worst by construction.
#   2. INTERLEAVED replicates (rep-major: 1,2,3 / 1,2,3 / 1,2,3, never 1,1,1 / 2,2,2 / 3,3,3). Any
#      drift over the session then hits every arm equally instead of loading onto the last one.
#   3. AN RTT GATE. A shard 2x further away is a different experiment. Matching is the operator's
#      job (same region and zone); this only refuses to run when it is not true.
#
# Usage:
#   DSN1=... DSN2=... DSN3=... ./shardladder.sh
# Knobs (env): BENCH (binary, default ./dwarf-bench), OUT (default ./shardladder-results),
#   REPS (default 3), CONC (default 4096), WINDOW/WARMUP, VCPUS (shard tier, default 8),
#   RTT_MAX_DELTA_MS (max slowest-minus-fastest, default 1.0), WORKLOAD (default linear),
#   RUN_ID (database namespace; defaults to a timestamp), EXTRA (extra dwarf-bench flags,
#   e.g. '-fairness-keys 256 -task-delay 5ms').
set -euo pipefail

# Shards are taken from DSN1, DSN2, ... consecutively; the ladder runs one arm per shard count up to
# however many are supplied. Each must be its OWN instance (see control 1 below).
DSNS=()
while :; do
  varname="DSN$(( ${#DSNS[@]} + 1 ))"
  [[ -n "${!varname:-}" ]] || break
  DSNS+=("${!varname}")
done
if [[ ${#DSNS[@]} -lt 2 ]]; then
  echo "set at least DSN1 and DSN2 (one Cloud SQL instance each); DSN3, DSN4, ... extend the ladder" >&2
  exit 1
fi
MAXSHARDS=${#DSNS[@]}
BENCH="${BENCH:-./dwarf-bench}"
OUT="${OUT:-./shardladder-results}"
REPS="${REPS:-3}"
CONC="${CONC:-4096}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-15s}"
VCPUS="${VCPUS:-8}"
WORKLOAD="${WORKLOAD:-linear}"
RTT_MAX_DELTA_MS="${RTT_MAX_DELTA_MS:-1.0}"

# RUN_ID namespaces every database this script creates and drops. It exists because the script's only
# destructive operation is DROP DATABASE, and an unnamespaced ladder would use predictable names
# (dwarf_l1shard_r1) that a concurrent campaign on the same instance could plausibly also pick - so a
# rerun would silently reap someone else's in-flight fill. Every DROP below is an exact match on a name
# built from this id: the script can only ever destroy databases it created in this run.
RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"

mkdir -p "$OUT"
echo "run id: ${RUN_ID}  (databases: dwarf_lad_${RUN_ID}_<arm>s_r<rep>), ladder to ${MAXSHARDS} shards"

# dsn_with_db rewrites a DSN's database name, keeping the query string. Each run gets its own
# database so no run inherits another's table bloat, statistics, or cache residency.
dsn_with_db() { # $1 = dsn, $2 = dbname
  local dsn="$1" db="$2" base query=""
  base="${dsn%%\?*}"
  [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"
  echo "${base%/*}/${db}${query}"
}

# admin_dsn points at the always-present `postgres` database so CREATE/DROP DATABASE can run (a
# database cannot be dropped from a connection to itself).
admin_dsn() { dsn_with_db "$1" postgres; }

psq() { psql "$1" -X -q -At -c "$2"; }

# --- Control 3: the RTT gate ----------------------------------------------------------------------
# Round trips inside ONE psql session, MINIMUM taken (the statistic the engine's own probeRTT uses; a
# minimum rejects scheduler and queueing noise that a mean absorbs).
#
# The single session is the whole trick, and getting it wrong makes the gate WORSE THAN USELESS. The
# obvious spelling - time a `psql -c "SELECT 1"` per sample - measures process startup + TCP + TLS
# handshake, which on this rig is ~29ms and buries a sub-millisecond network RTT under three orders of
# magnitude of constant overhead. Measured that way, the original campaign's 2x skew (0.42ms vs 0.83ms)
# reads as 29.2 vs 29.6 = a 1.01x spread, and the gate PASSES the exact experiment it exists to reject.
# psql's own \timing brackets the query alone on an already-established connection, so the constant
# drops out and the gate can actually see the variable it is gating on.
probe_rtt_ms() { # $1 = dsn
  local dsn="$1" script i
  script=$'\\timing on\n'
  for i in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/ {v=$2+0; if (m=="" || v<m) m=v} END {if (m=="") m=0; printf "%.4f", m}'
}

echo "== RTT gate =="
RTTS=()
for i in "${!DSNS[@]}"; do
  r=$(probe_rtt_ms "$(admin_dsn "${DSNS[$i]}")")
  RTTS+=("$r")
  printf "  shard %d  rtt %s ms\n" "$((i + 1))" "$r"
done
RTT_MIN=$(printf '%s\n' "${RTTS[@]}" | sort -g | head -1)
RTT_MAX=$(printf '%s\n' "${RTTS[@]}" | sort -g | tail -1)
RTT_DELTA=$(awk -v a="$RTT_MAX" -v b="$RTT_MIN" 'BEGIN{printf "%.3f", a-b}')
RTT_RATIO=$(awk -v a="$RTT_MAX" -v b="$RTT_MIN" 'BEGIN{printf "%.2f", a/b}')
echo "  spread ${RTT_DELTA} ms absolute (${RTT_RATIO}x), tolerance ${RTT_MAX_DELTA_MS} ms"

# The gate is on the ABSOLUTE delta, not the ratio, and that is not a detail. RTT does not divide
# throughput, because it is OVERLAPPED: a worker blocked on the wire holds no database CPU, so by
# Little's law an extra L of per-step wire time costs only (throughput x L) additional workers in
# flight. At 3,520 steps/s per shard, a 0.1ms delta over ~7 round trips buys ~2.5 extra workers
# against a pool of ~384 - it disappears. A RATIO gate on sub-millisecond values reads that as a
# damning 2.3x and aborts a perfectly good campaign; measured here, the fastest-RTT shard posted the
# MIDDLE refiller scan time, i.e. no correlation whatsoever.
#
# What genuinely binds is a delta large enough that (throughput x 7 x delta) approaches the worker
# pool - cross-region territory (a 30ms delta needs ~740 workers per shard just to cover the wire).
# 1ms is set an order of magnitude below that: comfortably inert for same-zone placement jitter,
# while still catching a shard accidentally landed in another region or routed through a proxy.
if awk -v d="$RTT_DELTA" -v t="$RTT_MAX_DELTA_MS" 'BEGIN{exit !(d>t)}'; then
  cat >&2 <<EOF

ABORT: shard RTTs differ by ${RTT_DELTA} ms (tolerance ${RTT_MAX_DELTA_MS} ms).

A delta this size is not placement jitter - it is a shard in a different region, behind a proxy, or
otherwise not where the others are. At that scale RTT stops being absorbed by worker concurrency and
starts consuming the pool, so "this shard count did not scale" and "this shard was far away" become
inseparable - which is exactly the ambiguity that made the original ladder uninterpretable.

Put every instance in the same region AND zone as the bench VM, then re-run. To measure a
deliberately heterogeneous fleet, raise RTT_MAX_DELTA_MS explicitly - but then the RTT spread is a
variable of the experiment and belongs in the writeup.
EOF
  exit 1
fi

# --- The ladder -----------------------------------------------------------------------------------
# Control 2: rep-major order, so session drift is spread across arms rather than loaded onto one.
echo
echo "== ladder: ${REPS} reps x 1..${MAXSHARDS} shards, conc ${CONC}, ${WINDOW} window =="
for rep in $(seq 1 "$REPS"); do
  for arm in $(seq 1 "$MAXSHARDS"); do
    tag="l${arm}shard-r${rep}"
    db="dwarf_lad_${RUN_ID}_${arm}s_r${rep}"
    # shellcheck disable=SC2206  # word splitting is the intended parse of EXTRA
    args=(-workload "$WORKLOAD" -vcpus "$VCPUS" -concurrency "$CONC"
      -window "$WINDOW" -warmup "$WARMUP"
      ${EXTRA:-}
      -label "shard ladder ${arm}-shard rep ${rep}"
      -out "${OUT}/r-${tag}.json")

    # Control 1: a fresh database per run, on this arm's own instances.
    for i in $(seq 0 $((arm - 1))); do
      adm=$(admin_dsn "${DSNS[$i]}")
      psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
      psq "$adm" "CREATE DATABASE ${db}" >/dev/null
      args+=(-dsn "$((i + 1))=$(dsn_with_db "${DSNS[$i]}" "$db")")
    done

    echo "-- ${tag}"
    "$BENCH" "${args[@]}"

    # Reclaim immediately: an accumulating instance is control 1's whole point, and the next arm
    # runs on these same instances.
    for i in $(seq 0 $((arm - 1))); do
      psq "$(admin_dsn "${DSNS[$i]}")" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    done
  done
done

# --- Summary --------------------------------------------------------------------------------------
# Reports the ladder with its spread, plus the refiller instruments, which are what make this run
# able to answer WHY rather than only WHAT. Per-shard scan latency separates "one shard is slow"
# from "waiting for the slowest shard is expensive"; the discarded/selected ratio says whether the
# refiller is oversupplying the workers at all.
echo
echo "== summary =="
python3 - "$OUT" <<'PY'
import glob, json, os, statistics, sys

out = sys.argv[1]
arms = {}
for path in sorted(glob.glob(os.path.join(out, "r-l*shard-r*.json"))):
    doc = json.load(open(path))
    n = int(os.path.basename(path).split("l")[1].split("shard")[0])
    for r in doc.get("results", []):
        arms.setdefault(n, []).append((r, doc))

print(f"{'shards':>6} {'n':>2} {'steps/s mean':>13} {'spread':>14} {'p99ms':>8} {'rtt p50':>8}")
means = {}
for n in sorted(arms):
    sp = [r["stepsPerSec"] for r, _ in arms[n]]
    p99 = [r["p99Ms"] for r, _ in arms[n]]
    rtt = [d.get("rtt", {}).get("p50Ms", 0) for _, d in arms[n]]
    means[n] = statistics.mean(sp)
    spread = f"{min(sp):.0f}-{max(sp):.0f}"
    print(f"{n:>6} {len(sp):>2} {means[n]:>13.0f} {spread:>14} {statistics.mean(p99):>8.0f} {statistics.mean(rtt):>8.3f}")

if 1 in means:
    print("\nscaling vs 1 shard:")
    for n in sorted(means):
        print(f"  {n} shard(s): x{means[n]/means[1]:.2f}")
    if len(arms.get(1, [])) > 1:
        sp1 = [r["stepsPerSec"] for r, _ in arms[1]]
        noise = (max(sp1) - min(sp1)) / statistics.mean(sp1)
        print(f"\n1-shard run-to-run spread is {noise*100:.0f}% of its mean - any arm-to-arm")
        print("difference smaller than this is not resolved by this campaign.")

print("\n-- refiller --")
hdr = False
for n in sorted(arms):
    sel = disc = 0
    per_shard, whole = {}, []
    for r, _ in arms[n]:
        c = r.get("engineCounters", {})
        sel += c.get("dwarf_refill_candidates_selected", 0)
        disc += c.get("dwarf_refill_candidates_discarded", 0)
        for h in r.get("engineHistograms", []):
            if h["count"] == 0:
                continue
            mean_ms = h["sumSeconds"] / h["count"] * 1000
            if h["name"] == "dwarf_refill_duration_seconds":
                whole.append(mean_ms)
            elif h["name"] == "dwarf_refill_query_duration_seconds":
                a = h.get("attrs", {})
                per_shard.setdefault((a.get("shard"), a.get("phase")), []).append(mean_ms)
    if not hdr:
        print(f"{'shards':>6} {'waste':>7} {'pass ms':>8}  per-shard scan ms (shard/phase)")
        hdr = True
    waste = f"{disc/sel*100:.0f}%" if sel else "n/a"
    pass_ms = f"{statistics.mean(whole):.2f}" if whole else "n/a"
    detail = "  ".join(
        f"{s}/{p[:4]}={statistics.mean(v):.2f}"
        for (s, p), v in sorted(per_shard.items(), key=lambda kv: (kv[0][1], kv[0][0]))
    )
    print(f"{n:>6} {waste:>7} {pass_ms:>8}  {detail}")

print("""
Reading it:
  - per-shard scan times DIVERGING between shards  -> one shard is genuinely slower
  - pass ms >> the per-shard max                   -> the cost is waiting for the slowest shard
  - waste near 100%                                -> the refiller oversupplies the workers
  - none of the above, and the ladder still flat   -> the refiller is not the constraint; look
                                                      elsewhere before redesigning it""")
PY
