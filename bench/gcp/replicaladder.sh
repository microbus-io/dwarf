#!/usr/bin/env bash
#
# Replica ladder: measures throughput at R = 1, 2, 4, 8 engine replicas sharing the SAME shard(s),
# holding offered load constant. Runs ON the bench VM, next to the deployed dwarf-bench binary.
#
# WHY THIS SCRIPT EXISTS. Every claim about R is currently derived, not measured. The engine splits
# each shard's connection budget by the replica count it reads from the dwarf_peers registry
# (M = min(Y-headroom, ~6*V) / R), and docs/benchmark-cloud.md lists that division as the first known
# gap. Two questions have never been put to a rig:
#   - Does the split HOLD? R replicas sharing one shard should sum to about what R=1 delivers - the
#     database's capacity is fixed, so the ladder should read FLAT. A rising ladder means the fleet is
#     over-connecting the shard; a falling one means the split is costing more than it saves.
#   - Does the REFILLER survive it? The refiller runs per replica per shard and its cost does NOT
#     divide by R: R replicas x S shards = R*S scan streams against one backlog, each paying the
#     per-pass floor, each discarding a fraction of what it selects (21-45% at R=1, campaign 11).
#     This is the predicted failure mode, and the summary below is built to show it.
#
# THE CONTROLS, and why each is not optional:
#   1. FIXED TOTAL CONCURRENCY. -concurrency is the TOTAL submitter count, round-robined across the
#      replicas (bench/loadgen.go: "a client behind a load balancer"). Holding it constant across arms
#      is what makes R the only variable. Scaling it with R would measure offered load instead.
#   2. FRESH DATABASE PER RUN - and here that is about dwarf_peers, not table bloat. A hard-killed
#      engine leaves its registry row behind; because R is counted over a fresh window of 4x
#      pingInterval and the engine id is random per restart, a replica starting within that window on
#      the same database counts the corpse, halves its derived pool, and craters throughput (~180 vs
#      ~7,500 steps/s, measured in results/10-fanout-fix-degradation-20260722). A fresh database means
#      an empty registry, which is the only way an arm's R is the R the script asked for.
#   3. REP-MAJOR INTERLEAVE (1,2,4,8 / 1,2,4,8 / ...), so session drift hits every arm equally.
#   4. A WARMUP LONGER THAN THE HEARTBEAT. Replicas discover each other through the registry at
#      startup and re-read it every pingInterval (10s), so pool sizes converge on a heartbeat, not
#      instantly. A warmup shorter than that measures the fleet mid-convergence - which is a real
#      transient, but not this experiment. Default 30s = 3 beats.
#   5. AN RTT GATE, as in shardladder.sh: a shard 2x further away is a different experiment.
#
# WHAT THIS RIG CANNOT DO. The replicas are goroutine fleets in ONE process (bench/main.go). That is
# right for the pool split, peer discovery and the refiller multiplication - all of which are
# database-mediated and blind to process boundaries - but it gives no per-replica RSS attribution and
# no kill -9. The stale-peer hazard in control 2 therefore CANNOT be reproduced here; it needs
# separate processes and is a follow-up.
#
# Usage:
#   DSN1=... [DSN2=...] ./replicaladder.sh
# Knobs (env): BENCH (binary, default ./dwarf-bench), OUT (default ./replicaladder-results),
#   RLADDER (replica counts, default '1,2,4,8'), SHARDS (how many DSNs to use, default 1),
#   REPS (default 3), CONC (default 4096), WINDOW (default 60s), WARMUP (default 30s, see control 4),
#   VCPUS (shard tier, default 16), WORKLOAD (default linear), RTT_MAX_DELTA_MS (default 1.0),
#   RUN_ID (database namespace; defaults to a timestamp), EXTRA (extra dwarf-bench flags,
#   e.g. '-fairness-keys 256 -task-delay 5ms').
set -euo pipefail

DSNS=()
while :; do
  varname="DSN$(( ${#DSNS[@]} + 1 ))"
  [[ -n "${!varname:-}" ]] || break
  DSNS+=("${!varname}")
done
if [[ ${#DSNS[@]} -lt 1 ]]; then
  echo "set at least DSN1 (one Cloud SQL instance); DSN2, ... allow SHARDS>1" >&2
  exit 1
fi

BENCH="${BENCH:-./dwarf-bench}"
OUT="${OUT:-./replicaladder-results}"
RLADDER="${RLADDER:-1,2,4,8}"
SHARDS="${SHARDS:-1}"
REPS="${REPS:-3}"
CONC="${CONC:-4096}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-30s}"
VCPUS="${VCPUS:-16}"
WORKLOAD="${WORKLOAD:-linear}"
RTT_MAX_DELTA_MS="${RTT_MAX_DELTA_MS:-1.0}"

if [[ "$SHARDS" -gt "${#DSNS[@]}" ]]; then
  echo "SHARDS=$SHARDS but only ${#DSNS[@]} DSN(s) supplied" >&2
  exit 1
fi
IFS=',' read -r -a ARMS <<<"$RLADDER"

# RUN_ID namespaces every database this script creates and drops, so its only destructive operation
# (DROP DATABASE) can only ever match a name this run built. See shardladder.sh for the full argument.
RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"

mkdir -p "$OUT"
echo "run id: ${RUN_ID}  (databases: dwarf_rep_${RUN_ID}_r<R>_r<rep>)"
echo "ladder: R in {${RLADDER}} over ${SHARDS} shard(s), conc ${CONC} held constant, ${REPS} reps"

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

# --- The RTT gate ---------------------------------------------------------------------------------
# Round trips inside ONE psql session, MINIMUM taken. The single session is the whole trick: timing a
# `psql -c "SELECT 1"` per sample measures ~29ms of process startup and buries a sub-millisecond RTT,
# so the gate would pass the exact experiment it exists to reject. See shardladder.sh for the full
# argument, including why the gate is on the ABSOLUTE delta rather than the ratio.
probe_rtt_ms() { # $1 = dsn
  local dsn="$1" script i
  script=$'\\timing on\n'
  for i in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/ {v=$2+0; if (m=="" || v<m) m=v} END {if (m=="") m=0; printf "%.4f", m}'
}

echo
echo "== RTT gate =="
RTTS=()
for i in $(seq 0 $((SHARDS - 1))); do
  r=$(probe_rtt_ms "$(admin_dsn "${DSNS[$i]}")")
  RTTS+=("$r")
  printf "  shard %d  rtt %s ms\n" "$((i + 1))" "$r"
done
RTT_MIN=$(printf '%s\n' "${RTTS[@]}" | sort -g | head -1)
RTT_MAX=$(printf '%s\n' "${RTTS[@]}" | sort -g | tail -1)
RTT_DELTA=$(awk -v a="$RTT_MAX" -v b="$RTT_MIN" 'BEGIN{printf "%.3f", a-b}')
echo "  spread ${RTT_DELTA} ms absolute, tolerance ${RTT_MAX_DELTA_MS} ms"
if awk -v d="$RTT_DELTA" -v t="$RTT_MAX_DELTA_MS" 'BEGIN{exit !(d>t)}'; then
  echo >&2
  echo "ABORT: shard RTTs differ by ${RTT_DELTA} ms (tolerance ${RTT_MAX_DELTA_MS} ms)." >&2
  echo "Put every instance in the same region AND zone as the bench VM, then re-run." >&2
  exit 1
fi

# --- The ladder -----------------------------------------------------------------------------------
echo
echo "== ladder: ${REPS} reps x R in {${RLADDER}}, ${SHARDS} shard(s), conc ${CONC}, ${WINDOW} window =="
for rep in $(seq 1 "$REPS"); do
  for R in "${ARMS[@]}"; do
    tag="R${R}-r${rep}"
    db="dwarf_rep_${RUN_ID}_r${R}_r${rep}"
    # shellcheck disable=SC2206  # word splitting is the intended parse of EXTRA
    args=(-workload "$WORKLOAD" -vcpus "$VCPUS" -replicas "$R" -concurrency "$CONC"
      -window "$WINDOW" -warmup "$WARMUP"
      ${EXTRA:-}
      -label "replica ladder R=${R} rep ${rep} (${SHARDS} shards)"
      -out "${OUT}/r-${tag}.json")

    # Control 2: a fresh database per run, hence an empty dwarf_peers registry.
    for i in $(seq 0 $((SHARDS - 1))); do
      adm=$(admin_dsn "${DSNS[$i]}")
      psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
      psq "$adm" "CREATE DATABASE ${db}" >/dev/null
      args+=(-dsn "$((i + 1))=$(dsn_with_db "${DSNS[$i]}" "$db")")
    done

    echo "-- ${tag}"
    "$BENCH" "${args[@]}"

    # Read the registry BEFORE dropping. Graceful shutdown deregisters every replica, so a clean run
    # leaves 0 rows. A non-zero count means a replica did not shut down cleanly, and the NEXT run on
    # this instance would have been the one to suffer for it - which is precisely the failure control
    # 2 exists to prevent, so it is worth surfacing even though the drop below makes it moot here.
    for i in $(seq 0 $((SHARDS - 1))); do
      left=$(psq "$(dsn_with_db "${DSNS[$i]}" "$db")" "SELECT COUNT(*) FROM dwarf_peers" 2>/dev/null || echo "?")
      [[ "$left" == "0" ]] || echo "   WARNING: shard $((i + 1)) left ${left} dwarf_peers row(s) after shutdown"
    done

    for i in $(seq 0 $((SHARDS - 1))); do
      psq "$(admin_dsn "${DSNS[$i]}")" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    done
  done
done

# --- Summary --------------------------------------------------------------------------------------
echo
echo "== summary =="
python3 - "$OUT" <<'PY'
import glob, json, os, re, statistics, sys

out = sys.argv[1]
arms = {}
for path in sorted(glob.glob(os.path.join(out, "r-R*-r*.json"))):
    doc = json.load(open(path))
    m = re.search(r"r-R(\d+)-r\d+\.json$", os.path.basename(path))
    if not m:
        continue
    for r in doc.get("results", []):
        arms.setdefault(int(m.group(1)), []).append((r, doc))

def mean(xs):
    return statistics.mean(xs) if xs else 0.0

print(f"{'R':>3} {'n':>2} {'steps/s mean':>13} {'spread':>14} {'p99ms':>8} {'cores':>6} {'steps/core':>10} {'goroutines':>10}")
means = {}
for n in sorted(arms):
    sp = [r["stepsPerSec"] for r, _ in arms[n]]
    means[n] = mean(sp)
    print(f"{n:>3} {len(sp):>2} {means[n]:>13.0f} {min(sp):>7.0f}-{max(sp):<6.0f} "
          f"{mean([r['p99Ms'] for r, _ in arms[n]]):>8.0f} "
          f"{mean([r.get('host', {}).get('cpuCores', 0) for r, _ in arms[n]]):>6.2f} "
          f"{mean([r.get('host', {}).get('stepsPerCore', 0) for r, _ in arms[n]]):>10.0f} "
          f"{mean([r.get('goroutines', 0) for r, _ in arms[n]]):>10.0f}")

if 1 in means and means[1]:
    print("\nvs R=1 (the pool split holds if this stays FLAT - one database's capacity is fixed):")
    for n in sorted(means):
        print(f"  R={n}: x{means[n]/means[1]:.2f}")
    if len(arms.get(1, [])) > 1:
        sp1 = [r["stepsPerSec"] for r, _ in arms[1]]
        print(f"\nR=1 run-to-run spread is {(max(sp1)-min(sp1))/mean(sp1)*100:.0f}% of its mean - any arm-to-arm")
        print("difference smaller than this is not resolved by this campaign.")

# The refiller block is the point of the campaign: its cost is per replica per shard and does not
# divide by R, so R*S scan streams contend for one backlog. Waste is the tell - each replica selects
# candidates a peer has already claimed, and discards them.
print("\n-- refiller (per replica per shard: the cost that does NOT divide by R) --")
print(f"{'R':>3} {'waste':>7} {'pass ms':>8} {'sel/s':>9} {'poolwait ms/acq':>16}")
for n in sorted(arms):
    sel = disc = 0
    whole, waits = [], []
    for r, _ in arms[n]:
        c = r.get("engineCounters", {})
        sel += c.get("dwarf_refill_candidates_selected", 0)
        disc += c.get("dwarf_refill_candidates_discarded", 0)
        for h in r.get("engineHistograms", []):
            if h["count"] and h["name"] == "dwarf_refill_duration_seconds":
                whole.append(h["sumSeconds"] / h["count"] * 1000)
        before, after = r.get("gaugesBefore", {}), r.get("gaugesAfter", {})
        dc = after.get("sequel_pool_wait_count", 0) - before.get("sequel_pool_wait_count", 0)
        dd = after.get("sequel_pool_wait_duration_seconds", 0) - before.get("sequel_pool_wait_duration_seconds", 0)
        if dc > 0:
            waits.append(dd / dc * 1000)
    secs = sum(r["windowSec"] for r, _ in arms[n]) or 1
    waste = f"{disc/sel*100:.0f}%" if sel else "n/a"
    print(f"{n:>3} {waste:>7} {mean(whole):>8.2f} {sel/secs:>9.0f} {mean(waits):>16.2f}")

print("""
Reading it:
  - ladder FLAT vs R=1, waste flat           -> the pool split works; R is a free axis
  - ladder FALLS as R rises, waste RISING    -> the refiller's R*S multiplication is the cost:
                                                replicas are selecting each other's candidates
  - ladder FALLS, waste flat, poolwait RISING-> the per-replica pool is too small to schedule with;
                                                the split is dividing past the tier's knee
  - ladder RISES                             -> the fleet is over-connecting the shard: R is being
                                                undercounted (check the registry) or the divisor is
                                                not being applied
  - cores near the host's total              -> the rig is engine-bound, not measuring R at all;
                                                stepsPerCore flat across arms is the confirming tell""")
PY
