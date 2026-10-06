#!/usr/bin/env bash
#
# One cell of the cloud grid - one workload at one RTT on one shard tier - measured end to end: the peak,
# the pool sweep at 2/3 of it, and a confirming peak when the sweep's pool differs from the shipped one.
# Runs ON the bench VM.
#
# THE ORDER RESOLVES A CHICKEN AND EGG. Peak depends on the pool and the pool choice depends on the peak, so:
#   1. PEAK at the derived pool - what ships. Open loop, flat out, the backlog capped by -max-outstanding,
#      PEAK_REPS runs. Peak = the saturated window mean, never a best rung.
#   2. POOL SWEEP at 2/3 of that peak. Throughput at 2/3 load is 2/3 of the peak by definition, so these runs
#      measure LATENCY and which pools run clean. Optimum = the smallest pool that delivers what it is
#      offered, with p99 within P99_SLACK of the best p99 in the sweep and no stall.
#   3. CONFIRM: if the optimum differs from the derived pool by more than CONFIRM_DIFF, the peak runs once
#      more at the optimum. That is the number to publish; if it drops, the 2/3-chosen pool caps the database
#      below its peak - a finding to bring back, not to paper over.
#
# THE POOL IS PINNED in every run. The derived pool comes from the engine's own table for the TARGET rtt
# (`-print-pool`), because the engine's Startup probe can land either side of a bucket edge and an unpinned
# run would confound pool size with probe noise.
#
# netem complements the rig's own distance to land the TARGET total RTT. It can add delay, never take it
# away, so a rig whose base RTT is above the target aborts here: recreate the VM (same zone) and redraw. The
# measured RTT goes into every label. THE QDISC IS THE HAZARD - the trap removes it on any exit and the
# script verifies it is gone; check `tc qdisc show` by hand after a SIGKILL.
#
# Per run: a fresh lower-case database created before and dropped right after, an idle RTT gate with
# cooldown (RTT degrades across a session, rho ~= -0.90 against throughput), and a dwarf_peers check. The
# summary drops any artifact whose end-of-window collection failed, and flags any run with peer churn.
#
# Usage:
#   DSN=postgres://... TARGET_RTT=0.25 VCPUS=16 WORKLOAD=linear ./cell.sh
# Knobs (env): BENCH (./dwarf-bench), WORKLOAD (linear), PAYLOAD (0), WIDTH (8, fanout/scatter),
#   DEPTH (1, scatter), LINEAR_STEPS (10), TARGET_RTT (ms, required), VCPUS (16), PEAK_REPS (3),
#   WINDOW (10m), WARMUP (2m), CONC (256 creators), MAX_OUTSTANDING (20000), SWEEP ("0.5 0.75 1.0 1.25 1.5",
#   multiples of the derived pool), P99_SLACK (0.10), CONFIRM_DIFF (0.15), RTT_SLACK_MS (0.15),
#   RTT_TRIES (10), COOLDOWN (30s), RESERVE (30), OUT, RUN_ID, EXTRA
set -euo pipefail

command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found (Debian: sudo apt-get install -y postgresql-client)." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ABORT: python3 required" >&2; exit 1; }
TC="${TC:-$(command -v tc || echo /sbin/tc)}"
sudo "$TC" -V >/dev/null 2>&1 || {
  echo "ABORT: cannot run '$TC' under sudo (Debian: sudo apt-get install -y iproute2)." >&2; exit 1; }

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
TARGET_RTT="${TARGET_RTT:?set TARGET_RTT in ms (0.25, 0.5, 1.0, 1.5 or 2.0)}"
BENCH="${BENCH:-./dwarf-bench}"
WORKLOAD="${WORKLOAD:-linear}"
PAYLOAD="${PAYLOAD:-0}"
WIDTH="${WIDTH:-8}"
DEPTH="${DEPTH:-1}"
LINEAR_STEPS="${LINEAR_STEPS:-10}"
VCPUS="${VCPUS:-16}"
PEAK_REPS="${PEAK_REPS:-3}"
WINDOW="${WINDOW:-10m}"
WARMUP="${WARMUP:-2m}"
CONC="${CONC:-256}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-20000}"
SWEEP="${SWEEP:-0.5 0.75 1.0 1.25 1.5}"
P99_SLACK="${P99_SLACK:-0.10}"
CONFIRM_DIFF="${CONFIRM_DIFF:-0.15}"
RTT_SLACK_MS="${RTT_SLACK_MS:-0.15}"
RTT_TRIES="${RTT_TRIES:-10}"
COOLDOWN="${COOLDOWN:-30s}"
RESERVE="${RESERVE:-30}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
CELL="${WORKLOAD}-p${PAYLOAD}-rtt${TARGET_RTT}-v${VCPUS}"
OUT="${OUT:-./cell-results/${CELL}-${RUN_ID}}"
mkdir -p "$OUT"

# Steps per flow converts a commanded FLOW rate into the step rate the exec/gen share divides by.
case "$WORKLOAD" in
  linear)  SPF="$LINEAR_STEPS" ;;
  fanout)  SPF=$(( WIDTH + 2 )) ;;
  scatter) SPF=$(( WIDTH * DEPTH + 2 )) ;;
  *) echo "ABORT: cell.sh drives linear, fanout or scatter, not ${WORKLOAD}" >&2; exit 1 ;;
esac
WORKLOAD_ARGS=(-workload "$WORKLOAD" -payload "$PAYLOAD" -fanout-width "$WIDTH" -branch-depth "$DEPTH" -linear-steps "$LINEAR_STEPS")

DB_HOST="$(printf '%s' "$DSN" | sed -E 's|^[^@]*@||; s|[:/].*$||')"
DEV="$(ip route get "$DB_HOST" | sed -nE 's/.* dev ([^ ]+).*/\1/p' | head -1)"
[[ -n "$DEV" ]] || { echo "ABORT: could not resolve the interface toward ${DB_HOST}" >&2; exit 1; }
clear_netem() { sudo "$TC" qdisc del dev "$DEV" root 2>/dev/null || true; }
trap clear_netem EXIT INT TERM

ADMIN="${DSN%/*}/postgres?sslmode=disable"
psq() { psql "$1" -X -q -At -c "$2"; }

# Minimum of many `SELECT 1`s in ONE psql session, the first discarded: a process per sample measures
# process and TLS startup, and the minimum approximates the wire where a mean carries scheduler jitter.
probe_rtt() {
  local script=""
  script+=$'\\timing on\n'
  for _ in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$1" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    sed -nE 's/^Time: ([0-9.]+) ms.*/\1/p' | tail -n +2 | sort -g | head -1
}

# Waits, idle, for the measured RTT to come back within RTT_SLACK_MS of the target. A run that starts on a
# degraded server is a reading of its fatigue, not of the configuration.
rtt_gate() {
  local r limit
  limit="$(python3 -c "print(${TARGET_RTT} + ${RTT_SLACK_MS})")"
  for i in $(seq 1 "$RTT_TRIES"); do
    r="$(probe_rtt "$ADMIN")"
    if awk -v v="$r" -v t="$limit" 'BEGIN{exit !(v>0 && v<=t)}'; then
      printf "   idle rtt %s ms (gate %s ms)\n" "$r" "$limit"; GATE_RTT="$r"; return 0
    fi
    printf "   idle rtt %s ms ABOVE gate %s ms - cooling down (%d/%d)\n" "${r:-?}" "$limit" "$i" "$RTT_TRIES"
    sleep "${COOLDOWN%s}"
  done
  echo "ABORT: idle RTT never returned under ${limit} ms; the server is degraded, not the build." >&2
  exit 1
}

clear_netem
BASE_RTT="$(probe_rtt "$ADMIN")"
[[ -n "$BASE_RTT" ]] || { echo "ABORT: could not measure the base RTT" >&2; exit 1; }
KNOB="$(python3 -c "
d = ${TARGET_RTT} - ${BASE_RTT}
print('abort' if d < -0.05 else (f'{d:.3f}' if d > 0.02 else '0'))")"
if [[ "$KNOB" == "abort" ]]; then
  echo "ABORT: base RTT ${BASE_RTT} ms is above the ${TARGET_RTT} ms target; netem cannot remove delay." >&2
  echo "       Recreate the VM in the same zone and redraw." >&2
  exit 1
fi
if [[ "$KNOB" != "0" ]]; then sudo "$TC" qdisc add dev "$DEV" root netem delay "${KNOB}ms"; fi

MAXCONN="$(psq "$ADMIN" "SHOW max_connections")"
BUDGET=$(( MAXCONN - RESERVE ))
POOL="$("$BENCH" -vcpus "$VCPUS" -print-pool "$TARGET_RTT")"
(( POOL <= BUDGET )) || { echo "ABORT: derived pool ${POOL} exceeds max_connections ${MAXCONN} - ${RESERVE} reserved" >&2; exit 1; }

echo "cell ${CELL}  run ${RUN_ID}  ${SPF} steps/flow"
echo "base rtt ${BASE_RTT} ms, netem +${KNOB} ms on ${DEV} -> target ${TARGET_RTT} ms"
echo "derived pool ${POOL} (${VCPUS} vCPU), max_connections ${MAXCONN}, window ${WINDOW} after ${WARMUP}"
echo

# run_one TAG POOL RATE - one run on a fresh database. RATE 0 = flat out.
run_one() {
  local tag="$1" pool="$2" rate="$3" db left
  db="dwarf_cell_${RUN_ID}_${tag//[^a-z0-9]/_}"
  echo "-- ${tag}  pool ${pool}  rate ${rate:-0} flows/s"
  rtt_gate
  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db} WITH (FORCE)" >/dev/null
  psq "$ADMIN" "CREATE DATABASE ${db}" >/dev/null
  # shellcheck disable=SC2086  # word splitting is the intended parse of EXTRA
  "$BENCH" -dsn "${DSN%/*}/${db}?sslmode=disable" "${WORKLOAD_ARGS[@]}" \
    -vcpus "$VCPUS" -max-open-conns "$pool" -concurrency "$CONC" \
    -open-loop -arrival-rate "$rate" -max-outstanding "$MAX_OUTSTANDING" \
    -warmup "$WARMUP" -window "$WINDOW" -stats-interval 10s \
    -label "cell ${CELL} ${tag} pool ${pool} rate ${rate} gate-rtt ${GATE_RTT}ms" \
    -out "${OUT}/${tag}.json" ${EXTRA:-} > "${OUT}/${tag}.log" 2>&1 || echo "   bench exited nonzero; see ${OUT}/${tag}.log"
  left=$(psq "${DSN%/*}/${db}?sslmode=disable" "SELECT COUNT(*) FROM dwarf_peers" 2>/dev/null || echo "?")
  [[ "$left" == "0" ]] || echo "   WARNING: left ${left} dwarf_peers row(s) after shutdown"
  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db} WITH (FORCE)" >/dev/null
  sleep "${COOLDOWN%s}"
}

# summarize - the JSON the later steps read, plus the table.
summarize() {
  python3 - "$OUT" "$SPF" "$POOL" "$P99_SLACK" "$WORKLOAD" "$PAYLOAD" "$WIDTH" "$DEPTH" "$TARGET_RTT" "$VCPUS" <<'PY'
import json, glob, os, re, statistics, sys
out, spf, derived, slack = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
cell = dict(workload=sys.argv[5], payload=int(sys.argv[6]), width=int(sys.argv[7]), depth=int(sys.argv[8]),
            targetRtt=float(sys.argv[9]), vcpus=int(sys.argv[10]), stepsPerFlow=spf)
runs = []
for path in sorted(glob.glob(os.path.join(out, "*.json"))):
    if os.path.basename(path) == "cell.json":
        continue
    doc = json.load(open(path)); r = doc["results"][0]; c = r.get("engineCounters") or {}
    tag = os.path.basename(path)[:-5]
    bad = []
    if not c or r.get("gaugesAfter") is None:
        bad.append("end-of-window metrics missing")
    if not doc.get("valid", False):
        bad.append("invalid: " + doc.get("invalidity", ""))
    if c.get("dwarf_peer_changes", 0):
        bad.append(f"peer churn ({c['dwarf_peer_changes']})")
    st, se = c.get("dwarf_flows_started"), c.get("dwarf_steps_executed")
    m = re.search(r"pool (\d+) rate (\d+)", doc.get("label", ""))
    runs.append(dict(tag=tag, pool=int(m.group(1)) if m else None, rate=int(m.group(2)) if m else None,
        steps=r["stepsPerSec"], flows=r["flowsPerSec"], p50=r["p50Ms"], p99=r["p99Ms"],
        share=(se / (st * spf)) if st and se else None, rtt=doc["rtt"]["p50Ms"], db=r.get("db"), bad=bad))

peaks = [x for x in runs if x["tag"].startswith("peak-") and not x["bad"]]
peak_flows = statistics.mean(x["flows"] for x in peaks) if peaks else None
sweep = [x for x in runs if x["tag"].startswith("sweep-") and not x["bad"]]
best_p99 = min((x["p99"] for x in sweep), default=None)
def clean(x):
    # Delivers what it is offered, latency near the best the sweep saw, and no stall (p99 within 3x p50
    # is "clean" on this system: 1.2x when healthy, 6.5x at a stall's onset).
    return (x["share"] is not None and x["share"] >= 0.99 and best_p99 is not None
            and x["p99"] <= best_p99 * (1 + slack) and x["p50"] > 0 and x["p99"] / x["p50"] <= 3)
ok = sorted((x for x in sweep if clean(x)), key=lambda x: x["pool"])
optimum = ok[0]["pool"] if ok else None
json.dump(dict(cell=cell, derivedPool=derived, peakFlowsPerSec=peak_flows, optimumPool=optimum, runs=runs),
          open(os.path.join(out, "cell.json"), "w"), indent=2)

print(f"{'run':>18} {'pool':>5} {'rate':>6} {'steps/s':>9} {'exec/gen':>9} {'p50ms':>7} {'p99ms':>8} {'p99/p50':>8} {'rtt':>6}  note")
for x in runs:
    note = "; ".join(x["bad"]) or ("clean" if x in ok else "")
    share = f"{x['share']*100:8.1f}%" if x["share"] is not None else "       ?"
    ratio = x["p99"] / x["p50"] if x["p50"] else 0
    print(f"{x['tag']:>18} {x['pool'] or 0:>5} {x['rate'] or 0:>6} {x['steps']:>9.0f} {share} "
          f"{x['p50']:>7.0f} {x['p99']:>8.0f} {ratio:>8.1f} {x['rtt']:>6.2f}  {note}")
print(f"\npeak {peak_flows and round(peak_flows, 1)} flows/s at the derived pool {derived}; optimum pool {optimum}")
PY
}

# 1. Peak at the derived pool.
for rep in $(seq 1 "$PEAK_REPS"); do
  run_one "peak-r${rep}" "$POOL" 0
done
summarize >/dev/null
PEAK_FLOWS="$(python3 -c "import json; print(json.load(open('${OUT}/cell.json'))['peakFlowsPerSec'] or 0)")"
RATE23="$(python3 -c "print(int(${PEAK_FLOWS} * 2 / 3))")"
(( RATE23 > 0 )) || { echo "ABORT: no valid peak run to take 2/3 of" >&2; summarize; exit 1; }
echo "peak ${PEAK_FLOWS} flows/s -> 2/3 = ${RATE23} flows/s"

# 2. Pool sweep at 2/3, in an order shuffled per run id so no pool always runs on the freshest server.
POOLS="$(python3 -c "
import random
r = random.Random('${RUN_ID}')
ps = sorted({max(2, round(${POOL} * float(f))) for f in '${SWEEP}'.split()})
r.shuffle(ps); print(' '.join(str(p) for p in ps))")"
for p in $POOLS; do
  (( p <= BUDGET )) || { echo "-- SKIP pool ${p}: above max_connections - ${RESERVE}"; continue; }
  run_one "sweep-p${p}" "$p" "$RATE23"
done

# 3. Confirm the peak at the optimum when it is not the derived pool.
summarize >/dev/null
OPT="$(python3 -c "import json; print(json.load(open('${OUT}/cell.json'))['optimumPool'] or 0)")"
if (( OPT > 0 )) && python3 -c "import sys; sys.exit(0 if abs(${OPT} - ${POOL}) / ${POOL} > ${CONFIRM_DIFF} else 1)"; then
  run_one "confirm-p${OPT}" "$OPT" 0
fi

clear_netem
if sudo "$TC" qdisc show dev "$DEV" | grep -q netem; then
  echo "ABORT: netem STILL INSTALLED on ${DEV} - every later run on this VM is corrupted until removed." >&2
  exit 1
fi
echo "netem removed from ${DEV} (verified)"
echo
echo "== ${CELL} =="
summarize
echo "
Database CPU is not in the artifacts (a managed instance). Pull them and run ./dbcpu.sh <artifact>... from
the operator machine. Per-run stored and WAL bytes are in each artifact's results[0].db."
