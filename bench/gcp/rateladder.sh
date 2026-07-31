#!/usr/bin/env bash
#
# Arrival-rate ladder: how much offered load can one shard actually serve, and what binds when it stops?
# Runs ON the bench VM.
#
# WHY THIS SCRIPT EXISTS. Every other campaign here fixes the commanded rate and varies something about the
# engine. This one varies the rate itself, which is the only way to locate the KNEE - the point where
# throughput stops tracking what is offered - and the only way to tell a ceiling apart from a shortfall.
#
# THE NUMBER TO READ IS STEPS EXECUTED AS A SHARE OF STEPS GENERATED, not steps/s. A rung where the engine
# executes ~100% of what was created is a rung where the GENERATOR was the limit, whatever its steps/s says;
# a rung where the share falls below 1 is the engine or the database at its ceiling. Reading steps/s alone
# conflates the two, and has: the task-delay arms looked 20% short of convergence until the share showed
# they had executed 96-100% of everything offered.
#
# READ DATABASE CPU ALONGSIDE IT (bench/gcp/dbcpu.sh, run afterwards on the artifacts). On this rig the
# saturated arms sat at ~60% of 16 vCPUs - 9.5 cores - with throughput flat, which says the ceiling is NOT
# CPU. If a rung ever shows CPU climbing toward the tier's limit as throughput flattens, that is a different
# ceiling and the conclusion changes.
#
# CONTROLS, inherited from the other campaigns because each was earned by a corrupted run: an idle RTT gate
# with cooldown before every rung (RTT degrades across a session and correlates with throughput at
# rho ~= -0.90, so a rung that starts degraded is a reading of server fatigue, not of load); a fresh
# database per rung, dropped after; and rungs run SEQUENTIALLY in one process - never queue a second
# campaign behind a running one by polling for its worker processes, which reports "finished" during any
# gap between arms and lands two campaigns on one database.
#
# The window defaults to 120s rather than 60s because Cloud Monitoring samples database CPU about once a
# minute: a 60s window yields 3-4 points and its mean is sampling noise (measured: three arms with identical
# throughput reported 22%, 51% and 59%).
#
# Usage:
#   DSN=postgres://... ./rateladder.sh
# Knobs (env): BENCH (./dwarf-bench-t8), RATES ("700 800 1000 1200"), REPS (1), DELAY (0), WINDOW (120s),
#   WARMUP (20s), VCPUS (16), CONC (256), MAX_OUTSTANDING (100000), COOLDOWN (30s), RTT_MAX_MS (1.0),
#   RTT_TRIES (10), OUT, RUN_ID, EXTRA
set -euo pipefail

# PREFLIGHT: psql must exist - see the note in taskdelay.sh. A missing psql makes the RTT gate cool down
# forever while printing nothing that names the cause.
command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found. Install it (Debian: sudo apt-get install -y postgresql-client)." >&2
  exit 1
}

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
BENCH="${BENCH:-./dwarf-bench-t8}"
RATES="${RATES:-700 800 1000 1200}"
REPS="${REPS:-1}"
DELAY="${DELAY:-0}"
WINDOW="${WINDOW:-120s}"
WARMUP="${WARMUP:-20s}"
VCPUS="${VCPUS:-16}"
CONC="${CONC:-256}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-100000}"
COOLDOWN="${COOLDOWN:-30s}"
RTT_MAX_MS="${RTT_MAX_MS:-1.0}"
RTT_TRIES="${RTT_TRIES:-10}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
OUT="${OUT:-./rateladder-results}"
mkdir -p "$OUT"

admin_dsn() { echo "${1%/*}/postgres?sslmode=disable"; }
psq() { psql "$1" -X -q -At -c "$2"; }

# Round trips inside ONE psql session, MINIMUM taken - a psql per sample measures process startup, not the
# path the engine traverses.
probe_rtt() {
  local dsn="$1" script=""
  script+=$'\\timing on\n'
  for _ in $(seq 1 12); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/{print $2}' | tail -n +2 | sort -n | head -1
}

rtt_gate() {
  local r
  for i in $(seq 1 "$RTT_TRIES"); do
    r="$(probe_rtt "$1")"
    if awk -v v="$r" -v t="$RTT_MAX_MS" 'BEGIN{exit !(v>0 && v<=t)}'; then
      printf "   idle rtt %s ms (gate %s ms)\n" "$r" "$RTT_MAX_MS"; return 0
    fi
    printf "   idle rtt %s ms ABOVE gate %s ms - cooling down (%d/%d)\n" "$r" "$RTT_MAX_MS" "$i" "$RTT_TRIES"
    sleep "${COOLDOWN%s}"
  done
  echo "ABORT: idle RTT never returned under ${RTT_MAX_MS} ms; the server is degraded, not the build." >&2
  exit 1
}

ADMIN="$(admin_dsn "$DSN")"
echo "run id: ${RUN_ID}  rates: ${RATES}  reps: ${REPS}  delay ${DELAY}  window ${WINDOW}  shard ${VCPUS} vCPU"
echo

for rep in $(seq 1 "$REPS"); do
  for rate in $RATES; do
    db="dwarf_rl_${RUN_ID}_r${rate}_${rep}"
    echo "-- rate ${rate} flows/s  rep ${rep}"
    rtt_gate "$ADMIN"
    psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}"
    psq "$ADMIN" "CREATE DATABASE ${db}"
    # shellcheck disable=SC2206  # word splitting is the intended parse of EXTRA
    "$BENCH" -dsn "${DSN%/*}/${db}?sslmode=disable" \
      -workload linear -vcpus "$VCPUS" -concurrency "$CONC" \
      -task-delay "$DELAY" -open-loop -arrival-rate "$rate" \
      -max-outstanding "$MAX_OUTSTANDING" -warmup "$WARMUP" -window "$WINDOW" \
      -label "rate ${rate} flows/s rep ${rep}" -out "${OUT}/r-rate${rate}-r${rep}.json" \
      ${EXTRA:-}
    psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}"
    sleep "${COOLDOWN%s}"
  done
done

echo
echo "== summary =="
python3 - "$OUT" <<'PY'
import json, glob, sys, statistics
rows = {}
for f in glob.glob(sys.argv[1] + "/r-rate*.json"):
    j = json.load(open(f)); r = j["results"][0]; c = r.get("engineCounters") or {}
    rate = int(f.split("-rate")[1].split("-")[0])
    st, se = c.get("dwarf_flows_started"), c.get("dwarf_steps_executed")
    rows.setdefault(rate, []).append(dict(
        steps=r["stepsPerSec"], flows=r["flowsPerSec"], rtt=j["rtt"]["p50Ms"],
        share=(se / (st * 10) * 100) if st and se else None,
        pending=(r["gauges"]["gaugesMean"] or {}).get("dwarf_steps_pending|priority=100"),
        oldest=(r["gauges"]["gaugesMean"] or {}).get("dwarf_steps_oldest_pending_age_seconds|priority=100"),
        cpu=r["host"]["cpuCores"], adm=r["createP99Ms"]))
print(f"{'rate':>6} {'n':>2} {'steps/s':>9} {'%cmd':>6} {'flows/s':>8} {'exec/gen':>9} "
      f"{'pending':>9} {'oldest':>8} {'rtt':>6} {'admP99':>8} {'hostCPU':>8}")
for rate in sorted(rows):
    v = rows[rate]
    m = lambda k: statistics.mean(x[k] for x in v if x[k] is not None) if any(x[k] is not None for x in v) else float("nan")
    print(f"{rate:>6} {len(v):>2} {m('steps'):>9.0f} {100*m('steps')/(rate*10):>5.0f}% {m('flows'):>8.1f} "
          f"{m('share'):>8.1f}% {m('pending'):>9.0f} {m('oldest'):>7.1f}s {m('rtt'):>6.2f} "
          f"{m('adm'):>8.0f} {m('cpu'):>8.2f}")
print("""
Reading it:
  - exec/gen ~100% with %cmd < 100  -> the GENERATOR was short; the engine served all it was offered
  - exec/gen < 100%                 -> the engine or database is at its ceiling; this is the knee
  - pending flat, oldest bounded    -> the backlog is draining in age order (oldest ~= pending/steps)
  - oldest growing rung over rung   -> the queue is no longer draining; past the ceiling
Then run bench/gcp/dbcpu.sh on the artifacts: flat throughput with CPU well under the tier means the
ceiling is not CPU (on this rig, ~60% of 16 vCPU at saturation).""")
PY
