#!/usr/bin/env bash
#
# Pool sweep at ONE fixed distance: separates the two terms of `occupancy = k*RTT + s`.
# Runs ON the bench VM.
#
# WHY THIS SCRIPT EXISTS, AND WHY IT IS NOT rttpoolladder.sh. That ladder raises the pool AS distance
# grows, which makes its two terms collinear: M is chosen as a linear function of (k*RTT + s), so any
# growth of `s` with pool size is absorbed into the apparent slope `k` and the fit cannot tell the two
# apart. Measured on this rig: the ladder fit 11.40*RTT + 10.16, against 9.11 / 9.49 from a fixed-pool
# ladder - a 1.25x difference that is EITHER a bigger k OR an `s` that doubled with the pool.
#
# HOLDING RTT FIXED AND SWEEPING M BREAKS THE COLLINEARITY:
#
#	occupancy(RTT, M) = k*RTT + s(M)      at fixed RTT   ->   occupancy(M) = const + s(M)
#
# so this run measures the SHAPE of s(M) directly. Subtracting that shape from the ladder's arms leaves
# k*RTT + s(M_ref), whose regression on RTT is a clean k. Neither run can do it alone.
#
# IT ALSO FINDS THE OVER-CONNECTION EDGE AT DISTANCE, which is the stability half of the question. The
# collapse mode (active backends spike, WAL share of waits collapses, CPU:running rises, throughput
# drops ~15x) was only ever characterised at SHORT RTT. Theory says distance makes it safer - fewer
# backends active at once - and that is exactly the class of claim this repo measures rather than
# reasons about. Arms are ordered LOW TO HIGH so a collapse lands at the end, after the useful points.
#
# READ DB CPU ALONGSIDE THROUGHPUT (dbcpu.sh). Throughput flattening with CPU still climbing is the
# server reaching its knee; throughput FALLING with CPU spiking is the collapse.
#
# THE QDISC IS THE HAZARD. A netem left installed silently corrupts every later campaign on this VM and
# is invisible in the artifacts, so the trap removes it on ANY exit and the script verifies it is gone
# before returning. Check `tc qdisc show` by hand if this is ever interrupted with SIGKILL.
#
# Usage:
#   DSN=postgres://... KNOB=3.86 ./poolsweep.sh
# Knobs (env): BENCH (./dwarf-bench), KNOB (netem one-way ms; "" = no injection), POOLS
#   ("100 200 300 400 500 600"), REPS (1), RATE (1400 flows/s - ABOVE the top arm's ceiling so no arm
#   is generator-capped), MAX_OUTSTANDING (2000), WINDOW (60s), WARMUP (20s), VCPUS (16), CONC (256),
#   COOLDOWN (20s), RESERVE (30), OUT, RUN_ID, EXTRA
set -euo pipefail

command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found (Debian: sudo apt-get install -y postgresql-client)." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ABORT: python3 required" >&2; exit 1; }
TC="${TC:-$(command -v tc || echo /sbin/tc)}"
sudo "$TC" -V >/dev/null 2>&1 || {
  echo "ABORT: cannot run '$TC' under sudo (Debian: sudo apt-get install -y iproute2)." >&2; exit 1; }

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
BENCH="${BENCH:-./dwarf-bench}"
# ${KNOB-...}, NOT ${KNOB:-...}: an EMPTY knob means "no injection, run at the rig's own distance", and
# the colon form would silently substitute the default and inject 3.86 ms into the base-RTT sweep.
KNOB="${KNOB-3.86}"
POOLS="${POOLS:-100 200 300 400 500 600}"
REPS="${REPS:-1}"
RATE="${RATE:-1400}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-2000}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-20s}"
VCPUS="${VCPUS:-16}"
CONC="${CONC:-256}"
COOLDOWN="${COOLDOWN:-20s}"
RESERVE="${RESERVE:-30}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
OUT="${OUT:-./poolsweep-results}"
mkdir -p "$OUT"

DB_HOST="$(printf '%s' "$DSN" | sed -E 's|^[^@]*@||; s|[:/].*$||')"
DEV="$(ip route get "$DB_HOST" | sed -nE 's/.* dev ([^ ]+).*/\1/p' | head -1)"
[[ -n "$DEV" ]] || { echo "ABORT: could not resolve the interface toward ${DB_HOST}" >&2; exit 1; }

clear_netem() { sudo "$TC" qdisc del dev "$DEV" root 2>/dev/null || true; }
trap clear_netem EXIT INT TERM

ADMIN="${DSN%/*}/postgres?sslmode=disable"
psq() { psql "$1" -X -q -At -c "$2"; }

probe_rtt() {
  local dsn="$1" script=""
  script+=$'\\timing on\n'
  for _ in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    sed -nE 's/^Time: ([0-9.]+) ms.*/\1/p' | sort -g | head -1
}

clear_netem
MAXCONN="$(psq "$ADMIN" "SHOW max_connections")"
BUDGET=$(( MAXCONN - RESERVE ))
BASE_RTT="$(probe_rtt "$ADMIN")"

echo "run id: ${RUN_ID}   dev ${DEV} -> ${DB_HOST}"
echo "base rtt ${BASE_RTT} ms, netem knob ${KNOB:-none}"
echo "max_connections ${MAXCONN}, usable ${BUDGET} after ${RESERVE} reserved"
echo "pools: ${POOLS}   reps ${REPS}   ${RATE} flows/s"
echo

for rep in $(seq 1 "$REPS"); do
  for m in $POOLS; do
    if (( m > BUDGET )); then
      echo "-- SKIP pool ${m}: above the server's usable ${BUDGET}"
      continue
    fi
    db="dwarf_poolsweep_${RUN_ID}_m${m}_r${rep}"

    clear_netem
    if [[ -n "$KNOB" ]]; then sudo "$TC" qdisc add dev "$DEV" root netem delay "${KNOB}ms"; fi

    # Re-probed every arm rather than once: the whole sweep is at ONE distance only if that is TRUE, and
    # a drifting rig would otherwise be reported as an s(M) effect.
    rtt="$(probe_rtt "$ADMIN")"
    echo "-- pool ${m}  rtt ${rtt} ms"

    psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    psq "$ADMIN" "CREATE DATABASE ${db}" >/dev/null

    # shellcheck disable=SC2206
    "$BENCH" -dsn "${DSN%/*}/${db}?sslmode=disable" \
      -workload linear -vcpus "$VCPUS" -concurrency "$CONC" \
      -max-open-conns "$m" \
      -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING" \
      -warmup "$WARMUP" -window "$WINDOW" \
      -label "poolsweep pool ${m} rtt ${rtt}ms rep ${rep}" \
      -out "${OUT}/r-m${m}-r${rep}.json" ${EXTRA:-}

    left=$(psq "${DSN%/*}/${db}?sslmode=disable" "SELECT COUNT(*) FROM dwarf_peers" 2>/dev/null || echo "?")
    [[ "$left" == "0" ]] || echo "   WARNING: left ${left} dwarf_peers row(s) after shutdown"
    psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    clear_netem
    sleep "${COOLDOWN%s}"
  done
done

clear_netem
if sudo "$TC" qdisc show dev "$DEV" | grep -q netem; then
  echo "ABORT: netem STILL INSTALLED on ${DEV} - every later run on this VM is corrupted until removed." >&2
  exit 1
fi
echo "netem removed from ${DEV} (verified)"

echo
echo "== summary =="
python3 - "$OUT" <<'PY'
import json, glob, os, re, statistics, sys

rows, dropped = {}, []
for path in sorted(glob.glob(os.path.join(sys.argv[1], "r-m*.json"))):
    doc = json.load(open(path))
    m = int(re.match(r"r-m(\d+)-r\d+\.json", os.path.basename(path)).group(1))
    r = doc["results"][0]
    if not (r.get("engineCounters") or {}) or r.get("gaugesAfter") is None:
        dropped.append((os.path.basename(path), "end-of-window metrics missing"))
        continue
    g = r["gaugesAfter"]
    inuse = sum(v for k, v in g.items() if k.startswith("sequel_pool_in_use"))
    openc = sum(v for k, v in g.items() if k.startswith("sequel_pool_open_connections"))
    c = r["engineCounters"]
    st, se = c.get("dwarf_flows_started"), c.get("dwarf_steps_executed")
    rows.setdefault(m, []).append(dict(
        steps=r["stepsPerSec"], rtt=doc["rtt"]["p50Ms"], inuse=inuse, openc=openc, p99=r["p99Ms"],
        occ=(inuse / r["stepsPerSec"] * 1000) if r["stepsPerSec"] else 0,
        share=(se / (st * 10.0) * 100) if st and se else None))

for name, why in dropped:
    print(f"DROPPED {name}: {why}")
if dropped:
    print()

ms = sorted(rows)
print(f"{'pool':>5} {'n':>2} {'rtt':>6} {'inuse':>6} {'steps/s':>9} {'vs prev':>8} {'occ ms':>7} "
      f"{'s(M)-s(M0)':>11} {'p99 s':>7}")
base_occ = None
prev = None
for m in ms:
    v = rows[m]
    f = lambda k: statistics.mean(x[k] for x in v if x[k] is not None)
    occ = f("occ")
    if base_occ is None:
        base_occ = occ
    # At FIXED rtt the k*RTT term is a constant, so every change in occupancy IS a change in s(M).
    delta = occ - base_occ
    note = ""
    if f("inuse") < 0.95 * f("openc"):
        note = "   <- POOL NEVER FILLED: the server, not the pool, is the constraint here"
    vs = f"{f('steps')/prev*100:>7.0f}%" if prev else f"{'-':>8}"
    prev = f("steps")
    print(f"{m:>5} {len(v):>2} {f('rtt'):>6.2f} {f('inuse'):>6.0f} {f('steps'):>9.0f} {vs} "
          f"{occ:>7.2f} {delta:>+11.2f} {f('p99')/1000:>7.2f}{note}")

print("""
Reading it:
  - s(M) flat across the sweep    -> `s` does NOT grow with the pool; the ladder's steeper slope is a
                                     genuinely larger k, and k*RTT is the whole story
  - s(M) rising with M            -> queueing. The ladder's 11.40 was k plus this; subtract it before
                                     quoting a k, and the sizing formula needs an s(M) term
  - steps/s flattening, CPU up    -> the server's knee: more connections stop buying throughput
  - steps/s FALLING, CPU spiking  -> the over-connection collapse, at distance. This is the stability
                                     answer; note the pool it started at
  - vs prev ~100% with pool unfilled -> saturation of the DB, not of the pool
""")
PY
