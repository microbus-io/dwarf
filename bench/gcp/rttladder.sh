#!/usr/bin/env bash
#
# RTT ladder: how much throughput does one shard lose per millisecond of distance to its database?
# Runs ON the bench VM.
#
# WHY THIS SCRIPT EXISTS. A saturated shard obeys `throughput = M / (k*RTT + s)` - M connections divided
# by how long each step OCCUPIES one. A worker inside a task holds no connection, so adding workers cannot
# move it; the only terms are the pool (an operator's vCPU count) and the per-step occupancy, of which
# `k*RTT` is the part that travels. k is the round trips one step makes, ~9.6-11 by reading the code, and
# it has never been measured end to end. Fitting this ladder measures it.
#
# It also settles a contradiction inside docs/benchmark-cloud.md, which reports a 16-vCPU shard at pool 96
# twice - 7,491 steps/s in the vertical-scaling table and 5,355 in the connection sweep. Two RTTs, one
# equation: the rig that measured the low number was simply further from its database. Nothing else about
# the two rigs differed.
#
# WHAT IT MEASURES, AND WHAT IT DOES NOT. Latency is injected with `tc netem` on the VM's EGRESS only, so
# an arm is not identical to a genuinely distant database (the return path is untouched, and jitter/MTU
# behaviour differ). For the ceiling equation that should not matter - occupancy only cares about total
# round-trip time - and the check is that arms with GENUINE placement RTT land on the same curve. Quote
# the fit, not a single arm.
#
# THE ARM MUST BE SATURATED OR IT MEASURES THE GENERATOR, so the commanded rate sits above every arm's
# ceiling (the slowest arm here serves ~170 flows/s). `-max-outstanding` then bounds the backlog: an
# unbounded one at 4 ms would pile up hundreds of thousands of same-band steps and inflate the phase-1
# band scan, and the arm would be measuring the SCAN rather than the pool - the documented way this rig
# lies. Read `exec/gen` well under 1 as confirmation the arm was genuinely at its ceiling.
#
# THE QDISC IS THE HAZARD. A netem left installed silently corrupts every later campaign on this VM and is
# invisible in the artifacts, so the trap removes it on ANY exit and the script verifies it is gone before
# returning. Check `tc qdisc show` by hand if this is ever interrupted with SIGKILL.
#
# Usage:
#   DSN=postgres://... ./rttladder.sh
# Knobs (env): BENCH (./dwarf-bench-t8), DELAYS ("0 0.5 1 2 4" ms injected one-way), REPS (2),
#   RATE (900), MAX_OUTSTANDING (2000), WINDOW (60s), WARMUP (20s), VCPUS (16), CONC (256),
#   COOLDOWN (20s), OUT, RUN_ID, EXTRA
set -euo pipefail

command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found (Debian: sudo apt-get install -y postgresql-client)." >&2; exit 1; }
# tc lives in /sbin and is NOT on a login user's PATH on Debian, so `command -v tc` reports it missing on
# a host that has it. Resolve it explicitly and prove it runs under the sudo this script needs anyway.
TC="${TC:-$(command -v tc || echo /sbin/tc)}"
sudo "$TC" -V >/dev/null 2>&1 || {
  echo "ABORT: cannot run '$TC' under sudo (Debian: sudo apt-get install -y iproute2)." >&2; exit 1; }

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
BENCH="${BENCH:-./dwarf-bench-t8}"
DELAYS="${DELAYS:-0 0.5 1 2 4}"
REPS="${REPS:-2}"
RATE="${RATE:-900}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-2000}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-20s}"
VCPUS="${VCPUS:-16}"
CONC="${CONC:-256}"
COOLDOWN="${COOLDOWN:-20s}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
OUT="${OUT:-./rttladder-results}"
mkdir -p "$OUT"

# The interface the DATABASE is reached over - never a hardcoded ens4, which is right on GCE today and
# wrong on the next image.
DB_HOST="$(printf '%s' "$DSN" | sed -E 's|^[^@]*@||; s|[:/].*$||')"
DEV="$(ip route get "$DB_HOST" | sed -nE 's/.* dev ([^ ]+).*/\1/p' | head -1)"
[[ -n "$DEV" ]] || { echo "ABORT: could not resolve the interface toward ${DB_HOST}" >&2; exit 1; }

clear_netem() { sudo "$TC" qdisc del dev "$DEV" root 2>/dev/null || true; }
# ANY exit - success, error, or Ctrl-C - must leave the interface clean.
trap clear_netem EXIT INT TERM

admin_dsn() { echo "${1%/*}/postgres?sslmode=disable"; }
psq() { psql "$1" -X -q -At -c "$2"; }

probe_rtt() {
  local dsn="$1" script=""
  script+=$'\\timing on\n'
  for _ in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/{v=$2+0; if (m=="" || v<m) m=v} END {printf "%.4f", (m==""?0:m)}'
}

ADMIN="$(admin_dsn "$DSN")"
echo "run id: ${RUN_ID}  dev ${DEV} -> ${DB_HOST}  delays: ${DELAYS} ms  reps: ${REPS}"
echo "commanded ${RATE} flows/s, max-outstanding ${MAX_OUTSTANDING}, ${VCPUS} vCPU shard"
echo

for rep in $(seq 1 "$REPS"); do
  for d in $DELAYS; do
    tag="d${d}-r${rep}"
    db="dwarf_rtt_${RUN_ID}_$(echo "$d" | tr -d '.')_r${rep}"

    clear_netem
    if [[ "$d" != "0" ]]; then sudo "$TC" qdisc add dev "$DEV" root netem delay "${d}ms"; fi

    # Measured, never assumed: netem delays EGRESS only, so the added RTT is not necessarily 2x the knob,
    # and this is the x-axis the fit uses.
    rtt="$(probe_rtt "$ADMIN")"
    echo "-- ${tag}  injected ${d} ms  measured rtt ${rtt} ms"

    psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    psq "$ADMIN" "CREATE DATABASE ${db}" >/dev/null

    # shellcheck disable=SC2206
    "$BENCH" -dsn "${DSN%/*}/${db}?sslmode=disable" \
      -workload linear -vcpus "$VCPUS" -concurrency "$CONC" \
      -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING" \
      -warmup "$WARMUP" -window "$WINDOW" \
      -label "injected ${d}ms rtt ${rtt}ms rep ${rep}" \
      -out "${OUT}/r-d${d}-r${rep}.json" ${EXTRA:-}

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

rows = {}
dropped = []
for path in sorted(glob.glob(os.path.join(sys.argv[1], "r-d*.json"))):
    doc = json.load(open(path))
    d = float(re.match(r"r-d([0-9.]+)-r\d+\.json", os.path.basename(path)).group(1))
    r = doc["results"][0]
    # A failed END-OF-WINDOW collection leaves gaugesAfter/engineHistograms null and engineCounters
    # empty, while the artifact still says valid:true and stepsPerSec reads 0. Averaging that zero in
    # is how the turnstile ladder reported its best multiple as its worst (2026-08-01). Drop, do not
    # average - and SAY SO, because a silently smaller n reads as a clean result.
    if not (r.get("engineCounters") or {}) or r.get("gaugesAfter") is None:
        dropped.append((os.path.basename(path), "end-of-window metrics missing"))
        continue
    g = r.get("gaugesAfter") or {}
    inuse = sum(v for k, v in g.items() if k.startswith("sequel_pool_in_use"))
    openc = sum(v for k, v in g.items() if k.startswith("sequel_pool_open_connections"))
    c = r.get("engineCounters") or {}
    st, se = c.get("dwarf_flows_started"), c.get("dwarf_steps_executed")
    # Steps per flow is NOT always 10: a fanout of width W is ~W+2. Hardcoding 10 made the fanout arms
    # report exec/gen = 180%, which is 18/10 and says nothing about saturation.
    spf = float(os.environ.get("STEPS_PER_FLOW", "10"))
    rows.setdefault(d, []).append(dict(
        steps=r["stepsPerSec"], rtt=doc["rtt"]["p50Ms"], inuse=inuse, openc=openc,
        occ=(inuse / r["stepsPerSec"] * 1000) if r["stepsPerSec"] else 0,
        share=(se / (st * spf) * 100) if st and se else None,
        cpu=r["host"]["cpuCores"]))

for name, why in dropped:
    print(f"DROPPED {name}: {why}")
if dropped:
    print()

print(f"{'inject':>7} {'n':>2} {'rtt':>7} {'steps/s':>9} {'spread':>15} {'open':>5} {'inuse':>6} "
      f"{'ms/step':>8} {'exec/gen':>9} {'hostCPU':>8}")
pts = []
unsaturated = []
for d in sorted(rows):
    v = rows[d]
    m = lambda k: statistics.mean(x[k] for x in v if x[k] is not None)
    sp = [x["steps"] for x in v]
    # ONLY A SATURATED ARM IS A CEILING. An arm whose pool never filled was capped by the generator, and
    # including it drags the slope toward zero - measured: two generator-capped fanout arms pulled the
    # fit from k~7.1 (saturated pair) to k=7.45 while reporting a confident-looking R^2. Mark and exclude.
    saturated = m("inuse") >= 0.95 * m("openc")
    if saturated:
        pts.append((m("rtt"), m("steps"), m("inuse")))
    else:
        unsaturated.append(d)
    print(f"{d:>6}m {len(v):>2} {m('rtt'):>7.3f} {m('steps'):>9.0f} "
          f"{f'{min(sp):.0f}-{max(sp):.0f}':>15} {m('openc'):>5.0f} {m('inuse'):>6.0f} "
          f"{m('occ'):>8.2f} {m('share'):>8.1f}% {m('cpu'):>8.2f}"
          f"{'' if saturated else '   <- NOT SATURATED, excluded from fit'}")

# occupancy = k*RTT + s, least squares over the arms. M cancels: it is the same pool everywhere.
if unsaturated:
    print(f"\nEXCLUDED from the fit (pool never filled): {sorted(unsaturated)} ms")
if len(pts) >= 2:
    xs = [p[0] for p in pts]
    ys = [p[2] / p[1] * 1000 for p in pts]          # in_use / steps_per_sec -> ms of occupancy per step
    n = len(xs); mx = sum(xs) / n; my = sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    k = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else float("nan")
    s = my - k * mx
    M = statistics.mean(p[2] for p in pts)
    print(f"\nfit  occupancy_ms = {k:.2f} x RTT_ms + {s:.2f}")
    print(f"  k = {k:.2f} round trips per step (the engine's cost model says ~9.6-11)")
    print(f"  s = {s:.2f} ms of per-step time that is NOT round-trip (server work, WAL, queueing)")
    print(f"  at M = {M:.0f} connections, ceiling(RTT) = {M:.0f} / ({k:.2f}*RTT + {s:.2f}) steps/s")
    for probe in (0.32, 0.82):
        print(f"    RTT {probe:.2f} ms -> {M / (k * probe + s) * 1000:>6.0f} steps/s")
print("""
Reading it:
  - k landing outside ~9.6-11        -> a step is making round trips nobody has accounted for
  - exec/gen ~100% on any arm        -> that arm was NOT saturated; its ceiling is a lower bound only
  - in_use < open on any arm         -> likewise: the pool never filled, so the fit point is soft
  - a genuine-placement arm off the  -> netem is not a faithful stand-in for distance; trust the
    curve                               real points and say so""")
PY
