#!/usr/bin/env bash
#
# RTT-compensated pool ladder: can MORE CONNECTIONS buy back the throughput that DISTANCE costs?
# Runs ON the bench VM.
#
# WHY THIS SCRIPT EXISTS. `rttladder.sh` holds the pool FIXED and sweeps distance, measuring the loss
# (`throughput = M / (k*RTT + s)`). This one sweeps distance and RAISES THE POOL AT EACH RUNG by the
# amount that equation says should restore the ceiling. The two are different questions and want
# different scripts: one measures a cost, this one tests a proposed fix.
#
# THE MECHANISM UNDER TEST. Only `s` is time spent INSIDE the database; `k*RTT` is time on the wire
# holding a connection that is doing nothing. So a pool of M presents the server with only
# `M * s/(k*RTT + s)` busy connections - the duty cycle. At 4.8 ms that is 18%, so a 96-connection pool
# reaches a 16-vCPU instance whose knee is 96-192 with the equivalent of 17 busy connections, and the
# instance sits two-thirds idle (measured: 33% DB CPU with the pool pinned 96/96). Holding SERVER-SIDE
# load constant instead of connection count gives the arm's pool:
#
#	M(RTT) = M_ref * (k*RTT + s) / (k*RTT_ref + s)
#
# THE HYPOTHESIS IS A FLAT LINE. Every arm should serve what the base arm serves. Read throughput and
# DB CPU TOGETHER, because the two failure modes are only separable with both: throughput falling while
# CPU stays flat means the formula under-corrects (`s` grew more than assumed); throughput falling while
# CPU spikes is the over-connection collapse arriving at a distance it has never been characterised at.
#
# WHY netem AND NOT sequel's SimulateRTT. SimulateRTT pauses BEFORE the operation reaches the database,
# and for a DB-level statement `database/sql` acquires the pooled connection inside that operation - so
# the pause holds NO CONNECTION. Inside a transaction it is faithful (the connection is held from BEGIN
# onward), but every standalone query is not. Since this campaign is about connection OCCUPANCY, that
# bias runs the wrong way: it makes the pool look less binding than it is and fits `k` too small. netem
# delays real packets, so the occupancy is real.
#
# THE POOL IS CLAMPED TO THE SERVER'S max_connections, and a clamped arm is NOT a test of the formula -
# it is a test of the server's limit. Such arms are named in the summary and excluded from the verdict.
#
# THE QDISC IS THE HAZARD. A netem left installed silently corrupts every later campaign on this VM and
# is invisible in the artifacts, so the trap removes it on ANY exit and the script verifies it is gone
# before returning. Check `tc qdisc show` by hand if this is ever interrupted with SIGKILL.
#
# Usage:
#   DSN=postgres://... ./rttpoolladder.sh
# Knobs (env): BENCH (./dwarf-bench - the PLAIN build; turnstilePassesPerConn is 8 in the tree, so no
#   special binary is wanted here), TARGETS ("1 2 3 4 5" ms of TOTAL rtt, plus the base arm),
#   REPS (1), RATE (1000 flows/s), MAX_OUTSTANDING (2000), WINDOW (60s), WARMUP (20s), VCPUS (16),
#   CONNS_PER_VCPU (6), CONC (256), COOLDOWN (20s), RESERVE (30), K (9.11), S (9.49), OUT, RUN_ID, EXTRA
set -euo pipefail

command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found (Debian: sudo apt-get install -y postgresql-client)." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ABORT: python3 required" >&2; exit 1; }
# tc lives in /sbin and is NOT on a login user's PATH on Debian, so `command -v tc` reports it missing on
# a host that has it. Resolve it explicitly and prove it runs under the sudo this script needs anyway.
TC="${TC:-$(command -v tc || echo /sbin/tc)}"
sudo "$TC" -V >/dev/null 2>&1 || {
  echo "ABORT: cannot run '$TC' under sudo (Debian: sudo apt-get install -y iproute2)." >&2; exit 1; }

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
BENCH="${BENCH:-./dwarf-bench}"
TARGETS="${TARGETS:-1 2 3 4 5}"
REPS="${REPS:-1}"
RATE="${RATE:-1000}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-2000}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-20s}"
VCPUS="${VCPUS:-16}"
CONNS_PER_VCPU="${CONNS_PER_VCPU:-6}"
CONC="${CONC:-256}"
COOLDOWN="${COOLDOWN:-20s}"
# Headroom left below max_connections: Cloud SQL's own agent, superuser_reserved_connections, and this
# script's psql probes all draw from the same budget, and exhausting it fails the RUN, not just the arm.
RESERVE="${RESERVE:-30}"
# The occupancy fit the arm pools are derived from (campaign 27, M=96): occupancy_ms = K*RTT_ms + S.
K="${K:-9.11}"
S="${S:-9.49}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
OUT="${OUT:-./rttpoolladder-results}"
mkdir -p "$OUT"

# The interface the DATABASE is reached over - never a hardcoded ens4, which is right on GCE today and
# wrong on the next image.
DB_HOST="$(printf '%s' "$DSN" | sed -E 's|^[^@]*@||; s|[:/].*$||')"
DEV="$(ip route get "$DB_HOST" | sed -nE 's/.* dev ([^ ]+).*/\1/p' | head -1)"
[[ -n "$DEV" ]] || { echo "ABORT: could not resolve the interface toward ${DB_HOST}" >&2; exit 1; }

clear_netem() { sudo "$TC" qdisc del dev "$DEV" root 2>/dev/null || true; }
# ANY exit - success, error, or Ctrl-C - must leave the interface clean.
trap clear_netem EXIT INT TERM

ADMIN="${DSN%/*}/postgres?sslmode=disable"
psq() { psql "$1" -X -q -At -c "$2"; }

# Minimum of many `SELECT 1`s in ONE psql session. A process per sample measures ~29 ms of process, TCP
# and TLS startup and buries a sub-millisecond RTT entirely; the minimum approximates pure network time,
# where a mean carries scheduler jitter.
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
[[ -n "$BASE_RTT" ]] || { echo "ABORT: could not measure the base RTT" >&2; exit 1; }
M_REF=$(( VCPUS * CONNS_PER_VCPU ))

echo "run id: ${RUN_ID}   dev ${DEV} -> ${DB_HOST}"
echo "base rtt ${BASE_RTT} ms, reference pool ${M_REF} (${CONNS_PER_VCPU}x ${VCPUS} vCPU)"
echo "max_connections ${MAXCONN}, usable ${BUDGET} after ${RESERVE} reserved"
echo "targets: base + ${TARGETS} ms total rtt   reps ${REPS}   ${RATE} flows/s   fit K=${K} S=${S}"
echo

# The base arm is the reference the formula is anchored on, so it runs first and at the SHIPPED ratio.
run_arm() { # $1=tag $2=netem knob ms (0 = none) $3=rep
  local tag="$1" knob="$2" rep="$3" rtt m conc db clamped=""

  clear_netem
  if [[ "$knob" != "0" ]]; then sudo "$TC" qdisc add dev "$DEV" root netem delay "${knob}ms"; fi

  # Measured, never assumed: netem delays EGRESS only, so the added RTT is not necessarily the knob, and
  # the pool this arm gets is derived from what the wire ACTUALLY costs.
  rtt="$(probe_rtt "$ADMIN")"
  [[ -n "$rtt" ]] || { echo "   SKIP ${tag}: RTT probe failed"; clear_netem; return 0; }

  m="$(python3 -c "
import sys
k,s,base,rtt,ref = ${K},${S},${BASE_RTT},${rtt},${M_REF}
print(max(1, round(ref * (k*rtt + s) / (k*base + s))))")"
  if (( m > BUDGET )); then
    clamped=" CLAMPED from ${m}"
    m=$BUDGET
  fi
  conc="$CONC"
  db="dwarf_rttpool_${RUN_ID}_${tag//./}_r${rep}"

  echo "-- ${tag}  knob ${knob}ms  measured rtt ${rtt} ms  pool ${m}${clamped}"

  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
  psq "$ADMIN" "CREATE DATABASE ${db}" >/dev/null

  # shellcheck disable=SC2206
  "$BENCH" -dsn "${DSN%/*}/${db}?sslmode=disable" \
    -workload linear -vcpus "$VCPUS" -concurrency "$conc" \
    -max-open-conns "$m" \
    -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING" \
    -warmup "$WARMUP" -window "$WINDOW" \
    -label "rttpool target ${tag} rtt ${rtt}ms pool ${m} rep ${rep}" \
    -out "${OUT}/r-${tag}-r${rep}.json" ${EXTRA:-}

  # A killed engine leaves peer rows that inflate R and gut the derived pool of every LATER arm sharing
  # the database. Fresh database per arm makes that impossible; this only catches a dirty shutdown.
  local left
  left=$(psq "${DSN%/*}/${db}?sslmode=disable" "SELECT COUNT(*) FROM dwarf_peers" 2>/dev/null || echo "?")
  [[ "$left" == "0" ]] || echo "   WARNING: left ${left} dwarf_peers row(s) after shutdown"
  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
  clear_netem
  sleep "${COOLDOWN%s}"
}

for rep in $(seq 1 "$REPS"); do
  run_arm "base" 0 "$rep"
  for t in $TARGETS; do
    # Complement the rig's own distance so the arm lands on a ROUND total RTT: a 0.3 ms rig takes a
    # 0.7 ms knob to reach 1 ms. Arms whose target is already below the base are skipped, not negated.
    knob="$(python3 -c "
d = ${t} - ${BASE_RTT}
print(f'{d:.2f}' if d > 0.05 else '0')")"
    if [[ "$knob" == "0" ]]; then
      echo "-- SKIP ${t}ms: below the rig's own base RTT (${BASE_RTT} ms)"
      continue
    fi
    run_arm "${t}ms" "$knob" "$rep"
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
BUDGET="$BUDGET" python3 - "$OUT" <<'PY'
import json, glob, os, re, statistics, sys

rows, dropped = {}, []
for path in sorted(glob.glob(os.path.join(sys.argv[1], "r-*.json"))):
    doc = json.load(open(path))
    tag = re.match(r"r-(.+)-r\d+\.json", os.path.basename(path)).group(1)
    r = doc["results"][0]
    # A failed END-OF-WINDOW collection leaves gaugesAfter null and engineCounters empty while the
    # artifact still says valid:true and stepsPerSec reads 0. Averaging that zero in is how the
    # turnstile ladder reported its best multiple as its worst. Drop, do not average - and SAY SO,
    # because a silently smaller n reads as a clean result.
    if not (r.get("engineCounters") or {}) or r.get("gaugesAfter") is None:
        dropped.append((os.path.basename(path), "end-of-window metrics missing"))
        continue
    g = r["gaugesAfter"]
    inuse = sum(v for k, v in g.items() if k.startswith("sequel_pool_in_use"))
    openc = sum(v for k, v in g.items() if k.startswith("sequel_pool_open_connections"))
    c = r["engineCounters"]
    st, se = c.get("dwarf_flows_started"), c.get("dwarf_steps_executed")
    rows.setdefault(tag, []).append(dict(
        steps=r["stepsPerSec"], rtt=doc["rtt"]["p50Ms"], inuse=inuse, openc=openc,
        occ=(inuse / r["stepsPerSec"] * 1000) if r["stepsPerSec"] else 0,
        share=(se / (st * 10.0) * 100) if st and se else None))

for name, why in dropped:
    print(f"DROPPED {name}: {why}")
if dropped:
    print()

budget = int(os.environ.get("BUDGET", "0"))
order = sorted(rows, key=lambda t: -1 if t == "base" else float(t.rstrip("ms")))
base = None
print(f"{'arm':>6} {'n':>2} {'rtt':>7} {'pool':>5} {'inuse':>6} {'steps/s':>9} {'vs base':>8} "
      f"{'ms/step':>8} {'exec/gen':>9}")
for tag in order:
    v = rows[tag]
    m = lambda k: statistics.mean(x[k] for x in v if x[k] is not None)
    steps = m("steps")
    if tag == "base":
        base = steps
    note = ""
    # A pool that hit the server's limit is measuring max_connections, not the formula.
    if budget and round(m("openc")) >= budget:
        note += "   <- POOL CLAMPED at max_connections; not a test of the formula"
    # in_use < open means the arm never filled its pool: its ceiling is a lower bound only.
    elif m("inuse") < 0.95 * m("openc"):
        note += "   <- POOL NEVER FILLED; ceiling is a lower bound"
    print(f"{tag:>6} {len(v):>2} {m('rtt'):>7.3f} {m('openc'):>5.0f} {m('inuse'):>6.0f} "
          f"{steps:>9.0f} {(steps/base*100 if base else 0):>7.0f}% "
          f"{m('occ'):>8.2f} {m('share'):>8.1f}%{note}")

print("""
Reading it:
  - vs base ~100% at every rung   -> the formula holds; distance is fully recoverable with connections
  - vs base falling, DB CPU flat  -> under-corrected: `s` grew with the pool more than the fit assumed
  - vs base falling, DB CPU spike -> the over-connection collapse, now at distance. Read dbcpu.sh.
  - exec/gen ~100% on any arm     -> that arm was generator-capped, not at its ceiling: raise -arrival-rate
  - ms/step should track K*rtt+S  -> a large miss means the pool change moved `s`, which is the one
                                     extrapolation this ladder cannot make for itself

DB CPU is NOT in the artifact - it is a managed instance the host cannot see. Pull the JSONs and run
  ./dbcpu.sh <artifact>...   (needs gcloud auth; run it from the operator machine, not the VM)
""")
PY
