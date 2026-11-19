#!/usr/bin/env bash
#
# RTT ladder with the pool DERIVED BY THE ENGINE, not pinned by the harness. Runs on the admin host.
#
# WHY THIS SCRIPT EXISTS, AND HOW IT DIFFERS FROM cell.sh. cell.sh pins every arm's pool with
# -max-connections, so the engine's RTT ramp and server clamp never run. This one tests the SHIPPED PATH: it pins nothing,
# so the engine probes the (netem-padded) RTT at Startup and sizes its own pool from its budget rule. What
# is under test is therefore the code, not an equation - a wrong ramp, a probe that ran before netem was
# installed, and a pool that lands on a collapse all show up here and cannot show up there.
#
# THE ORDERING IS LOAD-BEARING. The engine probes RTT ONCE, at Startup, and holds it. netem must
# therefore be installed BEFORE the binary runs, or the arm sizes for the unpadded distance and silently
# measures nothing. run_arm installs, then probes, then runs, in that order, and the probe printed beside
# each arm is what proves the padding was live when sizing happened.
#
# WHAT TO READ. Each arm prints the pool the engine SHOULD derive (from a copy of the table) next to the
# distance actually measured. The artifact does not record the pool the engine derived, so that pairing is
# the check that the padding was live when sizing happened - the failure this ordering exists to prevent.
# Then read throughput and database CPU TOGETHER: throughput falling with CPU flat means the table
# under-compensates; throughput falling with CPU spiking is the over-connection collapse.
#
# WHY netem AND NOT sequel's SimulateRTT. SimulateRTT pauses BEFORE the operation reaches the database,
# and for a DB-level statement database/sql acquires the pooled connection inside that operation - so the
# pause holds NO CONNECTION. Since this campaign is about connection occupancy, that bias runs the wrong
# way. netem delays real packets, so the occupancy is real.
#
# THE QDISC IS THE HAZARD. A netem left installed silently corrupts every later campaign on this VM and
# is invisible in the artifacts, so the trap removes it on ANY exit and the script clears it before every
# arm. Check `tc qdisc show` by hand if this is ever interrupted with SIGKILL.
#
# Usage:
#   DSN=postgres://... ./rttderived.sh
# Knobs (env): BENCH (./dwarf-bench), TARGETS ("0.5 1 1.5 2" ms of TOTAL rtt, plus the base arm),
#   REPS (1), RATE (400 flows/s), MAX_OUTSTANDING (100000), WINDOW (120s), WARMUP (20s), VCPUS (8),
#   CONC (256), COOLDOWN (30s), OUT, RUN_ID, EXTRA
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
TARGETS="${TARGETS:-0.5 1 1.5 2}"
REPS="${REPS:-1}"
RATE="${RATE:-400}"
# SIZE THIS FOR A SATURATED ARM, NOT AN UNDER-CEILING ONE. An arm that tracks its offered rate reaches
# only ~rate x latency outstanding (~1,100 at 400 flows/s), which invites a small cap - but the moment an
# arm exceeds the ceiling the backlog grows without bound and the cap BINDS, silently throttling
# admission below the commanded rate. The artifact then reports a lower flowsPerSec (it is the ADMISSION
# rate, not the commanded one) and the arm is no longer a measurement of the rate it was asked for.
# Measured: a 20,000 cap throttled a 700 flows/s arm to 552-590, so 7,000 steps/s was commanded and only
# ~5,500 ever offered. 100,000 does not bind at any rate this rig can drive.
MAX_OUTSTANDING="${MAX_OUTSTANDING:-100000}"
# 120s, not 60s: Cloud Monitoring samples database CPU about once a minute, so a 60s window yields 3-4
# points whose mean is sampling noise (measured: three arms with identical throughput reported 22%, 51%
# and 59%).
WINDOW="${WINDOW:-120s}"
WARMUP="${WARMUP:-20s}"
VCPUS="${VCPUS:-8}"
CONC="${CONC:-256}"
COOLDOWN="${COOLDOWN:-30s}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
OUT="${OUT:-./rttderived-results}"
mkdir -p "$OUT"

# The pool the engine should choose at a measured RTT, for the EXPECTED column only: the bench's own
# -print-pool, which calls the engine's budget rule, so the prediction cannot drift from the code. Nothing
# here is passed to the binary. MAXCONN is read below, before the first arm.
expected_pool() { # $1 = measured rtt ms
  "$BENCH" -print-pool "$1" -server-max-connections "$MAXCONN"
}

# The interface the DATABASE is reached over - never a hardcoded ens4, which is right on GCE today and
# wrong on the next image.
DB_HOST="$(printf '%s' "$DSN" | sed -E 's|^[^@]*@||; s|[:/].*$||')"
DEV="$(ip route get "$DB_HOST" | sed -nE 's/.* dev ([^ ]+).*/\1/p' | head -1)"
[[ -n "$DEV" ]] || { echo "ABORT: could not resolve the interface toward ${DB_HOST}" >&2; exit 1; }

clear_netem() { sudo "$TC" qdisc del dev "$DEV" root 2>/dev/null || true; }
trap clear_netem EXIT INT TERM

ADMIN="${DSN%/*}/postgres?sslmode=disable"
psq() { psql "$1" -X -q -At -c "$2"; }

# Minimum of many `SELECT 1`s in ONE psql session. A process per sample measures ~29 ms of process, TCP
# and TLS startup and buries a sub-millisecond RTT entirely.
probe_rtt() {
  local dsn="$1" script=""
  script+=$'\\timing on\n'
  for _ in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    sed -nE 's/^Time: ([0-9.]+) ms.*/\1/p' | sort -g | head -1
}

clear_netem
MAXCONN="$(psq "$ADMIN" "SHOW max_connections")"
BASE_RTT="$(probe_rtt "$ADMIN")"
[[ -n "$BASE_RTT" ]] || { echo "ABORT: could not measure the base RTT" >&2; exit 1; }

echo "run id: ${RUN_ID}   dev ${DEV} -> ${DB_HOST}"
echo "base rtt ${BASE_RTT} ms   max_connections ${MAXCONN}   ${VCPUS} vCPU shard"
echo "targets: base + ${TARGETS} ms total rtt   reps ${REPS}   ${RATE} flows/s   pool DERIVED by the engine"
echo

run_arm() { # $1=tag $2=netem knob ms (0 = none) $3=rep
  local tag="$1" knob="$2" rep="$3" rtt want db left

  # Install BEFORE probing and BEFORE the binary runs: the engine probes once at Startup, so a qdisc
  # added later would size the pool for the unpadded distance.
  clear_netem
  if [[ "$knob" != "0" ]]; then sudo "$TC" qdisc add dev "$DEV" root netem delay "${knob}ms"; fi

  rtt="$(probe_rtt "$ADMIN")"
  [[ -n "$rtt" ]] || { echo "   SKIP ${tag}: RTT probe failed"; clear_netem; return 0; }
  want="$(expected_pool "$rtt")"
  db="dwarf_rttderived_${RUN_ID}_${tag//./}_r${rep}"

  echo "-- ${tag}  knob ${knob}ms  measured rtt ${rtt} ms  expect pool ${want}"

  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db}" >/dev/null
  psq "$ADMIN" "CREATE DATABASE ${db}" >/dev/null

  # No -max-connections: the engine derives it. That is the whole point of this script.
  # shellcheck disable=SC2206
  "$BENCH" -dsn "${DSN%/*}/${db}?sslmode=disable" \
    -workload linear -concurrency "$CONC" \
    -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING" \
    -warmup "$WARMUP" -window "$WINDOW" \
    -label "rttderived target ${tag} rtt ${rtt}ms expect ${want} rep ${rep}" \
    -out "${OUT}/r-${tag}-r${rep}.json" ${EXTRA:-}

  # A killed engine leaves peer rows that inflate R and gut the derived pool of every LATER arm sharing
  # the database. Fresh database per arm makes that impossible; this only catches a dirty shutdown.
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
  echo "FATAL: netem still installed on ${DEV} - remove it before any further run" >&2
  exit 1
fi
echo
echo "artifacts in ${OUT}"
