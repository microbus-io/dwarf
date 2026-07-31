#!/usr/bin/env bash
#
# Task-duration ladder: does throughput depend on how long tasks take? Runs ON the bench VM, next to
# the deployed dwarf-bench binary.
#
# WHY THIS SCRIPT EXISTS. Throughput should be set by what the DATABASE can serve, not by the task
# duration - a worker inside ExecuteTask holds no connection, so a slow task should cost workers, not
# steps/s. Measured against one 16-vCPU shard at a commanded 600 flows/s, with -task-delay the only
# variable, it was not:
#
#     delay   steps/s   goroutines
#     0s      6,005     792          <- serves the full commanded rate
#     1s      1,020     1,157
#     8s        637     5,298        <- 8x the task time bought 4.6x the crew and LESS throughput
#
# Those three arms are preserved as ladder_r600 / delay1s_r600 / delay8s_r600 in
# bench/results/14-vertical-20260727/, each with its own per-arm database CPU, so this script compares
# against recorded artifacts rather than remembered numbers.
#
# WHAT IT IS FOR NOW. The engine gates DATABASE work per shard and grows workers on idleness rather
# than on "every worker is in a task", so the crew is free to grow for long tasks while the database
# sees the concurrency it always saw. The prediction is that the 1s and 8s arms CONVERGE on the 0s arm
# (all three serving the commanded 6,000 steps/s). If they do not, the gate is not what was binding -
# that is the falsifier, and it is the reason to run this before anything else.
#
# The 0s arm is NOT predicted to be identical. Admission cannot bind where the candidate cache already
# capped concurrency, but the completion path debits the same gate WITHOUT waiting, which throttles
# admission under a completion storm and can shorten the pool's waiter queue. Read it against
# dwarf_turnstile_available (printed below): sustained ZERO means admission is the binding constraint,
# task improvement is as plausible as a null.
#
# CONTROLS, each earned by a corrupted campaign:
#   1. REP-MAJOR INTERLEAVE (0,1,8 / 0,1,8 / 0,1,8, never 0,0,0 / 1,1,1 / 8,8,8). Session drift then
#      hits every arm equally instead of loading onto the last one.
#   2. AN IDLE RTT GATE WITH COOLDOWN, before every arm. RTT degrades across a session and correlates
#      with throughput at rho ~= -0.90, so an arm that starts degraded is not a measurement - it is a
#      reading of how tired the server is. Interleaving does not save you: it spreads the damage
#      evenly rather than removing it.
#   3. A FRESH DATABASE PER RUN, dropped after. An accumulating instance depresses throughput ~2.6x
#      uniformly, and a leftover dwarf_peers row inflates the observed replica count, halves the
#      derived pool and craters throughput (~180 vs ~7,500 steps/s).
#   4. WARMUP SCALED TO THE DELAY. A 60 s window opened before the pipeline fills measures the fill,
#      not the steady state, and the fill takes longer the longer tasks are. The defaults reproduce
#      the baseline arms exactly (20 / 45 / 150 s), which is what makes the comparison sound.
#
# Usage:
#   DSN=postgres://... ./taskdelay.sh
# Knobs (env): BENCH (default ./dwarf-bench), OUT (default ./taskdelay-results), REPS (default 3),
#   RATE (commanded flows/s, default 600), VCPUS (shard tier, default 16), WINDOW (default 60s),
#   DELAYS (default "0 1s 8s"), WARMUPS (matching DELAYS, default "20s 45s 150s"),
#   MAX_OUTSTANDING (default 100000), COOLDOWN (default 30s), RTT_MAX_MS (gate, default 1.0),
#   RTT_TRIES (default 10), RUN_ID, EXTRA (extra dwarf-bench flags).
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

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
BENCH="${BENCH:-./dwarf-bench}"
OUT="${OUT:-./taskdelay-results}"
REPS="${REPS:-3}"
RATE="${RATE:-600}"
VCPUS="${VCPUS:-16}"
WINDOW="${WINDOW:-60s}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-100000}"
# CONC is the number of CREATOR goroutines, not a load level: open-loop load is set by -arrival-rate,
# and this only bounds how fast the generator can issue Creates. It must be pinned to ONE value, and
# 128 is the baseline arms' value. Leaving dwarf-bench's default (a 8,16,32,64,128 sweep) runs five
# windows per arm - five times the rig time - and the low rungs are generator-bound rather than
# engine-bound (measured at an 8 s delay: 4,006 steps/s at 8 creators against 5,840 at 128), so they
# quietly mix "the generator could not issue" into an experiment about what the engine can serve.
CONC="${CONC:-128}"
COOLDOWN="${COOLDOWN:-30s}"
RTT_MAX_MS="${RTT_MAX_MS:-1.0}"
RTT_TRIES="${RTT_TRIES:-10}"
read -r -a DELAYS <<<"${DELAYS:-0 1s 8s}"
read -r -a WARMUPS <<<"${WARMUPS:-20s 45s 150s}"
if [[ ${#DELAYS[@]} -ne ${#WARMUPS[@]} ]]; then
  echo "DELAYS and WARMUPS must have the same number of entries" >&2
  exit 1
fi

# RUN_ID namespaces every database this script creates and drops. The only destructive operation here
# is DROP DATABASE, and an unnamespaced run would use predictable names that a concurrent campaign on
# the same instance could also pick - so a rerun could silently reap someone else's in-flight fill.
RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"

mkdir -p "$OUT"
echo "run id: ${RUN_ID}  (databases: dwarf_td_${RUN_ID}_<arm>_r<rep>)"
echo "arms: ${DELAYS[*]}  reps: ${REPS}  commanded: ${RATE} flows/s  shard: ${VCPUS} vCPU"

dsn_with_db() { # $1 = dsn, $2 = dbname
  local dsn="$1" db="$2" base query=""
  base="${dsn%%\?*}"
  [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"
  echo "${base%/*}/${db}${query}"
}
admin_dsn() { dsn_with_db "$1" postgres; }
psq() { psql "$1" -X -q -At -c "$2"; }

# Round trips inside ONE psql session, MINIMUM taken. The single session is the whole trick: timing a
# `psql -c "SELECT 1"` per sample measures process startup + TCP + TLS (~29 ms on this rig), which
# buries a sub-millisecond RTT under three orders of magnitude of constant overhead and makes the gate
# worse than useless - it passes everything.
probe_rtt_ms() { # $1 = dsn
  local dsn="$1" script i
  script=$'\\timing on\n'
  for i in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/ {v=$2+0; if (m=="" || v<m) m=v} END {if (m=="") m=0; printf "%.4f", m}'
}

# The gate probes while IDLE and waits for the server to recover rather than aborting the campaign:
# a degraded server is usually transient, and throwing away the remaining arms costs more than waiting.
# It aborts only when the server never comes back, which is a rig problem, not a run to interpret.
rtt_gate() { # $1 = admin dsn
  local adm="$1" r i
  for i in $(seq 1 "$RTT_TRIES"); do
    r=$(probe_rtt_ms "$adm")
    if awk -v v="$r" -v t="$RTT_MAX_MS" 'BEGIN{exit !(v>0 && v<=t)}'; then
      printf "   idle rtt %s ms (gate %s ms)\n" "$r" "$RTT_MAX_MS"
      return 0
    fi
    printf "   idle rtt %s ms ABOVE gate %s ms - cooling down (%d/%d)\n" "$r" "$RTT_MAX_MS" "$i" "$RTT_TRIES"
    sleep "${COOLDOWN%s}"
  done
  echo "ABORT: idle RTT never returned under ${RTT_MAX_MS} ms; the server is degraded, not the build." >&2
  exit 1
}

echo
echo "== ladder: ${REPS} reps x ${#DELAYS[@]} delays, ${WINDOW} window, open-loop at ${RATE} flows/s =="
for rep in $(seq 1 "$REPS"); do
  for i in "${!DELAYS[@]}"; do
    delay="${DELAYS[$i]}"
    warmup="${WARMUPS[$i]}"
    safe="${delay//[^a-zA-Z0-9]/_}"
    tag="d${safe}-r${rep}"
    db="dwarf_td_${RUN_ID}_${safe}_r${rep}"

    echo "-- ${tag}  (delay ${delay}, warmup ${warmup})"
    rtt_gate "$(admin_dsn "$DSN")"

    adm=$(admin_dsn "$DSN")
    psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    psq "$adm" "CREATE DATABASE ${db}" >/dev/null

    # shellcheck disable=SC2206  # word splitting is the intended parse of EXTRA
    args=(-dsn "$(dsn_with_db "$DSN" "$db")"
      -workload linear -vcpus "$VCPUS" -concurrency "$CONC"
      -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING"
      -task-delay "$delay" -window "$WINDOW" -warmup "$warmup"
      ${EXTRA:-}
      -label "task delay ${delay} rep ${rep} @ ${RATE} flows/s"
      -out "${OUT}/r-${tag}.json")
    "$BENCH" "${args[@]}"

    # Read the peer registry BEFORE dropping. The fresh database already guarantees an empty
    # dwarf_peers, so this asserts that guarantee rather than establishing it - but the failure it
    # guards against is severe and silent, and this campaign's whole signal is worker growth, which a
    # halved pool would also depress.
    left=$(psq "$(dsn_with_db "$DSN" "$db")" "SELECT COUNT(*) FROM dwarf_peers" 2>/dev/null || echo "?")
    [[ "$left" == "0" ]] || echo "   WARNING: left ${left} dwarf_peers row(s) after shutdown"

    psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    sleep "${COOLDOWN%s}"
  done
done

echo
echo "== summary =="
python3 - "$OUT" "$RATE" <<'PY'
import glob, json, os, re, statistics, sys

out, rate = sys.argv[1], float(sys.argv[2])

# The recorded arms this campaign exists to beat (bench/results/14-vertical-20260727/), keyed by the
# delay they ran at. Absolute numbers only transfer because the rig is the same shape; the shape of
# the result - do the long-task arms reach the short-task arm - is what the conclusion rests on.
BASELINE = {"0": 6005.0, "1s": 1020.0, "8s": 637.0}

arms = {}
for path in sorted(glob.glob(os.path.join(out, "r-d*-r*.json"))):
    doc = json.load(open(path))
    delay = re.match(r"r-d(.+)-r\d+\.json", os.path.basename(path)).group(1)
    for r in doc.get("results", []):
        arms.setdefault(delay, []).append((r, doc))


def gauge(r, name):
    """Last observed value of an engine gauge, summed over its label sets.

    Artifact gauges are a flat dict keyed `name` for the bare total and `name|k=v` per label set, so a
    per-shard gauge has one entry per shard and no bare key.
    """
    total, found = 0.0, False
    for key, value in (r.get("gaugesAfter") or {}).items():
        if key == name or key.startswith(name + "|"):
            total += value
            found = True
    return total if found else None


# End-to-end latency is NOT printed. Open-loop samples it from flows that COMPLETE inside the window,
# so an 8 s-per-task chain contributes almost nothing and its p99 reads near zero - the one number here
# that would be read confidently and be wrong. Admission latency (the Create call) is measured on every
# create, and outstanding flows is what the long-task arms actually strain.
print(f"{'delay':>6} {'n':>2} {'steps/s':>9} {'spread':>15} {'flows/s':>8} {'workers':>8} "
      f"{'turnsAvl':>9} {'heapMB':>8} {'outstd':>8} {'admP99':>7} {'baseline':>9} {'vs base':>8}")
means = {}
for delay in sorted(arms, key=lambda d: (len(d), d)):
    rows = arms[delay]
    sp = [r["stepsPerSec"] for r, _ in rows]
    means[delay] = statistics.mean(sp)
    fl = statistics.mean(r["flowsPerSec"] for r, _ in rows)
    gor = statistics.mean(r["goroutines"] for r, _ in rows)
    heap = statistics.mean((r.get("host") or {}).get("heapAllocMB", 0) for r, _ in rows)
    outstd = statistics.mean(r.get("maxOutstandingObserved", 0) for r, _ in rows)
    adm = statistics.mean(r.get("createP99Ms", 0) for r, _ in rows)
    perm = [gauge(r, "dwarf_turnstile_available") for r, _ in rows]
    perm = statistics.mean(p for p in perm if p is not None) if any(p is not None for p in perm) else None
    base = BASELINE.get(delay)
    print(f"{delay:>6} {len(sp):>2} {means[delay]:>9.0f} {f'{min(sp):.0f}-{max(sp):.0f}':>15} "
          f"{fl:>8.1f} {gor:>8.0f} {('n/a' if perm is None else f'{perm:.0f}'):>9} {heap:>8.0f} "
          f"{outstd:>8.0f} {adm:>7.1f} {('' if base is None else f'{base:.0f}'):>9} "
          f"{('' if base is None else f'x{means[delay]/base:.2f}'):>8}")

print(f"\ncommanded {rate:.0f} flows/s = {rate*10:.0f} steps/s on the 10-step linear chain.")

if "0" in means:
    print("\nconvergence on the 0s arm (the prediction under test):")
    for delay in sorted(means, key=lambda d: (len(d), d)):
        print(f"  {delay:>3}: x{means[delay]/means['0']:.2f}")
    sp0 = [r["stepsPerSec"] for r, _ in arms["0"]]
    if len(sp0) > 1:
        noise = (max(sp0) - min(sp0)) / statistics.mean(sp0)
        print(f"\n0s run-to-run spread is {noise*100:.0f}% of its mean - a long-task arm inside that")
        print("band has converged; one outside it has not.")

print("""
Reading it:
  - long-task arms reach the 0s arm            -> throughput is independent of task duration
  - long-task arms still short, crew still small -> the crew is not growing; the gate is not the cause
  - long-task arms short with a LARGE crew     -> growth works, something else binds (check DB CPU)
  - turnstile available sustained ZERO         -> admission is saturated; expect it under short tasks,
                                                  not long, since a task holds no turn
  - flows/s below commanded                    -> creation could not keep up; the arm measures the
                                                  generator, not the engine""")
PY
