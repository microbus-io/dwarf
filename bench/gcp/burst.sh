#!/usr/bin/env bash
#
# Burst/quiet profile: does the crew come back DOWN when load falls, and does throughput recover? Runs ON
# the bench VM, next to the deployed dwarf-bench binary.
#
# WHY THIS SCRIPT EXISTS. Every other arm in this directory holds the exec term constant for its whole
# window, so the crew grows to a plateau and the artifact reports the plateau - whatever the engine would
# have done next. That is the one shape no constant-delay arm can produce, and it is the shape both open
# questions need:
#
#   - a worker retires when it has held a candidate for too little of its own recent wall clock, so the
#     crew should settle at required_concurrency / threshold and NOT at its high-water mark,
#   - and throughput after a burst should return to what the same rig serves with no delay at all.
#
# THE PREDICTION HAS INVERTED SINCE THIS EXPERIMENT WAS FIRST WRITTEN DOWN, and running it against the old
# one would read a success as a failure. The original hypothesis was a RATCHET - crew and heap do not come
# back down, because `resident` only ever grew and nothing retired a worker. Both halves changed: every exit
# decrements `resident`, and a surplus worker retires on a coin flip. So the expected result is that the
# crew DOES come down. If it does not, that is the finding.
#
# WHAT IT CANNOT DO, said plainly because the artifact looks like it can. A burst arm's window MEAN is an
# average of both halves and means nothing on its own; the answer is a SHAPE over time. That shape comes
# from the -stats-interval readout, which this script tees to a per-arm log and parses below - the artifact
# records gauge means and peaks, but no trough, and the trough is the entire question. Per-phase THROUGHPUT
# is likewise not separable from one window, which is why the two steady arms exist: they bracket the burst
# arm rather than being redundant with it.
#
# THE RETIREMENT WINDOW IS A SHIPPED CONSTANT (2 min) WITH NO KNOB, deliberately - so this measures the real
# policy at its real pace, and the quiet half has to be long enough for several of those windows to elapse.
# Each round retires about a quarter of the surplus, so the crew decays as 0.75^n: ~3 rounds (6 min) to lose
# half of it, ~7 (14 min) to lose seven eighths. QUIET defaults to 15m for that reason and shortening it
# below ~6m measures the onset only. Nothing here needs the window to ALIGN with the cycle - every readout
# line carries the delay in force when it was taken, so each line classifies itself.
#
# CONTROLS, each earned by a corrupted campaign (see taskdelay.sh, which this follows):
#   1. REP-MAJOR INTERLEAVE across the three arms, ROTATED each rep (Latin square), so session drift hits
#      all three equally instead of loading onto whichever runs last. It matters more here than in a ladder:
#      `burst` is the arm under test, and a fixed order would put it in the most-fatigued slot of every rep.
#      A fixed order alone once produced a clean-looking replica ladder whose entire signal was position.
#   2. AN IDLE RTT GATE WITH COOLDOWN before every arm. RTT correlates with throughput at rho ~= -0.90 and
#      degrades across a session; an arm that starts degraded is a reading of server fatigue.
#   3. A FRESH DATABASE PER ARM, dropped after. A leftover dwarf_peers row inflates the observed replica
#      count, halves the derived pool and craters throughput (~180 vs ~7,500 steps/s) - and this campaign's
#      whole signal is the worker count, which a halved pool also depresses.
#   4. ARMS WHOSE METRIC COLLECTION FAILED ARE DROPPED AND NAMED. A failed end-of-window scrape reads as
#      `stepsPerSec: 0` with `valid: true`, and the engine's gauge callback queries the shard databases -
#      so it fails exactly under the load this campaign creates.
#
# Usage:
#   DSN=postgres://... ./burst.sh
# Knobs (env): BENCH (default ./dwarf-bench), OUT (default ./burst-results), REPS (default 1),
#   RATE (commanded flows/s, default 450), VCPUS (default 16), DELAY (the burst's exec term, default 8s),
#   BURST (default 5m), QUIET (default 15m), WINDOW (default 40m - two full cycles),
#   WARMUP (default 60s), STATS (readout interval, default 15s), CONC (creators, default 128),
#   MAX_OUTSTANDING (default 100000), COOLDOWN (default 30s), RTT_MAX_MS (default 1.0),
#   RTT_TRIES (default 10), PPROF (set to a directory to profile every arm), RUN_ID, EXTRA.
set -euo pipefail

# PREFLIGHT: psql must exist. The RTT gate pipes stderr to /dev/null (a failed probe is an ordinary outcome
# it retries), so on a host without psql the gate reads "no measurement" as "RTT too high" and cools down
# forever, printing nothing that names the cause. Fail here, where the message is the actual problem.
command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found. Install it (Debian: sudo apt-get install -y postgresql-client)." >&2
  exit 1
}

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
BENCH="${BENCH:-./dwarf-bench}"
OUT="${OUT:-./burst-results}"
REPS="${REPS:-1}"
RATE="${RATE:-450}"
VCPUS="${VCPUS:-16}"
DELAY="${DELAY:-8s}"
BURST="${BURST:-5m}"
QUIET="${QUIET:-15m}"
WINDOW="${WINDOW:-40m}"
WARMUP="${WARMUP:-60s}"
STATS="${STATS:-15s}"
# CONC is the number of CREATOR goroutines, not a load level: open-loop load is set by -arrival-rate, and
# this only bounds how fast the generator can issue Creates. Pinned to one value - dwarf-bench's default is
# a five-rung sweep, which would run five windows per arm and, at 40 minutes each, turn one arm into a
# three-hour arm.
CONC="${CONC:-128}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-100000}"
COOLDOWN="${COOLDOWN:-30s}"
RTT_MAX_MS="${RTT_MAX_MS:-1.0}"
RTT_TRIES="${RTT_TRIES:-10}"
PPROF="${PPROF:-}"

# RUN_ID namespaces every database this script creates and drops. The only destructive operation here is
# DROP DATABASE, and an unnamespaced run would use predictable names a concurrent campaign on the same
# instance could also pick - so a rerun could silently reap someone else's in-flight fill.
RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"

mkdir -p "$OUT"
[[ -n "$PPROF" ]] && mkdir -p "$PPROF"

# The three arms. steady0 is the recovery TARGET (what this rig serves with no delay), steadyN is the
# PLATEAU (what a constant long delay grows the crew to and holds it at), and burst is the arm under test,
# which should reach the plateau during a burst and fall back toward steady0's crew during the quiet half.
# Naming them here keeps the loop and the summary reading the same list.
ARMS=(steady0 steadyN burst)

# rotate echoes ARMS rotated left by $1, which is what makes the order a Latin square across reps.
rotate() { # $1 = offset
  local n=${#ARMS[@]} i out=()
  for ((i = 0; i < n; i++)); do out+=("${ARMS[$(((i + $1) % n))]}"); done
  echo "${out[@]}"
}

echo "run id: ${RUN_ID}  (databases: dwarf_bu_${RUN_ID}_<arm>_r<rep>)"
echo "arms: ${ARMS[*]}  reps: ${REPS}  commanded: ${RATE} flows/s  shard: ${VCPUS} vCPU"
echo "burst profile: ${DELAY} for ${BURST}, then 0 for ${QUIET}; window ${WINDOW}, readout every ${STATS}"

dsn_with_db() { # $1 = dsn, $2 = dbname
  local dsn="$1" db="$2" base query=""
  base="${dsn%%\?*}"
  [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"
  echo "${base%/*}/${db}${query}"
}
admin_dsn() { dsn_with_db "$1" postgres; }
psq() { psql "$1" -X -q -At -c "$2"; }

# Round trips inside ONE psql session, MINIMUM taken. The single session is the whole trick: timing a
# `psql -c "SELECT 1"` per sample measures process startup + TCP + TLS (~29 ms on this rig), which buries a
# sub-millisecond RTT under three orders of magnitude of constant overhead and passes everything.
probe_rtt_ms() { # $1 = dsn
  local dsn="$1" script i
  script=$'\\timing on\n'
  for i in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/ {v=$2+0; if (m=="" || v<m) m=v} END {if (m=="") m=0; printf "%.4f", m}'
}

# The gate probes while IDLE and waits for the server to recover rather than aborting the campaign: a
# degraded server is usually transient, and throwing away the remaining arms costs more than waiting. It
# aborts only when the server never comes back, which is a rig problem rather than a run to interpret.
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

# arm_args echoes the profile flags for one arm. Only the exec term differs between the three; every other
# flag is held identical, which is what makes them a comparison rather than three runs.
arm_args() { # $1 = arm
  case "$1" in
    steady0) echo "-task-delay 0" ;;
    steadyN) echo "-task-delay ${DELAY}" ;;
    burst)   echo "-task-delay ${DELAY} -task-burst ${BURST} -task-quiet ${QUIET}" ;;
    *) echo "unknown arm $1" >&2; exit 1 ;;
  esac
}

echo
echo "== ${REPS} rep(s) x ${#ARMS[@]} arms, ${WINDOW} window, open-loop at ${RATE} flows/s =="
for rep in $(seq 1 "$REPS"); do
  read -r -a order <<<"$(rotate $((rep - 1)))"
  echo "-- rep ${rep} order: ${order[*]}"
  for arm in "${order[@]}"; do
    tag="${arm}-r${rep}"
    db="dwarf_bu_${RUN_ID}_${arm}_r${rep}"

    echo "-- ${tag}"
    rtt_gate "$(admin_dsn "$DSN")"

    adm=$(admin_dsn "$DSN")
    psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    psq "$adm" "CREATE DATABASE ${db}" >/dev/null

    # shellcheck disable=SC2206  # word splitting is the intended parse of arm_args and EXTRA
    args=(-dsn "$(dsn_with_db "$DSN" "$db")"
      -workload linear -vcpus "$VCPUS" -concurrency "$CONC"
      -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING"
      $(arm_args "$arm")
      -window "$WINDOW" -warmup "$WARMUP" -stats-interval "$STATS"
      ${PPROF:+-pprof "$PPROF"}
      ${EXTRA:-}
      -label "${arm} rep ${rep} @ ${RATE} flows/s run ${RUN_ID}"
      -out "${OUT}/r-${tag}.json")
    # The readout goes to a log as well as the terminal: it is the ONLY record of the crew's shape over
    # time, and the artifact does not carry it.
    "$BENCH" "${args[@]}" 2>&1 | tee "${OUT}/r-${tag}.log"

    # Read the peer registry BEFORE dropping. The fresh database already guarantees an empty dwarf_peers,
    # so this asserts the guarantee rather than establishing it - but the failure is severe and silent, and
    # a halved pool would depress the very worker count this campaign reads.
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

# One readout line: eight numeric fields. The run's own trailing summary line has thirteen and the header
# has none, so the field count alone separates them - no need to track where the header was printed.
LINE = re.compile(r"^\s*([\d.]+)\s+(\d+)\s+([\d.]+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*$")


def readout(path):
    """The stats lines from one arm's log, as dicts. This is the only record of the crew over time."""
    rows = []
    for line in open(path, errors="replace"):
        m = LINE.match(line)
        if m:
            t, delay, crew, gor, heap, turns, infl, pending = (float(x) for x in m.groups())
            rows.append(dict(t=t, delay=delay, crew=crew, gor=gor, heap=heap,
                             turns=turns, infl=infl, pending=pending))
    return rows


arms = {}
dropped = []
for path in sorted(glob.glob(os.path.join(out, "r-*.json"))):
    tag = re.match(r"r-(.+)\.json", os.path.basename(path)).group(1)
    doc = json.load(open(path))
    for r in doc.get("results", []):
        # A failed end-of-window scrape reads as stepsPerSec 0 with the gauges missing, and the engine's
        # gauge callback queries the shard databases - so it fails exactly under this campaign's load.
        # Drop such an arm and NAME it: averaging a zero into a mean is how one reads as a real result.
        if not doc.get("valid", True) or not r.get("engineCounters") or not (r.get("gaugesAfter") or {}):
            dropped.append(tag)
            continue
        arm = tag.rsplit("-r", 1)[0]
        arms.setdefault(arm, []).append((r, readout(os.path.join(out, f"r-{tag}.log"))))

if dropped:
    print(f"DROPPED (collection failed, not a measurement): {', '.join(sorted(set(dropped)))}\n")

print(f"{'arm':>8} {'n':>2} {'steps/s':>9} {'flows/s':>8} {'crewMax':>8} {'crewMin':>8} "
      f"{'crewEnd':>8} {'heapMaxMB':>10} {'heapEndMB':>10}")
stats = {}
for arm in ("steady0", "steadyN", "burst"):
    if arm not in arms:
        continue
    reps = arms[arm]
    sp = statistics.mean(r["stepsPerSec"] for r, _ in reps)
    fl = statistics.mean(r["flowsPerSec"] for r, _ in reps)
    crews = [row["crew"] for _, rows in reps for row in rows]
    heaps = [row["heap"] for _, rows in reps for row in rows]
    ends = [rows[-1]["crew"] for _, rows in reps if rows]
    hends = [rows[-1]["heap"] for _, rows in reps if rows]
    if not crews:
        print(f"{arm:>8} {len(reps):>2} {sp:>9.0f} {fl:>8.1f}  (no readout lines - was -stats-interval set?)")
        continue
    stats[arm] = dict(steps=sp, crew_max=max(crews), crew_min=min(crews),
                      crew_end=statistics.mean(ends) if ends else 0)
    print(f"{arm:>8} {len(reps):>2} {sp:>9.0f} {fl:>8.1f} {max(crews):>8.0f} {min(crews):>8.0f} "
          f"{(statistics.mean(ends) if ends else 0):>8.0f} {max(heaps):>10.0f} "
          f"{(statistics.mean(hends) if hends else 0):>10.0f}")

# The burst arm's own shape, split by the delay in force when each line was taken. This is the measurement;
# everything above is context for it.
if "burst" in arms:
    hi = [row for _, rows in arms["burst"] for row in rows if row["delay"] > 0]
    lo = [row for _, rows in arms["burst"] for row in rows if row["delay"] == 0]
    print("\nburst arm, by phase of the cycle:")
    for name, rows in (("burst (tasks long)", hi), ("quiet (tasks instant)", lo)):
        if not rows:
            continue
        crew = [r["crew"] for r in rows]
        print(f"  {name:<22} n={len(rows):<4} crew {min(crew):>7.0f}-{max(crew):<7.0f} "
              f"mean {statistics.mean(crew):>7.0f}   pending mean {statistics.mean(r['pending'] for r in rows):>8.0f}")

    # Does it come back down WITHIN a quiet stretch? Compare the first reading of each quiet run to its
    # last: a crew that is falling shows it here even when the stretch was too short to finish falling.
    for _, rows in arms["burst"]:
        stretches, cur = [], []
        for row in rows:
            if row["delay"] == 0:
                cur.append(row)
            elif cur:
                stretches.append(cur)
                cur = []
        if cur:
            stretches.append(cur)
        for i, st in enumerate(stretches, 1):
            if len(st) < 2:
                continue
            first, last = st[0]["crew"], st[-1]["crew"]
            span = st[-1]["t"] - st[0]["t"]
            pct = (last - first) / first * 100 if first else 0
            print(f"  quiet stretch {i}: crew {first:.0f} -> {last:.0f} over {span:.0f}s ({pct:+.0f}%)")

# The two brackets, named rather than left to be read off the table - and spelled "dropped" rather than
# "nan" when an arm did not survive the validity check, so a missing bracket reads as missing.
def bracket(arm):
    return f"{stats[arm]['crew_max']:.0f}" if arm in stats else "dropped"


if "burst" in stats:
    print(f"\nrecovery target (steady0 crew): {bracket('steady0')}")
    print(f"plateau       (steadyN crew): {bracket('steadyN')}")

print(f"\ncommanded {rate:.0f} flows/s = {rate*10:.0f} steps/s on the 10-step linear chain.")
print("""
Reading it:
  - burst arm's crew falls within each quiet stretch, toward steady0's  -> retirement works; the shrink
                                                                          rate is the slope
  - crew flat across the quiet stretches at the steadyN plateau         -> THE RATCHET IS BACK. Check that
                                                                          the quiet half outlasts several
                                                                          2-minute retirement windows
                                                                          before concluding it
  - crew falls BELOW steady0's                                          -> it is retiring past what the
                                                                          load needs; expect throughput to
                                                                          sag on the next burst
  - burst steps/s far under the mean of the two steady arms             -> the shrink costs throughput on
                                                                          the way back up
  - flows/s below commanded on ANY arm                                  -> creation could not keep up; that
                                                                          arm measures the generator
  - crewMin == crewMax on steady0                                       -> the crew never grew at all, so
                                                                          the load is too light to have a
                                                                          surplus to retire""")
PY
