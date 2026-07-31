#!/usr/bin/env bash
#
# Permit-multiplier ladder: how many workers should be allowed into a step's DATABASE phases at once,
# as a multiple of that shard's connection pool? Runs ON the bench VM.
#
# WHY THIS SCRIPT EXISTS. The engine admits `turnstilePassesPerConn x pool` callers to the database at
# once and orders the rest by band and then by how long the asking job has been running. The multiple is
# NOT a bound on database concurrency - the pool is - it is how deep a queue is allowed to form in front
# of the pool, and therefore how much of the contention is ordered rather than arbitrary.
#
# Sizing it to the pool (1x) is measured catastrophic: 281 steps/s against 1,687 for the gate it replaced,
# a 6x collapse, because with as many turns as connections every handoff gap is idle connection time. The
# open question is the other direction. Locally, 8x/10x/12x measured 1,559/1,688/1,734 steps/s with
# IDENTICAL per-connection service time across all three - so the extra multiple bought queue depth, not
# throughput - but those arms differed by less than that rig's own RTT drift (0.66-4.15 ms idle), which
# is why they settle nothing and this runs on a rig that holds RTT steady.
#
# WHAT WOULD SETTLE IT: a stressed arm where 10x serves materially more than 8x, or does not. 8x matches
# workersPerConnBudget, so it is the multiple that keeps the contending population the same size the cache
# and resident worker set are already derived for; 10x has to earn the extra queue.
#
# CONTROLS: rep-major interleave (8,4,2,1 / 8,4,2,1 / ...) so session drift hits every arm equally; an
# idle RTT gate with cooldown before each arm, since RTT degrades across a session and correlates with
# throughput at rho ~= -0.90; a fresh database per run, dropped after.
#
# Usage:
#   DSN=postgres://... ./permitladder.sh
# Knobs (env): BINS (default "8:./dwarf-bench-t8 10:./dwarf-bench-t10"),
#   OUT, REPS (3), RATE (600), DELAY (8s), WARMUP (150s), WINDOW (60s), VCPUS (16), CONC (128),
#   MAX_OUTSTANDING (100000), COOLDOWN (30s), RTT_MAX_MS (1.0), RTT_TRIES (10), RUN_ID, EXTRA.
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
OUT="${OUT:-./permitladder-results}"
REPS="${REPS:-3}"
RATE="${RATE:-600}"
DELAY="${DELAY:-8s}"
WARMUP="${WARMUP:-150s}"
WINDOW="${WINDOW:-60s}"
VCPUS="${VCPUS:-16}"
CONC="${CONC:-128}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-100000}"
COOLDOWN="${COOLDOWN:-30s}"
RTT_MAX_MS="${RTT_MAX_MS:-1.0}"
RTT_TRIES="${RTT_TRIES:-10}"
read -r -a BINS <<<"${BINS:-8:./dwarf-bench-t8 10:./dwarf-bench-t10}"

RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"

mkdir -p "$OUT"
echo "run id: ${RUN_ID}  arms: ${BINS[*]}"
echo "delay ${DELAY}, commanded ${RATE} flows/s, ${REPS} reps, ${VCPUS} vCPU shard"

dsn_with_db() { local dsn="$1" db="$2" base query=""; base="${dsn%%\?*}"
  [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"; echo "${base%/*}/${db}${query}"; }
admin_dsn() { dsn_with_db "$1" postgres; }
psq() { psql "$1" -X -q -At -c "$2"; }

# One psql session, minimum taken: a psql process per sample would measure ~29 ms of startup and bury a
# sub-millisecond RTT under it, passing every gate it exists to fail.
probe_rtt_ms() { local dsn="$1" script i; script=$'\\timing on\n'
  for i in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/ {v=$2+0; if (m=="" || v<m) m=v} END {if (m=="") m=0; printf "%.4f", m}'; }

rtt_gate() { local adm="$1" r i
  for i in $(seq 1 "$RTT_TRIES"); do
    r=$(probe_rtt_ms "$adm")
    if awk -v v="$r" -v t="$RTT_MAX_MS" 'BEGIN{exit !(v>0 && v<=t)}'; then
      printf "   idle rtt %s ms\n" "$r"; return 0
    fi
    printf "   idle rtt %s ms ABOVE gate %s - cooling (%d/%d)\n" "$r" "$RTT_MAX_MS" "$i" "$RTT_TRIES"
    sleep "${COOLDOWN%s}"
  done
  echo "ABORT: idle RTT never recovered; the server is degraded, not the build." >&2; exit 1; }

for rep in $(seq 1 "$REPS"); do
  for spec in "${BINS[@]}"; do
    mult="${spec%%:*}"; bin="${spec#*:}"
    tag="p${mult}-r${rep}"
    db="dwarf_pl_${RUN_ID}_p${mult}_r${rep}"

    echo "-- ${tag}  (turnstilePassesPerConn ${mult}, ${bin})"
    rtt_gate "$(admin_dsn "$DSN")"
    adm=$(admin_dsn "$DSN")
    psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    psq "$adm" "CREATE DATABASE ${db}" >/dev/null

    # shellcheck disable=SC2206
    args=(-dsn "$(dsn_with_db "$DSN" "$db")" -workload linear -vcpus "$VCPUS" -concurrency "$CONC"
      -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING"
      -task-delay "$DELAY" -window "$WINDOW" -warmup "$WARMUP"
      ${EXTRA:-}
      -label "turnstilePassesPerConn ${mult} rep ${rep}, delay ${DELAY} @ ${RATE} flows/s"
      -out "${OUT}/r-${tag}.json")
    "$bin" "${args[@]}"

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
commanded = rate * 10  # 10-step linear chain

def pool(g, key):
    return sum(v for k, v in (g or {}).items() if k.startswith("sequel_pool_" + key))

arms = {}
for path in sorted(glob.glob(os.path.join(out, "r-p*-r*.json"))):
    doc = json.load(open(path))
    mult = int(re.match(r"r-p(\d+)-r\d+\.json", os.path.basename(path)).group(1))
    for r in doc.get("results", []):
        arms.setdefault(mult, []).append(r)

# Per-acquire wait is a WINDOW DELTA of two cumulative counters: sql.DBStats exposes totals since the
# pool opened, so the raw value includes the warmup (150 s of it here) and understates the window.
# WaitCount counts only acquisitions that BLOCKED, so this is the mean among waiters, not among all.
print(f"{'permits':>8} {'n':>2} {'steps/s':>9} {'spread':>15} {'%cmd':>6} {'flows/s':>8} {'workers':>8} "
      f"{'permitAvl':>10} {'waitMs':>7} {'blocked':>10} {'passMs':>7} {'sel/s':>7} {'outstd':>8}")
means = {}
for mult in sorted(arms, reverse=True):
    rows = arms[mult]
    sp = [r["stepsPerSec"] for r in rows]
    means[mult] = statistics.mean(sp)
    waits, blocked, passms, sels = [], [], [], []
    for r in rows:
        b, a = r.get("gaugesBefore"), r.get("gaugesAfter")
        dc = pool(a, "wait_count") - pool(b, "wait_count")
        dd = pool(a, "wait_duration_seconds") - pool(b, "wait_duration_seconds")
        waits.append(dd / dc * 1000 if dc else 0)
        blocked.append(dc)
        h = {x["name"]: x for x in r.get("engineHistograms", []) if x.get("count")}
        rf = h.get("dwarf_refill_duration_seconds")
        passms.append(rf["sumSeconds"] / rf["count"] * 1000 if rf else 0)
        sels.append(r.get("engineCounters", {}).get("dwarf_refill_candidates_selected", 0) / r["windowSec"])
    perm = statistics.mean(sum(v for k, v in (r.get("gaugesAfter") or {}).items()
                              if k.startswith("dwarf_turnstile_available")) for r in rows)
    print(f"{mult:>7}x {len(sp):>2} {means[mult]:>9.0f} {f'{min(sp):.0f}-{max(sp):.0f}':>15} "
          f"{means[mult]/commanded*100:>5.0f}% {statistics.mean(r['flowsPerSec'] for r in rows):>8.1f} "
          f"{statistics.mean(r['goroutines'] for r in rows):>8.0f} {perm:>10.0f} "
          f"{statistics.mean(waits):>7.1f} {statistics.mean(blocked):>10,.0f} {statistics.mean(passms):>7.0f} "
          f"{statistics.mean(sels):>7.0f} "
          f"{statistics.mean(r.get('maxOutstandingObserved', 0) for r in rows):>8.0f}")

print(f"\ncommanded {rate:.0f} flows/s = {commanded:.0f} steps/s.")
print("""
Reading it:
  - a tighter multiplier reaching %cmd ~100 with a LOWER waitMs -> 8x was buying queueing, not work
  - every multiplier equally short                             -> the pool, not the gate, is the limit
  - the tightest arm short while a middle one is not           -> permitted-but-idle slots now bind:
                                                                  the floor is between those two
  - passMs/sel/s recovering as the multiplier falls            -> candidate supply was losing the pool,
                                                                  which is the stall's actual mechanism""")
PY
