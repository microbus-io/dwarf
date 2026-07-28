#!/usr/bin/env bash
#
# Permit-multiplier ladder: how many workers should be allowed into a step's DATABASE phases at once,
# as a multiple of that shard's connection pool? Runs ON the bench VM.
#
# WHY THIS SCRIPT EXISTS. The engine admits `permitsPerConn x pool` workers to the database phases and
# blocks the rest. The shipped 8 was chosen to reproduce the concurrency the candidate cache already
# allowed, so that adding the gate introduced no new database-concurrency regime. Measured on one
# 16-vCPU shard (96 connections, so 768 permits) with 8 s tasks at a commanded 600 flows/s, that is
# visibly too loose: the arms ran 1,369-6,824 steps/s across three identical runs, the pool sat at
# 96 in use / 0 idle, and each blocked acquire waited 27-105 ms - with the SLOWEST arm the one that
# waited longest. See bench/results/19-permits-20260728/.
#
# The mechanism this ladder tests: a step makes ~9-11 round trips, so at 8x every one of them queues
# behind ~7 other permitted workers. That inflates the time a worker holds its slot, which raises the
# number of workers concurrently in database phases, which lengthens the queue further. Tighter permits
# trade admission concurrency for shorter per-query waits. The pool itself has room either way -
# 96 connections at ~7.7 ms of database time per step is ~12,000 steps/s, twice the commanded rate -
# so if a tighter multiplier serves the full rate, the looser one was buying nothing but queueing.
#
# WHAT WOULD FALSIFY THE TIGHTENING: a low multiplier that fails to reach the commanded rate. At 1x the
# permitted set equals the pool, so any worker holding a permit while NOT holding a connection (between
# statements, or resolving state) idles a slot that another worker could have used. If 1x undershoots
# while 2x and 4x do not, that idle-slot cost is real and bounds how tight this can go.
#
# Each arm is a SEPARATE BINARY built with a different permitsPerConn, because the multiplier is a
# compile-time constant. Build them with:
#   for n in 8 4 2 1; do
#     sed -i '' "s/^\tpermitsPerConn = .*/\tpermitsPerConn = $n/" engine/poolsize.go
#     GOOS=linux GOARCH=arm64 go build -o dwarf-bench-p$n ./bench
#   done; git checkout engine/poolsize.go
# The artifacts therefore record an IDENTICAL config for every arm - the multiplier lives only in the
# label and the file name, so do not read the config block to tell arms apart.
#
# CONTROLS: rep-major interleave (8,4,2,1 / 8,4,2,1 / ...) so session drift hits every arm equally; an
# idle RTT gate with cooldown before each arm, since RTT degrades across a session and correlates with
# throughput at rho ~= -0.90; a fresh database per run, dropped after.
#
# Usage:
#   DSN=postgres://... ./permitladder.sh
# Knobs (env): BINS (default "8:./dwarf-bench-p8 4:./dwarf-bench-p4 2:./dwarf-bench-p2 1:./dwarf-bench-p1"),
#   OUT, REPS (3), RATE (600), DELAY (8s), WARMUP (150s), WINDOW (60s), VCPUS (16), CONC (128),
#   MAX_OUTSTANDING (100000), COOLDOWN (30s), RTT_MAX_MS (1.0), RTT_TRIES (10), RUN_ID, EXTRA.
set -euo pipefail

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
read -r -a BINS <<<"${BINS:-8:./dwarf-bench-p8 4:./dwarf-bench-p4 2:./dwarf-bench-p2 1:./dwarf-bench-p1}"

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

    echo "-- ${tag}  (permitsPerConn ${mult}, ${bin})"
    rtt_gate "$(admin_dsn "$DSN")"
    adm=$(admin_dsn "$DSN")
    psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
    psq "$adm" "CREATE DATABASE ${db}" >/dev/null

    # shellcheck disable=SC2206
    args=(-dsn "$(dsn_with_db "$DSN" "$db")" -workload linear -vcpus "$VCPUS" -concurrency "$CONC"
      -open-loop -arrival-rate "$RATE" -max-outstanding "$MAX_OUTSTANDING"
      -task-delay "$DELAY" -window "$WINDOW" -warmup "$WARMUP"
      ${EXTRA:-}
      -label "permitsPerConn ${mult} rep ${rep}, delay ${DELAY} @ ${RATE} flows/s"
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
                              if k.startswith("dwarf_permits_available")) for r in rows)
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
