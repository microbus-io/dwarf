#!/usr/bin/env bash
#
# Enter/exit permit split A/B: should work about to START and work being RECORDED draw on separate
# permit reservations, or on one shared pool?
#
#   control (debit8)     = ONE pool of 8x conns. Entering workers block on it; a worker recording a
#                          finished step debits without waiting, driving the count negative.
#   treatment (split812) = TWO reservations, 8x conns to enter and 12x to exit. Both simply block. No
#                          debit, no priority rule, no wait budget, nothing forced past zero.
#
# WHY THE SPLIT. One pool forces a choice about who wins a contested permit, and BOTH answers were
# measured failing on this rig class. Served evenly, completions lose at random and queue behind
# admission: 286 of them waited out a full second, with persist transactions inflating 3ms -> 54ms.
# Served with completions given strict precedence, ENTRY starves - and entry is dispatch, so short-task
# throughput collapsed 3x (4,416 vs 7,964 steps/s) with creation itself throttled to 714 of 800
# commanded flows/s. Separate counts remove the choice instead of answering it: neither side can be
# blocked by the other, so neither needs a priority rule or an escape hatch.
#
# WHY 8 AND 12. Entry is sized to the candidate cache's own bound (it holds 8 x conns entries, so at
# most that many workers can hold a candidate regardless of any permit) - so the permit and the cache
# bind at the same point instead of one silently shadowing the other. The exit side follows the round
# trips: 7 of a step's ~11 are on the exit side, so ~4:7, and 2:3 tracks it closely.
#
# WHAT WOULD FALSIFY IT: lease recoveries. A worker waiting to record a finished step is holding a
# lease it has already earned, and if waiting ever costs one, the step is re-claimed and the task
# RE-EXECUTES - the expensive failure the non-blocking debit existed to prevent. Every run's `valid`
# flag fires on any recovery counter, so the campaign self-reports it.
#
# TWO REGIMES, because the two failures above appeared in different ones:
#   - 0s tasks at a SATURATING rate: where entry starves if the exit side is favoured, and where runs
#     went bimodal (2,373-8,093 steps/s on identical config).
#   - 8s tasks: where the worker crew reaches ~70,000 and per-acquire pool wait hit 105ms, and where
#     gating the exit path measured +14%.
#
# CONTROLS: arm order ROTATED per rep (a fixed order ties each arm to the same position in the server's
# recovery cycle - measured locally as correlation(rtt, steps/s) = -0.94 with one arm drawing the good
# server every time), fresh database per run, idle RTT gate with cooldown before each arm.
#
# Usage:
#   DSN=postgres://... ./exitgate.sh
# Knobs (env): ARMS (default "split812:./gcp-split812 debit8:./gcp-debit8ctl"), REPS (3), OUT, VCPUS (16),
#   CONC (128), DELAYS / RATES / WARMUPS (parallel arrays, default "0 8s" / "800 600" / "20s 150s"),
#   WINDOW (60s), COOLDOWN (30s), RTT_MAX_MS (1.0), RTT_TRIES (10), MAX_OUTSTANDING (100000), RUN_ID, EXTRA.
set -euo pipefail

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
REPS="${REPS:-3}"
OUT="${OUT:-./exitgate-results}"
VCPUS="${VCPUS:-16}"
# dwarf-bench's default -concurrency is a five-value SWEEP, so PIN it or every arm runs five windows
# and the rig costs 5x its time for nothing. Open-loop runs command the rate; concurrency only sizes
# the creator pool.
CONC="${CONC:-128}"
WINDOW="${WINDOW:-60s}"
COOLDOWN="${COOLDOWN:-30}"
RTT_MAX_MS="${RTT_MAX_MS:-1.0}"
RTT_TRIES="${RTT_TRIES:-10}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-100000}"
read -r -a ARMS <<<"${ARMS:-split812:./gcp-split812 debit8:./gcp-debit8ctl}"
read -r -a DELAYS <<<"${DELAYS:-0 8s}"
read -r -a RATES <<<"${RATES:-800 600}"
read -r -a WARMUPS <<<"${WARMUPS:-20s 150s}"

RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"
mkdir -p "$OUT"
echo "run id: ${RUN_ID}  arms: ${ARMS[*]}  regimes: ${DELAYS[*]} @ ${RATES[*]} flows/s  reps: ${REPS}"

dsn_with_db() { local dsn="$1" db="$2" base query=""; base="${dsn%%\?*}"
  [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"; echo "${base%/*}/${db}${query}"; }
admin_dsn() { dsn_with_db "$1" postgres; }
psq() { psql "$1" -X -q -At -c "$2"; }

# One psql session, minimum taken: a psql process per sample measures ~29ms of startup and buries a
# sub-millisecond RTT under it, so the gate would pass everything it exists to fail.
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
    printf "   idle rtt %s ms ABOVE gate - cooling (%d/%d)\n" "$r" "$i" "$RTT_TRIES"
    sleep "$COOLDOWN"
  done
  echo "ABORT: idle RTT never recovered; the server is degraded, not the build." >&2; exit 1; }

for rep in $(seq 1 "$REPS"); do
  for i in "${!DELAYS[@]}"; do
    delay="${DELAYS[$i]}"; rate="${RATES[$i]}"; warmup="${WARMUPS[$i]}"
    safe="${delay//[^a-zA-Z0-9]/_}"
    # Rotate which arm goes first, so position in the recovery cycle cannot align with treatment.
    order=()
    for j in "${!ARMS[@]}"; do order+=("${ARMS[$(( (j + rep - 1) % ${#ARMS[@]} ))]}"); done
    for spec in "${order[@]}"; do
      name="${spec%%:*}"; bin="${spec#*:}"
      tag="d${safe}-${name}-r${rep}"
      db="dwarf_eg_${RUN_ID}_${safe}_${name}_r${rep}"

      echo "-- ${tag}  (delay ${delay}, ${rate} flows/s, ${bin})"
      rtt_gate "$(admin_dsn "$DSN")"
      adm=$(admin_dsn "$DSN")
      psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
      psq "$adm" "CREATE DATABASE ${db}" >/dev/null

      # shellcheck disable=SC2206
      args=(-dsn "$(dsn_with_db "$DSN" "$db")" -workload linear -vcpus "$VCPUS" -concurrency "$CONC"
        -open-loop -arrival-rate "$rate" -max-outstanding "$MAX_OUTSTANDING"
        -task-delay "$delay" -window "$WINDOW" -warmup "$warmup"
        ${EXTRA:-}
        -label "${name} delay ${delay} rep ${rep} @ ${rate} flows/s"
        -out "${OUT}/r-${tag}.json")
      "$bin" "${args[@]}"

      left=$(psq "$(dsn_with_db "$DSN" "$db")" "SELECT COUNT(*) FROM dwarf_peers" 2>/dev/null || echo "?")
      [[ "$left" == "0" ]] || echo "   WARNING: left ${left} dwarf_peers row(s) after shutdown"
      psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
      sleep "$COOLDOWN"
    done
  done
done

echo
echo "== summary =="
python3 - "$OUT" <<'PY'
import glob, json, os, re, statistics, sys
out = sys.argv[1]

def hist(r, n):
    t = c = 0.0
    for h in r.get("engineHistograms", []):
        if h["name"] == n and h.get("count"):
            t += h["sumSeconds"]; c += h["count"]
    return t, c

cells = {}
for path in sorted(glob.glob(os.path.join(out, "r-d*.json"))):
    m = re.match(r"r-d(\w+?)-(\w+?)-r(\d+)\.json", os.path.basename(path))
    doc = json.load(open(path)); r = doc["results"][0]
    cells.setdefault((m.group(1), m.group(2)), []).append((r, doc))

print(f"{'delay':>6} {'arm':>9} {'n':>2} {'steps/s':>8} {'spread':>15} {'flows/s':>8} "
      f"{'p50ms':>8} {'p99ms':>8} {'maxOut':>7} {'poolWait':>8} {'txMs':>6} {'inTx':>6} "
      f"{'entWait':>8} {'exitWait':>8} {'valid':>6}")
for (delay, arm) in sorted(cells, key=lambda k: (len(k[0]), k[0], k[1])):
    rows = cells[(delay, arm)]
    sp = [r["stepsPerSec"] for r, _ in rows]
    waits, txs, intx, ent, ext = [], [], [], [], []
    for r, _ in rows:
        b, a = r.get("gaugesBefore") or {}, r.get("gaugesAfter") or {}
        pool = lambda g, k: sum(v for kk, v in g.items() if kk.startswith("sequel_pool_" + k))
        dc = pool(a, "wait_count") - pool(b, "wait_count")
        dd = pool(a, "wait_duration_seconds") - pool(b, "wait_duration_seconds")
        waits.append(dd / dc * 1000 if dc else 0)
        ts, tc = hist(r, "sequel_transaction_duration")
        txs.append(ts / tc * 1000 if tc else 0); intx.append(ts / r["windowSec"])
        # The two permit-wait histograms exist only in the split build; the control reads 0 because the
        # instruments are ABSENT, not because nothing queued. Compare them across ROLES within the
        # treatment arm, never treatment-vs-control.
        es, ec = hist(r, "dwarf_permit_enter_wait_seconds")
        xs, xc = hist(r, "dwarf_permit_exit_wait_seconds")
        ent.append(es / ec * 1000 if ec else 0)
        ext.append(xs / xc * 1000 if xc else 0)
    mean = lambda k: statistics.mean(r[k] for r, _ in rows)
    print(f"{delay:>6} {arm:>9} {len(sp):>2} {statistics.mean(sp):>8.0f} "
          f"{f'{min(sp):.0f}-{max(sp):.0f}':>15} {mean('flowsPerSec'):>8.1f} "
          f"{mean('p50Ms'):>8.0f} {mean('p99Ms'):>8.0f} {mean('maxOutstandingObserved'):>7.0f} "
          f"{statistics.mean(waits):>8.1f} "
          f"{statistics.mean(txs):>6.1f} {statistics.mean(intx):>6.0f} {statistics.mean(ent):>8.3f} "
          f"{statistics.mean(ext):>8.3f} "
          f"{sum(1 for _,d in rows if d.get('valid')):>3}/{len(rows)}")

print("""
Reading it, in this order:
  1. valid < n in the treatment  -> STOP: a worker waiting to record its step lost its lease and the
                                   task re-executed. That kills the design outright.
  2. entWait vs exitWait         -> which HALF of the split is binding. Both are treatment-only (the
                                   control has no such instrument, so its 0 means ABSENT, not idle). A
                                   high exitWait with a low entWait says raise the exit side and vice
                                   versa; both near zero says neither reservation is what limits this
                                   workload.
  2b. DO NOT judge an arm by enter+exit TOTAL, and do not chase a smaller one. At saturation the queue
                                   RELOCATES rather than shrinking, so a tighter total looks better on
                                   poolWait while paying far more at the permit - 3/5 measured 18ms pool
                                   wait against 588-1459ms enter wait. It also carried the biggest crew
                                   (82-87k workers vs 59-62k) and backlog, which is LITTLE'S LAW, not a
                                   growth misfire: permit wait is residence time, so the same rate needs
                                   proportionally more workers. Read poolWait and the two permit waits
                                   TOGETHER, or a tighter setting wins on whichever metric happens to be
                                   instrumented.
  3. spread                      -> NARROWER is the bigger prize than any mean. The bimodal collapse
                                   (a rep serving a fraction of commanded while its twin serves it all)
                                   is what none of this has fixed yet; check flows/s against commanded
                                   to tell a throttled creator from a slow engine.
  4. poolWait / txMs / inTx      -> whether the gate actually shaped the population saturating the pool.
                                   inTx is the concurrent persister count by Little's law.""")
PY
