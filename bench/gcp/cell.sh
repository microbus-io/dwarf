#!/usr/bin/env bash
#
# One cell of the cloud grid - one workload at one RTT on one shard tier - measured end to end: the peak,
# the pool sweep at 2/3 of it, and a confirming peak when the sweep's pool differs from the shipped one.
# Runs ON the bench VM.
#
# THE ORDER RESOLVES A CHICKEN AND EGG. Peak depends on the pool and the pool choice depends on the peak, so:
#   1. PEAK at the derived pool - what ships. Open loop, flat out, the backlog capped by -max-outstanding,
#      PEAK_REPS runs. Peak = the saturated window mean, never a best rung.
#   2. POOL SWEEP at 2/3 of that peak. Throughput at 2/3 load is 2/3 of the peak by definition, so these runs
#      measure LATENCY and which pools run clean. Optimum = the smallest pool that delivers what it is
#      offered, with p99 within P99_SLACK of the best p99 in the sweep and no stall.
#   3. CONFIRM: if the optimum differs from the derived pool by more than CONFIRM_DIFF, the peak runs once
#      more at the optimum. That is the number to publish; if it drops, the 2/3-chosen pool caps the database
#      below its peak - a finding to bring back, not to paper over.
#
# THE POOL IS PINNED in every run. The derived pool comes from the engine's own budget rule for the TARGET
# rtt and this server's max_connections (`-print-pool`), because the engine's Startup probe varies run to run
# and an unpinned run would confound pool size with probe noise.
#
# netem complements the rig's own distance to land the TARGET total RTT. It can add delay, never take it
# away, so a rig whose base RTT is above the target aborts here: recreate the VM (same zone) and redraw. The
# measured RTT goes into every label. THE QDISC IS THE HAZARD - the trap removes it on any exit and the
# script verifies it is gone; check `tc qdisc show` by hand after a SIGKILL.
#
# Per run: a fresh lower-case database created before and dropped right after, an idle RTT gate with
# cooldown (RTT degrades across a session, rho ~= -0.90 against throughput), and a dwarf_peers check. The
# summary drops any artifact whose end-of-window collection failed, and flags any run with peer churn.
#
# Usage:
#   DSN=postgres://... TARGET_RTT=0.25 VCPUS=16 WORKLOAD=linear ./cell.sh
# Knobs (env): BENCH (./dwarf-bench), WORKLOAD (linear), PAYLOAD (0), WIDTH (8, fanout/scatter),
#   DEPTH (1, scatter), LINEAR_STEPS (10), TARGET_RTT (ms, required), VCPUS (16), PEAK_REPS (3),
#   WINDOW (16m: two or three checkpoint cycles, and minute slots enough to read a trend), WARMUP (2m), CONC (256 creators), MAX_OUTSTANDING (20000), SWEEP ("0.5 0.75 1.0 1.25 1.5",
#   multiples of the derived pool; "" = no sweep), P99_SLACK (0.10), STALL_FLOOR (0.8: a clean run never has a
#   10 s interval below this share of the commanded step rate), CONFIRM_DIFF (0.15),
#   RTT_SLACK_MS (0.15),
#   RTT_TRIES (10), COOLDOWN (30s), RESERVE (30), OUT, RUN_ID, EXTRA,
#   NEAR (empty; 1 = run at the rig's own RTT with no netem, the gate holding the idle reading at or under
#   TARGET_RTT + RTT_SLACK_MS - the near cell, where topping a lower base up to the target is not wanted),
#   PIN_POOL (empty = the derived pool; a number runs every run at that pool instead - the k/s fit at M=8),
#   RATES (empty; fractions of the measured peak, e.g. "0.5 0.67 1.0", each run once at the pool between the
#   peak and the sweep - the rate ladder that checks the saturated mean against the highest fully-served rate),
#   STEP_RATES (empty; ABSOLUTE offered steps/s, e.g. "4500 5000 5300", each run once at the pool, shuffled -
#   finding where a cell stops being sustainable without leaning on a noisy single peak; with PEAK_REPS=0,
#   SWEEP="" and no RATES, nothing else runs)
set -euo pipefail

command -v psql >/dev/null 2>&1 || {
  echo "ABORT: psql not found (Debian: sudo apt-get install -y postgresql-client)." >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ABORT: python3 required" >&2; exit 1; }
TC="${TC:-$(command -v tc || echo /sbin/tc)}"
sudo "$TC" -V >/dev/null 2>&1 || {
  echo "ABORT: cannot run '$TC' under sudo (Debian: sudo apt-get install -y iproute2)." >&2; exit 1; }

DSN="${DSN:?set DSN (one Cloud SQL instance, private IP)}"
TARGET_RTT="${TARGET_RTT:?set TARGET_RTT in ms (0.25, 0.5, 1.0, 1.5 or 2.0)}"
BENCH="${BENCH:-./dwarf-bench}"
WORKLOAD="${WORKLOAD:-linear}"
PAYLOAD="${PAYLOAD:-0}"
WIDTH="${WIDTH:-8}"
DEPTH="${DEPTH:-1}"
LINEAR_STEPS="${LINEAR_STEPS:-10}"
VCPUS="${VCPUS:-16}"
PEAK_REPS="${PEAK_REPS:-3}"
WINDOW="${WINDOW:-16m}"
WARMUP="${WARMUP:-2m}"
CONC="${CONC:-256}"
MAX_OUTSTANDING="${MAX_OUTSTANDING:-20000}"
SWEEP="${SWEEP-0.5 0.75 1.0 1.25 1.5}" # no colon: SWEEP="" means no sweep
P99_SLACK="${P99_SLACK:-0.10}"
STALL_FLOOR="${STALL_FLOOR:-0.8}"
CONFIRM_DIFF="${CONFIRM_DIFF:-0.15}"
RTT_SLACK_MS="${RTT_SLACK_MS:-0.15}"
RTT_TRIES="${RTT_TRIES:-10}"
COOLDOWN="${COOLDOWN:-30s}"
RESERVE="${RESERVE:-30}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
# Database names are built from it, and Postgres folds unquoted identifiers: lower-case, alphanumerics only.
DB_RUN="$(printf '%s' "$RUN_ID" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9' '_')"
case "$WORKLOAD" in
  linear) SHAPE="" ;;
  fanout) SHAPE="-w${WIDTH}" ;;
  *)      SHAPE="-w${WIDTH}d${DEPTH}" ;;
esac
CELL="${WORKLOAD}${SHAPE}-p${PAYLOAD}-rtt${TARGET_RTT}-v${VCPUS}"
OUT="${OUT:-./cell-results/${CELL}-${RUN_ID}}"
mkdir -p "$OUT"

# Steps per flow converts a commanded FLOW rate into the step rate the exec/gen share divides by.
case "$WORKLOAD" in
  linear)  SPF="$LINEAR_STEPS" ;;
  fanout)  SPF=$(( WIDTH + 2 )) ;;
  scatter) SPF=$(( WIDTH * DEPTH + 2 )) ;;
  *) echo "ABORT: cell.sh drives linear, fanout or scatter, not ${WORKLOAD}" >&2; exit 1 ;;
esac
WORKLOAD_ARGS=(-workload "$WORKLOAD" -payload "$PAYLOAD" -fanout-width "$WIDTH" -branch-depth "$DEPTH" -linear-steps "$LINEAR_STEPS")

DB_HOST="$(printf '%s' "$DSN" | sed -E 's|^[^@]*@||; s|[:/].*$||')"
DEV="$(ip route get "$DB_HOST" | sed -nE 's/.* dev ([^ ]+).*/\1/p' | head -1)"
[[ -n "$DEV" ]] || { echo "ABORT: could not resolve the interface toward ${DB_HOST}" >&2; exit 1; }
clear_netem() { sudo "$TC" qdisc del dev "$DEV" root 2>/dev/null || true; }
# ANY exit must leave the interface clean, and an interrupt must END the cell: a handler that only cleared
# netem would let the next run start at the base RTT under the target RTT's label.
trap clear_netem EXIT
trap 'clear_netem; exit 130' INT TERM

ADMIN="${DSN%/*}/postgres?sslmode=disable"
psq() { psql "$1" -X -q -At -c "$2"; }

# Minimum of many `SELECT 1`s in ONE psql session, the first discarded: a process per sample measures
# process and TLS startup, and the minimum approximates the wire where a mean carries scheduler jitter.
probe_rtt() {
  local script=""
  script+=$'\\timing on\n'
  for _ in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$1" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    sed -nE 's/^Time: ([0-9.]+) ms.*/\1/p' | tail -n +2 | sort -g | head -1
}

# Waits, idle, for the measured RTT to come back within RTT_SLACK_MS of the target. A run that starts on a
# degraded server is a reading of its fatigue, not of the configuration. Under netem the reading must also
# not fall BELOW the target by more than the slack: that means the delay is no longer installed.
rtt_gate() {
  local r hi lo
  hi="$(python3 -c "print(${TARGET_RTT} + ${RTT_SLACK_MS})")"
  lo="$(python3 -c "print(${TARGET_RTT} - ${RTT_SLACK_MS} if '${KNOB}' != '0' else 0)")"
  for i in $(seq 1 "$RTT_TRIES"); do
    r="$(probe_rtt "$ADMIN")"
    if awk -v v="$r" -v t="$hi" -v l="$lo" 'BEGIN{exit !(v>0 && v<=t && v>=l)}'; then
      printf "   idle rtt %s ms (gate %s-%s ms)\n" "$r" "$lo" "$hi"; GATE_RTT="$r"; return 0
    fi
    if awk -v v="$r" -v l="$lo" 'BEGIN{exit !(v>0 && v<l)}'; then
      echo "ABORT: idle RTT ${r} ms is below ${lo} ms - the netem delay is gone." >&2
      exit 1
    fi
    printf "   idle rtt %s ms ABOVE gate %s ms - cooling down (%d/%d)\n" "${r:-?}" "$hi" "$i" "$RTT_TRIES"
    sleep "${COOLDOWN%s}"
  done
  echo "ABORT: idle RTT never returned under ${hi} ms; the server is degraded, not the build." >&2
  exit 1
}

clear_netem
BASE_RTT="$(probe_rtt "$ADMIN")"
[[ -n "$BASE_RTT" ]] || { echo "ABORT: could not measure the base RTT" >&2; exit 1; }
KNOB="$(python3 -c "
d = ${TARGET_RTT} - ${BASE_RTT}
print('abort' if d < -0.05 else (f'{d:.3f}' if d > 0.02 and '${NEAR:-}' != '1' else '0'))")"
if [[ "$KNOB" == "abort" ]]; then
  echo "ABORT: base RTT ${BASE_RTT} ms is above the ${TARGET_RTT} ms target; netem cannot remove delay." >&2
  echo "       Recreate the VM in the same zone and redraw." >&2
  exit 1
fi
if [[ "$KNOB" != "0" ]]; then sudo "$TC" qdisc add dev "$DEV" root netem delay "${KNOB}ms"; fi

MAXCONN="$(psq "$ADMIN" "SHOW max_connections")"
BUDGET=$(( MAXCONN - RESERVE ))
POOL="${PIN_POOL:-$("$BENCH" -print-pool "$TARGET_RTT" -server-max-connections "$MAXCONN")}"
(( POOL <= BUDGET )) || { echo "ABORT: derived pool ${POOL} exceeds max_connections ${MAXCONN} - ${RESERVE} reserved" >&2; exit 1; }

echo "cell ${CELL}  run ${RUN_ID}  ${SPF} steps/flow"
echo "base rtt ${BASE_RTT} ms, netem +${KNOB} ms on ${DEV} -> target ${TARGET_RTT} ms"
echo "$([[ -n "${PIN_POOL:-}" ]] && echo pinned || echo derived) pool ${POOL} (${VCPUS} vCPU label), max_connections ${MAXCONN}, window ${WINDOW} after ${WARMUP}"
echo

# run_one TAG POOL RATE - one run on a fresh database. RATE 0 = flat out.
run_one() {
  local tag="$1" pool="$2" rate="$3" db left
  db="dwarf_cell_${DB_RUN}_${tag//[^a-z0-9]/_}"
  echo "-- ${tag}  pool ${pool}  rate ${rate:-0} flows/s"
  rtt_gate
  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db} WITH (FORCE)" >/dev/null
  psq "$ADMIN" "CREATE DATABASE ${db}" >/dev/null
  # shellcheck disable=SC2086  # word splitting is the intended parse of EXTRA
  "$BENCH" -dsn "${DSN%/*}/${db}?sslmode=disable" "${WORKLOAD_ARGS[@]}" \
    -max-connections "$pool" -concurrency "$CONC" \
    -open-loop -arrival-rate "$rate" -max-outstanding "$MAX_OUTSTANDING" \
    -warmup "$WARMUP" -window "$WINDOW" -stats-interval 10s \
    -label "cell ${CELL} ${tag} pool ${pool} rate ${rate} gate-rtt ${GATE_RTT}ms" \
    -out "${OUT}/${tag}.json" ${EXTRA:-} > "${OUT}/${tag}.log" 2>&1 || echo "   bench exited nonzero; see ${OUT}/${tag}.log"
  left=$(psq "${DSN%/*}/${db}?sslmode=disable" "SELECT COUNT(*) FROM dwarf_peers" 2>/dev/null || echo "?")
  [[ "$left" == "0" ]] || echo "   WARNING: left ${left} dwarf_peers row(s) after shutdown"
  psq "$ADMIN" "DROP DATABASE IF EXISTS ${db} WITH (FORCE)" >/dev/null
  sleep "${COOLDOWN%s}"
}

# summarize - the JSON the later steps read, plus the table.
summarize() {
  python3 - "$OUT" "$SPF" "$POOL" "$P99_SLACK" "$WORKLOAD" "$PAYLOAD" "$WIDTH" "$DEPTH" "$TARGET_RTT" "$VCPUS" "$STALL_FLOOR" "$MAX_OUTSTANDING" <<'PY'
import json, glob, os, re, statistics, sys
out, spf, derived, slack = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), float(sys.argv[4])
floor = float(sys.argv[11])
cell = dict(workload=sys.argv[5], payload=int(sys.argv[6]), width=int(sys.argv[7]), depth=int(sys.argv[8]),
            targetRtt=float(sys.argv[9]), vcpus=int(sys.argv[10]), stepsPerFlow=spf)
runs = []
for path in sorted(glob.glob(os.path.join(out, "*.json"))):
    if os.path.basename(path) == "cell.json":
        continue
    tag = os.path.basename(path)[:-5]
    try:
        doc = json.load(open(path)); r = doc["results"][0]
    except (ValueError, KeyError, IndexError, OSError) as e:
        # A run that died mid-write leaves a truncated or empty artifact; record it, never crash the cell.
        runs.append(dict(tag=tag, pool=None, rate=None, steps=0, flows=0, p50=0, p99=0, share=None, rtt=0,
                         db=None, lo=None, hi=None, bad=[f"unreadable artifact: {e}"]))
        continue
    c = r.get("engineCounters") or {}
    bad = []
    if not c or r.get("gaugesAfter") is None:
        bad.append("end-of-window metrics missing")
    if not doc.get("valid", False):
        bad.append("invalid: " + doc.get("invalidity", ""))
    if c.get("dwarf_peer_changes", 0):
        bad.append(f"peer churn ({c['dwarf_peer_changes']})")
    st, se = c.get("dwarf_flows_started"), c.get("dwarf_steps_executed")
    m = re.search(r"pool (\d+) rate ([0-9.]+)", doc.get("label", ""))
    # The slowest and fastest -stats-interval readings: the mean hides a stall or a sagging tail.
    ser = [p["stepsPerSec"] for p in ((r.get("gauges") or {}).get("series") or [])]
    runs.append(dict(tag=tag, pool=int(m.group(1)) if m else None, rate=float(m.group(2)) if m else None,
        steps=r["stepsPerSec"], flows=r["flowsPerSec"], p50=r["p50Ms"], p99=r["p99Ms"],
        share=(se / (st * spf)) if st and se else None, rtt=doc["rtt"]["p50Ms"], db=r.get("db"),
        lo=min(ser) if ser else None, hi=max(ser) if ser else None, bad=bad))

peaks = [x for x in runs if x["tag"].startswith("peak-") and not x["bad"]]
# SERVED flows/s: executed steps over steps per flow. An open-loop window's flowsPerSec is the admission rate,
# which runs ahead of what was served by up to -max-outstanding flows; 2/3 of it can command past the ceiling.
peak_flows = statistics.mean(x["steps"] for x in peaks) / spf if peaks else None
sweep = [x for x in runs if x["tag"].startswith("sweep-") and not x["bad"]]
def delivers(x):
    # Admitted the commanded rate AND executed what it admitted. The first half matters: a small pool that
    # slows creation along with dispatch admits less, runs lightly loaded and posts a low p99 - it must not
    # win on that. A stall is a 10 s interval below STALL_FLOOR of the commanded step rate. Not p99/p50: at
    # 2/3 load on linear, 4% dips that caught up within the next interval put p99/p50 at 9.4 with no stall,
    # while a real stall at 87% fell to 9% of the commanded rate for 70-110 s.
    return (x["rate"] and x["flows"] >= 0.99 * x["rate"] and x["share"] is not None and x["share"] >= 0.99
            and x["lo"] is not None and x["lo"] >= floor * x["rate"] * spf)
best_p99 = min((x["p99"] for x in sweep if delivers(x)), default=None)
def clean(x):
    return delivers(x) and x["p99"] <= best_p99 * (1 + slack)
ok = sorted((x for x in sweep if clean(x)), key=lambda x: x["pool"])
optimum = ok[0]["pool"] if ok else None
# Minute slots: the 10 s series folded into whole minutes. Within one run they measure how steady it was and how
# precise its mean is - not run-to-run variance (instance, placement, time of day), which only a repeat prices.
# A minute is about one forced checkpoint cycle on a busy tier, so a burst minute tends to be followed by a
# catch-up minute: a NEGATIVE lag-1 autocorrelation is noise, a POSITIVE one is a trend.
# Verdict on the backlog: "at cap" (pinned at -max-outstanding, i.e. saturated), "drains" (each minute's floor
# comes back down), or "accumulates" (the floor ratchets up - the offered rate is past what the database sustains).
cap = int(sys.argv[12])
def minutes(path):
    try:
        r = json.load(open(path))["results"][0]
    except (ValueError, KeyError, IndexError, OSError):
        return None
    ser = (r.get("gauges") or {}).get("series") or []
    rate = [p["stepsPerSec"] for p in ser]
    out = [p.get("outstanding", p.get("pending", 0)) for p in ser]
    n = len(rate) // 6
    if n < 3:
        return None
    means = [statistics.mean(rate[i*6:(i+1)*6]) for i in range(n)]
    floors = [min(out[i*6:(i+1)*6]) for i in range(n)]
    peaks = [max(out[i*6:(i+1)*6]) for i in range(n)]
    m = statistics.mean(means)
    sd = statistics.stdev(means)
    den = sum((x - m) ** 2 for x in means)
    lag1 = sum((means[i] - m) * (means[i + 1] - m) for i in range(n - 1)) / den if den else 0.0
    first, last = max(floors[:2]), max(floors[-2:])
    if min(floors[-3:]) >= 0.9 * cap:
        verdict = "at cap"
    elif last <= max(1000, 5 * first):
        verdict = "drains"
    else:
        verdict = "accumulates"
    return dict(means=[round(x) for x in means], mean=round(m), sdPct=round(sd / m * 100, 2) if m else None,
                sePct=round(sd / n ** 0.5 / m * 100, 2) if m else None, lag1=round(lag1, 2),
                floors=floors, peaks=peaks, verdict=verdict)
for x in runs:
    x["minutes"] = minutes(os.path.join(out, x["tag"] + ".json"))
json.dump(dict(cell=cell, derivedPool=derived, peakFlowsPerSec=peak_flows, optimumPool=optimum, runs=runs),
          open(os.path.join(out, "cell.json"), "w"), indent=2)

print(f"{'run':>18} {'pool':>5} {'rate':>7} {'steps/s':>9} {'min10s':>7} {'max10s':>7} {'exec/gen':>9} {'p50ms':>7} {'p99ms':>8} {'p99/p50':>8} {'rtt':>6}  note")
for x in runs:
    note = "; ".join(x["bad"]) or ("clean" if x in ok else "")
    share = f"{x['share']*100:8.1f}%" if x["share"] is not None else "       ?"
    ratio = x["p99"] / x["p50"] if x["p50"] else 0
    lo = f"{x['lo']:7.0f}" if x.get("lo") is not None else "      -"
    hi = f"{x['hi']:7.0f}" if x.get("hi") is not None else "      -"
    print(f"{x['tag']:>18} {x['pool'] or 0:>5} {x['rate'] or 0:>7.2f} {x['steps']:>9.0f} {lo} {hi} {share} "
          f"{x['p50']:>7.0f} {x['p99']:>8.0f} {ratio:>8.1f} {x['rtt']:>6.2f}  {note}")
print(f"\n{'run':>18} {'1-min mean':>10} {'sd%':>6} {'se%':>6} {'lag1':>6}  {'backlog floor first->last':>25}  verdict   minute means")
for x in runs:
    mm = x.get("minutes")
    if not mm:
        continue
    fl = f"{max(mm['floors'][:2])} -> {max(mm['floors'][-2:])}"
    print(f"{x['tag']:>18} {mm['mean']:>10} {mm['sdPct']:>6} {mm['sePct']:>6} {mm['lag1']:>+6.2f}  {fl:>25}  {mm['verdict']:<9} "
          + " ".join(str(v) for v in mm["means"]))
print(f"\npeak {peak_flows and round(peak_flows, 2)} flows/s at the derived pool {derived}; optimum pool {optimum}")
if sweep and optimum is None:
    print("NO POOL QUALIFIED: no sweep run delivered the commanded rate cleanly, so there is no optimum and no "
          "confirming peak. Read min10s against rate x steps/flow - if it fails everywhere, STALL_FLOOR may be wrong for this shape.")
PY
}

# Optional absolute-rate ladder, needing no peak, in an order shuffled per run id like the sweep.
STEP_TAGS="$(python3 -c "
import random
r = random.Random('${RUN_ID}-steps')
fs = '${STEP_RATES:-}'.split(); r.shuffle(fs); print(' '.join(fs))")"
for sr in $STEP_TAGS; do
  run_one "steps-${sr}" "$POOL" "$(python3 -c "print(round(${sr} / ${SPF}, 3))")"
done

# 1. Peak at the derived pool.
for rep in $(seq 1 "$PEAK_REPS"); do
  run_one "peak-r${rep}" "$POOL" 0
done
summarize >/dev/null
if [[ -n "$SWEEP" || -n "${RATES:-}" ]]; then
  PEAK_FLOWS="$(python3 -c "import json; print(json.load(open('${OUT}/cell.json'))['peakFlowsPerSec'] or 0)")"
  RATE23="$(python3 -c "print(round(${PEAK_FLOWS} * 2 / 3, 3))")"
  python3 -c "import sys; sys.exit(0 if ${RATE23} > 0 else 1)" || { echo "ABORT: no valid peak run to take 2/3 of" >&2; summarize; exit 1; }
  echo "peak ${PEAK_FLOWS} flows/s -> 2/3 = ${RATE23} flows/s"
fi

# Optional rate ladder at the pool, in an order shuffled per run id like the sweep.
RATE_TAGS="$(python3 -c "
import random
r = random.Random('${RUN_ID}')
fs = '${RATES:-}'.split(); r.shuffle(fs); print(' '.join(fs))")"
for f in $RATE_TAGS; do
  run_one "rate-${f}" "$POOL" "$(python3 -c "print(round(${PEAK_FLOWS} * ${f}, 3))")"
done

# 2. Pool sweep at 2/3, in an order shuffled per run id so no pool always runs on the freshest server.
POOLS="$(python3 -c "
import random
r = random.Random('${RUN_ID}')
ps = sorted({max(2, round(${POOL} * float(f))) for f in '${SWEEP}'.split()})
r.shuffle(ps); print(' '.join(str(p) for p in ps))")"
for p in $POOLS; do
  (( p <= BUDGET )) || { echo "-- SKIP pool ${p}: above max_connections - ${RESERVE}"; continue; }
  run_one "sweep-p${p}" "$p" "$RATE23"
done

# 3. Confirm the peak at the optimum when it is not the derived pool.
summarize >/dev/null
OPT="$(python3 -c "import json; print(json.load(open('${OUT}/cell.json'))['optimumPool'] or 0)")"
if (( OPT > 0 )) && python3 -c "import sys; sys.exit(0 if abs(${OPT} - ${POOL}) / ${POOL} > ${CONFIRM_DIFF} else 1)"; then
  run_one "confirm-p${OPT}" "$OPT" 0
fi

clear_netem
if sudo "$TC" qdisc show dev "$DEV" | grep -q netem; then
  echo "ABORT: netem STILL INSTALLED on ${DEV} - every later run on this VM is corrupted until removed." >&2
  exit 1
fi
echo "netem removed from ${DEV} (verified)"
echo
echo "== ${CELL} =="
summarize
echo "
Database CPU is not in the artifacts (a managed instance). Pull them and run ./dbcpu.sh <artifact>... from
the operator machine. Per-run stored and WAL bytes are in each artifact's results[0].db."
