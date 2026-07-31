#!/usr/bin/env bash
#
# Refiller decoupling A/B: the SAME workload at a FIXED shard count, run against two engine builds -
# `base` (the barriered refiller: one goroutine fanning out through ShardSet.OnEach) and `new` (one
# refiller per shard, planning globally over a shared census, fetching only its own slice). Runs ON
# the bench VM, next to both deployed binaries.
#
# WHY A SEPARATE SCRIPT FROM shardladder.sh. That script's axis is the shard COUNT with one binary;
# this one's axis is the BINARY at one shard count. Interleaving has to happen on the axis under test
# (control 2 below), so an A/B cannot be spelled as two sequential shardladder runs - that loads all
# session drift onto whichever build ran second, which is precisely the confound that made the
# original ladder uninterpretable.
#
# WHERE TO LOOK FIRST, and why it is NOT throughput. This rig's deep-backlog throughput is bimodal
# (a fixed-configuration control arm has spanned 2.1x), so a throughput delta smaller than that spread
# is not resolved no matter how many replicates are run. The decoupling's mechanism, by contrast, is
# directly instrumented: the barrier's cost is the max-over-shards wait, measured as the gap between
# the merged pass duration and the per-shard scan maximum. That gap is what this change deletes, and
# it is a low-variance phase-level number. Read the refiller section first; throughput second.
#
# THE THREE CONTROLS, inherited from shardladder.sh because each has already saved a campaign here:
#   1. SEPARATE INSTANCES, not just separate databases (accumulated databases on one instance depress
#      throughput ~2.6x uniformly).
#   2. INTERLEAVED replicates, rep-major over the BUILD axis (base,new / base,new / ...), so session
#      drift hits both builds equally.
#   3. AN RTT GATE that aborts rather than silently producing another ambiguous result.
#
# Usage:
#   DSN1=... ... DSN6=... ./decouple-ab.sh
# Knobs (env): BASE/NEW (binaries), OUT, REPS (default 3), SHARDS (default 6; e.g. "5 6"),
#   CONC, WINDOW, WARMUP, VCPUS, WORKLOAD, RTT_MAX_DELTA_MS, RUN_ID, EXTRA.
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

DSNS=()
while :; do
  varname="DSN$(( ${#DSNS[@]} + 1 ))"
  [[ -n "${!varname:-}" ]] || break
  DSNS+=("${!varname}")
done
BASE="${BASE:-./dwarf-bench-base}"
NEW="${NEW:-./dwarf-bench-new}"
# BUILDS is the axis under test: space-separated name=path pairs, run in this order inside every rep
# (control 2). Names must be dash-free - the summary parses them out of the artifact filename. The
# default is the two-way A/B; a margin/knob sweep passes more arms, and should always keep `base` among
# them as the interleaved control rather than comparing against a previous campaign (the baseline
# itself has moved 10%+ between sessions on this rig).
BUILDS="${BUILDS:-base=$BASE new=$NEW}"
OUT="${OUT:-./decouple-ab-results}"
REPS="${REPS:-3}"
SHARDS="${SHARDS:-6}"
CONC="${CONC:-4096}"
WINDOW="${WINDOW:-60s}"
WARMUP="${WARMUP:-15s}"
VCPUS="${VCPUS:-8}"
WORKLOAD="${WORKLOAD:-linear}"
RTT_MAX_DELTA_MS="${RTT_MAX_DELTA_MS:-1.0}"
# Campaign 7's workload, so the absolute numbers stay comparable to its 6-shard baseline (14,913
# steps/s). A single fairness key would be the degenerate single-partition case for the refiller.
EXTRA="${EXTRA:--fairness-keys 256 -task-delay 5ms}"

MAXNEEDED=0
for s in $SHARDS; do [[ $s -gt $MAXNEEDED ]] && MAXNEEDED=$s; done
if [[ ${#DSNS[@]} -lt $MAXNEEDED ]]; then
  echo "need $MAXNEEDED DSNs (one Cloud SQL instance each) for SHARDS='$SHARDS'; got ${#DSNS[@]}" >&2
  exit 1
fi
for spec in $BUILDS; do
  name="${spec%%=*}"; path="${spec#*=}"
  [[ "$name" == *-* ]] && { echo "build name must not contain '-': $name" >&2; exit 1; }
  [[ -x "$path" ]] || { echo "not executable: $path" >&2; exit 1; }
done

RUN_ID="${RUN_ID:-$(date +%Y%m%d%H%M%S)}"
RUN_ID="${RUN_ID//[^a-zA-Z0-9]/_}"
mkdir -p "$OUT"
echo "run id: ${RUN_ID}  (databases: dwarf_ab_${RUN_ID}_<build>_<n>s_r<rep>)"

dsn_with_db() { # $1 = dsn, $2 = dbname
  local dsn="$1" db="$2" base query=""
  base="${dsn%%\?*}"
  [[ "$dsn" == *\?* ]] && query="?${dsn#*\?}"
  echo "${base%/*}/${db}${query}"
}
admin_dsn() { dsn_with_db "$1" postgres; }
psq() { psql "$1" -X -q -At -c "$2"; }

# --- Control 3: the RTT gate ----------------------------------------------------------------------
# One psql session, MINIMUM taken. Timing a psql process per sample would measure ~29ms of process +
# TCP + TLS startup and bury the sub-millisecond signal (see shardladder.sh for the full rationale).
probe_rtt_ms() { # $1 = dsn
  local dsn="$1" script i
  script=$'\\timing on\n'
  for i in $(seq 1 25); do script+=$'SELECT 1;\n'; done
  psql "$dsn" -X -q -At -v ON_ERROR_STOP=1 <<<"$script" 2>/dev/null |
    awk '/^Time:/ {v=$2+0; if (m=="" || v<m) m=v} END {if (m=="") m=0; printf "%.4f", m}'
}

echo "== RTT gate =="
RTTS=()
for i in $(seq 0 $((MAXNEEDED - 1))); do
  r=$(probe_rtt_ms "$(admin_dsn "${DSNS[$i]}")")
  RTTS+=("$r")
  printf "  shard %d  rtt %s ms\n" "$((i + 1))" "$r"
done
RTT_MIN=$(printf '%s\n' "${RTTS[@]}" | sort -g | head -1)
RTT_MAX=$(printf '%s\n' "${RTTS[@]}" | sort -g | tail -1)
RTT_DELTA=$(awk -v a="$RTT_MAX" -v b="$RTT_MIN" 'BEGIN{printf "%.3f", a-b}')
echo "  spread ${RTT_DELTA} ms absolute, tolerance ${RTT_MAX_DELTA_MS} ms"
if awk -v d="$RTT_DELTA" -v t="$RTT_MAX_DELTA_MS" 'BEGIN{exit !(d>t)}'; then
  echo "ABORT: shard RTTs differ by ${RTT_DELTA} ms (tolerance ${RTT_MAX_DELTA_MS} ms) - not placement jitter." >&2
  exit 1
fi

# --- The A/B --------------------------------------------------------------------------------------
echo
echo "== A/B: ${REPS} reps x {$(for s in $BUILDS; do printf '%s ' "${s%%=*}"; done)} x shards {${SHARDS}}, conc ${CONC}, ${WINDOW} window =="
for rep in $(seq 1 "$REPS"); do
  for n in $SHARDS; do
    # Control 2: every build runs back to back inside one rep, so drift cannot load onto one build.
    for spec in $BUILDS; do
      build="${spec%%=*}"; binpath="${spec#*=}"
      tag="${build}-${n}s-r${rep}"
      db="dwarf_ab_${RUN_ID}_${build}_${n}s_r${rep}"
      # shellcheck disable=SC2206  # word splitting is the intended parse of EXTRA
      args=(-workload "$WORKLOAD" -vcpus "$VCPUS" -concurrency "$CONC"
        -window "$WINDOW" -warmup "$WARMUP"
        ${EXTRA:-}
        -label "decouple A/B ${build} ${n}-shard rep ${rep}"
        -out "${OUT}/r-${tag}.json")

      # Control 1: a fresh database per run, on this arm's own instances.
      for i in $(seq 0 $((n - 1))); do
        adm=$(admin_dsn "${DSNS[$i]}")
        psq "$adm" "DROP DATABASE IF EXISTS ${db}" >/dev/null
        psq "$adm" "CREATE DATABASE ${db}" >/dev/null
        args+=(-dsn "$((i + 1))=$(dsn_with_db "${DSNS[$i]}" "$db")")
      done

      echo "-- ${tag}"
      "$binpath" "${args[@]}"

      for i in $(seq 0 $((n - 1))); do
        psq "$(admin_dsn "${DSNS[$i]}")" "DROP DATABASE IF EXISTS ${db}" >/dev/null
      done
    done
  done
done

# --- Summary --------------------------------------------------------------------------------------
echo
echo "== summary =="
python3 - "$OUT" <<'PY'
import glob, json, os, statistics, sys

out = sys.argv[1]
arms = {}   # (build, shards) -> [(result, doc)]
for path in sorted(glob.glob(os.path.join(out, "r-*-*s-r*.json"))):
    doc = json.load(open(path))
    b = os.path.basename(path)
    build = b.split("-")[1]
    n = int(b.split("-")[2].rstrip("s"))
    for r in doc.get("results", []):
        arms.setdefault((build, n), []).append((r, doc))

def stat(key, sel):
    vals = [sel(r, d) for r, d in arms[key]]
    return statistics.mean(vals), min(vals), max(vals)

shard_counts = sorted({n for _, n in arms})
# Preserve the order builds were run in, with the control first.
seen, build_order = set(), []
for path in sorted(glob.glob(os.path.join(out, "r-*-*s-r*.json"))):
    b = os.path.basename(path).split("-")[1]
    if b not in seen:
        seen.add(b); build_order.append(b)
build_order.sort(key=lambda b: (b != "base", b))

print("-- throughput and tail (BOTH high variance on this rig; read the refiller section too) --")
print(f"{'shards':>6} {'build':>6} {'n':>2} {'steps/s':>9} {'spread':>15} {'p99ms':>7} {'p99 spread':>15} {'steps/core':>10}")
for n in shard_counts:
    for build in build_order:
        if (build, n) not in arms:
            continue
        m, lo, hi = stat((build, n), lambda r, d: r["stepsPerSec"])
        p99, plo, phi = stat((build, n), lambda r, d: r["p99Ms"])
        spc, _, _ = stat((build, n), lambda r, d: r.get("host", {}).get("stepsPerCore", 0))
        cnt = len(arms[(build, n)])
        print(f"{n:>6} {build:>6} {cnt:>2} {m:>9.0f} {lo:>7.0f}-{hi:<7.0f} {p99:>7.0f} {plo:>7.0f}-{phi:<7.0f} {spc:>10.0f}")
    if ("base", n) not in arms:
        continue
    mb, lob, hib = stat(("base", n), lambda r, d: r["stepsPerSec"])
    pb, plob, phib = stat(("base", n), lambda r, d: r["p99Ms"])
    for build in build_order:
        if build == "base" or (build, n) not in arms:
            continue
        mn, lon, hin = stat((build, n), lambda r, d: r["stepsPerSec"])
        pn, plon, phin = stat((build, n), lambda r, d: r["p99Ms"])
        # The noise floor is the WIDER of the two arms, not the control's alone: an arm whose own
        # replicates span 40% cannot resolve a 15% difference no matter how tight the control is.
        noise = max((hib - lob) / mb, (hin - lon) / mn) if mb and mn else 0
        delta = mn / mb - 1
        sep = "resolved (arms do not overlap)" if (lon > hib or hin < lob) else (
            "WITHIN NOISE - not resolved" if abs(delta) <= noise else "exceeds noise but arms overlap")
        psep = "resolved" if (plon > phib or phin < plob) else "not resolved (arms overlap)"
        print(f"       -> {build}/base steps x{mn/mb:.2f} ({delta*100:+.0f}%), noise floor {noise*100:.0f}%: {sep}")
        print(f"          {build}/base p99   x{pn/pb:.2f} ({(pn/pb-1)*100:+.0f}%): {psep}")

print("\n-- refiller (the mechanism under test) --")
print("NOTE: dwarf_refill_duration_seconds means DIFFERENT things per build. In `base` it is one")
print("MERGED pass over all shards, so (pass - per_shard_max) is the barrier's straggler tax. In")
print("`new` it is ONE SHARD's own pass (the instrument gained a shard attribute), so there is no")
print("merged pass to measure and the tax is structurally absent. Do not read the two pass columns")
print("as the same quantity; the claim is that the GAP in base is real and is gone in new.")
print()
print(f"{'shards':>6} {'build':>6} {'waste':>6} {'pass ms':>8} {'shardmax':>9} {'gap ms':>7} {'phase1 ms':>10} {'phase3 ms':>10} {'batch':>7}")
for n in shard_counts:
    for build in build_order:
        if (build, n) not in arms:
            continue
        sel = disc = 0
        passes, q1, q3, per_shard_pass = [], [], [], {}
        for r, _ in arms[(build, n)]:
            c = r.get("engineCounters", {})
            sel += c.get("dwarf_refill_candidates_selected", 0)
            disc += c.get("dwarf_refill_candidates_discarded", 0)
            shard_phase = {}
            for h in r.get("engineHistograms", []):
                if h["count"] == 0:
                    continue
                mean_ms = h["sumSeconds"] / h["count"] * 1000
                a = h.get("attrs", {}) or {}
                if h["name"] == "dwarf_refill_duration_seconds":
                    passes.append(mean_ms)
                    if "shard" in a:
                        per_shard_pass.setdefault(a["shard"], []).append(mean_ms)
                elif h["name"] == "dwarf_refill_query_duration_seconds":
                    shard_phase.setdefault((a.get("phase"), a.get("shard")), []).append(mean_ms)
            for (phase, sh), v in shard_phase.items():
                (q1 if phase == "band_keys" else q3).append((sh, statistics.mean(v)))
        # The per-shard maximum WITHIN a run is the barrier's floor: OnEach returns when the slowest
        # shard does, so a merged pass can never beat it.
        maxq = statistics.mean([max(v for _, v in q1)]) if q1 else 0
        pass_ms = statistics.mean(passes) if passes else 0
        gap = pass_ms - maxq if (passes and q1 and build == "base") else float("nan")
        waste = f"{disc/sel*100:.0f}%" if sel else "n/a"
        m1 = statistics.mean([v for _, v in q1]) if q1 else 0
        m3 = statistics.mean([v for _, v in q3]) if q3 else 0
        gaps = f"{gap:>7.1f}" if gap == gap else "    n/a"
        npass = sum(h["count"] for r, _ in arms[(build, n)] for h in r.get("engineHistograms", [])
                    if h["name"] == "dwarf_refill_duration_seconds")
        batch = sel / npass if npass else 0
        print(f"{n:>6} {build:>6} {waste:>6} {pass_ms:>8.1f} {maxq:>9.1f} {gaps} {m1:>10.1f} {m3:>10.1f} {batch:>7.0f}")

print("""
Reading it:
  - base `gap ms` large            -> the barrier's max-over-shards tax is real on this rig
  - new `pass ms` ~ its phase1+3   -> a shard's cycle is its own work; no straggler term remains
  - waste (discarded/selected)     -> refiller oversupply; also the gauge for re-tuning refillPace
                                      against the shorter independent cycle
  - steps/core flat across builds  -> the GENERATOR is the limit, and no refiller change can show up
                                      in throughput on this rig; report the phase numbers instead""")
PY
