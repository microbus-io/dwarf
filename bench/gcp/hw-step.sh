# HW (x-rig step: `BIN=bench-<sha> x-rig.sh hw 1000 hw-step.sh`, $BIN from one clean commit). The standard rig: one
# 32 vCPU instance at the recommended settings, one c4a-16, a fresh database per arm, 2 min warmup. One draw, n=1.
# Three questions: does the fan-in hold keep a wide forEach of instant tasks from growing the crew to its ceiling,
# does fillfactor 80 on dwarf_flows buy anything at the default's cost, and does fillfactor 50 on dwarf_steps (the
# hw-sf arms, ~50 min of the run) cut WAL per step without costing throughput.
#   hw-warm   3 min saturated, not read: a fresh instance's first checkpoints create its WAL files (bench/CLAUDE.md).
#   hw-a      saturated linear 0 B, cap 2,000, 6 min: this build's headline on this draw, and the rate the arms below
#             are set from.
#   hw-wide   open-loop linear 0 B at 1,416 flows/s (the rate the unpaced run took, 2026-11-15), or two thirds of
#             hw-a if that is lower; 2 min + 12 min window. From about window minute 1 an await-only bench process
#             creates one `fanout` of 10,000 no-op branches, then 100,000, 270,000 and 1,000,000, a minute apart
#             (inject.sh; times in out/hw-wide-inject.log), on the background's fairness key.
#             Pass, fixed in advance: dwarf_workers_resident (the series' crew column) peaks within the crew's floor,
#             plus the hold's depth (the dispatch count), plus one cache of candidates, plus what the background needs
#             - thousands, against 578,209 unpaced. -stack-groups says where the workers wait.
#             Read beside it: out/rss-hw-wide.log, every bench process's RSS each second; the artifact's
#             dwarf_turnstile_waiting peak (346,160 unpaced); the injections' end-to-end times; the spawn's
#             `INSERT ... unnest` statements in pgss-hw-wide.txt (calls x mean over the branches is the spawn's cost
#             per branch); the background's 10 s series and p99 while the wide flows run (the head-of-line wait).
#             The drain reports the injector's four flows "missing" (-4): this process's engine completes them and
#             the accounting is per process (bench/CLAUDE.md), so `valid: false` for that reason alone is expected.
#   hw-ff100  open-loop linear 1 KB -payload-json at 45% of hw-a's rate (about two thirds of a 1 KB ceiling), 2 min
#   hw-ff80   + 8 min, dwarf_flows at fillfactor 100 (the default, set explicitly so both arms take one path) then
#             80, set by ff-set.sh as the migrations make the table. A flow row there is ~1.1-2 KB inline: the
#             final state's ~1 KB beside the graph. Read: out/ff-<tag>.log (row size, and pgstattuple's live, dead
#             and free bytes at 2.5 to ~10 min), the .tables file of out/mon-<tag>.log for dwarf_flows (n_tup_upd,
#             n_tup_hot_upd, n_tup_newpage_upd, relation size), and WAL KB per flow (results[].db.walBytes over the
#             window's flows). 80 earns its place only if the default's dead and free bytes are material, 80 cuts
#             them, and WAL per flow is no worse.
#   hw-sf100a saturated like hw-a (cap 2,000), 2 min + 6 min each, dwarf_steps at fillfactor 100 (set explicitly so
#   hw-sf50a  every arm takes one path) or 50, set by ff-set.sh (FF_TABLE=dwarf_steps) as the migrations make the
#   hw-sf50b  table. a and b at 1 KB -payload-json, alternated 100/50/50/100: the expected effect is close to the
#   hw-sf100b run-to-run noise, so one pair cannot settle it. z at 0 B; its control is hw-sf100z when the time allows
#   hw-sf50z  it, else hw-a - which ran first on the instance, so that pair is confounded by position. Why: at 1 KB
#   hw-sf100z inline, measured locally, the step row's updates are 42% of WAL per step, most of them updates that
#             leave their page and log the whole ~1.7 KB row, and fillfactor 50 cut WAL per step 13-19%. What a local
#             run cannot show and this one reads: steps/s and flush time where the flush is byte-bound, and page
#             images per step, since 50 doubles the live table's pages and no local run crossed a checkpoint.
#             Read per arm over the window: steps/s, WAL KB and page images per step, flush ms and commits per
#             flush (the window line logged after the arm); dwarf_steps in the .tables file of out/mon-<tag>.log
#             (n_tup_upd, n_tup_hot_upd, n_tup_newpage_upd, relation size); out/ff-<tag>.log, pgstattuple's live,
#             dead and free bytes at window minutes 3 and ~6 only, since each sample is an exact scan holding a
#             snapshot that holds back vacuum on a saturated dwarf_steps.
#             Decision, fixed in advance: 50 earns its place only if, at 1 KB, WAL per step is lower in BOTH pairs
#             and steps/s is lower in neither; at 0 B neither WAL per step nor steps/s is worse beyond noise; and
#             dwarf_steps' size levels off within the window rather than growing. A mixed or small result keeps the
#             default.
#   hw-sf100k8, hw-sf50k8, hw-sf50k64, hw-sf100k64 (group sfbig): the same at 8 KB and 64 KB -payload-json, one pair
#             each, alternated. Expected to show little or nothing: PostgreSQL compresses this JSON ~3.4x before
#             moving it out of line, so 8 KB sits at the TOAST threshold and 64 KB is out of line for certain, and an
#             update that leaves a TOASTed value alone re-logs only its pointer. One pair per size can confirm a
#             near-zero effect or flag a surprise, not settle a small one.
#   hw-wide-k last, the one arm a short session drops: hw-wide with the wide flows on a fairness key of their own
#             (INJECT_ARGS='-fairness-keys 2'; the background keeps the default key) - the background's latency when
#             a wide flow does what the docs advise.
# Every arm: pgss-<tag>.txt, and out/slow-<tag>.log on the wide arms, the statements active over 500 ms every 250 ms.
# HW_STOP (an epoch) moves the orchestrator's hard deadline earlier: past it the run stops where it is, pulls the
# artifacts and tears the rig down; hw-sf100z and hw-wide-k are skipped whole when they would end past it.
# HW_ARMS picks the groups to run (default: warm a wide ff sf sfbig wide-k); a group run on a rig the earlier groups
# already used reads hw-a's rate from out/hw-a.json there.
HARD=${HW_STOP:-$HARD}
gc compute scp --zone=$ZONE "$D/slow-mon.sh" "$D/rss-mon.sh" "$D/inject.sh" "$D/ff-set.sh" "$ADMIN":~/ >/dev/null 2>&1 &&
  vmssh "$ADMIN" 'chmod +x slow-mon.sh rss-mon.sh inject.sh ff-set.sh; sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq jq >/dev/null 2>&1; command -v jq' >/dev/null ||
  { log "HW DEPLOY FAILED"; exit 1; }
SAT="-workload linear -payload 0 -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000"
# arm <group>: whether HW_ARMS asks for the group.
arm() { [[ " ${HW_ARMS:-warm a wide ff sf sfbig wide-k} " == *" $1 "* ]]; }
# fits <minutes>: whether an arm that long ends before the hard deadline, so a short session skips it whole rather
# than cutting it off mid-window.
fits() { (( $(date +%s) + $1 * 60 < HARD )); }
# admok <command>: whether a command on the admin VM succeeds. vmssh retries a command that fails, so it answers instead.
admok() { [ "$(vmssh "$ADMIN" "$1 && echo y || echo n")" = y ]; }
# slowmon <tag> <window-min>: the slow-statement sampler on the arm's database, from 45 s in (one_run makes the
# database just before its 30 s lead-in) to the end of the drain.
slowmon() {
  vmssh "$ADMIN" "setsid nohup sh -c 'sleep 45; ./slow-mon.sh $(( (2 + $2) * 60 + 60 )) out/slow-$1.log \"$(dsn "${IPS[0]}" "dwarf_${1//-/_}")\"' > /dev/null 2>&1 < /dev/null &"
}
# walread <tag>: the window's steps/s, WAL KB and page images per step, flush time and commits per flush.
walread() {
  log "  $1 window: $(vmssh "$ADMIN" "jq -r '.results[0] | (.stepsPerSec * .windowSec) as \$n | \"\\(.stepsPerSec | floor) steps/s, \\(.db.walBytes / \$n / 1024 * 10 | floor / 10) KB WAL/step, \\(.db.walFpi / \$n * 100 | floor / 100) FPI/step, flush \\(.db.flushMs * 100 | floor / 100) ms, \\(.db.commits / .db.walSyncs | floor) commits/flush\"' out/$1.json")"
}

arm warm && WIN_MIN=3 one_run hw-warm $SAT
arm a && { WIN_MIN=6 one_run hw-a $SAT; walread hw-a; }
sat=$(vmssh "$ADMIN" "jq -r '.results[0].stepsPerSec // 0' out/hw-a.json" | tr -d '[:space:]')
rate=$(awk -v s="${sat:-0}" 'BEGIN { r = s * 2 / 3 / 10; if (r < 100 || r > 1416) r = 1416; printf "%d", r }')
ffrate=$(awk -v s="${sat:-0}" 'BEGIN { r = s * 0.45 / 10; if (r < 100) r = 900; printf "%d", r }')
log "hw-a ${sat:-?} steps/s: wide background at $rate flows/s, fillfactor arms at $ffrate flows/s"

# wide <tag> [injection flags]: the background, with the four injections from about window minute 1.
wide() {
  local tag=$1
  vmssh "$ADMIN" "setsid nohup ./rss-mon.sh $(( 30 + 20 * 60 )) out/rss-$tag.log > /dev/null 2>&1 < /dev/null &"
  vmssh "$ADMIN" "INJECT_ARGS='${2:-}' setsid nohup ./inject.sh 210 '$(dsn "${IPS[0]}" "dwarf_${tag//-/_}")' $tag 10000 100000 270000 1000000 > /dev/null 2>&1 < /dev/null &"
  slowmon "$tag" 12
  WIN_MIN=12 one_run "$tag" -workload linear -payload 0 -open-loop -arrival-rate "$rate" -concurrency 256 -max-outstanding 20000 -stack-groups 4
  until admok "grep -q done out/$tag-inject.log"; do late; sleep 20; done
  log "  $tag injections: $(vmssh "$ADMIN" "awk '{ if (\$2 == \"start\") s[\$3] = \$1; else if (\$2 == \"end\") printf \"%s %ds (exit %s), \", \$3, \$1 - s[\$3], \$5 }' out/$tag-inject.log")"
  # The stats line's fourth column is the crew (dwarf_workers_resident); the stack groups below it are indented.
  log "  $tag crew peak, RSS peak (MB): $(vmssh "$ADMIN" "grep -E '^[0-9]+ +[0-9]+ ' out/$tag.log | awk '\$4 + 0 > m { m = \$4 + 0 } END { print m + 0 }'"), $(vmssh "$ADMIN" "tr ' ' '\n' < out/rss-$tag.log | awk -F: 'NF >= 2 && \$2 + 0 > m { m = \$2 + 0 } END { print m + 0 }'")"
}
arm wide && wide hw-wide

# ff <fillfactor>: dwarf_flows' setting goes on as the migrations make the table, then five readings of the table.
ff() {
  local tag=hw-ff$1
  vmssh "$ADMIN" "setsid nohup ./ff-set.sh '$(dsn "${IPS[0]}" "dwarf_${tag//-/_}")' $1 out/ff-$tag.log 150 270 390 510 590 > /dev/null 2>&1 < /dev/null &"
  WIN_MIN=8 one_run "$tag" -workload linear -payload 1024 -payload-json -open-loop -arrival-rate "$ffrate" -concurrency 256 -max-outstanding 20000
  log "  $tag dwarf_flows (epoch, rows, mean bytes; pgstattuple's len, live, live bytes, dead, dead bytes, free, free%):"
  vmssh "$ADMIN" "cat out/ff-$tag.log" | sed 's/^/    /'
}
arm ff && { ff 100; ff 80; }

# sf <suffix> <fillfactor> <payload flags>...: saturated linear with dwarf_steps at the fillfactor from the
# migrations on, read at window minutes 3 and ~6.
sf() {
  local tag=hw-sf$1 fill=$2; shift 2
  vmssh "$ADMIN" "FF_TABLE=dwarf_steps setsid nohup ./ff-set.sh '$(dsn "${IPS[0]}" "dwarf_${tag//-/_}")' $fill out/ff-$tag.log 300 470 > /dev/null 2>&1 < /dev/null &"
  WIN_MIN=6 one_run "$tag" -workload linear "$@" -open-loop -arrival-rate 0 -concurrency 256 -max-outstanding 2000
  walread "$tag"
  log "  $tag dwarf_steps (epoch, rows, mean bytes; pgstattuple's len, live, live bytes, dead, dead bytes, free, free%):"
  vmssh "$ADMIN" "cat out/ff-$tag.log" | sed 's/^/    /'
}
if arm sf; then
  sf 100a 100 -payload 1024 -payload-json
  sf 50a 50 -payload 1024 -payload-json
  sf 50b 50 -payload 1024 -payload-json
  sf 100b 100 -payload 1024 -payload-json
  sf 50z 50 -payload 0
  if fits 11; then sf 100z 100 -payload 0; else log "hw-sf100z skipped: it would end past the deadline"; fi
fi
if arm sfbig; then
  sf 100k8 100 -payload 8192 -payload-json
  sf 50k8 50 -payload 8192 -payload-json
  sf 50k64 50 -payload 65536 -payload-json
  sf 100k64 100 -payload 65536 -payload-json
fi

if arm wide-k; then
  if fits 17; then wide hw-wide-k '-fairness-keys 2'; else log "hw-wide-k skipped: it would end past the deadline"; fi
fi
