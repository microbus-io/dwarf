#!/usr/bin/env bash
#
# Reports Cloud SQL CPU for the window each bench artifact covers.
#
# WHY THIS EXISTS. The harness's cpuCores is getrusage on the BENCH PROCESS - the load generator plus the
# engine - and says nothing about the database, which on this rig is a managed instance the host cannot
# see. Every "is the engine short or is the database saturated?" question needs both numbers, and without
# this the second one was being guessed at (taskdelay.sh's own reading guide says "check DB CPU" and offers
# no way to). It is fetched afterwards rather than sampled during the run so it costs the measurement
# nothing: each artifact records its own startedAt/endedAt, which is exactly the window to ask about.
#
# Cloud Monitoring samples this metric once a minute, so a 60s window yields ~1-2 points and the MAX is the
# number to read; widen the window (-p) to get a mean worth trusting. `gcloud monitoring` has no
# time-series verb, hence the REST call.
#
# Usage:
#   ./dbcpu.sh artifact.json [artifact.json ...]
#   ./dbcpu.sh -w 2026-07-31T12:00:00Z 2026-07-31T12:05:00Z
# Knobs (env): PROJECT, DB_INSTANCE, VCPUS (for the cores conversion, default 16),
#   PAD_SECONDS (widen each side of the window, default 60)
set -euo pipefail

PROJECT="${PROJECT:-dwarf-bench-mbus}"
DB_INSTANCE="${DB_INSTANCE:-dwarf-bench-db}"
VCPUS="${VCPUS:-16}"
PAD_SECONDS="${PAD_SECONDS:-60}"

command -v python3 >/dev/null 2>&1 || { echo "ABORT: python3 required" >&2; exit 1; }

fetch() { # $1=start $2=end $3=label
  local token filter
  token="$(gcloud auth print-access-token 2>/dev/null)" || {
    echo "ABORT: not authenticated (gcloud auth login)" >&2; exit 1; }
  filter="metric.type=\"cloudsql.googleapis.com/database/cpu/utilization\" AND resource.labels.database_id=\"${PROJECT}:${DB_INSTANCE}\""
  curl -sG "https://monitoring.googleapis.com/v3/projects/${PROJECT}/timeSeries" \
    -H "Authorization: Bearer ${token}" \
    --data-urlencode "filter=${filter}" \
    --data-urlencode "interval.startTime=$1" \
    --data-urlencode "interval.endTime=$2" |
  VCPUS="$VCPUS" LABEL="$3" python3 -c '
import json,os,sys
d=json.load(sys.stdin)
label=os.environ["LABEL"]; v=float(os.environ["VCPUS"])
if "error" in d:
    msg = d["error"].get("message", "")
    print("%-28s ERROR %s" % (label, msg[:80])); sys.exit()
ts=d.get("timeSeries") or []
if not ts:
    print(f"{label:28} no data (window too narrow? the metric samples ~1/min)"); sys.exit()
p=[x["value"]["doubleValue"] for x in ts[0]["points"]]
print(f"{label:28} n={len(p):>3}  mean {100*sum(p)/len(p):>5.1f}%  max {100*max(p):>5.1f}%   "
      f"({v*sum(p)/len(p):>5.2f} / {v*max(p):>5.2f} of {v:.0f} cores)")
'
}

if [[ "${1:-}" == "-w" ]]; then
  fetch "$2" "$3" "window"
  exit 0
fi

[[ $# -gt 0 ]] || { echo "usage: $0 artifact.json [...] | $0 -w <start> <end>" >&2; exit 1; }

printf '%-28s %s\n' "artifact" "cloud sql cpu"
for f in "$@"; do
  read -r start end < <(PAD="$PAD_SECONDS" python3 -c '
import json,os,sys,datetime
j=json.load(open(sys.argv[1])); pad=int(os.environ["PAD"])
def shift(t,secs):
    d=datetime.datetime.fromisoformat(t.replace("Z","+00:00"))+datetime.timedelta(seconds=secs)
    return d.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
print(shift(j["startedAt"],-pad), shift(j["endedAt"],pad))
' "$f")
  fetch "$start" "$end" "$(basename "$f" .json)"
done
