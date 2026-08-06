#!/usr/bin/env bash
#
# Runs a command with a netem qdisc held at a FIXED delay for the command's whole duration, then removes
# it. Runs ON the bench VM.
#
# WHY A ZERO-DELAY QDISC IS WORTH INSTALLING. netem is not free even at 0ms: it replaces the default
# queueing discipline on the interface, so a run WITH it and a run WITHOUT it differ in packet scheduling
# as well as in delay. A campaign whose base arm has no qdisc and whose distance arms do therefore
# confounds "the effect of distance" with "the effect of netem being in the path at all". Installing it
# at 0ms for the ceiling run keeps that variable fixed, so a later ladder that only changes the delay is
# comparing like with like.
#
# THE QDISC IS THE HAZARD, which is the other half of why this exists. A netem left installed silently
# corrupts every later campaign on this VM and is invisible in the artifacts. The trap removes it on ANY
# exit - success, failure or Ctrl-C - and the script verifies it is gone before returning nonzero if not.
# After a SIGKILL, check `tc qdisc show` by hand.
#
# Usage:
#   DB_HOST=10.125.0.17 ./withnetem.sh 0 -- ./rateladder.sh
#   DB_HOST=10.125.0.17 DELAY_MS=1.5 ./withnetem.sh -- ./rateladder.sh
# Args: the first argument is the delay in ms (or set DELAY_MS); everything after `--` is the command.
set -euo pipefail

TC="${TC:-$(command -v tc || echo /sbin/tc)}"
sudo "$TC" -V >/dev/null 2>&1 || {
  echo "ABORT: cannot run '$TC' under sudo (Debian: sudo apt-get install -y iproute2)." >&2; exit 1; }

DELAY_MS="${DELAY_MS:-}"
if [[ "${1:-}" != "--" && -n "${1:-}" ]]; then DELAY_MS="$1"; shift; fi
[[ "${1:-}" == "--" ]] && shift
DELAY_MS="${DELAY_MS:-0}"
[[ $# -gt 0 ]] || { echo "ABORT: no command given after --" >&2; exit 1; }

DB_HOST="${DB_HOST:?set DB_HOST (the database this run reaches, so the right interface is found)}"
# The interface the DATABASE is reached over - never a hardcoded ens4, which is right on GCE today and
# wrong on the next image.
DEV="$(ip route get "$DB_HOST" | sed -nE 's/.* dev ([^ ]+).*/\1/p' | head -1)"
[[ -n "$DEV" ]] || { echo "ABORT: could not resolve the interface toward ${DB_HOST}" >&2; exit 1; }

clear_netem() { sudo "$TC" qdisc del dev "$DEV" root 2>/dev/null || true; }
trap clear_netem EXIT INT TERM

clear_netem
sudo "$TC" qdisc add dev "$DEV" root netem delay "${DELAY_MS}ms"
echo "netem: ${DELAY_MS}ms on ${DEV} -> ${DB_HOST}"
sudo "$TC" qdisc show dev "$DEV" | head -1

status=0
"$@" || status=$?

clear_netem
if sudo "$TC" qdisc show dev "$DEV" | grep -q netem; then
  echo "FATAL: netem still installed on ${DEV} - remove it before any further run" >&2
  exit 1
fi
echo "netem removed from ${DEV}"
exit "$status"
