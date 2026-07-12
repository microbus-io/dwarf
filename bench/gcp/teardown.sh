#!/usr/bin/env bash
#
# Tears down the billable dwarf-benchmark resources (VM + Cloud SQL). Run at
# the end of EVERY session. By default keeps the free-ish network plumbing
# (VPC, firewall rule, private-services range) so the next provision is
# faster; pass -all to remove everything.
set -euo pipefail

PROJECT="${PROJECT:-dwarf-bench-mbus}"
REGION="${REGION:-us-central1}"
ZONE="${ZONE:-${REGION}-a}"
NETWORK="dwarf-bench-net"
DB_INSTANCE="${DB_INSTANCE:-dwarf-bench-db}"
VM_NAME="${VM_NAME:-dwarf-bench-vm}"

gc() { gcloud --project="$PROJECT" "$@"; }
try() { "$@" || echo "   (skipped: $*)"; }

echo "== deleting VM =="
try gc compute instances delete "$VM_NAME" --zone="$ZONE" --quiet

echo "== deleting Cloud SQL instance =="
try gc sql instances delete "$DB_INSTANCE" --quiet

if [[ "${1:-}" == "-all" ]]; then
  echo "== deleting network plumbing =="
  try gc compute firewall-rules delete dwarf-bench-ssh --quiet
  try gc services vpc-peerings delete --service=servicenetworking.googleapis.com \
    --network="$NETWORK" --quiet
  try gc compute addresses delete dwarf-bench-psa --global --quiet
  try gc compute networks delete "$NETWORK" --quiet
fi

echo "== remaining instances (should be empty) =="
gc compute instances list 2>/dev/null || true
gc sql instances list 2>/dev/null || true
