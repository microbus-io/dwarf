#!/usr/bin/env bash
#
# Provisions the dwarf cloud-benchmark stack on GCP (see _BENCH.md phase 2):
# a dedicated VPC with private-services access, one Cloud SQL for PostgreSQL
# instance reachable via PRIVATE IP ONLY (never the Auth Proxy - it would
# pollute the RTT measurement), and one Compute Engine VM to run the bench
# host. Idempotent-ish: each resource is skipped if it already exists.
#
# Billable resources: the Cloud SQL instance and the VM. Tear down with
# teardown.sh at the end of every session. Note DB_DISK_GB defaults to 1TB
# (see below) - a multi-shard ladder provisions that per instance.
#
# Usage:
#   DB_PASSWORD=... ./provision.sh
# Knobs (env vars):
#   PROJECT, REGION, ZONE, DB_INSTANCE, DB_TIER, DB_DISK_GB, DB_MAX_CONNECTIONS,
#   VM_NAME, VM_TYPE
set -euo pipefail

PROJECT="${PROJECT:-dwarf-bench-mbus}"
REGION="${REGION:-us-central1}"          # c4a (Axion ARM) availability
ZONE="${ZONE:-${REGION}-a}"
NETWORK="dwarf-bench-net"
DB_INSTANCE="${DB_INSTANCE:-dwarf-bench-db}"
DB_TIER="${DB_TIER:-db-custom-2-8192}"   # baseline: 2 vCPU / 8GB
# Disk size is the IOPS proxy on Cloud SQL: PD-SSD provisions ~30 IOPS/GB, so the former 100GB default
# capped a rig at ~3,000 IOPS. That was enough for linear load (whose write-IOPS peaked at half the
# budget) but throttled every write-heavy fan-out fill, and it manufactured a "volume cliff" at ~16M
# rows that vanished on a 1TB disk. 1TB gives ~30,000 IOPS - deliberately not the binding resource, so
# a campaign measures the engine rather than the disk. Drop it only to measure the disk on purpose.
DB_DISK_GB="${DB_DISK_GB:-1000}"
DB_MAX_CONNECTIONS="${DB_MAX_CONNECTIONS:-}" # empty = tier default
VM_NAME="${VM_NAME:-dwarf-bench-vm}"
VM_TYPE="${VM_TYPE:-c4a-standard-4}"     # baseline host: 4 vCPU ARM
DB_PASSWORD="${DB_PASSWORD:?set DB_PASSWORD (the postgres user password)}"

gc() { gcloud --project="$PROJECT" "$@"; }
exists() { "$@" >/dev/null 2>&1; }

echo "== network =="
exists gc compute networks describe "$NETWORK" ||
  gc compute networks create "$NETWORK" --subnet-mode=auto

echo "== firewall (SSH from operator IP only) =="
MYIP="$(curl -fsS https://checkip.amazonaws.com | tr -d '[:space:]')"
if exists gc compute firewall-rules describe dwarf-bench-ssh; then
  gc compute firewall-rules update dwarf-bench-ssh --source-ranges="$MYIP/32"
else
  gc compute firewall-rules create dwarf-bench-ssh \
    --network="$NETWORK" --allow=tcp:22 --source-ranges="$MYIP/32"
fi

echo "== private services access (Cloud SQL private IP) =="
exists gc compute addresses describe dwarf-bench-psa --global ||
  gc compute addresses create dwarf-bench-psa --global --purpose=VPC_PEERING \
    --prefix-length=16 --network="$NETWORK"
gc services vpc-peerings connect --service=servicenetworking.googleapis.com \
  --ranges=dwarf-bench-psa --network="$NETWORK" 2>/dev/null ||
  echo "   (peering already connected)"

echo "== Cloud SQL instance ($DB_TIER, ${DB_DISK_GB}GB SSD) =="
if ! exists gc sql instances describe "$DB_INSTANCE"; then
  FLAGS=()
  if [[ -n "$DB_MAX_CONNECTIONS" ]]; then
    FLAGS+=(--database-flags="max_connections=$DB_MAX_CONNECTIONS")
  fi
  # ${FLAGS[@]+...} keeps an empty array from tripping set -u on bash 3.2 (macOS default).
  # --zone (mutually exclusive with --region; it implies it) pins the DB to the VM's zone: same-zone is
  # the baseline placement (left unpinned, Cloud SQL picks its own zone and the run silently becomes
  # cross-zone).
  gc sql instances create "$DB_INSTANCE" \
    --database-version=POSTGRES_16 --tier="$DB_TIER" --edition=enterprise \
    --zone="$ZONE" --storage-type=SSD --storage-size="$DB_DISK_GB" \
    --network="projects/$PROJECT/global/networks/$NETWORK" --no-assign-ip \
    --root-password="$DB_PASSWORD" ${FLAGS[@]+"${FLAGS[@]}"}
fi
exists gc sql databases describe dwarf --instance="$DB_INSTANCE" ||
  gc sql databases create dwarf --instance="$DB_INSTANCE"

echo "== VM ($VM_TYPE) =="
exists gc compute instances describe "$VM_NAME" --zone="$ZONE" ||
  gc compute instances create "$VM_NAME" \
    --zone="$ZONE" --machine-type="$VM_TYPE" --network="$NETWORK" \
    --image-family=debian-12-arm64 --image-project=debian-cloud \
    --labels=app=dwarf-bench

echo "== VM bootstrap (postgresql-client) =="
# The campaign scripts drop/create their own per-run database and gate on an idle RTT probe, both through
# psql, so a VM without it cannot run any of them. It is installed HERE rather than left to the operator
# because the failure is silent and expensive: the RTT probe suppresses its own stderr (a failed probe is
# an ordinary outcome the gate retries), so a missing psql reads as "RTT still too high" and the gate cools
# down forever. Measured: 40 minutes parked on the first arm with nothing in the log naming the cause.
#
# Retried because SSH is not ready the instant the instance reports RUNNING.
for i in $(seq 1 10); do
  if gc compute ssh "$VM_NAME" --zone="$ZONE" --command \
      'sudo apt-get update -qq >/dev/null 2>&1 && sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq postgresql-client >/dev/null 2>&1 && command -v psql' 2>/dev/null; then
    break
  fi
  echo "   ssh not ready yet ($i/10)"; sleep 15
done
gc compute ssh "$VM_NAME" --zone="$ZONE" --command 'command -v psql >/dev/null' 2>/dev/null || {
  echo "WARNING: could not install postgresql-client on $VM_NAME - the campaign scripts will abort at their preflight." >&2
}

DB_IP="$(gc sql instances describe "$DB_INSTANCE" --format='value(ipAddresses[0].ipAddress)')"
VM_IP="$(gc compute instances describe "$VM_NAME" --zone="$ZONE" \
  --format='value(networkInterfaces[0].accessConfigs[0].natIP)')"

echo
echo "provisioned:"
echo "  db private ip: $DB_IP"
echo "  vm public ip:  $VM_IP"
echo "  dsn:           postgres://postgres:\$DB_PASSWORD@$DB_IP:5432/dwarf?sslmode=disable"
echo
echo "deploy + run:"
echo "  GOOS=linux GOARCH=arm64 go build -o /tmp/dwarf-bench ./bench"
echo "  gcloud compute scp /tmp/dwarf-bench $VM_NAME:~ --zone=$ZONE --project=$PROJECT"
echo "  gcloud compute ssh $VM_NAME --zone=$ZONE --project=$PROJECT -- \\"
echo "    './dwarf-bench -dsn \"postgres://postgres:...@$DB_IP:5432/dwarf?sslmode=disable\" ...'"
