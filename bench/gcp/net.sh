#!/usr/bin/env bash
# Usage: net.sh [-delete]   The network every orchestrator creates its instances and VMs on, made once per project and
# kept between rigs, since none of it is billed while idle: the VPC dwarf-bench-net, the firewall rule dwarf-bench-ssh
# (SSH from this workstation's address alone; an orchestrator re-points it at the current address whenever ssh fails),
# and the private services range dwarf-bench-psa peered with Cloud SQL, without which an instance gets no private IP.
# The orchestrators create none of it and fail at their first create without it. Safe to rerun. -delete removes it all,
# once no instance or VM is left on the network.
set -euo pipefail
PROJECT=${PROJECT:-dwarf-bench-mbus} NETWORK=dwarf-bench-net
gc() { gcloud --project="$PROJECT" "$@"; }
exists() { "$@" >/dev/null 2>&1; }

if [[ "${1:-}" == -delete ]]; then
  gc compute firewall-rules delete dwarf-bench-ssh --quiet || true
  gc services vpc-peerings delete --service=servicenetworking.googleapis.com --network="$NETWORK" --quiet || true
  gc compute addresses delete dwarf-bench-psa --global --quiet || true
  gc compute networks delete "$NETWORK" --quiet || true
  exit 0
fi

exists gc compute networks describe "$NETWORK" || gc compute networks create "$NETWORK" --subnet-mode=auto
ip=$(curl -fsS -m 10 https://checkip.amazonaws.com | tr -d '[:space:]')
if exists gc compute firewall-rules describe dwarf-bench-ssh; then
  gc compute firewall-rules update dwarf-bench-ssh --source-ranges="$ip/32"
else
  gc compute firewall-rules create dwarf-bench-ssh --network="$NETWORK" --allow=tcp:22 --source-ranges="$ip/32"
fi
exists gc compute addresses describe dwarf-bench-psa --global ||
  gc compute addresses create dwarf-bench-psa --global --purpose=VPC_PEERING --prefix-length=16 --network="$NETWORK"
gc services vpc-peerings connect --service=servicenetworking.googleapis.com --ranges=dwarf-bench-psa \
  --network="$NETWORK" 2>/dev/null || echo "peering already connected"
