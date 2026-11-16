#!/usr/bin/env bash
# Usage: ./pgvm-setup.sh <password-file>   On a fresh Debian 12 VM: PostgreSQL 16 from the PostgreSQL apt repository at
# the rig's recommended settings (bench/CLAUDE.md, "Running the cloud rig"), the postgres user's password from the
# file, password login from the private network, pg_stat_statements in every new database - and the wall clock left
# free to be stepped: every time sync that could undo a step is off (chrony / timesyncd, and the guest agent's
# resync after a live migration). Prints one line with the PostgreSQL version and the clock's sync state.
set -eu
pw=$1
export DEBIAN_FRONTEND=noninteractive
sudo -E apt-get update -qq
sudo -E apt-get install -y -qq postgresql-common ca-certificates >/dev/null
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y >/dev/null 2>&1
sudo -E apt-get install -y -qq postgresql-16 >/dev/null
mem=$(awk '/MemTotal/ {print int($2 / 1024)}' /proc/meminfo)
sudo -u postgres psql -X -q <<SQL
ALTER SYSTEM SET listen_addresses = '*';
ALTER SYSTEM SET max_connections = 500;
ALTER SYSTEM SET shared_buffers = '$(( mem / 4 ))MB';
ALTER SYSTEM SET effective_cache_size = '$(( mem * 3 / 4 ))MB';
ALTER SYSTEM SET maintenance_work_mem = '1GB';
ALTER SYSTEM SET autovacuum_work_mem = '1GB';
ALTER SYSTEM SET max_wal_size = '20GB';
ALTER SYSTEM SET checkpoint_timeout = '15min';
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
ALTER SYSTEM SET track_io_timing = on;
ALTER SYSTEM SET track_wal_io_timing = on;
ALTER SYSTEM SET log_checkpoints = on;
ALTER SYSTEM SET log_lock_waits = on;
ALTER SYSTEM SET log_autovacuum_min_duration = '10s';
ALTER SYSTEM SET log_line_prefix = '%m [%p] %q%u@%d ';
SQL
sudo -u postgres psql -X -q -v pw="$(cat "$pw")" <<'SQL'
ALTER USER postgres PASSWORD :'pw';
SQL
echo "host all all 10.0.0.0/8 scram-sha-256" | sudo tee -a /etc/postgresql/16/main/pg_hba.conf >/dev/null
sudo systemctl restart postgresql
for d in template1 postgres; do sudo -u postgres psql -X -q -d $d -c 'CREATE EXTENSION IF NOT EXISTS pg_stat_statements'; done

# The clock. An NTP daemon would slew or step a moved clock back; the guest agent resyncs it after a live migration.
sudo timedatectl set-ntp false 2>/dev/null || true
for s in chrony chronyd systemd-timesyncd ntp ntpsec; do sudo systemctl disable --now $s >/dev/null 2>&1 || true; done
f=/etc/default/instance_configs.cfg
if sudo grep -q '^\[Daemons\]' $f 2>/dev/null; then
  sudo sed -i '/^\[Daemons\]/a clock_skew_daemon = false' $f
else
  printf '\n[Daemons]\nclock_skew_daemon = false\n' | sudo tee -a $f >/dev/null
fi
sudo systemctl restart google-guest-agent >/dev/null 2>&1 || true
echo "$(sudo -u postgres psql -X -At -c 'SHOW server_version') ntp=$(timedatectl show -p NTP --value) chrony=$(systemctl is-active chrony 2>/dev/null) timesyncd=$(systemctl is-active systemd-timesyncd 2>/dev/null)"
