#!/usr/bin/env bash
set -euo pipefail

PGDATA=/var/lib/postgresql/18/main
PGCONF=/etc/postgresql/18/main/postgresql.conf
PGHBA=/etc/postgresql/18/main/pg_hba.conf

# ── First-start: initialize cluster ──────────────────────────────────────────
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    echo "[entrypoint] Initialising PostgreSQL 18 cluster..."
    pg_createcluster 18 main --start-conf=manual

    # paimon_heap GUCs — points at Ozone S3 gateway
    cat >> "$PGCONF" << 'EOF'

# ── paimon_heap ───────────────────────────────────────────────────────────────
shared_preload_libraries = 'paimon_heap'
listen_addresses         = '*'
port                     = 5432

paimon_heap.warehouse          = '/tmp/paimon-staging'
paimon_heap.s3_bucket          = 'paimon-warehouse'
paimon_heap.s3_prefix          = ''
paimon_heap.s3_region          = 'us-east-1'
paimon_heap.s3_endpoint        = 'http://minio:9000'
paimon_heap.flush_interval_txns = 1
paimon_heap.flush_interval_secs = 5
EOF

    # Allow password-less TCP connections (prepend so it beats scram rules)
    sed -i '1s/^/host all all 0.0.0.0\/0 trust\n/' "$PGHBA"

    # Brief start → create user / db / extension → stop
    echo "[entrypoint] Starting PG to run one-time setup..."
    gosu postgres /usr/lib/postgresql/18/bin/pg_ctl \
        -D "$PGDATA" -o "-c config_file=$PGCONF" -w start

    gosu postgres psql -v ON_ERROR_STOP=1 << 'EOSQL'
CREATE USER demo SUPERUSER;
CREATE DATABASE demo OWNER demo;
\connect demo
CREATE EXTENSION IF NOT EXISTS paimon_heap;
EOSQL

    echo "[entrypoint] One-time setup done."
    gosu postgres /usr/lib/postgresql/18/bin/pg_ctl -D "$PGDATA" -m fast -w stop
fi

echo "[entrypoint] Starting PostgreSQL 18 (paimon_heap enabled)..."
exec gosu postgres /usr/lib/postgresql/18/bin/postgres \
    -D "$PGDATA" \
    -c config_file="$PGCONF"
