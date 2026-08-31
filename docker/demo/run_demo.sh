#!/usr/bin/env bash
# paimon_heap + Apache Ozone end-to-end demo
# Usage: ./run_demo.sh [--no-build]
set -euo pipefail

DEMO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DEMO_DIR"

BUILD_FLAG="--build"
[[ "${1:-}" == "--no-build" ]] && BUILD_FLAG=""

# ── 1. Build images ──────────────────────────────────────────────────────────
echo ""
echo "▶  Building images (first run: ~30 min for AWS SDK; cached after that)..."
docker compose build $BUILD_FLAG postgres spark

# ── 2. Start Ozone cluster (SCM + OM + Datanode + S3 Gateway) ────────────────
echo ""
echo "▶  Starting Apache Ozone cluster..."
docker compose up -d ozone-scm ozone-om ozone-datanode ozone-s3g

echo "   Waiting for S3 Gateway (this takes ~60 s on first start)..."
until docker compose exec -T ozone-s3g curl -sf http://localhost:9878/ > /dev/null 2>&1; do
    printf "."
    sleep 4
done
echo " ready."

# ── 3. Create Ozone bucket ───────────────────────────────────────────────────
echo ""
echo "▶  Creating Ozone volume + bucket..."
docker compose run --rm ozone-init

# ── 4. Start PostgreSQL ───────────────────────────────────────────────────────
echo ""
echo "▶  Starting PostgreSQL 18 with paimon_heap..."
docker compose up -d postgres

echo "   Waiting for PostgreSQL to be healthy..."
until docker compose exec -T postgres \
    gosu postgres psql -U demo -d demo -c "SELECT 1" > /dev/null 2>&1; do
    printf "."
    sleep 3
done
echo " ready."

# ── 5. Insert demo data ───────────────────────────────────────────────────────
echo ""
echo "▶  Creating table and inserting demo records..."
docker compose exec -T postgres \
    gosu postgres psql -U demo -d demo -f /init.sql

# ── 6. Wait for bgworker flush ────────────────────────────────────────────────
echo ""
echo "▶  Waiting 12 s for paimon_heap bgworker to flush rows to Ozone..."
sleep 12

# ── 7. Read from Paimon on Ozone with Spark ──────────────────────────────────
echo ""
echo "▶  Reading from Paimon on Apache Ozone with PySpark..."
echo ""
docker compose run --rm \
    -e PYSPARK_SUBMIT_ARGS="--jars /opt/spark/extra-jars/paimon-spark.jar,/opt/spark/extra-jars/hadoop-aws.jar,/opt/spark/extra-jars/aws-java-sdk-bundle.jar pyspark-shell" \
    spark \
    python /opt/demo/read_paimon.py

echo ""
echo "✓  Demo complete. Cluster still running."
echo "   psql:  docker compose exec postgres gosu postgres psql -U demo -d demo"
echo "   stop:  docker compose down -v"
