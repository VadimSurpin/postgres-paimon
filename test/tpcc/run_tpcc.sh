#!/usr/bin/env bash
# One command: a BenchBase TPC-C workload replicated through paimon_slot + paimon-writer, then a
# strict comparison of PostgreSQL's final state against the Paimon warehouse. No containers.
#
#   usage: test/tpcc/run_tpcc.sh
#
# Everything is configurable from the environment; the defaults are the CI configuration.
#
#   TPCC_RUN_DIR        runtime state: cluster, logs, warehouse, BenchBase   (default /tmp/pgpaimon-tpcc)
#   TPCC_WAREHOUSES     TPC-C scale factor, in warehouses                    (default 1)
#   TPCC_TIME           execute phase, seconds                               (default 60)
#   TPCC_RATE           execute phase ceiling, transactions per second       (default 25)
#   TPCC_TERMINALS      concurrent BenchBase terminals                       (default 1)
#   TPCC_BATCHSIZE      loader rows per batch, hence per transaction         (default 1024)
#   TPCC_PGPORT         port for this harness's own cluster                  (default 5443)
#   TPCC_SLOT           replication slot name                                (default paimon_tpcc)
#   BENCHBASE_HOME      an already-built BenchBase (dir holding benchbase.jar); built if unset
#   BENCHBASE_REF       BenchBase git tag/commit to build, unmodified        (default v2023)
#   JAVA21_HOME         a JDK >= 21, which BenchBase needs to build and run
#   PGBIN               PostgreSQL 18 binaries         (default /usr/lib/postgresql/18/bin)
#
# The writer and the comparison run on the repository's own JDK 17; only BenchBase needs 21.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

RUN="${TPCC_RUN_DIR:-/tmp/pgpaimon-tpcc}"      # deliberately outside the worktree
PGBIN="${PGBIN:-/usr/lib/postgresql/18/bin}"
PGPORT="${TPCC_PGPORT:-5443}"                  # 5441 is pg_regress, 5442 the acceptance harness
PGHOST=127.0.0.1
PGUSER=postgres
DB=benchbase
SLOT="${TPCC_SLOT:-paimon_tpcc}"
WAREHOUSE="$RUN/warehouse"
WRITER_JAR="$REPO/java/writer/target/paimon-writer.jar"
READER_JAR="$REPO/java/target/paimon-reader-1.0.jar"
BENCHBASE_REF="${BENCHBASE_REF:-v2023}"
WAREHOUSES="${TPCC_WAREHOUSES:-1}"
TIME_S="${TPCC_TIME:-60}"
# 25, not "as fast as it goes". The writer commits batches of whole transactions (up to
# PGPAIMON_BATCH_MAX_ROWS rows, _MAX_BYTES bytes or _MAX_INTERVAL_MS ms; PGPAIMON_BATCH_MAX_ROWS=1
# restores one Paimon snapshot per transaction), and both the drain and the changelog scan are
# linear in the snapshot count, so the transaction rate is what sets this harness's runtime. 60s at 25 tps
# is ~1500 transactions: enough for ~650 Payments, ~675 NewOrders and ~60 Deliveries, which is
# every update and delete path TPC-C has. Raise it if you want more; it costs time, not rigour.
RATE="${TPCC_RATE:-25}"
TERMINALS="${TPCC_TERMINALS:-1}"
BATCHSIZE="${TPCC_BATCHSIZE:-1024}"
READY_TIMEOUT="${READY_TIMEOUT:-60}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-1800}"         # with PGPAIMON_BATCH_MAX_ROWS=1 the writer commits once per transaction
COMPARE_HEAP="${COMPARE_HEAP:-3g}"
PIDFILE="$RUN/writer.pid"
WRITER_PID=""

export PATH="$PGBIN:$PATH" PGHOST PGPORT PGUSER

die() { echo "FAIL: $*" >&2; exit 1; }
say() { echo ">> $*"; }

tail_log() {
  echo "---- last ${2:-40} lines of $1 ----" >&2
  tail -n "${2:-40}" "$1" >&2 2>/dev/null || echo "(no such log)" >&2
}

psql_q() { psql -X -v ON_ERROR_STOP=1 -qAt -d "${DB_OVERRIDE:-$DB}" "$@"; }

# ── prerequisites ───────────────────────────────────────────────────────────────

find_java21() {
  if [ -n "${JAVA21_HOME:-}" ]; then echo "$JAVA21_HOME"; return; fi
  local c
  # setup-java exports JAVA_HOME_<major>_<arch>; a local box usually has it under /usr/lib/jvm.
  for c in "${JAVA_HOME_21_X64:-}" "${JAVA_HOME_21_ARM64:-}" \
           /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/temurin-21* /usr/lib/jvm/java-21-*; do
    [ -n "$c" ] && [ -x "$c/bin/java" ] && { echo "$c"; return; }
  done
  echo ""
}

require_prereqs() {
  [ -f "$WRITER_JAR" ] || die "missing $WRITER_JAR (run: cd java/writer && mvn -q package)"
  [ -f "$READER_JAR" ] || die "missing $READER_JAR (run: mvn -f java/pom.xml -q package -DskipTests)"
  [ -x "$PGBIN/pg_ctl" ] || die "missing $PGBIN/pg_ctl (PostgreSQL 18 server binaries)"
  command -v java >/dev/null || die "missing java on PATH"
  command -v mvn  >/dev/null || die "missing mvn on PATH"
  JAVA21="$(find_java21)"
  [ -n "$JAVA21" ] || die "no JDK 21+ found; BenchBase $BENCHBASE_REF targets Java 21. Set JAVA21_HOME."
  say "BenchBase JDK: $JAVA21"
  mkdir -p "$RUN"
  if (exec 3<>"/dev/tcp/$PGHOST/$PGPORT") 2>/dev/null; then
    pg_ctl -D "$RUN/pgdata" status >/dev/null 2>&1 \
      || die "port $PGPORT is in use by something that is not $RUN/pgdata"
  fi
}

# ── BenchBase ───────────────────────────────────────────────────────────────────

build_benchbase() {
  if [ -n "${BENCHBASE_HOME:-}" ]; then
    [ -f "$BENCHBASE_HOME/benchbase.jar" ] || die "no benchbase.jar in BENCHBASE_HOME=$BENCHBASE_HOME"
    say "using prebuilt BenchBase at $BENCHBASE_HOME"
    return
  fi
  BENCHBASE_HOME="$RUN/benchbase/benchbase-postgres"
  if [ -f "$BENCHBASE_HOME/benchbase.jar" ]; then
    say "reusing BenchBase at $BENCHBASE_HOME"
    return
  fi
  # cmu-db/benchbase publishes no release artifact, so there is nothing to download: build it.
  # Pinned to a tag, not a branch, so the schema and the loader cannot move under the harness.
  #
  # BUILT UNMODIFIED. Nothing is patched and the TPC-C schema is exactly as it ships: the one
  # table paimon_slot cannot replicate (`history`, which has no primary key) is left out of
  # replication rather than altered. See setup.sql.
  local src="$RUN/benchbase/src"
  if [ ! -d "$src/.git" ]; then
    say "cloning BenchBase $BENCHBASE_REF"
    rm -rf "$src"
    git clone --quiet --depth 1 --branch "$BENCHBASE_REF" \
      https://github.com/cmu-db/benchbase.git "$src"
  fi
  say "building BenchBase (postgres profile; about 6 minutes cold, 1 warm)"
  JAVA_HOME="$JAVA21" mvn -B -ntp -f "$src/pom.xml" clean package -P postgres -DskipTests \
    >"$RUN/benchbase-build.log" 2>&1 \
    || { tail_log "$RUN/benchbase-build.log"; die "BenchBase build failed"; }
  rm -rf "$BENCHBASE_HOME"
  mkdir -p "$RUN/benchbase"
  tar -xzf "$src/target/benchbase-postgres.tgz" -C "$RUN/benchbase"
  [ -f "$BENCHBASE_HOME/benchbase.jar" ] || die "benchbase.jar not in the built tarball"
}

write_config() {
  cat >"$RUN/tpcc.xml" <<XML
<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://$PGHOST:$PGPORT/$DB?sslmode=disable&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>$PGUSER</username>
    <password></password>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <!-- Rows per loader batch, and so rows per PostgreSQL transaction, and so rows per Paimon
         commit: the writer never splits a transaction, and commits several together. Larger batches
         keep the snapshot count (and the run time) sane without weakening anything. -->
    <batchsize>$BATCHSIZE</batchsize>
    <scalefactor>$WAREHOUSES</scalefactor>
    <terminals>$TERMINALS</terminals>
    <works>
        <work>
            <time>$TIME_S</time>
            <rate>$RATE</rate>
            <!-- The stock TPC-C mix: NewOrder, Payment, OrderStatus, Delivery, StockLevel.
                 Payment and NewOrder are 88% of it, so warehouse, district, customer and stock
                 are updated constantly; Delivery's 4% is what produces oorder/order_line
                 updates and new_order deletes. -->
            <weights>45,43,4,4,4</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
XML
}

benchbase() { # phase-name, benchbase args...
  local phase="$1"; shift
  say "benchbase $phase"
  ( cd "$BENCHBASE_HOME" && JAVA_HOME="$JAVA21" "$JAVA21/bin/java" -jar benchbase.jar \
      -b tpcc -c "$RUN/tpcc.xml" -d "$RUN/benchbase-results" "$@" ) \
    >"$RUN/benchbase-$phase.log" 2>&1 \
    || { tail_log "$RUN/benchbase-$phase.log" 60; die "benchbase $phase failed"; }
}

# ── cluster ─────────────────────────────────────────────────────────────────────

wait_for_cluster() {
  local i
  for ((i = 0; i < READY_TIMEOUT * 2; i++)); do
    pg_isready -q -h "$PGHOST" -p "$PGPORT" && return 0
    sleep 0.5
  done
  tail_log "$RUN/pg.log"
  die "PostgreSQL on $PGHOST:$PGPORT did not become ready within ${READY_TIMEOUT}s"
}

start_cluster() {
  if [ ! -d "$RUN/pgdata" ]; then
    say "initdb $RUN/pgdata"
    initdb -D "$RUN/pgdata" -U "$PGUSER" -A trust -E UTF8 >"$RUN/initdb.log" 2>&1 \
      || { tail_log "$RUN/initdb.log"; die "initdb failed"; }
    cat >>"$RUN/pgdata/postgresql.conf" <<CONF
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
port = $PGPORT
listen_addresses = '$PGHOST'
unix_socket_directories = ''
extension_control_path = '$REPO/sql/paimon_slot/_install:\$system'
# A TPC-C load retains a lot of WAL behind the slot while the writer catches up.
max_slot_wal_keep_size = 8GB
# The workload is not the thing under test; the replicated result is. Keep it cheap.
shared_buffers = 512MB
max_connections = 32
fsync = off
synchronous_commit = off
full_page_writes = off
CONF
  fi
  if ! pg_ctl -D "$RUN/pgdata" status >/dev/null 2>&1; then
    say "starting PostgreSQL on $PGHOST:$PGPORT"
    pg_ctl -D "$RUN/pgdata" -l "$RUN/pg.log" -w -t "$READY_TIMEOUT" start >"$RUN/pg_ctl.out" 2>&1 \
      || { cat "$RUN/pg_ctl.out" >&2; tail_log "$RUN/pg.log"; die "pg_ctl start failed"; }
  fi
  wait_for_cluster
}

reset_database() {
  make -s -C "$REPO/sql/paimon_slot" install-local || die "make install-local failed"
  # Fresh state. The slot pins the database, so it must go before the database can be dropped.
  DB_OVERRIDE=postgres psql_q -c \
    "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL" >/dev/null
  DB_OVERRIDE=postgres psql_q -c \
    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots" >/dev/null
  DB_OVERRIDE=postgres psql_q -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null
  DB_OVERRIDE=postgres psql_q -c "CREATE DATABASE $DB" >/dev/null
  rm -rf "$WAREHOUSE"; mkdir -p "$WAREHOUSE"
}

# ── writer ──────────────────────────────────────────────────────────────────────

start_writer() {
  say "starting writer (log: $RUN/writer.log)"
  : >"$RUN/writer.log"
  (
    export PGPAIMON_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$DB"
    export PGPAIMON_USER="$PGUSER" PGPAIMON_PASSWORD=""
    export PGPAIMON_SLOT="$SLOT"
    export PGPAIMON_WAREHOUSE="$WAREHOUSE"
    # _xid is optional (spec section 6) and off here: the comparison does not need it, and a
    # column fewer is a column fewer to canonicalise.
    nohup java -Xmx2g -jar "$WRITER_JAR" >>"$RUN/writer.log" 2>&1 </dev/null &
    echo $! >"$PIDFILE"
  )
  WRITER_PID="$(cat "$PIDFILE")"
  local i active
  for ((i = 0; i < READY_TIMEOUT * 2; i++)); do
    kill -0 "$WRITER_PID" 2>/dev/null \
      || { tail_log "$RUN/writer.log"; die "writer exited before the slot became active"; }
    active="$(psql_q -c "SELECT active FROM pg_replication_slots WHERE slot_name = '$SLOT'" || true)"
    [ "$active" = "t" ] && { say "slot '$SLOT' is active"; return 0; }
    sleep 0.5
  done
  tail_log "$RUN/writer.log"
  die "slot '$SLOT' did not become active within ${READY_TIMEOUT}s"
}

# Wait until the writer has confirmed everything the workload produced. The target is taken once,
# after the workload has stopped, so later WAL from autovacuum cannot move the finish line.
drain_writer() {
  local target
  target="$(psql_q -c "SELECT pg_current_wal_insert_lsn()")"
  say "waiting for the writer to confirm $target (timeout ${DRAIN_TIMEOUT}s)"
  local i lag behind
  for ((i = 0; i < DRAIN_TIMEOUT; i++)); do
    kill -0 "$WRITER_PID" 2>/dev/null \
      || { tail_log "$RUN/writer.log"; die "writer died while draining"; }
    behind="$(psql_q -c "SELECT pg_wal_lsn_diff('$target', confirmed_flush_lsn)::bigint
                           FROM pg_replication_slots WHERE slot_name = '$SLOT'")"
    [ -n "$behind" ] || die "slot '$SLOT' disappeared while draining"
    if [ "$behind" -le 0 ]; then
      say "writer is caught up"
      return 0
    fi
    if (( i % 15 == 0 )); then
      lag="$(psql_q -c "SELECT pg_size_pretty(pg_wal_lsn_diff('$target', confirmed_flush_lsn))
                          FROM pg_replication_slots WHERE slot_name = '$SLOT'")"
      say "  ${i}s: $lag of WAL still unconfirmed"
    fi
    sleep 1
  done
  tail_log "$RUN/writer.log"
  die "writer did not catch up within ${DRAIN_TIMEOUT}s"
}

stop_writer() {
  [ -f "$PIDFILE" ] && WRITER_PID="$(cat "$PIDFILE")"
  if [ -n "$WRITER_PID" ] && kill -0 "$WRITER_PID" 2>/dev/null; then
    say "stopping the writer (SIGTERM)"
    kill "$WRITER_PID"
    local i
    for ((i = 0; i < 120; i++)); do kill -0 "$WRITER_PID" 2>/dev/null || break; sleep 0.5; done
    if kill -0 "$WRITER_PID" 2>/dev/null; then
      echo "writer ignored SIGTERM; kill -9" >&2
      kill -9 "$WRITER_PID"
      return 1
    fi
  fi
  WRITER_PID=""
  return 0
}

# ── comparison ──────────────────────────────────────────────────────────────────

compare() {
  say "warehouse layout (one Paimon database per PostgreSQL schema)"
  ls -1 "$WAREHOUSE" | sed 's/^/     /'
  ls -1 "$WAREHOUSE"/public.db 2>/dev/null | sed 's/^/     public.db\//'
  say "comparing PostgreSQL against Paimon"
  # log4j.properties quiets hadoop's bundled INFO logger, which would otherwise bury the
  # comparison under one line per Parquet decompressor in the artifact a failure is read from.
  java -Xmx"$COMPARE_HEAP" "-Dlog4j.configuration=file:$HERE/log4j.properties" \
      -cp "$READER_JAR" pro.surpin.pgpaimon.TpccCompare \
      "$WAREHOUSE" "jdbc:postgresql://$PGHOST:$PGPORT/$DB?user=$PGUSER" public \
      2>&1 | tee "$RUN/compare.out"
  return "${PIPESTATUS[0]}"
}

# ── teardown ────────────────────────────────────────────────────────────────────

stop_all() {
  local rc=$?
  trap - EXIT
  set +e
  stop_writer
  # The slot is cluster-wide state: drop it and prove it is gone, so no later run inherits it.
  DB_OVERRIDE=postgres psql_q -c \
    "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots" >/dev/null 2>&1
  local left
  left="$(DB_OVERRIDE=postgres psql_q -c "SELECT count(*) FROM pg_replication_slots" 2>/dev/null)"
  echo ">> replication slots remaining on this cluster: ${left:-unknown}"
  [ -x "$PGBIN/pg_ctl" ] && pg_ctl -D "$RUN/pgdata" -m fast -w stop >/dev/null 2>&1
  echo ">> left in place for inspection: $RUN (writer.log, compare.out, warehouse). rm -rf $RUN to clean"
  exit "$rc"
}
trap stop_all EXIT

main() {
  require_prereqs
  build_benchbase
  write_config
  start_cluster
  reset_database

  # 1. BenchBase creates the stock schema, and loads nothing.
  benchbase create --create=true --load=false --execute=false

  # 2. Register every table while it is still empty, and before the slot exists. The Paimon
  #    tables come from the extension's own `create` messages; nothing here writes the warehouse.
  # Eight of the nine: `history` has no primary key in TPC-C, so paimon_slot refuses it by
  # design. setup.sql proves that refusal, and the comparison proves the table stays absent.
  say "registering the eight replicable TPC-C tables"
  psql_q -f "$HERE/setup.sql" || die "setup.sql failed"

  # 3. The writer creates the slot and starts streaming.
  start_writer

  # 4. The bulk load, through the slot.
  benchbase load --create=false --load=true --execute=false

  # 5. The update-heavy workload.
  benchbase execute --create=false --load=false --execute=true

  # 6. Let the writer finish, then stop it cleanly, so the comparison reads a settled warehouse.
  drain_writer
  stop_writer || die "the writer had to be killed; the warehouse may be mid-commit"

  # 7. The verdict.
  local rc=0
  compare || rc=$?
  if [ "$rc" -eq 0 ]; then
    echo ">> TPC-C VERIFICATION PASSED"
  else
    echo ">> TPC-C VERIFICATION FAILED (comparison exit $rc)" >&2
  fi
  exit "$rc"
}
main "$@"
