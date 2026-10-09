#!/usr/bin/env bash
# One command: bring up PostgreSQL + the Paimon slot writer locally (no containers), run the
# JDBC acceptance suites, tear down, and exit with the suites' status.
#   usage: run_acceptance.sh [SuiteName ...]     (no argument = every *IT suite)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
RUN="$HERE/run"                     # git-ignored runtime state
PGBIN=/usr/lib/postgresql/18/bin
PGPORT=5442                         # 5441 belongs to the pg_regress cluster: never touch it
PGHOST=127.0.0.1
PGUSER=postgres
DB=paimon_acc
WAREHOUSE="$RUN/warehouse"
JAR="$REPO/java/writer/target/paimon-writer.jar"
SLOT=paimon
READY_TIMEOUT=${READY_TIMEOUT:-60}  # seconds, for both the cluster and the writer gates
WRITER_PID=""
export PATH="$PGBIN:$PATH" PGHOST PGPORT PGUSER

die() { echo "FAIL: $*" >&2; exit 1; }

tail_log() { # file, lines
  echo "---- last ${2:-40} lines of $1 ----" >&2
  tail -n "${2:-40}" "$1" >&2 2>/dev/null || echo "(no such log)" >&2
}

psql_q() { psql -X -v ON_ERROR_STOP=1 -qAt -d "${DB_OVERRIDE:-$DB}" "$@"; }

require_prereqs() {
  [ -f "$JAR" ] || die "missing: java/writer/target/paimon-writer.jar (run: cd java/writer && mvn -q package)"
  [ -x "$PGBIN/pg_ctl" ] || die "missing: $PGBIN/pg_ctl (PostgreSQL 18 server binaries)"
  [ -f "$HERE/setup.sql" ] || die "missing: test/slot-acceptance/setup.sql"
  command -v java >/dev/null || die "missing: java on PATH"
  command -v mvn  >/dev/null || die "missing: mvn on PATH"
  mkdir -p "$RUN"
  # A port that already answers is not ours, unless it is our own leftover cluster.
  if (exec 3<>"/dev/tcp/$PGHOST/$PGPORT") 2>/dev/null; then
    if ! pg_ctl -D "$RUN/pgdata" status >/dev/null 2>&1; then
      die "port $PGPORT is already in use by something that is not $RUN/pgdata"
    fi
  fi
}

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
    echo ">> initdb $RUN/pgdata"
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
CONF
  fi
  if ! pg_ctl -D "$RUN/pgdata" status >/dev/null 2>&1; then
    echo ">> starting PostgreSQL on $PGHOST:$PGPORT"
    pg_ctl -D "$RUN/pgdata" -l "$RUN/pg.log" -w -t "$READY_TIMEOUT" start >"$RUN/pg_ctl.out" 2>&1 \
      || { cat "$RUN/pg_ctl.out" >&2; tail_log "$RUN/pg.log"; die "pg_ctl start failed"; }
  fi
  wait_for_cluster
}

load_extension() {
  make -s -C "$REPO/sql/paimon_slot" install-local || die "make install-local failed"
  # Fresh state every run. The slot is cluster-wide and pins the database, so drop it first;
  # add_table must precede slot creation, so a stale slot would make this run's setup unusable.
  DB_OVERRIDE=postgres psql_q -c "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL" >/dev/null
  DB_OVERRIDE=postgres psql_q -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots" >/dev/null
  DB_OVERRIDE=postgres psql_q -c "DROP DATABASE IF EXISTS $DB WITH (FORCE)" >/dev/null
  DB_OVERRIDE=postgres psql_q -c "CREATE DATABASE $DB" >/dev/null
  rm -rf "$WAREHOUSE"; mkdir -p "$WAREHOUSE"
  echo ">> setup.sql"
  psql_q -f "$HERE/setup.sql" >/dev/null || die "setup.sql failed"
}

start_writer() {
  echo ">> starting writer (log: $RUN/writer.log)"
  # flush interval 500ms (default 1000): halves the smoke test's wait without making tiny commits
  # PGPAIMON_COMMIT_USER is left to its default (paimon-writer-<slot>): it must be stable.
  PGPAIMON_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$DB" \
  PGPAIMON_USER="$PGUSER" PGPAIMON_PASSWORD="" \
  PGPAIMON_SLOT="$SLOT" PGPAIMON_WAREHOUSE="$WAREHOUSE" \
  PGPAIMON_FLUSH_MAX_INTERVAL_MS=500 \
    nohup java -jar "$JAR" >"$RUN/writer.log" 2>&1 &
  WRITER_PID=$!
  echo "$WRITER_PID" >"$RUN/writer.pid"
  local i active
  for ((i = 0; i < READY_TIMEOUT * 2; i++)); do
    if ! kill -0 "$WRITER_PID" 2>/dev/null; then
      tail_log "$RUN/writer.log"; die "writer exited before the slot became active"
    fi
    active="$(psql_q -c "SELECT active FROM pg_replication_slots WHERE slot_name = '$SLOT'" || true)"
    [ "$active" = "t" ] && { echo ">> slot '$SLOT' is active"; return 0; }
    sleep 0.5
  done
  tail_log "$RUN/writer.log"
  die "slot '$SLOT' did not become active within ${READY_TIMEOUT}s (writer still running, pid $WRITER_PID)"
}

run_suites() {
  local args=(-f "$REPO/java/pom.xml" test)
  if [ "$#" -gt 0 ]; then
    # The smoke suite always runs: it proves a JDBC suite can see replicated changes.
    local IFS=,; args+=("-Dtest=SlotHarnessSmokeIT,$*")
  fi
  echo ">> mvn ${args[*]}"
  # The writer module's live IT (BackfillerTest) creates and then DROPS the slot named by
  # PGPAIMON_SLOT, default "paimon" - the live writer's slot. Give suites a different name so
  # it can never kill the running writer. Do not "simplify" this back to one name.
  local rc=0
  E2E_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$DB?user=$PGUSER" \
  PGPAIMON_IT_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$DB?user=$PGUSER" \
  PGPAIMON_IT_WAREHOUSE="$WAREHOUSE" \
  PGPAIMON_HARNESS=1 \
  PGPAIMON_SLOT="${SLOT}_it" \
    mvn -B -ntp "${args[@]}" || rc=$?
  return "$rc"
}

stop_all() {
  local rc=$?
  trap - EXIT
  set +e
  if [ -n "$WRITER_PID" ] && kill -0 "$WRITER_PID" 2>/dev/null; then
    kill "$WRITER_PID"
    for _ in $(seq 1 20); do kill -0 "$WRITER_PID" 2>/dev/null || break; sleep 0.5; done
    kill -0 "$WRITER_PID" 2>/dev/null && { echo "writer ignored SIGTERM; kill -9" >&2; kill -9 "$WRITER_PID"; }
  fi
  [ -x "$PGBIN/pg_ctl" ] && pg_ctl -D "$RUN/pgdata" -m fast -w stop >/dev/null 2>&1
  echo ">> stopped. Left in place for inspection: $RUN (logs, warehouse). Remove with: rm -rf $RUN"
  exit "$rc"
}
trap stop_all EXIT

main() {
  require_prereqs
  start_cluster
  load_extension
  start_writer
  local rc=0
  run_suites "$@" || rc=$?
  [ "$rc" -eq 0 ] && echo ">> ALL SUITES PASSED" || echo ">> SUITES FAILED (mvn exit $rc)" >&2
  exit "$rc"
}
main "$@"
