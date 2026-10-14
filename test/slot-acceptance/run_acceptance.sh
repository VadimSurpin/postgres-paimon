#!/usr/bin/env bash
# One command: bring up PostgreSQL + the Paimon slot writer locally (no containers), run the
# JDBC acceptance suites, tear down, and exit with the suites' status.
#   usage: run_acceptance.sh [SuiteName ...]     (no argument = every suite in ALL_SUITES)
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
NOT_RUN=""                          # checks a suite will skip: reported next to the verdict

# Every acceptance suite, named explicitly, because a bare invocation runs exactly this list.
#
# It used to pass no -Dtest at all when given no arguments, leaving the selection to Surefire's
# default includes -- which match *Test and *IT by pattern but, in this project's configuration,
# selected only the two *IT suites: 24 JUnit tests and 24 of the 186 checks, reported as
# "ALL SUITES PASSED". The most likely way anyone runs this (bare, from CI) was the way that
# covered an eighth of the suite and said nothing. Naming the set here makes it visible in the
# script, and the per-suite report check below then proves each one actually ran.
#
# Keep in step with java/src/test/java/pro/surpin/pgpaimon/: a new suite missing from this list is
# a visible omission here, not a silent gap in the run. (The old PaimonE2ETest was removed with the
# C++ implementation it exercised; naming a suite that does not exist fails the run, by design.)
ALL_SUITES=(SlotHarnessSmokeIT PaimonSchemaIT PaimonFeatureTest PaimonDdlTest
            PaimonAdvancedTest PaimonTypeTest PaimonSlotKeyMoveTest PaimonOffloadTest
            PaimonAdversarialTest PaimonHistoryTest TpccCompareSelfTest)
LAUNCHER="$RUN/start_writer.sh"
PIDFILE="$RUN/writer.pid"
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
  # The launch is a generated script so a suite can restart the writer with the identical
  # environment (PaimonAdvancedTest T16 kills it mid-load): it appends to the log and rewrites
  # writer.pid. _xid is optional and off by default (spec section 6); PaimonFeatureTest T4
  # asserts it is in the schema. PGPAIMON_COMMIT_USER is left to its default
  # (paimon-writer-<slot>): it must be stable across restarts.
  cat >"$LAUNCHER" <<LAUNCH
#!/usr/bin/env bash
export PGPAIMON_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$DB"
export PGPAIMON_USER="$PGUSER" PGPAIMON_PASSWORD=""
export PGPAIMON_SLOT="$SLOT"
# PGPAIMON_WAREHOUSE_OVERRIDE (and PGPAIMON_S3_* , inherited) let a suite relaunch the writer against
# s3://... or hdfs://... : see README "Remote warehouses". Unset = the local warehouse.
export PGPAIMON_WAREHOUSE="\${PGPAIMON_WAREHOUSE_OVERRIDE:-$WAREHOUSE}"
export PGPAIMON_XID_FIELD=_xid
nohup java -jar "$JAR" >>"$RUN/writer.log" 2>&1 </dev/null &
echo \$! >"$PIDFILE"
LAUNCH
  : >"$RUN/writer.log"
  bash "$LAUNCHER"
  WRITER_PID="$(cat "$PIDFILE")"
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
  # No suite named: run every one of them, explicitly. Never fall back to Surefire's default
  # includes -- see ALL_SUITES.
  if [ "$#" -eq 0 ]; then
    set -- "${ALL_SUITES[@]}"
    echo ">> no suite named: running all $# ($*)" >&2
  fi
  # The smoke suite always runs: it proves a JDBC suite can see replicated changes.
  case " $* " in *" SlotHarnessSmokeIT "*) ;; *) set -- SlotHarnessSmokeIT "$@" ;; esac

  local args=(-f "$REPO/java/pom.xml" test)
  # PaimonOffloadTest T18 needs a MinIO binary; say so up front rather than only in the test log.
  local minio="${E2E_MINIO_BIN:-/root/cloud-db/opt/minio3}"
  if [[ " $* " == *" PaimonOffloadTest "* ]] && [ ! -x "$minio" ]; then
    NOT_RUN="T18 (S3) NOT RUN: no executable MinIO at '$minio'; enable with E2E_MINIO_BIN=/path/to/minio"
    echo ">> NOTE: $NOT_RUN" >&2
  fi
  # Built with printf, not by setting IFS: `local IFS=,` lasts to the end of the function and
  # would also mangle "${args[*]}" in the echo below.
  local dtest
  dtest="$(printf '%s,' "$@")"
  args+=("-Dtest=${dtest%,}")
  echo ">> mvn ${args[*]}"
  # Surefire exits 0 when a -Dtest name matches nothing as long as another name matches (the smoke
  # test always does), and -DfailIfNoSpecifiedTests does not change that. So a mistyped suite, or a
  # class with no @Test, would print "ALL SUITES PASSED" having run nothing. Prove each named suite
  # ran: delete old reports first, then require a report with at least one test per name.
  local reports="$REPO/java/target/surefire-reports"
  rm -f "$reports"/TEST-*.xml
  # The writer module's live IT (BackfillerTest) creates and then DROPS the slot named by
  # PGPAIMON_SLOT, default "paimon" - the live writer's slot. Give suites a different name so
  # it can never kill the running writer. Do not "simplify" this back to one name.
  local rc=0
  E2E_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$DB?user=$PGUSER" \
  PGPAIMON_IT_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$DB?user=$PGUSER" \
  PGPAIMON_IT_WAREHOUSE="$WAREHOUSE" \
  PGPAIMON_HARNESS=1 \
  PGPAIMON_WRITER_LAUNCHER="$LAUNCHER" \
  PGPAIMON_WRITER_PID_FILE="$PIDFILE" \
  PGPAIMON_SLOT="${SLOT}_it" \
    mvn -B -ntp "${args[@]}" || rc=$?
  local name rpt n
  for name in "$@"; do
    rpt="$(ls "$reports"/TEST-*."$name".xml 2>/dev/null | head -n1 || true)"
    n=0
    [ -n "$rpt" ] && n="$(sed -n 's/.*<testsuite [^>]*tests="\([0-9]*\)".*/\1/p' "$rpt" | head -n1)"
    # A suite whose every test was skipped ran nothing either.
    local sk=0
    [ -n "$rpt" ] && sk="$(sed -n 's/.*<testsuite [^>]*skipped="\([0-9]*\)".*/\1/p' "$rpt" | head -n1)"
    if [ "${n:-0}" -ge 1 ] && [ "${sk:-0}" -ge "${n:-0}" ]; then
      echo "FAIL: suite '$name': all ${n} tests were skipped, nothing ran" >&2
      rc=1
    fi
    if [ "${n:-0}" -lt 1 ]; then
      echo "FAIL: suite '$name' matched no tests (no surefire report with tests > 0): mistyped name, or a class with no @Test" >&2
      rc=1
    fi
  done
  return "$rc"
}

# The writer module's live ITs (BackfillerTest's initial-load tests), which `mvn test` skips
# without PGPAIMON_IT_JDBC_URL: they are the only check of the partitioned-history truncate a
# reload depends on (review of the history tables, m7), and no other layer ran them. In a database
# of their own, because they register tables with rows (which the running writer's database must
# not get) and create and drop a slot of their own (paimon_it_wr; slot names are cluster-wide).
run_writer_its() {
  local itdb=paimon_it_writer
  DB_OVERRIDE=postgres psql_q -c "DROP DATABASE IF EXISTS $itdb WITH (FORCE)" >/dev/null
  DB_OVERRIDE=postgres psql_q -c "CREATE DATABASE $itdb" >/dev/null
  DB_OVERRIDE=$itdb psql_q -c "CREATE EXTENSION paimon_slot" >/dev/null
  rm -rf "$RUN/it-warehouse"; mkdir -p "$RUN/it-warehouse"
  local reports="$REPO/java/writer/target/surefire-reports" rc=0
  rm -f "$reports"/TEST-*BackfillerTest.xml
  echo ">> writer live ITs (BackfillerTest) against database $itdb"
  PGPAIMON_IT_JDBC_URL="jdbc:postgresql://$PGHOST:$PGPORT/$itdb?user=$PGUSER" \
  PGPAIMON_IT_WAREHOUSE="$RUN/it-warehouse" \
  PGPAIMON_IT_SLOT=paimon_it_wr \
    mvn -B -ntp -q -f "$REPO/java/writer/pom.xml" test -Dtest=BackfillerTest || rc=$?
  local rpt sk n
  rpt="$(ls "$reports"/TEST-*BackfillerTest.xml 2>/dev/null | head -n1 || true)"
  n=0; sk=0
  [ -n "$rpt" ] && n="$(sed -n 's/.*<testsuite [^>]*tests="\([0-9]*\)".*/\1/p' "$rpt" | head -n1)"
  [ -n "$rpt" ] && sk="$(sed -n 's/.*<testsuite [^>]*skipped="\([0-9]*\)".*/\1/p' "$rpt" | head -n1)"
  if [ "${n:-0}" -lt 1 ] || [ "${sk:-0}" -ne 0 ]; then
    echo "FAIL: writer live ITs: ${n:-0} tests, ${sk:-0} skipped (every one must run)" >&2
    rc=1
  else
    echo ">> writer live ITs: $n tests, none skipped"
  fi
  return "$rc"
}

stop_all() {
  local rc=$?
  trap - EXIT
  set +e
  # A suite may have restarted the writer: the pid file, not the launch-time pid, is current.
  [ -f "$PIDFILE" ] && WRITER_PID="$(cat "$PIDFILE")"
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
  if [ "$#" -eq 0 ]; then
    run_writer_its || rc=$?
  fi
  [ "$rc" -eq 0 ] && echo ">> ALL SUITES PASSED${NOT_RUN:+ -- except: $NOT_RUN}" || echo ">> SUITES FAILED (mvn exit $rc)" >&2
  exit "$rc"
}
main "$@"
