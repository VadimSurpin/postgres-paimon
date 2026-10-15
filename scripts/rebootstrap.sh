#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Re-bootstrap the paimon_slot replication: the one recovery for an invalidated or lost slot, a
# cluster or timeline mismatch, and a table changed while its guardrails were off.
# docs/logical-slot-architecture.md, section 3 "Re-bootstrapping", and section 8.
#
#   usage: PGPAIMON_SLOT=paimon scripts/rebootstrap.sh [psql connection options, e.g. -d mydb]
#
# What it does, in the replicated database:
#   1. refuses while a writer is attached to the slot (stop the writer first);
#   2. drops the slot, if there is one;
#   3. marks every registered table for a reload: UPDATE paimon.table_registry SET backfill_due.
# Then start the writer: it creates a new slot and snapshot, empties and reloads every main
# table, and streams on.
#
# What is lost: the changes made between the slot's loss and the reload never reach the
# history tables (<table>_hist) or the changelog; the reload copies each table's state at the
# new snapshot (one R row per row in the history). What is kept: every history table, its
# archives, and everything they held. The main tables are rebuilt from PostgreSQL.
set -euo pipefail
SLOT="${PGPAIMON_SLOT:-paimon}"
case "$SLOT" in *[!a-z0-9_]*|'') echo "slot name '$SLOT' must be lower-case letters, digits, _" >&2; exit 2;; esac
q() { psql -X -v ON_ERROR_STOP=1 -qAt "$@"; }

active="$(q "$@" -c "SELECT active_pid FROM pg_replication_slots WHERE slot_name = '$SLOT' AND active")"
if [ -n "$active" ]; then
  echo "refusing: a writer (pid $active on the server) is attached to slot '$SLOT'; stop it first" >&2
  exit 1
fi
if [ "$(q "$@" -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '$SLOT'")" = 1 ]; then
  q "$@" -c "SELECT pg_drop_replication_slot('$SLOT')" >/dev/null
  echo "dropped replication slot '$SLOT'"
else
  echo "replication slot '$SLOT' does not exist (already gone)"
fi
n="$(q "$@" -c "WITH u AS (UPDATE paimon.table_registry SET backfill_due = true RETURNING 1) SELECT count(*) FROM u")"
echo "marked $n registered table(s) for a reload; now start the writer"
