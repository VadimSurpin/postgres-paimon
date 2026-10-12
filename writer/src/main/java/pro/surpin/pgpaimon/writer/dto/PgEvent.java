// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Every relid and OID below is an unsigned 32-bit value held as long. */
public sealed

interface PgEvent {
  record Begin(
      long finalLsn,
      long commitTimestamp,
      long xid
  ) implements PgEvent {
  }

  record Commit(
      long commitLsn,
      long endLsn,
      long commitTimestamp
  ) implements PgEvent {
  }

  record Relation(
      @NotNull RelationMeta meta
  ) implements PgEvent {
  }

  record Insert(
      long relid,
      @NotNull TupleData newTuple
  ) implements PgEvent {
  }

  /** oldTuple is null when pgoutput sent none; oldIsKeyOnly is true for K, false for O. */
  record Update(
      long relid,
      @Nullable TupleData oldTuple,
      boolean oldIsKeyOnly,
      @NotNull TupleData newTuple
  ) implements PgEvent {
  }

  record Delete(
      long relid,
      @Nullable TupleData oldTuple,
      boolean oldIsKeyOnly
  ) implements PgEvent {
  }

  record Truncate(
      @NotNull List<@NotNull Long> relids,
      boolean cascade,
      boolean restartIdentity
  ) implements PgEvent {
  }

  record Message(
      boolean transactional,
      long lsn,
      @NotNull String prefix,
      @NotNull byte[] content
  ) implements PgEvent {
  }

  record Origin(
      long commitLsn,
      @NotNull String name
  ) implements PgEvent {
  }

  record TypeMsg(
      long typeOid,
      @NotNull String namespace,
      @NotNull String name
  ) implements PgEvent {
  }
}
