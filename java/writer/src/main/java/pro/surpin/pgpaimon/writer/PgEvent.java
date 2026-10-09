// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;

public sealed interface PgEvent {
    record Begin(long finalLsn, long commitTimestamp, long xid)          implements PgEvent {}
    record Commit(long commitLsn, long endLsn, long commitTimestamp)     implements PgEvent {}
    record Relation(RelationMeta meta)                                   implements PgEvent {}
    record Insert(int relid, TupleData newTuple)                         implements PgEvent {}
    /** oldTuple is null when pgoutput sent none; oldIsKeyOnly is true for K, false for O. */
    record Update(int relid, TupleData oldTuple, boolean oldIsKeyOnly,
                  TupleData newTuple)                                    implements PgEvent {}
    record Delete(int relid, TupleData oldTuple, boolean oldIsKeyOnly)   implements PgEvent {}
    record Truncate(List<Integer> relids, boolean cascade, boolean restartIdentity)
                                                                         implements PgEvent {}
    record Message(boolean transactional, long lsn, String prefix, byte[] content)
                                                                         implements PgEvent {}
    record Origin(long commitLsn, String name)                           implements PgEvent {}
    record TypeMsg(int typeOid, String namespace, String name)           implements PgEvent {}
}
