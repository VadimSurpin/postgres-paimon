// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;

/** Every relid and OID below is a long; see the OID rule in package-info. */
public sealed interface PgEvent {
    record Begin(long finalLsn, long commitTimestamp, long xid)          implements PgEvent {}
    record Commit(long commitLsn, long endLsn, long commitTimestamp)     implements PgEvent {}
    record Relation(RelationMeta meta)                                   implements PgEvent {}
    record Insert(long relid, TupleData newTuple)                         implements PgEvent {}
    /** oldTuple is null when pgoutput sent none; oldIsKeyOnly is true for K, false for O. */
    record Update(long relid, TupleData oldTuple, boolean oldIsKeyOnly,
                  TupleData newTuple)                                    implements PgEvent {}
    record Delete(long relid, TupleData oldTuple, boolean oldIsKeyOnly)   implements PgEvent {}
    record Truncate(List<Long> relids, boolean cascade, boolean restartIdentity)
                                                                         implements PgEvent {}
    record Message(boolean transactional, long lsn, String prefix, byte[] content)
                                                                         implements PgEvent {}
    record Origin(long commitLsn, String name)                           implements PgEvent {}
    record TypeMsg(long typeOid, String namespace, String name)           implements PgEvent {}
}
