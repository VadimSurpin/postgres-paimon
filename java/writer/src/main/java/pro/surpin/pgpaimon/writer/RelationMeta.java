// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;

/**
 * relid is an unsigned 32-bit PostgreSQL OID (0 .. 4294967295) widened to long.
 * Normalise both routes the same way: pgoutput binary Int32 via
 * Integer.toUnsignedLong(buffer.getInt()); DDL-message JSON string via
 * Long.parseLong(node.get("relid").asText()).
 */
public record RelationMeta(long relid, String namespace, String relName,
                           List<String> columnNames, List<Boolean> isKey) {}
