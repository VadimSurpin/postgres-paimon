// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;

/** relid: a PostgreSQL OID; see the OID rule in package-info. */
public record RelationMeta(long relid, String namespace, String relName,
                           List<String> columnNames, List<Boolean> isKey) {}
