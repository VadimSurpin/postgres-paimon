// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;

public record RelationMeta(int relid, String namespace, String relName,
                           List<String> columnNames, List<Boolean> isKey) {}
