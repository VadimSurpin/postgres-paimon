// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;

public record TupleData(List<ColumnValue> columns) {}
