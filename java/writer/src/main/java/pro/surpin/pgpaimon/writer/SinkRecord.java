// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.apache.paimon.types.RowKind;

public record SinkRecord(int relid, RowKind kind, List<String> values,
                         long seq, int opSeq, long xid) {}
