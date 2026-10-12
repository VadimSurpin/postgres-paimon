// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import pro.surpin.pgpaimon.writer.dto.*;

/** One changelog record as stored: its kind ("+I", "-U", "+U", "-D") and its {@code _seq}. */
record RawRecord(
    String rowKind,
    long seq
) {
}
