// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

/** One column as pgoutput delivered it.  NULL and "" are different states. */
public record ColumnValue(ColState state, String text) {

    private static final ColumnValue NULL_VALUE = new ColumnValue(ColState.NULL, null);
    private static final ColumnValue UNCHANGED  = new ColumnValue(ColState.UNCHANGED, null);

    public static ColumnValue ofNull()    { return NULL_VALUE; }
    public static ColumnValue unchanged() { return UNCHANGED; }

    public static ColumnValue of(String text) {
        if (text == null) {
            throw new IllegalArgumentException("use ofNull() for a SQL NULL");
        }
        return new ColumnValue(ColState.TEXT, text);
    }
}
