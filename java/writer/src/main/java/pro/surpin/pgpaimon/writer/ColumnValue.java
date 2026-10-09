// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

/**
 * One column as pgoutput delivered it.  NULL ('n'), "" ('t', length 0) and UNCHANGED ('u')
 * are different states. Callers must switch on {@code state()}, never test {@code text() == null}.
 */
public record ColumnValue(ColState state, String text) {

    /** Rejects a null state and any state/text combination other than TEXT with non-null text. */
    public ColumnValue {
        if (state == null) {
            throw new IllegalArgumentException("state must not be null");
        }
        if (state == ColState.TEXT && text == null) {
            throw new IllegalArgumentException("a TEXT value needs non-null text; use ofNull() for SQL NULL");
        }
        if (state != ColState.TEXT && text != null) {
            throw new IllegalArgumentException(state + " must not carry text");
        }
    }

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
