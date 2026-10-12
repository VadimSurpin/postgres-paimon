// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One column as pgoutput delivered it. NULL ('n'), "" ('t', length 0) and UNCHANGED ('u') are
 * different states. Callers must switch on {@code state()}, never test {@code text() == null}.
 */
public record ColumnValue(
    @NotNull ColState state,
    @Nullable String text
) {

  /** Rejects a null state and any state/text combination other than TEXT with non-null text. */
  public ColumnValue {
    if (state == ColState.TEXT && text == null) {
      throw new IllegalArgumentException(
          "a TEXT value needs non-null text; use ofNull() for SQL NULL");
    }
    if (state != ColState.TEXT && text != null) {
      throw new IllegalArgumentException(state + " must not carry text");
    }
  }

  private static final @NotNull ColumnValue NULL_VALUE = new ColumnValue(ColState.NULL, null);
  private static final @NotNull ColumnValue UNCHANGED = new ColumnValue(ColState.UNCHANGED, null);

  public static @NotNull ColumnValue ofNull() {
    return NULL_VALUE;
  }

  public static @NotNull ColumnValue unchanged() {
    return UNCHANGED;
  }

  public static @NotNull ColumnValue of(
      @NotNull String text
  ) {
    if (text == null) {
      throw new IllegalArgumentException("use ofNull() for a SQL NULL");
    }
    return new ColumnValue(ColState.TEXT, text);
  }
}
