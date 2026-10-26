// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import pro.surpin.pgpaimon.writer.dto.PgEvent;
import pro.surpin.pgpaimon.writer.dto.RelationMeta;
import pro.surpin.pgpaimon.writer.dto.SinkRecord;

/** Collects transaction events and flushes their records when a commit arrives. */
public interface TxnBatcher {

  /**
   * Builds the Paimon row for one record, purely to find out whether its values can be represented.
   * Only ever called for the single configured skip LSN, so it adds nothing to the normal path --
   * one {@code long} comparison per event.
   */
  public interface RowCheck {
    void check(
        @NotNull SinkRecord record,
        @NotNull RelationMeta meta
    );
  }

  public interface Flusher {
    /**
     * @param commitLsn becomes Paimon's commitIdentifier
     * @param endLsn the position confirmed to the server (after the commit record)
     */
    void flush(
        @NotNull List<SinkRecord> records,
        long commitLsn,
        long endLsn
    ) throws Exception;

    /**
     * As {@link #flush(List, long, long)}, and runs {@code afterCommit} once these records have
     * actually been committed to Paimon -- which, for a flusher that buffers (see
     * {@link MiniBatcher}), is not when this method returns. Never run if the commit fails or the
     * transaction is discarded unconfirmed. The default suits a flusher that commits before
     * returning.
     */
    default void flush(
        List<SinkRecord> records,
        long commitLsn,
        long endLsn,
        Runnable afterCommit
    ) throws Exception {
      flush(records, commitLsn, endLsn);
      afterCommit.run();
    }
  }

  TxnBatcher withMaxBytes(
      long maxBytes
  );

  final class TransactionTooLargeException extends IllegalStateException {
    TransactionTooLargeException(
        String message
    ) {
      super(message);
    }
  }

  boolean hasOpenTransaction();

  void discardOpenTransaction();

  /**
   * @param changeLsn the LSN of this specific message; becomes {@code _seq} of every record the
   *        event yields. The caller supplies it; it is never the commit LSN.
   * @param meta the relation of a row event; may be null for other events
   */
  void onEvent(
      @NotNull PgEvent event,
      long changeLsn,
      @Nullable RelationMeta meta
  ) throws Exception;
}
