// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import pro.surpin.pgpaimon.writer.dto.*;

/**
 * PGPAIMON_TXN_MAX_BYTES: a transaction larger than the writer may hold stops it with a diagnosis,
 * flushes nothing of it, and leaves nothing open; one under the limit is untouched.
 */
class TxnSizeLimitTest {

  static final RelationMeta META = TxnBatcherTest.META;

  private static TupleData row(
      String id,
      String v
  ) {
    return new TupleData(List.of(ColumnValue.of(id), ColumnValue.of(v)));
  }

  private static void txn(
      TxnBatcher b,
      int rows,
      long base
  ) throws Exception {
    b.onEvent(new PgEvent.Begin(base + 1000, 0L, 77L), base, null);
    for (int i = 0; i < rows; i++) {
      b.onEvent(new PgEvent.Insert(16412, row(String.valueOf(i), "x".repeat(100))), base + 1 + i,
          META);
    }
    b.onEvent(new PgEvent.Commit(base + 1000, base + 1001, 0L), base + 1000, null);
  }

  @Test
  void aTransactionOverTheLimitStopsWithTheRemedyAndFlushesNothing() throws Exception {
    TxnBatcherTest.Recorder rec = new TxnBatcherTest.Recorder();
    // 100 chars + 1-2 digit id + 48 overhead per record: about 150 bytes each.
    TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec).withMaxBytes(1_500);
    TxnBatcher.TransactionTooLargeException e =
        assertThrows(TxnBatcher.TransactionTooLargeException.class, () -> txn(b, 50, 10_000));
    assertTrue(e.getMessage().contains("TRANSACTION TOO LARGE"), e.getMessage());
    assertTrue(e.getMessage().contains("transaction 77"), e.getMessage());
    assertTrue(e.getMessage().contains("PGPAIMON_TXN_MAX_BYTES=1500"), e.getMessage());
    assertTrue(e.getMessage().contains("Nothing is lost"), e.getMessage());
    assertTrue(rec.batches.isEmpty(), "nothing of the transaction was flushed");
    assertFalse(b.hasOpenTransaction(), "the transaction is discarded, to replay whole");
  }

  @Test
  void aTransactionUnderTheLimitIsFlushedWhole() throws Exception {
    TxnBatcherTest.Recorder rec = new TxnBatcherTest.Recorder();
    TxnBatcher b = new TxnBatcher(new ToastResolver(), null, rec).withMaxBytes(1_500);
    txn(b, 9, 10_000);
    assertEquals(1, rec.batches.size());
    assertEquals(9, rec.batches.get(0).size());
    // The count restarts per transaction: the next one is checked on its own.
    txn(b, 9, 20_000);
    assertEquals(2, rec.batches.size());
  }

  @Test
  void theLimitIsConfigurableAndDefaultsToAQuarterOfTheHeap() {
    Map<String, String> env = new java.util.HashMap<>(
        Map.of("PGPAIMON_JDBC_URL", "jdbc:postgresql://h/db", "PGPAIMON_WAREHOUSE", "/tmp/wh"));
    assertEquals(Runtime.getRuntime().maxMemory() / 4, WriterConfig.fromEnv(env).txnMaxBytes());
    env.put("PGPAIMON_TXN_MAX_BYTES", "12345");
    assertEquals(12345, WriterConfig.fromEnv(env).txnMaxBytes());
    env.put("PGPAIMON_TXN_MAX_BYTES", "0");
    assertThrows(IllegalArgumentException.class, () -> WriterConfig.fromEnv(env));
  }
}
