// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.io.IOException;
import java.util.logging.Logger;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import pro.surpin.pgpaimon.writer.dto.*;

/**
 * Where a cleanly stopped writer left its slot, so that a slot consumed by someone else while the
 * writer was down is caught instead of silently missing changes.
 *
 * <p>
 * A logical slot remembers only how far it has been confirmed. While the writer is not attached,
 * anything with the REPLICATION attribute can move that position on --
 * {@code pg_logical_slot_get_changes}, {@code pg_replication_slot_advance}, a second writer with
 * the same {@code PGPAIMON_SLOT}, {@code pg_recvlogical} -- and the changes in between are then
 * never applied. Comparing the slot with the warehouse cannot tell: the slot's position is
 * legitimately far ahead of the last Paimon commit whenever transactions touched no replicated
 * table. So on a clean stop the writer records the position it last sent the server (which is at or
 * above what the server holds), and on the next start a slot that is further on was moved by
 * someone else: refused, with the procedure.
 *
 * <p>
 * The mark is a small file in the warehouse, {@code _pgpaimon_stopped_<slot>}. A start checks it
 * with the other refusals, before anything is written, and deletes it only once it is about to
 * stream ({@link #clear}), so a later crash leaves none behind and a refused start leaves it for
 * the next one. With the slot absent it is ignored: a new slot starts afresh. After a crash there
 * is no mark and no check: that case stays undetected (docs, section 8).
 */
final class StopMark {

  private static final Logger LOG = Logger.getLogger(StopMark.class.getName());

  /** The mark's storage; a file in the warehouse in production. */
  interface Store {
    /** The recorded text, or null when there is none. */
    String read() throws IOException;

    void write(
        String text
    ) throws IOException;

    void delete() throws IOException;

    String where();
  }

  private StopMark() {}

  static Store inWarehouse(
      WriterConfig config
  ) throws IOException {
    Options o = Options.fromMap(config.catalogOptions());
    Path path = new Path(config.warehouse(), "_pgpaimon_stopped_" + config.slotName());
    FileIO io = FileIO.get(path, CatalogContext.create(o));
    return new Store() {
      @Override
      public String read() throws IOException {
        return io.exists(path) ? io.readFileUtf8(path) : null;
      }

      @Override
      public void write(
          String text
      ) throws IOException {
        io.overwriteFileUtf8(path, text);
      }

      @Override
      public void delete() throws IOException {
        io.delete(path, false);
      }

      @Override
      public String where() {
        return path.toString();
      }
    };
  }

  /** Called after a clean stop with the flushed position last sent to the server. */
  static void record(
      Store store,
      long flushedLsn
  ) throws IOException {
    if (flushedLsn == 0) {
      return; // never streamed, or stopped while reconnecting: nothing known
    }
    store.write(Long.toUnsignedString(flushedLsn));
    LOG.info(
        "clean stop at " + SlotConsumer.formatLsn(flushedLsn) + ", recorded in " + store.where());
  }

  /**
   * At startup, with the slot's {@code confirmed_flush_lsn} (0 when the slot is absent). Throws
   * when the slot has been moved past where the writer left it. Reads only.
   */
  static void check(
      Store store,
      long slotConfirmed,
      String slotName
  ) throws IOException {
    String text = store.read();
    if (text == null) {
      return;
    }
    if (slotConfirmed == 0) {
      return; // no slot: a new one starts afresh, the mark means nothing now
    }
    long left = Long.parseUnsignedLong(text.trim());
    if (Long.compareUnsigned(slotConfirmed, left) > 0) {
      throw new IllegalStateException(
          "SLOT MOVED WHILE THE WRITER WAS STOPPED: the writer" + " stopped cleanly with slot '"
              + slotName + "' at " + SlotConsumer.formatLsn(left) + ", but it is now confirmed to "
              + SlotConsumer.formatLsn(slotConfirmed) + ". Something else consumed or advanced"
              + " it (pg_logical_slot_get_changes, pg_replication_slot_advance, a second writer"
              + " with the same PGPAIMON_SLOT, pg_recvlogical), and the changes in between will"
              + " never reach Paimon. Re-bootstrap (docs/logical-slot-architecture.md, section 3,"
              + " 'Re-bootstrapping'; scripts/rebootstrap.sh); find who did it (only roles with"
              + " REPLICATION can). If the loss is known and accepted, delete " + store.where()
              + " and start again.");
    }
  }

  /** Just before the stream opens: the mark has been checked and is spent. */
  static void clear(
      Store store
  ) throws IOException {
    if (store.read() != null) {
      store.delete();
    }
  }
}
