// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import pro.surpin.pgpaimon.writer.dto.*;

/**
 * A slot moved on while the writer was cleanly stopped is refused at the next start; one left where
 * the writer left it (or behind it) is not, and the mark is spent once the stream opens.
 */
class StopMarkTest {

  @TempDir
  Path warehouse;

  StopMark.Store store() throws Exception {
    Map<String, String> env = new HashMap<>();
    env.put("PGPAIMON_JDBC_URL", "jdbc:postgresql://localhost:5432/testdb");
    env.put("PGPAIMON_WAREHOUSE", warehouse.toString());
    return StopMark.inWarehouse(WriterConfigUtils.fromEnv(env));
  }

  @Test
  void aSlotMovedOnWhileStoppedIsRefusedUntilTheMarkIsGone() throws Exception {
    StopMark.Store s = store();
    StopMark.record(s, 0x5000);
    assertTrue(s.where().endsWith("_pgpaimon_stopped_paimon"), s.where());
    IllegalStateException e =
        assertThrows(IllegalStateException.class, () -> StopMark.check(s, 0x6000, "paimon"));
    assertTrue(e.getMessage().contains("SLOT MOVED WHILE THE WRITER WAS STOPPED"), e.getMessage());
    assertTrue(e.getMessage().contains("0/00005000") && e.getMessage().contains("0/00006000"),
        e.getMessage());
    assertTrue(e.getMessage().contains("scripts/rebootstrap.sh"), e.getMessage());
    // a refused start leaves the mark: the next start refuses too
    assertThrows(IllegalStateException.class, () -> StopMark.check(store(), 0x6000, "paimon"));
  }

  @Test
  void aSlotWhereTheWriterLeftItOrBehindIsFineAndTheMarkIsSpent() throws Exception {
    StopMark.Store s = store();
    StopMark.record(s, 0x5000);
    StopMark.check(s, 0x5000, "paimon");
    StopMark.check(s, 0x4000, "paimon"); // a server that lost its last status update
    StopMark.check(s, 0, "paimon"); // no slot: ignored
    StopMark.clear(s);
    assertNull(s.read());
    StopMark.check(s, 0x9000, "paimon"); // no mark (after a crash): nothing to compare
  }

  @Test
  void nothingIsRecordedWithoutACleanStop() throws Exception {
    StopMark.Store s = store();
    StopMark.record(s, 0);
    assertNull(s.read());
  }
}
