// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.*;
import org.junit.jupiter.api.Test;
import pro.surpin.pgpaimon.writer.dto.*;

/** The crash-test hook is inert unless named, and orders commits so it lands between the two. */
class CrashHookTest {

  static Map<Long, String> batch() {
    Map<Long, String> m = new LinkedHashMap<>();
    m.put(10L, "t");
    m.put(HistoryKeys.histKey(10L), "t_hist");
    m.put(11L, "u");
    m.put(HistoryKeys.histKey(11L), "u_hist");
    return m;
  }

  @Test
  void offByDefault() {
    assertSame(CrashHook.OFF, CrashHook.fromEnv(Map.of()));
    assertSame(CrashHook.OFF, CrashHook.fromEnv(Map.of(CrashHook.ENV, " ")));
    assertEquals(List.of("t", "t_hist", "u", "u_hist"),
        new ArrayList<>(CrashHook.OFF.order(batch()).values()));
  }

  @Test
  void namingAMainTableKeepsMainFirstAndNamingAHistoryTablePutsHistoryFirst() {
    assertEquals(List.of("t", "t_hist", "u", "u_hist"), new ArrayList<>(
        CrashHook.fromEnv(Map.of(CrashHook.ENV, "public.t")).order(batch()).values()));
    assertEquals(List.of("t_hist", "u_hist", "t", "u"), new ArrayList<>(
        CrashHook.fromEnv(Map.of(CrashHook.ENV, "public.t_hist")).order(batch()).values()));
  }

  @Test
  void historyKeysAreTheirOwnInverseAndNeverAnOid() {
    for (long oid : new long[] {0L, 1L, 16412L, 4294967295L}) {
      long k = HistoryKeys.histKey(oid);
      assertTrue(HistoryKeys.isHist(k));
      assertFalse(HistoryKeys.isHist(oid));
      assertEquals(oid, HistoryKeys.relidOf(k));
      assertEquals(oid, HistoryKeys.relidOf(oid));
    }
    assertThrows(IllegalArgumentException.class, () -> HistoryKeys.histKey(-1));
  }
}
