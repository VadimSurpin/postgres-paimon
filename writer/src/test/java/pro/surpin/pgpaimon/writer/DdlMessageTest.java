// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.Test;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.DdlMessage;



class DdlMessageTest {

  private static DdlMessage parse(
      String json
  ) {
    return DdlMessage.parse(json.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void createFixtureParsesByKeyName() {
    DdlMessage d = parse("""
        {"v":1,"op":"create","pk":["id"],
         "cols":[{"id":1,"name":"id","type":"INT NOT NULL"},
                 {"id":3,"name":"price","type":"DECIMAL(10, 2)"}],
         "relid":"19228","table":"cap_t","schema":"public",
         "options":{"changelog-producer":"input"}}""");
    assertEquals(1, d.version());
    assertEquals("create", d.op());
    assertEquals(19228L, d.relid());
    assertEquals("public", d.schema());
    assertEquals("cap_t", d.table());
    assertEquals(List.of("id"), d.pk());
    assertEquals(new DdlMessage.Col(3, "price", "DECIMAL(10, 2)"), d.cols().get(1));
    assertEquals("INT NOT NULL", d.cols().get(0).type());
    assertEquals("input", d.changelogProducer());
    assertNull(d.col());
    assertNull(d.id());
  }

  @Test
  void shapesDifferByOp() {
    DdlMessage r = parse("{\"v\":1,\"id\":2,\"op\":\"rename_col\",\"to\":\"quantity\","
        + "\"from\":\"qty\",\"relid\":\"19228\"}");
    assertEquals(2, r.id());
    assertEquals("qty", r.from());
    assertEquals("quantity", r.to());
    DdlMessage a = parse("{\"v\":1,\"id\":5,\"op\":\"alter_col\",\"name\":\"extra\","
        + "\"type\":\"STRING\",\"relid\":\"19228\"}");
    assertEquals(5, a.id());
    assertEquals("extra", a.name());
    assertEquals("STRING", a.type());
    DdlMessage d = parse("{\"v\":1,\"op\":\"drop_col\",\"name\":\"note\",\"relid\":\"19228\"}");
    assertEquals("note", d.name());
    assertNull(d.id());
    DdlMessage c = parse("{\"v\":1,\"op\":\"add_col\",\"col\":{\"id\":5,\"name\":\"extra\","
        + "\"type\":\"BIGINT\"},\"relid\":\"19228\"}");
    assertEquals(new DdlMessage.Col(5, "extra", "BIGINT"), c.col());
  }

  @Test
  void relidAboveIntMaxIsNonNegative() {
    assertEquals(4294967295L,
        parse("{\"v\":1,\"op\":\"drop_col\",\"relid\":\"4294967295\"}").relid());
  }

  /** Key order is unstable; shuffling must not change the parse. */
  @Test
  void keyOrderDoesNotMatter() {
    String[][] members = {{"v", "1"}, {"op", "\"create\""}, {"pk", "[\"id\"]"},
        {"cols",
            "[{\"type\":\"INT NOT NULL\",\"name\":\"id\",\"id\":1},"
                + "{\"id\":2,\"name\":\"p\",\"type\":\"DECIMAL(10, 2)\"}]"},
        {"relid", "\"19228\""}, {"table", "\"cap_t\""}, {"schema", "\"public\""},
        {"options", "{\"changelog-producer\":\"input\"}"}};
    List<String[]> list = new ArrayList<>(List.of(members));
    DdlMessage expected = null;
    Random rnd = new Random(42);
    for (int i = 0; i < 25; i++) {
      Collections.shuffle(list, rnd);
      StringBuilder sb = new StringBuilder("{");
      for (String[] m : list) {
        if (sb.length() > 1)
          sb.append(',');
        sb.append('"').append(m[0]).append("\":").append(m[1]);
      }
      DdlMessage d = parse(sb.append('}').toString());
      if (expected == null) {
        expected = d;
      }
      assertEquals(expected, d);
    }
    assertEquals(19228L, expected.relid());
    assertEquals(2, expected.cols().size());
  }

  @Test
  void invalidJsonIsAnError() {
    assertThrows(IllegalStateException.class, () -> parse("garbage"));
    assertThrows(IllegalStateException.class, () -> parse("[1]"));
  }
}
