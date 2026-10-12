// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer.dto;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * One DDL message emitted by the SQL half (paimon.emit_ddl / logical message).
 *
 * <p>
 * Parsed strictly by key name: the order of keys in the JSON is unspecified and differs between
 * ops. The shape also differs by op, so reference fields, including a missing {@code op}, may be
 * null; {@link pro.surpin.pgpaimon.writer.DdlMessageHandler} checks that the fields its op needs
 * are present. Parsing is deliberately lenient so that the handler can reject an unsupported {@code
 * v} or an unknown {@code op} by name before it complains about a missing field.
 *
 * <p>
 * {@code relid} is a JSON string (jsonb renders an oid as text) and is held as an unsigned 32-bit
 * value in a {@code long}. A Paimon type string carries its nullability ({@code "INT NOT
 * NULL"}); there is no separate boolean.
 */
public record DdlMessage(
    int version,
    @Nullable String op,
    long relid,
    @Nullable String schema,
    @Nullable String table,
    @Nullable List<@NotNull String> pk,
    @Nullable List<@NotNull Col> cols,
    @Nullable Col col,
    @Nullable Integer id,
    @Nullable String from,
    @Nullable String to,
    @Nullable String name,
    @Nullable String type,
    @Nullable Map<String, String> options
) {

  /**
   * {@code id} is the PostgreSQL attnum: advisory only, Paimon assigns its own field ids.
   */
  public record Col(
      int id,
      @Nullable String name,
      @Nullable String type
  ) {
  }

  /**
   * The changelog-producer option carried by {@code create}; null when absent.
   */
  public @Nullable String changelogProducer() {
    return options == null ? null : options.get("changelog-producer");
  }

  private static final @NotNull ObjectMapper MAPPER = new ObjectMapper();

  public static @NotNull DdlMessage parse(
      @NotNull byte[] json
  ) {
    JsonNode n;
    try {
      n = MAPPER.readTree(json);
    } catch (IOException e) {
      throw new IllegalStateException("DDL message is not valid JSON", e);
    }
    if (n == null || !n.isObject()) {
      throw new IllegalStateException("DDL message is not a JSON object");
    }
    JsonNode rel = n.get("relid");
    long relid = rel == null || rel.isNull() ? -1L : Long.parseLong(rel.asText());

    List<@NotNull String> pk = null;
    if (n.hasNonNull("pk")) {
      pk = new ArrayList<>();
      for (JsonNode p : n.get("pk")) {
        pk.add(p.asText());
      }
    }
    List<@NotNull Col> cols = null;
    if (n.hasNonNull("cols")) {
      cols = new ArrayList<>();
      for (JsonNode c : n.get("cols")) {
        cols.add(col(c));
      }
    }
    Map<String, String> options = null;
    if (n.hasNonNull("options")) {
      options = new HashMap<>();
      var it = n.get("options").fields();
      while (it.hasNext()) {
        var e = it.next();
        options.put(e.getKey(), e.getValue().asText());
      }
    }
    return new DdlMessage(n.hasNonNull("v") ? n.get("v").asInt() : 0, text(n, "op"), relid,
        text(n, "schema"), text(n, "table"), pk, cols,
        n.hasNonNull("col") ? col(n.get("col")) : null,
        n.hasNonNull("id") ? n.get("id").asInt() : null, text(n, "from"), text(n, "to"),
        text(n, "name"), text(n, "type"), options);
  }

  /**
   * Parses the {@code cols} jsonb of a {@code paimon.table_registry} row (a JSON array).
   */
  public static @NotNull List<@NotNull Col> parseCols(
      @NotNull String json
  ) {
    JsonNode n;
    try {
      n = MAPPER.readTree(json);
    } catch (IOException e) {
      throw new IllegalStateException("registry cols is not valid JSON", e);
    }
    if (n == null || !n.isArray()) {
      throw new IllegalStateException("registry cols is not a JSON array");
    }
    List<@NotNull Col> cols = new ArrayList<>();
    for (JsonNode c : n) {
      cols.add(col(c));
    }
    return cols;
  }

  private static @NotNull Col col(
      @NotNull JsonNode c
  ) {
    return new Col(c.hasNonNull("id") ? c.get("id").asInt() : 0, text(c, "name"), text(c, "type"));
  }

  private static @Nullable String text(
      @NotNull JsonNode n,
      @NotNull String key
  ) {
    return n.hasNonNull(key) ? n.get(key).asText() : null;
  }
}
