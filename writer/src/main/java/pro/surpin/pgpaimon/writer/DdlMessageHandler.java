// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import pro.surpin.pgpaimon.writer.dto.*;
import pro.surpin.pgpaimon.writer.dto.DdlMessage;
import pro.surpin.pgpaimon.writer.dto.PgEvent;


/**
 * Applies DDL messages to the {@link SchemaRegistry}, one at a time, in arrival order. Within one
 * statement the emitter orders them drop_col, then rename_col/alter_col, then add_col, so a dropped
 * column's name can be reused by a column added in the same ALTER TABLE; apply them exactly as
 * received.
 */
public final class DdlMessageHandler {

  private static final @NotNull Logger LOG = Logger.getLogger(DdlMessageHandler.class.getName());
  static final int WIRE_VERSION = 1;

  private final SchemaRegistry registry;
  private final String expectedPrefix;
  private String token;
  private final long skipLsn; // 0 = PGPAIMON_SKIP_LSN not set
  private final java.util.function.Supplier<String> currentToken; // null = cannot re-read

  /**
   * @param token the installation's credential, read from {@code paimon.secret} at startup.
   *        Required: a handler that accepted unauthenticated messages would let any role that can
   *        connect drive the writer (see {@link #handle}).
   */
  public DdlMessageHandler(
      SchemaRegistry registry,
      String expectedPrefix,
      String token
  ) {
    this(registry, expectedPrefix, token, 0, null);
  }

  /**
   * @param skipLsn {@code PGPAIMON_SKIP_LSN}: a rejected DDL message at exactly this LSN is skipped
   *        instead of stopping the writer (see {@link #handle})
   * @param currentToken re-reads {@code paimon.secret} when a message's credential differs from the
   *        one in hand, so that a secret rotated while the writer runs is followed rather than
   *        fatal; null when there is no database to ask
   */
  public DdlMessageHandler(
      SchemaRegistry registry,
      String expectedPrefix,
      String token,
      long skipLsn,
      java.util.function.Supplier<String> currentToken
  ) {
    if (token == null || token.isEmpty()) {
      throw new IllegalArgumentException(
          "the DDL message credential (paimon.secret) is" + " required");
    }
    this.registry = registry;
    this.expectedPrefix = expectedPrefix;
    this.token = token;
    this.skipLsn = skipLsn;
    this.currentToken = currentToken;
  }

  /** Whether {@link #handle} would act on this message (it ignores other prefixes). */
  public boolean isDdl(
      PgEvent.Message m
  ) {
    return expectedPrefix.equals(m.prefix());
  }

  /**
   * Applies one message; ignores (with a WARNING) one that is not a DDL message at all; and STOPS
   * the writer on a DDL message whose credential is missing or wrong.
   *
   * <p>
   * {@code pg_logical_emit_message()} is executable by PUBLIC, so anything under the prefix may
   * come from any role that can connect. Before the credential, one
   * {@code SELECT pg_logical_emit_message(true, 'paimon', 'not json')} by a role with no privileges
   * at all stopped the writer for good. Text that is not a JSON object, or an object with no
   * {@code op}, cannot be a DDL message of ours and is not one of anyone's: it is reported and
   * skipped (every message the extension emits is a JSON object with an op).
   *
   * <p>
   * A message that parses as a DDL message but does not carry this installation's credential is
   * different, because two very different things look exactly alike here. It may be forged, and
   * applying it could purge a table. Or it may be LEGITIMATE, emitted under another value of
   * {@code paimon.secret}: the secret was changed while the message was still in the slot, or the
   * extension was dropped and re-created. Ignoring that one silently loses the DDL for good --
   * measured: after a rotation, ADD COLUMN c was ignored, and every value of c was discarded, also
   * after a restart. So it is fatal, naming what to check. Before failing, the secret is read
   * again: a message carrying the CURRENT value is legitimate (the secret was rotated while the
   * writer ran), and is applied. The break-glass for a forged message is {@code PGPAIMON_SKIP_LSN}
   * set to the message's LSN, which skips exactly that message.
   *
   * <p>
   * A message that IS authenticated and still wrong is a defect of ours, and fatal too.
   */
  public void handle(
      @NotNull PgEvent.Message m
  ) {
    if (!expectedPrefix.equals(m.prefix())) {
      return;
    }
    DdlMessage d;
    try {
      d = DdlMessage.parse(m.content());
    } catch (RuntimeException e) {
      LOG.warning(ignored(m, "it is not a JSON object (" + e.getMessage() + ")"));
      return;
    }
    if (d.op() == null) {
      LOG.warning(ignored(m, "it is a JSON object with no \"op\", so not a DDL message"));
      return;
    }
    if (!authentic(d.auth())) {
      String why = d.auth() == null ? "carries no credential"
          : "carries a credential that is not this installation's paimon.secret";
      if (skipLsn != 0 && m.lsn() == skipLsn) {
        LOG.warning("SKIPPED DDL MESSAGE (PGPAIMON_SKIP_LSN=" + SlotConsumer.formatLsn(skipLsn)
            + "): " + describe(d, m) + " " + why + ". Nothing was applied. If it was a"
            + " legitimate DDL message, that DDL is now missing from Paimon: compare the"
            + " Paimon schema of relation " + d.relid() + " with paimon.table_registry and"
            + " re-register the table if they disagree. Unset PGPAIMON_SKIP_LSN before the"
            + " next restart.");
        return;
      }
      throw new IllegalStateException("REJECTED DDL MESSAGE: " + describe(d, m) + " " + why
          + ". The writer stops rather than guess: a legitimate DDL message ignored loses"
          + " that change for good, and a forged one applied could purge a table. Nothing was"
          + " applied. Check, in this order: (1) was paimon.secret changed, or the extension"
          + " dropped and re-created, while this message was in the slot? Then it is"
          + " legitimate: put back the token it was emitted under (UPDATE paimon.secret SET"
          + " token = '<the old token>') and restart; rotate the secret only when the slot"
          + " holds no unconsumed DDL (docs, section 5, 'DDL messages carry a credential')."
          + " (2) Otherwise some role emitted it with pg_logical_emit_message() under the"
          + " 'paimon' prefix (executable by PUBLIC, and any role that can read paimon.secret"
          + " -- pg_read_all_data members included -- can forge a credential): find who,"
          + " then set PGPAIMON_SKIP_LSN=" + SlotConsumer.formatLsn(m.lsn())
          + " and restart to skip exactly this message. If the old token is lost, skip it"
          + " the same way and re-register the table it names.");
    }
    if (d.version() != WIRE_VERSION) {
      throw new IllegalStateException("unsupported DDL message version " + d.version()
          + " (this writer understands " + WIRE_VERSION + ")");
    }
    String op = d.op();
    if (op == null) {
      throw new IllegalStateException("DDL message has no op");
    }
    switch (op) {
      case "create" -> registry.createTable(d.relid(), d.schema(), d.table(),
          need(d.pk(), "pk", op), need(d.cols(), "cols", op), d.changelogProducer(), d.buckets(),
          m.lsn(), new SchemaRegistry.HistSpec(d.histTable(), d.registration(), d.histBuckets(),
              d.histBucketsUser()));
      case "add_col", "drop_col", "rename_col", "alter_col", "drop_table" -> {
        if (!registry.tracked(d.relid())) {
          LOG.warning("ignoring " + op + " for unregistered relation " + d.relid());
          return;
        }
        // m.lsn() is the message's own position in WAL, from the pgoutput 'M' header.
        // It is stable across replays and is what SchemaRegistry records as the DDL
        // commit identifier, so a replayed message is skipped the way filterAndCommit
        // skips a replayed data commit.
        switch (op) {
          case "add_col" -> registry.addColumn(d.relid(), need(d.col(), "col", op), m.lsn());
          case "drop_col" -> registry.dropColumn(d.relid(), need(d.name(), "name", op), m.lsn());
          case "rename_col" -> registry.renameColumn(d.relid(), need(d.from(), "from", op),
              need(d.to(), "to", op), m.lsn());
          case "alter_col" -> registry.alterColumn(d.relid(), need(d.name(), "name", op),
              need(d.type(), "type", op), m.lsn());
          default -> registry.dropTable(d.relid(), d.schema(), d.table(), m.lsn());
        }
      }
      default -> throw new IllegalStateException("unknown DDL op '" + op + "'");
    }
  }

  /**
   * Whether the credential is this installation's: the one in hand, or else the current value of
   * paimon.secret, which then becomes the one in hand (a rotation while the writer runs).
   */
  private boolean authentic(
      String auth
  ) {
    if (auth == null) {
      return false;
    }
    if (same(auth, token)) {
      return true;
    }
    if (currentToken == null) {
      return false;
    }
    String now;
    try {
      now = currentToken.get();
    } catch (RuntimeException e) {
      LOG.warning("could not re-read paimon.secret to check a DDL message's credential: " + e);
      return false;
    }
    if (now != null && !now.isEmpty() && same(auth, now)) {
      LOG.info("paimon.secret has changed since startup; a DDL message carries the new value"
          + " and is accepted, and the new value is used from now on");
      token = now;
      return true;
    }
    return false;
  }

  private static boolean same(
      String a,
      String b
  ) {
    return java.security.MessageDigest.isEqual(a.getBytes(java.nio.charset.StandardCharsets.UTF_8),
        b.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static String describe(
      DdlMessage d,
      PgEvent.Message m
  ) {
    return "the DDL message at " + SlotConsumer.formatLsn(m.lsn()) + " (op '" + d.op()
        + "', relation " + d.relid()
        + (d.table() == null ? "" : ", " + d.schema() + "." + d.table()) + ")";
  }

  private String ignored(
      PgEvent.Message m,
      String why
  ) {
    return "IGNORED a logical message with prefix '" + expectedPrefix + "' at "
        + SlotConsumer.formatLsn(m.lsn()) + ": " + why + ", so it was not emitted by the"
        + " paimon_slot extension. Some role called pg_logical_emit_message() with this"
        + " prefix (it is executable by PUBLIC); nothing was applied. If it was meant to be"
        + " a DDL message, emit it through the extension instead";
  }

  private static <T> T need(
      T v,
      String field,
      String op
  ) {
    if (v == null) {
      throw new IllegalStateException("DDL op '" + op + "' lacks '" + field + "'");
    }
    return v;
  }
}
