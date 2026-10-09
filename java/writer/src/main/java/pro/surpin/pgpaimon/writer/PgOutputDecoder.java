// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decodes pgoutput protocol version 2 messages, one message per {@link ByteBuffer}.
 *
 * <p>Column state bytes are the four from src/include/replication/logicalproto.h:
 * {@code 'n'} NULL, {@code 'u'} UNCHANGED (an unreassembled TOAST pointer), {@code 't'} TEXT,
 * {@code 'b'} BINARY. The slot runs with binary 'false', so {@code 'b'} is a protocol
 * violation here, not a case to guess at. NULL ({@code 'n'}) and the empty string
 * ({@code 't'}, length 0) are different states and stay different.
 *
 * <p>Every OID is read as an unsigned 32-bit value into a {@code long}. The per-event lists
 * (tuple columns, truncate relids) are built fresh per message, handed over, and never retained
 * or mutated here. {@link RelationMeta} is the one thing deliberately retained: it is cached
 * per relid until a later {@code R} message replaces it. Tuple widths are checked against that
 * cached meta.
 *
 * <p>Input is validated defensively: lengths and counts larger than the bytes left in the
 * buffer (each buffer holds exactly one message) are rejected before anything is allocated, and
 * text must be well-formed UTF-8 (malformed bytes mean a desynchronised stream or a connection
 * that is not UTF-8, and are reported rather than replaced). The buffer must be big-endian,
 * which is the {@link ByteBuffer} default.
 *
 * <p>Not thread-safe: it holds the relation cache.
 */
public final class PgOutputDecoder {

    private final Map<Long, RelationMeta> relations = new HashMap<>();

    /** The last {@code R} message decoded for this relid, or {@code null}. */
    public RelationMeta relation(long relid) {
        return relations.get(relid);
    }

    /** Consumes exactly one message from {@code buf}. */
    public PgEvent decode(ByteBuffer buf) {
        char kind = (char) (buf.get() & 0xFF);
        switch (kind) {
            case 'B': {
                long finalLsn = buf.getLong();
                long ts = buf.getLong();
                long xid = Integer.toUnsignedLong(buf.getInt());
                return new PgEvent.Begin(finalLsn, ts, xid);
            }
            case 'C': {
                buf.get();                      // flags, unused
                long commitLsn = buf.getLong();
                long endLsn = buf.getLong();
                long ts = buf.getLong();
                return new PgEvent.Commit(commitLsn, endLsn, ts);
            }
            case 'R': {
                long relid = Integer.toUnsignedLong(buf.getInt());
                String nsp = cstring(buf);
                String rel = cstring(buf);
                buf.get();                      // replica identity setting
                int n = buf.getShort() & 0xFFFF;
                List<String> names = new ArrayList<>(n);
                List<Boolean> keys = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    keys.add((buf.get() & 1) == 1);
                    names.add(cstring(buf));
                    buf.getInt();               // type oid
                    buf.getInt();               // type modifier
                }
                // pgoutput sends an empty namespace for pg_catalog (logicalrep_write_namespace).
                RelationMeta meta = new RelationMeta(relid, nsp.isEmpty() ? "pg_catalog" : nsp,
                                                     rel, names, keys);
                relations.put(relid, meta);
                return new PgEvent.Relation(meta);
            }
            case 'I': {
                long relid = Integer.toUnsignedLong(buf.getInt());
                expect(buf.get(), 'N', "insert new-tuple marker");
                return new PgEvent.Insert(relid, tuple(buf, relid));
            }
            case 'U': {
                long relid = Integer.toUnsignedLong(buf.getInt());
                TupleData oldTuple = null;
                boolean keyOnly = false;
                byte marker = buf.get();
                if (marker == 'O' || marker == 'K') {
                    keyOnly = marker == 'K';
                    oldTuple = tuple(buf, relid);
                    marker = buf.get();
                }
                expect(marker, 'N', "update new-tuple marker");
                return new PgEvent.Update(relid, oldTuple, keyOnly, tuple(buf, relid));
            }
            case 'D': {
                long relid = Integer.toUnsignedLong(buf.getInt());
                byte marker = buf.get();
                if (marker != 'O' && marker != 'K') {
                    throw new IllegalStateException(
                        "delete without an old tuple marker: got '" + (char) (marker & 0xFF) + "'");
                }
                return new PgEvent.Delete(relid, tuple(buf, relid), marker == 'K');
            }
            case 'T': {
                int n = buf.getInt();
                int flags = buf.get();
                if (n < 0 || n > buf.remaining() / 4) {
                    throw new IllegalStateException("truncate relation count " + n
                        + " does not fit in the remaining " + buf.remaining() + " bytes");
                }
                List<Long> relids = new ArrayList<>(n);
                for (int i = 0; i < n; i++) {
                    relids.add(Integer.toUnsignedLong(buf.getInt()));
                }
                return new PgEvent.Truncate(relids, (flags & 1) != 0, (flags & 2) != 0);
            }
            case 'M': {
                boolean transactional = (buf.get() & 1) != 0;
                long lsn = buf.getLong();
                String prefix = cstring(buf);
                int len = buf.getInt();
                requireFits(len, buf, "logical message length");
                byte[] content = new byte[len];
                buf.get(content);
                return new PgEvent.Message(transactional, lsn, prefix, content);
            }
            case 'O': {
                long commitLsn = buf.getLong();
                return new PgEvent.Origin(commitLsn, cstring(buf));
            }
            case 'Y': {
                long oid = Integer.toUnsignedLong(buf.getInt());
                String nsp = cstring(buf);
                String name = cstring(buf);
                return new PgEvent.TypeMsg(oid, nsp, name);
            }
            case 'S': case 'E': case 'c': case 'A':
                throw new IllegalStateException(
                    "stream message '" + kind + "' received although streaming is off");
            default:
                throw new IllegalStateException("unknown pgoutput message type '" + kind + "'");
        }
    }

    private TupleData tuple(ByteBuffer buf, long relid) {
        int n = buf.getShort() & 0xFFFF;
        RelationMeta meta = relations.get(relid);
        if (meta != null && n != meta.columnNames().size()) {
            throw new IllegalStateException("tuple for relation " + relid + " has " + n
                + " columns but the last R message declared " + meta.columnNames().size()
                + " (missed or stale Relation message?)");
        }
        List<ColumnValue> cols = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            char state = (char) (buf.get() & 0xFF);
            switch (state) {
                case 'n' -> cols.add(ColumnValue.ofNull());
                case 'u' -> cols.add(ColumnValue.unchanged());
                case 't' -> {
                    int len = buf.getInt();
                    requireFits(len, buf, "text column length");
                    byte[] v = new byte[len];
                    buf.get(v);
                    cols.add(ColumnValue.of(utf8(v)));
                }
                case 'b' -> throw new IllegalStateException(
                    "binary column value received although the slot uses binary 'false'");
                default -> throw new IllegalStateException(
                    "unknown column state '" + state + "'");
            }
        }
        return new TupleData(cols);
    }

    /** Reads a NUL-terminated UTF-8 string, consuming the terminator. */
    private static String cstring(ByteBuffer buf) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(32);
        byte b;
        while ((b = buf.get()) != 0) {
            bytes.write(b);
        }
        return utf8(bytes.toByteArray());
    }

    /** Strict UTF-8: malformed input throws instead of becoming U+FFFD. */
    private static String utf8(byte[] bytes) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            throw new IllegalStateException("malformed UTF-8 in pgoutput message", e);
        }
    }

    /** A length must be non-negative and cannot exceed what is left in the one-message buffer. */
    private static void requireFits(int len, ByteBuffer buf, String what) {
        if (len < 0 || len > buf.remaining()) {
            throw new IllegalStateException(what + " " + len + " does not fit in the remaining "
                + buf.remaining() + " bytes");
        }
    }

    private static void expect(byte actual, char want, String what) {
        if (actual != (byte) want) {
            throw new IllegalStateException(
                what + ": expected '" + want + "', got '" + (char) (actual & 0xFF) + "'");
        }
    }
}
