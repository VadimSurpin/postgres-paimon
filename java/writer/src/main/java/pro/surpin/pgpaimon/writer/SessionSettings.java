// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * The session settings every connection that reads row values as text pins: the replication
 * connection (pgoutput formats each value with the walsender session's output functions) and the
 * initial load's COPY connection. Each is SET and then read back; a mismatch is a
 * misconfiguration, never connection loss.
 *
 * <p>Pinned because an administrator can change every one of them under the writer with
 * {@code ALTER DATABASE ... SET}, {@code ALTER ROLE ... SET} or postgresql.conf, and the value
 * text then changes with no row change and nothing in the stream:
 * <ul>
 *   <li>{@code IntervalStyle}: measured, {@code ALTER DATABASE ... SET IntervalStyle =
 *       'sql_standard'} turned {@code 1 year 2 days 03:04:05} into {@code +1-0 +2 +3:04:05} in
 *       Paimon (an interval is a STRING there), so one column held two spellings of one value;
 *       {@code iso_8601} gives a third.</li>
 *   <li>{@code DateStyle} and {@code extra_float_digits}: pgjdbc sends {@code DateStyle=ISO} and
 *       {@code extra_float_digits=3} in its startup packet, which outranks the database and role
 *       settings (measured: both held under {@code ALTER DATABASE ... SET DateStyle = 'German',
 *       extra_float_digits = -3}). Pinned here anyway, so the guarantee does not rest on a
 *       driver default: German dates do not parse ({@link TextDecoder}), and a negative
 *       {@code extra_float_digits} rounds every float to 15 or 6 digits, silently.</li>
 *   <li>{@code TimeZone}: a timestamptz's offset is printed in it (pgjdbc announces the JVM's
 *       zone, and the options property does not override that).</li>
 *   <li>{@code lc_monetary}: {@code money} is printed with the locale's symbol and separators;
 *       'C' makes it one spelling whatever the database's locale is.</li>
 *   <li>{@code client_encoding} UTF8 ({@link PgOutputDecoder} decodes strictly) and
 *       {@code bytea_output} hex (TextDecoder accepts only the {@code \x} form).</li>
 * </ul>
 */
final class SessionSettings {

    /** A setting: what is SET, and what SHOW must then say (compared ignoring case). */
    record Pin(String name, String set, String shows) {}

    static final List<Pin> PINS = List.of(
        new Pin("TimeZone", "'UTC'", "UTC"),
        new Pin("client_encoding", "'UTF8'", "UTF8"),
        new Pin("bytea_output", "'hex'", "hex"),
        new Pin("DateStyle", "'ISO, MDY'", "ISO, MDY"),
        new Pin("IntervalStyle", "'postgres'", "postgres"),
        new Pin("extra_float_digits", "3", "3"),
        new Pin("lc_monetary", "'C'", "C"));

    private SessionSettings() {
    }

    /** SETs every pin, then reads each back. Works on a replication=database session too. */
    static void pin(Connection conn) throws SQLException {
        try (Statement st = conn.createStatement()) {
            for (Pin p : PINS) {
                st.execute("SET " + p.name() + " = " + p.set());
            }
        }
        for (Pin p : PINS) {
            require(conn, p.name(), p.shows());
        }
    }

    /** A setting that is not what we pinned is a misconfiguration, never connection loss. */
    static void require(Connection conn, String name, String want) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SHOW " + name)) {
            rs.next();
            String got = rs.getString(1);
            if (!want.equalsIgnoreCase(got)) {
                throw new IllegalStateException("session has " + name + "=" + got
                    + " but the writer requires " + want + " (it reads values as text, and their"
                    + " text depends on it)");
            }
        }
    }
}
