// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Every setting a value's text depends on is SET and then verified, on both connections. */
class SessionSettingsTest {

    /** What a server pinned as asked answers to SHOW. */
    static final Map<String, String> PINNED = Map.of(
        "TimeZone", "UTC", "client_encoding", "UTF8", "bytea_output", "hex",
        "DateStyle", "ISO, MDY", "IntervalStyle", "postgres", "extra_float_digits", "3",
        "lc_monetary", "C");

    static FakeJdbc server(Map<String, String> shows) {
        return new FakeJdbc(sql -> sql.startsWith("SHOW ")
            ? List.of(shows.get(sql.substring(5))) : null);
    }

    @Test
    void everySettingAValuesTextDependsOnIsSetThenReadBack() throws Exception {
        FakeJdbc db = server(PINNED);
        SessionSettings.pin(db.connection());
        for (String s : List.of("SET TimeZone = 'UTC'", "SET client_encoding = 'UTF8'",
                                "SET bytea_output = 'hex'", "SET DateStyle = 'ISO, MDY'",
                                "SET IntervalStyle = 'postgres'", "SET extra_float_digits = 3",
                                "SET lc_monetary = 'C'")) {
            assertTrue(db.executed.contains(s), s + " in " + db.executed);
            String name = s.substring(4, s.indexOf(' ', 4));
            assertTrue(db.executed.indexOf(s) < db.executed.indexOf("SHOW " + name),
                       "set before it is read back: " + db.executed);
        }
    }

    /** A setting that did not take (a server, role or pooler that overrides it) refuses. */
    @Test
    void aSettingThatDidNotTakeRefusesWithItsName() {
        for (String name : PINNED.keySet()) {
            Map<String, String> m = new java.util.HashMap<>(PINNED);
            m.put(name, "something else");
            IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> SessionSettings.pin(server(m).connection()), name);
            assertTrue(e.getMessage().contains(name + "=something else"), e.getMessage());
        }
    }
}
