// SPDX-License-Identifier: Apache-2.0
package pro.surpin.pgpaimon.writer;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * A JDBC connection without a server, for the writer's few plain queries: every statement's SQL is
 * recorded in order, and {@code answer} gives each query its one-column result (a list of strings)
 * or throws the {@link SQLException} it returns.
 */
final class FakeJdbc {

    final List<String> executed = new ArrayList<>();
    private final Function<String, Object> answer;

    /** @param answer SQL to a {@code List<String>} of rows, or an {@link SQLException} to throw */
    FakeJdbc(Function<String, Object> answer) {
        this.answer = answer;
    }

    Connection connection() {
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {Connection.class}, (p, m, a) -> switch (m.getName()) {
                case "prepareStatement" -> prepared((String) a[0]);
                case "createStatement" -> plain();
                case "close" -> null;
                default -> throw new UnsupportedOperationException(m.getName());
            });
    }

    @SuppressWarnings("unchecked")
    private ResultSet run(String sql) throws SQLException {
        executed.add(sql);
        Object o = answer.apply(sql);
        if (o instanceof SQLException e) {
            throw e;
        }
        List<String> rows = o == null ? List.of() : (List<String>) o;
        int[] at = {-1};
        return (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {ResultSet.class}, (p, m, a) -> switch (m.getName()) {
                case "next" -> ++at[0] < rows.size();
                case "getString" -> rows.get(at[0]);
                case "close" -> null;
                default -> throw new UnsupportedOperationException(m.getName());
            });
    }

    private PreparedStatement prepared(String sql) {
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {PreparedStatement.class}, (p, m, a) -> switch (m.getName()) {
                case "executeQuery" -> run(sql);
                case "execute" -> { run(sql); yield false; }
                case "setString", "setLong", "setInt" -> null;
                case "close" -> null;
                default -> throw new UnsupportedOperationException(m.getName());
            });
    }

    private Statement plain() {
        return (Statement) Proxy.newProxyInstance(getClass().getClassLoader(),
            new Class<?>[] {Statement.class}, (p, m, a) -> switch (m.getName()) {
                case "execute" -> { run((String) a[0]); yield false; }
                case "executeQuery" -> run((String) a[0]);
                case "close" -> null;
                default -> throw new UnsupportedOperationException(m.getName());
            });
    }
}
