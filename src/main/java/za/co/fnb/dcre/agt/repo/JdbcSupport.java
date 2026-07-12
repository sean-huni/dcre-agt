package za.co.fnb.dcre.agt.repo;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** Shared plain-JDBC helpers for the agt_ops ledger repositories. */
final class JdbcSupport {

    private JdbcSupport() {
    }

    @FunctionalInterface
    interface Binder {
        void bind(PreparedStatement p) throws SQLException;
    }

    static void exec(DataSource ds, String sql, Binder binder) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            binder.bind(p);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("exec failed: " + sql, e);
        }
    }
}
