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

    /**
     * @return the affected row count. A CAS UPDATE whose WHERE clause did not
     *     match affects zero rows and throws NOTHING, so a caller that needs to
     *     know whether its write actually happened cannot learn it from the
     *     absence of an exception. Callers that do not care may discard this.
     */
    static int exec(DataSource ds, String sql, Binder binder) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            binder.bind(p);
            return p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("exec failed: " + sql, e);
        }
    }
}
