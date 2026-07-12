package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import za.co.fnb.dcre.agt.config.AgtConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * DB-backed singleton lease (SPEC-DAG 6a). Compare-and-set on the single
 * agt_lease row; every control loop no-ops unless this instance holds it.
 */
@ApplicationScoped
public class LeaseService {

    static final int TTL_SECONDS = 30;

    @Inject
    DataSource ds;

    @Inject
    AgtConfig config;

    @Scheduled(every = "5s")
    void renewOrAcquire() {
        tryAcquire(config.holderId());
    }

    /** Retry CRDB serialization aborts (SQLSTATE 40001) with backoff (SPEC-DAG 6b). */
    static <T> T retry40001(java.util.function.Supplier<T> op) {
        for (int attempt = 1; ; attempt++) {
            try {
                return op.get();
            } catch (IllegalStateException e) {
                if (attempt >= 4 || !(e.getCause() instanceof SQLException sql)
                        || !"40001".equals(sql.getSQLState())) {
                    throw e;
                }
                try {
                    Thread.sleep(50L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    /** CAS acquire/renew. @return true when this holder now owns the lease. */
    public boolean tryAcquire(String holder) {
        return retry40001(() -> casAcquire(holder));
    }

    private boolean casAcquire(String holder) {
        String sql = """
                INSERT INTO agt_lease (singleton, holder, expires_at)
                VALUES (true, ?, now() + INTERVAL '%d seconds')
                ON CONFLICT (singleton) DO UPDATE
                  SET holder = excluded.holder, expires_at = excluded.expires_at
                  WHERE agt_lease.expires_at < now() OR agt_lease.holder = excluded.holder
                """.formatted(TTL_SECONDS);
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, holder);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("lease CAS failed", e);
        }
        return holds(holder);
    }

    public boolean holdsLease() {
        return holds(config.holderId());
    }

    public boolean holds(String holder) {
        String sql = "SELECT count(*) FROM agt_lease WHERE holder=? AND expires_at > now()";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, holder);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lease check failed", e);
        }
    }
}
