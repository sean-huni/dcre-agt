package za.co.fnb.dcre.agt.repo;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Stage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Plain-JDBC access to the launch_intent ledger. AGT is the single writer (R-04);
 * every mutation is either an INSERT guarded by a unique constraint or a
 * status UPDATE, so replays are no-ops (R-05).
 */
@ApplicationScoped
public class IntentRepo {

    @Inject
    DataSource ds;

    /** Write-ahead intent. @return intent id, or empty when (arrival, stage) already intended. */
    public Optional<UUID> insertIntent(UUID arrivalId, Stage stage, String jobName) {
        String sql = """
                INSERT INTO launch_intent (arrival_id, stage, job_name, status)
                VALUES (?,?,?,'INTENDED')
                ON CONFLICT (arrival_id, stage) DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, arrivalId);
            p.setString(2, stage.name());
            p.setString(3, jobName);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.of(r.getObject(1, UUID.class)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertIntent failed", e);
        }
    }

    /** Write-ahead clock intent (no arrival). @return id, or empty when (stage, run_key) exists. */
    public Optional<UUID> insertClockIntent(Stage stage, String runKey, String jobName, String launchArgs) {
        String sql = """
                INSERT INTO launch_intent (stage, run_key, job_name, status, launch_args)
                VALUES (?,?,?,'INTENDED',?)
                ON CONFLICT DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, stage.name());
            p.setString(2, runKey);
            p.setString(3, jobName);
            p.setString(4, launchArgs);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.of(r.getObject(1, UUID.class)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertClockIntent failed", e);
        }
    }

    public void markIntentLaunched(UUID intentId, String jobUid) {
        JdbcSupport.exec(ds, "UPDATE launch_intent SET status='LAUNCHED', job_uid=? WHERE id=?", p -> {
            p.setString(1, jobUid);
            p.setObject(2, intentId);
        });
    }

    /** Durable launch args of a clock intent; the Reconciler recreates from these. */
    public Optional<String> intentLaunchArgs(UUID intentId) {
        try (Connection c = ds.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT launch_args FROM launch_intent WHERE id=?")) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.ofNullable(r.getString(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("intentLaunchArgs failed", e);
        }
    }

    public Optional<String> intentJobUid(UUID intentId) {
        try (Connection c = ds.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT job_uid FROM launch_intent WHERE id=?")) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.ofNullable(r.getString(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("intentJobUid failed", e);
        }
    }

    public java.time.OffsetDateTime intentCreatedAt(UUID intentId) {
        try (Connection c = ds.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT created_at FROM launch_intent WHERE id=?")) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getObject(1, java.time.OffsetDateTime.class);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("intentCreatedAt failed", e);
        }
    }

    public List<LaunchIntent> intentsForArrival(UUID arrivalId) {
        String sql = "SELECT id, arrival_id, stage, job_name, status, run_key FROM launch_intent WHERE arrival_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, arrivalId);
            try (ResultSet r = p.executeQuery()) {
                List<LaunchIntent> out = new ArrayList<>();
                while (r.next()) {
                    out.add(map(r));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("intentsForArrival failed", e);
        }
    }

    public List<LaunchIntent> intentsWithoutOutcome() {
        String sql = """
                SELECT i.id, i.arrival_id, i.stage, i.job_name, i.status, i.run_key
                FROM launch_intent i LEFT JOIN stage_outcome o ON o.intent_id = i.id
                WHERE o.id IS NULL""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql);
             ResultSet r = p.executeQuery()) {
            List<LaunchIntent> out = new ArrayList<>();
            while (r.next()) {
                out.add(map(r));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("intentsWithoutOutcome failed", e);
        }
    }

    public List<String> allIntentJobNames() {
        try (Connection c = ds.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT job_name FROM launch_intent");
             ResultSet r = p.executeQuery()) {
            List<String> out = new ArrayList<>();
            while (r.next()) {
                out.add(r.getString(1));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("allIntentJobNames failed", e);
        }
    }

    private static LaunchIntent map(ResultSet r) throws SQLException {
        return new LaunchIntent(r.getObject(1, UUID.class), r.getObject(2, UUID.class),
                Stage.valueOf(r.getString(3)), r.getString(4), r.getString(5), r.getString(6));
    }
}
