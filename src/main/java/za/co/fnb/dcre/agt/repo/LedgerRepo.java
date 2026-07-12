package za.co.fnb.dcre.agt.repo;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Plain-JDBC access to the agt_ops ledgers. AGT is the single writer (R-04);
 * every mutation is either an INSERT guarded by a unique constraint or a
 * status UPDATE, so replays are no-ops (R-05).
 */
@ApplicationScoped
public class LedgerRepo {

    @Inject
    DataSource ds;

    // ---- file_arrival -----------------------------------------------------

    /** @return arrival id, or empty when the identical (route, name, hash) already exists. */
    public Optional<UUID> insertArrival(String route, String name, String sha256,
                                        String clientToken, String msgIdToken,
                                        ArrivalStatus status, String quarantineReason) {
        String sql = """
                INSERT INTO file_arrival
                  (route_id, physical_filename, payload_sha256, client_token, msg_id_token, status, quarantine_reason)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT (route_id, physical_filename, payload_sha256) DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, route);
            p.setString(2, name);
            p.setString(3, sha256);
            p.setString(4, clientToken);
            p.setString(5, msgIdToken);
            p.setString(6, status.name());
            p.setString(7, quarantineReason);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.of(r.getObject(1, UUID.class)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertArrival failed", e);
        }
    }

    /** True when the same logical key (route + filename tokens) exists with a DIFFERENT hash. */
    public boolean sameKeyDifferentHashExists(String route, String clientToken, String msgIdToken, String sha256) {
        if (clientToken == null || msgIdToken == null) {
            return false;
        }
        String sql = """
                SELECT count(*) FROM file_arrival
                WHERE route_id=? AND client_token=? AND msg_id_token=? AND payload_sha256<>?
                  AND status <> 'QUARANTINED'""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, route);
            p.setString(2, clientToken);
            p.setString(3, msgIdToken);
            p.setString(4, sha256);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("sameKeyDifferentHashExists failed", e);
        }
    }

    public void updateArrivalStatus(UUID id, ArrivalStatus status) {
        exec("UPDATE file_arrival SET status=? WHERE id=?", p -> {
            p.setString(1, status.name());
            p.setObject(2, id);
        });
    }

    public void updateArrivalClaimedPath(UUID id, String claimedPath) {
        exec("UPDATE file_arrival SET claimed_path=? WHERE id=?", p -> {
            p.setString(1, claimedPath);
            p.setObject(2, id);
        });
    }

    public List<FileArrival> arrivalsByStatus(ArrivalStatus... statuses) {
        StringBuilder in = new StringBuilder();
        for (int i = 0; i < statuses.length; i++) {
            in.append(i == 0 ? "?" : ",?");
        }
        String sql = "SELECT id, route_id, physical_filename, payload_sha256, client_token, msg_id_token,"
                + " status, quarantine_reason, claimed_path FROM file_arrival WHERE status IN (" + in + ")";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            for (int i = 0; i < statuses.length; i++) {
                p.setString(i + 1, statuses[i].name());
            }
            try (ResultSet r = p.executeQuery()) {
                List<FileArrival> out = new ArrayList<>();
                while (r.next()) {
                    out.add(new FileArrival(r.getObject(1, UUID.class), r.getString(2), r.getString(3),
                            r.getString(4), r.getString(5), r.getString(6),
                            ArrivalStatus.valueOf(r.getString(7)), r.getString(8), r.getString(9)));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("arrivalsByStatus failed", e);
        }
    }

    // ---- launch_intent ----------------------------------------------------

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

    public void markIntentLaunched(UUID intentId) {
        exec("UPDATE launch_intent SET status='LAUNCHED' WHERE id=?", p -> p.setObject(1, intentId));
    }

    public List<LaunchIntent> intentsForArrival(UUID arrivalId) {
        String sql = "SELECT id, arrival_id, stage, job_name, status FROM launch_intent WHERE arrival_id=?";
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
                SELECT i.id, i.arrival_id, i.stage, i.job_name, i.status
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
                Stage.valueOf(r.getString(3)), r.getString(4), r.getString(5));
    }

    // ---- stage_outcome ----------------------------------------------------

    /** Idempotent: duplicate observations for the same intent are no-ops (UNIQUE(intent_id)). */
    public boolean insertOutcome(UUID intentId, Outcome outcome, Integer exitCode, String condition) {
        String sql = """
                INSERT INTO stage_outcome (intent_id, outcome, exit_code, k8s_condition)
                VALUES (?,?,?,?)
                ON CONFLICT (intent_id) DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, intentId);
            p.setString(2, outcome.name());
            if (exitCode == null) {
                p.setNull(3, java.sql.Types.INTEGER);
            } else {
                p.setInt(3, exitCode);
            }
            p.setString(4, condition);
            try (ResultSet r = p.executeQuery()) {
                return r.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertOutcome failed", e);
        }
    }

    /** stage -> outcome for one arrival. */
    public Map<Stage, Outcome> outcomesForArrival(UUID arrivalId) {
        String sql = """
                SELECT i.stage, o.outcome FROM launch_intent i
                JOIN stage_outcome o ON o.intent_id = i.id
                WHERE i.arrival_id=?""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, arrivalId);
            try (ResultSet r = p.executeQuery()) {
                Map<Stage, Outcome> out = new HashMap<>();
                while (r.next()) {
                    out.put(Stage.valueOf(r.getString(1)), Outcome.valueOf(r.getString(2)));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("outcomesForArrival failed", e);
        }
    }

    // ---- helpers ----------------------------------------------------------

    @FunctionalInterface
    interface Binder {
        void bind(PreparedStatement p) throws SQLException;
    }

    private void exec(String sql, Binder binder) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            binder.bind(p);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("exec failed: " + sql, e);
        }
    }
}
