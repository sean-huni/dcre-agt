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
    public Optional<UUID> insertIntent(UUID arrivalId, Stage stage, String jobName, String namespace) {
        String sql = """
                INSERT INTO launch_intent (arrival_id, stage, job_name, status, namespace)
                VALUES (?,?,?,'INTENDED',?)
                ON CONFLICT (arrival_id, stage) DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, arrivalId);
            p.setString(2, stage.name());
            p.setString(3, jobName);
            p.setString(4, namespace);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.of(r.getObject(1, UUID.class)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insertIntent failed", e);
        }
    }

    /** Write-ahead clock intent (no arrival). @return id, or empty when (stage, run_key) exists. */
    public Optional<UUID> insertClockIntent(Stage stage, String runKey, String jobName,
                                            String launchArgs, String namespace) {
        String sql = """
                INSERT INTO launch_intent (stage, run_key, job_name, status, launch_args, namespace)
                VALUES (?,?,?,'INTENDED',?,?)
                ON CONFLICT DO NOTHING
                RETURNING id""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, stage.name());
            p.setString(2, runKey);
            p.setString(3, jobName);
            p.setString(4, launchArgs);
            p.setString(5, namespace);
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

    /**
     * Re-adopt an ABANDONED intent onto its STILL-LIVE Job (M12/SCRUM-86): the
     * wedged-alive crash-window recovery. A relaunch that crashed after the
     * atomic claim (heartbeat_at nulled) but before the delete+recreate leaves
     * the old wedged Job running; the reconciler adopts it rather than recreate,
     * and RE-ARMS the stale-heartbeat clock (heartbeat_at = now()) so a pod that
     * is still wedged re-goes-stale within one TTL instead of dropping to the
     * activeDeadlineSeconds path. Distinct from markIntentLaunched precisely
     * because the latter must NOT set a heartbeat on a fresh INTENDED promote (a
     * slow-but-healthy boot would be false-flagged before its first real beat).
     */
    public void reAdoptWithHeartbeat(final UUID intentId, final String jobUid) {
        JdbcSupport.exec(ds, "UPDATE launch_intent SET status='LAUNCHED', job_uid=?, "
                + "heartbeat_at=now() WHERE id=?", p -> {
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

    public Optional<java.time.OffsetDateTime> lastAttemptAt(final UUID intentId) {
        try (Connection c = ds.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT last_attempt_at FROM launch_intent WHERE id=?")) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                return r.next()
                        ? Optional.ofNullable(r.getObject(1, java.time.OffsetDateTime.class))
                        : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lastAttemptAt failed", e);
        }
    }

    /** Arrival intents whose CURRENT attempt ended TECH-class: the orphan-sweep worklist. */
    public List<LaunchIntent> launchedArrivalIntentsWithTechCurrentAttempt() {
        String sql = """
                SELECT i.id, i.arrival_id, i.stage, i.job_name, i.status, i.run_key, i.attempt, i.namespace
                FROM launch_intent i JOIN stage_outcome o
                  ON o.intent_id = i.id AND o.attempt = i.attempt
                WHERE i.status='LAUNCHED' AND i.arrival_id IS NOT NULL
                  AND o.outcome IN ('TECH_FAILED')""";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql);
             ResultSet r = p.executeQuery()) {
            List<LaunchIntent> out = new ArrayList<>();
            while (r.next()) {
                out.add(map(r));
            }
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("launchedArrivalIntentsWithTechCurrentAttempt failed", e);
        }
    }

    /**
     * Wedged-but-alive worklist (M12/SCRUM-86, R-47): LAUNCHED arrival intents
     * whose heartbeat fell behind the TTL while the k8s Job is (or looks) still
     * Running, so the Failed-condition path never fires. A NULL heartbeat_at is
     * explicitly NOT stale: a never-heartbeated job (pre-migration, local, or
     * one just claimed for relaunch) stays on the k8s-status path, never here.
     * The cutoff is evaluated server-side against the DB clock that stamps
     * heartbeat_at, so AGT/CRDB clock skew cannot prematurely flag a live job.
     */
    public List<LaunchIntent> launchedArrivalIntentsWithStaleHeartbeat(final long ttlSeconds) {
        String sql = "SELECT id, arrival_id, stage, job_name, status, run_key, attempt, namespace"
                + " FROM launch_intent"
                + " WHERE status='" + LaunchIntent.LAUNCHED + "' AND arrival_id IS NOT NULL"
                + "   AND heartbeat_at IS NOT NULL"
                + "   AND heartbeat_at < now() - (INTERVAL '1 second' * ?)";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setLong(1, ttlSeconds);
            try (ResultSet r = p.executeQuery()) {
                List<LaunchIntent> out = new ArrayList<>();
                while (r.next()) {
                    out.add(map(r));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("launchedArrivalIntentsWithStaleHeartbeat failed", e);
        }
    }

    /**
     * Single atomic relaunch claim shared by BOTH orphan paths (k8s-Failed and
     * stale-heartbeat) so one intent is never double-relaunched within a tick or
     * across incarnations (M12/SCRUM-86, R-47). It flips LAUNCHED -> ABANDONED
     * (write-ahead claim marker: the guard makes a concurrent second claim miss),
     * consumes an attempt slot, stamps last_attempt_at, and clears heartbeat_at
     * so the just-claimed intent is no longer a stale-heartbeat candidate even
     * after the relaunching pod re-marks it LAUNCHED (the new incarnation
     * re-heartbeats). 0 rows returned = another sweep/incarnation already claimed
     * it this tick.
     * @return the new attempt number, or empty when the claim was lost.
     */
    public Optional<Integer> claimForRelaunch(final UUID intentId) {
        String sql = "UPDATE launch_intent"
                + " SET status='" + LaunchIntent.ABANDONED + "', attempt = attempt + 1,"
                + "     last_attempt_at = now(), heartbeat_at = NULL"
                + " WHERE id = ? AND status = '" + LaunchIntent.LAUNCHED + "'"
                + " RETURNING attempt";
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? Optional.of(r.getInt(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("claimForRelaunch failed", e);
        }
    }

    public List<LaunchIntent> intentsForArrival(UUID arrivalId) {
        String sql = "SELECT id, arrival_id, stage, job_name, status, run_key, attempt, namespace "
                + "FROM launch_intent WHERE arrival_id=?";
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

    /** Intents whose CURRENT attempt has no outcome yet (earlier attempts' rows do not count). */
    public List<LaunchIntent> intentsWithoutOutcome() {
        String sql = """
                SELECT i.id, i.arrival_id, i.stage, i.job_name, i.status, i.run_key, i.attempt, i.namespace
                FROM launch_intent i LEFT JOIN stage_outcome o
                  ON o.intent_id = i.id AND o.attempt = i.attempt
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
                Stage.valueOf(r.getString(3)), r.getString(4), r.getString(5), r.getString(6),
                r.getInt(7), r.getString(8));
    }
}
