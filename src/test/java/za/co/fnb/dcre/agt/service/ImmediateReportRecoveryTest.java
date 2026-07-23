package za.co.fnb.dcre.agt.service;

import io.agroal.api.AgroalDataSource;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.kubernetes.client.KubernetesServer;
import io.quarkus.test.kubernetes.client.KubernetesTestServer;
import io.quarkus.test.kubernetes.client.WithKubernetesTestServer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.CrdbTestResource;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-90 recovery contract: a SIGKILLed PRG IMMEDIATE report is recovered by
 * the M12 sweeps. The report is EVENT-scoped one-shot work (per parent arrival),
 * but AGT used to launch it as a CLOCK intent (arrival_id NULL), which BOTH
 * sweeps exclude (arrival_id IS NOT NULL); a killed one-shot report has no next
 * window to self-heal into, so it stranded LAUNCHED/attempt=0 forever. Driving
 * the production path (ReportTrigger.tick -> launcher -> intent + Job on the
 * fabric8 CRUD mock, launch enabled) means these tests fail against the old
 * clock-type launch (attempt stays 0) and pass once IMMEDIATE reports are
 * arrival-scoped (attempt bumps once, exactly like a stage orphan).
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
@TestProfile(ImmediateReportRecoveryTest.RecoveryProfile.class)
class ImmediateReportRecoveryTest {

    /** Launch enabled against the CRUD mock (prod-faithful relaunch: createJob
     *  re-marks the intent LAUNCHED). Backoff 0 so the sweep is never blocked by
     *  the relaunch throttle - the arrival scope, not backoff, is under test. */
    public static class RecoveryProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true", "agt.prg-image", "dcre-prg:test",
                    "agt.orphan-backoff-seconds", "0");
        }
    }

    private static final String CLIENT = "FNBCC01";

    @KubernetesTestServer
    KubernetesServer mockServer;

    @Inject
    ReportTrigger trigger;

    @Inject
    OrphanRelauncher relauncher;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    LeaseService lease;

    @Inject
    AgtConfig config;

    @Inject
    DataSource opsDs;

    @Inject
    @io.quarkus.agroal.DataSource("collections")
    AgroalDataSource collectionsDs;

    private KubernetesClient k8s;

    @BeforeEach
    void setUp() {
        k8s = mockServer.getClient();
        exec(collectionsDs, "CREATE TABLE IF NOT EXISTS prg_report_due_seed ("
                + "client VARCHAR(16) NOT NULL, source_msg_id VARCHAR(35) NOT NULL, reason VARCHAR(16) NOT NULL)");
        exec(collectionsDs, "CREATE OR REPLACE VIEW prg_report_due AS "
                + "SELECT client, source_msg_id, reason FROM prg_report_due_seed");
        exec(collectionsDs, "DELETE FROM prg_report_due_seed");
        exec(opsDs, "UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: this instance holds the lease");
    }

    @Test
    void staleHeartbeatKilledImmediateReportIsRelaunchedOnce() {
        final String msgId = "DCRECC2026072300099001";
        final UUID parent = seedCompletedParentArrival(msgId);
        seedDue(msgId);

        trigger.tick(); // production launch of the PRG IMMEDIATE report

        final String jobName = reportJobName(msgId);
        final UUID intentId = intentIdByJobName(jobName);
        assertNotNull(intentId, "ReportTrigger launched the IMMEDIATE report intent");
        assertEquals(LaunchIntent.LAUNCHED, statusOf(intentId), "the launched report intent is LAUNCHED");
        assertEquals(parent, arrivalIdOf(intentId),
                "SCRUM-90: the IMMEDIATE report intent is arrival-scoped to its parent book");

        setHeartbeat(intentId, "now() - INTERVAL '60 seconds'"); // wedged past the 45s TTL

        relauncher.sweepStaleHeartbeat(Map.of(jobName, jobOf(jobName)));

        assertEquals(1, attemptOf(intentId),
                "a stale-heartbeat IMMEDIATE report is relaunched exactly once (attempt 0 -> 1)");
        assertTrue(heartbeatIsNull(intentId),
                "the atomic claim cleared heartbeat_at so the report is not double-swept");
        assertEquals(LaunchIntent.LAUNCHED, statusOf(intentId),
                "createJob re-marked the report intent LAUNCHED after the ABANDONED claim");
        assertEquals(ArrivalStatus.DAG_COMPLETE, arrivalRepo.arrivalById(parent).orElseThrow().status(),
                "the parent arrival stays DAG_COMPLETE across the report relaunch");
    }

    @Test
    void k8sFailedKilledImmediateReportIsRelaunchedOnce() {
        final String msgId = "DCRECC2026072300099002";
        final UUID parent = seedCompletedParentArrival(msgId);
        seedDue(msgId);

        trigger.tick();

        final String jobName = reportJobName(msgId);
        final UUID intentId = intentIdByJobName(jobName);
        assertNotNull(intentId, "ReportTrigger launched the IMMEDIATE report intent");
        // A Failed Job condition minted a TECH_FAILED outcome on the current attempt.
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 137, "Failed/PodKill"));

        relauncher.sweepTechOrphans(Map.of(jobName, jobOf(jobName)));

        assertEquals(1, attemptOf(intentId),
                "a k8s-Failed IMMEDIATE report is relaunched exactly once (attempt 0 -> 1)");
        assertTrue(intentRepo.lastAttemptAt(intentId).isPresent(), "relaunch stamped last_attempt_at");
        assertEquals(ArrivalStatus.DAG_COMPLETE, arrivalRepo.arrivalById(parent).orElseThrow().status(),
                "the parent arrival stays DAG_COMPLETE across the report relaunch");
    }

    /** The parent source book: a completed request-route arrival whose
     *  (client_token, msg_id_token) the report-due row points at. */
    private UUID seedCompletedParentArrival(final String msgId) {
        return arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req",
                CLIENT + "_" + msgId + ".txt", "sha-" + msgId, CLIENT, msgId,
                ArrivalStatus.DAG_COMPLETE, null,
                "/exchange/claimed/" + CLIENT + "_" + msgId + ".txt").orElseThrow();
    }

    private void seedDue(final String msgId) {
        try (Connection c = collectionsDs.getConnection();
             PreparedStatement p = c.prepareStatement(
                     "INSERT INTO prg_report_due_seed (client, source_msg_id, reason) VALUES (?, ?, ?)")) {
            p.setString(1, CLIENT);
            p.setString(2, msgId);
            p.setString(3, "COMPLETE");
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("seedDue failed", e);
        }
    }

    private static String reportJobName(final String msgId) {
        return JobLauncher.clockJobName(Flow.COL, Stage.PRG, CLIENT + "-imm-" + ReportTrigger.parentDigest(msgId));
    }

    private Job jobOf(final String name) {
        return k8s.batch().v1().jobs().inNamespace(config.namespaceCol()).withName(name).get();
    }

    private UUID intentIdByJobName(final String jobName) {
        try (Connection c = opsDs.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT id FROM launch_intent WHERE job_name=?")) {
            p.setString(1, jobName);
            try (ResultSet r = p.executeQuery()) {
                return r.next() ? r.getObject(1, UUID.class) : null;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("intentIdByJobName failed", e);
        }
    }

    private UUID arrivalIdOf(final UUID intentId) {
        try (Connection c = opsDs.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT arrival_id FROM launch_intent WHERE id=?")) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getObject(1, UUID.class);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("arrivalIdOf failed", e);
        }
    }

    private String statusOf(final UUID intentId) {
        try (Connection c = opsDs.getConnection();
             PreparedStatement p = c.prepareStatement("SELECT status FROM launch_intent WHERE id=?")) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("statusOf failed", e);
        }
    }

    private int attemptOf(final UUID intentId) {
        return queryInt("SELECT attempt FROM launch_intent WHERE id=?", intentId);
    }

    private boolean heartbeatIsNull(final UUID intentId) {
        return queryInt("SELECT (heartbeat_at IS NULL)::INT FROM launch_intent WHERE id=?", intentId) == 1;
    }

    private void setHeartbeat(final UUID intentId, final String expr) {
        exec("UPDATE launch_intent SET heartbeat_at = " + expr + " WHERE id=?", intentId);
    }

    private int queryInt(final String sql, final UUID id) {
        try (Connection c = opsDs.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    private void exec(final String sql, final UUID id) {
        try (Connection c = opsDs.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    private static void exec(final DataSource ds, final String sql) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }
}
