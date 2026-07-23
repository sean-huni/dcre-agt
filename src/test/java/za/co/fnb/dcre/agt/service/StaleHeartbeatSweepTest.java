package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
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
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M12/SCRUM-86 (R-47): AGT stale-heartbeat orphan detection - the wedged-but-alive
 * path. Run against the fabric8 CRUD mock with launch ENABLED so the relaunch
 * is prod-faithful (createJob re-marks the intent LAUNCHED), which is what makes
 * the atomic-claim double-relaunch test meaningful: the guard has to hold once
 * the intent is LAUNCHED again, not only while it sits ABANDONED in a
 * launch-disabled unit test.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
@TestProfile(StaleHeartbeatSweepTest.StaleHeartbeatProfile.class)
class StaleHeartbeatSweepTest {

    /** Prod-faithful: launch enabled against the CRUD mock; CRR image feeds the
     *  Job spec build of the relaunch. Default heartbeat TTL is 45s. Backoff is
     *  set to 0 ON PURPOSE: with the default 60s backoff, the first relaunch's
     *  last_attempt_at stamp would block the second sweep and the "exactly once"
     *  assertion would pass for the WRONG reason (backoff, not the atomic claim /
     *  heartbeat reset). At 0 the claim is the only thing that can stop a second
     *  relaunch, so the test genuinely exercises it. */
    public static class StaleHeartbeatProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true", "agt.crr-image", "dcre-crr:test",
                    "agt.orphan-backoff-seconds", "0");
        }
    }

    @KubernetesTestServer
    KubernetesServer mockServer;

    @Inject
    OrphanRelauncher relauncher;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    DataSource ds;

    private KubernetesClient k8s;

    @BeforeEach
    void client() {
        k8s = mockServer.getClient();
    }

    @Test
    void staleHeartbeatWedgedJobRelaunchesOnce() {
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival();
        UUID intentId = launchedWithJob(arrivalId, name);
        String wedgedUid = uidOf(name);
        setHeartbeat(intentId, "now() - INTERVAL '60 seconds'"); // stale vs the 45s TTL

        relauncher.sweepStaleHeartbeat(Map.of(name, jobOf(name)));

        assertEquals(1, attemptOf(intentId), "a stale heartbeat relaunches once, bumping the attempt");
        Job recreated = jobOf(name);
        assertNotNull(recreated, "same-identity relaunch recreates the Job");
        assertNotEquals(wedgedUid, recreated.getMetadata().getUid(),
                "the wedged Job object was deleted and recreated (new uid)");
        assertTrue(heartbeatIsNull(intentId),
                "the claim clears heartbeat_at so the relaunched intent is no longer a stale candidate");
        assertEquals(LaunchIntent.LAUNCHED, statusOf(intentId),
                "createJob re-marks the intent LAUNCHED after the ABANDONED claim (launch enabled)");
    }

    @Test
    void freshHeartbeatNotTouched() {
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival();
        UUID intentId = launchedWithJob(arrivalId, name);
        String uid = uidOf(name);
        setHeartbeat(intentId, "now()"); // beating right now

        relauncher.sweepStaleHeartbeat(Map.of(name, jobOf(name)));

        assertEquals(0, attemptOf(intentId), "a fresh heartbeat is never swept");
        assertEquals(uid, uidOf(name), "the live Job is left in place (not deleted/recreated)");
    }

    @Test
    void nullHeartbeatNotTouched() {
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival();
        UUID intentId = launchedWithJob(arrivalId, name);
        String uid = uidOf(name);
        // heartbeat_at stays NULL: a never-heartbeated job stays on the k8s-status
        // path, never the stale-heartbeat path (pre-migration/local jobs).

        relauncher.sweepStaleHeartbeat(Map.of(name, jobOf(name)));

        assertEquals(0, attemptOf(intentId), "NULL heartbeat is not stale: k8s path only");
        assertEquals(uid, uidOf(name), "the live Job is left in place");
    }

    @Test
    void k8sFailedAndStaleHeartbeatRelaunchExactlyOnce() {
        // The one intent is flagged by BOTH paths in the same tick: a TECH_FAILED
        // outcome (k8s Failed condition) AND a stale heartbeat. It must relaunch
        // exactly once - attempt 0 -> 1, never 0 -> 2.
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival();
        UUID intentId = launchedWithJob(arrivalId, name);
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 137, "Failed/PodKill"));
        setHeartbeat(intentId, "now() - INTERVAL '60 seconds'");

        // One reconciler tick: k8s-Failed sweep first, then stale-heartbeat sweep
        // (the exact order Reconciler.tick uses). Launch is enabled, so the first
        // sweep's createJob re-marks the intent LAUNCHED - the realistic state the
        // second sweep would race. The claim cleared heartbeat_at, so the second
        // sweep's query no longer matches: no second relaunch.
        Map<String, Job> live = Map.of(name, jobOf(name));
        relauncher.sweepTechOrphans(live);
        assertEquals(1, attemptOf(intentId), "the k8s-Failed sweep relaunches once");
        assertEquals(LaunchIntent.LAUNCHED, statusOf(intentId), "createJob re-marked it LAUNCHED");
        assertTrue(heartbeatIsNull(intentId), "the claim cleared heartbeat_at");

        relauncher.sweepStaleHeartbeat(Map.of(name, jobOf(name)));

        assertEquals(1, attemptOf(intentId),
                "the shared atomic claim keeps the second (stale-heartbeat) sweep from re-relaunching: 1, not 2");
        assertFalse(intentRepo.launchedArrivalIntentsWithStaleHeartbeat(45).stream()
                        .anyMatch(i -> i.id().equals(intentId)),
                "a just-relaunched intent (heartbeat cleared) is off the stale worklist");
    }

    @Test
    void claimForRelaunchIsSingleWinnerAcrossConcurrentSweeps() {
        // The concurrent / multi-incarnation guard, isolated at the repo seam:
        // two claims against the same LAUNCHED intent (as two incarnations, or
        // both orphan sweeps, would issue) must NOT both bump the attempt. The
        // first flips LAUNCHED -> ABANDONED; the second's status='LAUNCHED' guard
        // then misses, so it returns empty and the attempt moves exactly once.
        String name = "col-crr-" + suffix();
        UUID intentId = launchedWithJob(insertArrival(), name);

        var first = intentRepo.claimForRelaunch(intentId);
        var second = intentRepo.claimForRelaunch(intentId);

        assertEquals(1, first.orElseThrow(), "first claim wins: attempt 0 -> 1");
        assertTrue(second.isEmpty(), "second claim loses (no longer LAUNCHED): no double bump");
        assertEquals(1, attemptOf(intentId), "the intent's attempt advanced exactly once");
    }

    private UUID launchedWithJob(final UUID arrivalId, final String name) {
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, "dcre-col").orElseThrow();
        Job job = k8s.batch().v1().jobs().inNamespace("dcre-col").resource(
                new JobBuilder().withNewMetadata().withName(name).withNamespace("dcre-col")
                        .addToLabels(JobLauncher.LABEL_MANAGED_BY, "agt").endMetadata()
                        .build()).create();
        intentRepo.markIntentLaunched(intentId, job.getMetadata().getUid());
        return intentId;
    }

    private Job jobOf(final String name) {
        return k8s.batch().v1().jobs().inNamespace("dcre-col").withName(name).get();
    }

    private String uidOf(final String name) {
        Job j = jobOf(name);
        return j != null ? j.getMetadata().getUid() : null;
    }

    private UUID insertArrival() {
        String tag = suffix();
        return arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req",
                "FNBCC01_M" + tag + ".txt", "sha-" + tag, "FNBCC01", "M" + tag,
                ArrivalStatus.DAG_RUNNING, null, "/exchange/claimed/FNBCC01_M" + tag + ".txt").orElseThrow();
    }

    private void setHeartbeat(final UUID intentId, final String expr) {
        exec("UPDATE launch_intent SET heartbeat_at = " + expr + " WHERE id=?", intentId);
    }

    private int attemptOf(final UUID intentId) {
        return queryInt("SELECT attempt FROM launch_intent WHERE id=?", intentId);
    }

    private String statusOf(final UUID intentId) {
        try (Connection c = ds.getConnection();
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

    private boolean heartbeatIsNull(final UUID intentId) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(
                "SELECT heartbeat_at IS NULL FROM launch_intent WHERE id=?")) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("heartbeatIsNull failed", e);
        }
    }

    private void exec(final String sql, final UUID id) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    private int queryInt(final String sql, final UUID id) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
