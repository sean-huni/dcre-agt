package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.batch.v1.Job;
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
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M10/SCRUM-78 (A-71): the two GLOBAL MSR CLOCK sweeps. MSR's expiry
 * (WHERE state='PDNG') and suspend (WHERE state='ACCP') sweeps scan every mandate
 * in dcre_man, so AGT launches ONE sweep per interval window on a pure clock
 * (HcsScheduler/CrwScheduler pattern), NEVER a per-client loop: the run key is
 * client-free (man-msr-expiry-w&lt;window&gt;), exactly one intent per window even
 * with many clients' arrivals present. Each sweep carries the DCRE_MSR_JOB_NAME env
 * (msrExpiryJob / msrSuspendJob), a deterministic sweep.instant (the window-start
 * instant, never now()), and the dcre_man DB URL; the MAR -&gt; MSR projection DAG
 * launch is left WITHOUT the override (default msrJob).
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
@TestProfile(MsrSweepSchedulerTest.MsrSweepProfile.class)
class MsrSweepSchedulerTest {

    /** Long windows (3600s) keep the deterministic keys stable across ticks. */
    public static class MsrSweepProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true",
                    "agt.msr-image", "dcre-msr:test",
                    "agt.msr-expiry-interval-seconds", "3600",
                    "agt.msr-suspend-interval-seconds", "3600");
        }
    }

    @KubernetesTestServer
    KubernetesServer mockServer;

    @Inject
    MsrExpiryScheduler expiryScheduler;

    @Inject
    MsrSuspendScheduler suspendScheduler;

    @Inject
    JobLauncher launcher;

    @Inject
    LeaseService lease;

    @Inject
    AgtConfig config;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    DataSource ds;

    @BeforeEach
    void holdLease() {
        exec("UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: lease held");
    }

    @Test
    void expirySweepLaunchesExactlyOneGlobalWindowNotPerClient() {
        // Arrivals for THREE clients: the sweep is global, so they must NOT fan out
        // into three per-client windows. Exactly one client-free window is minted.
        insertArrival("onhost-req", "FNBCC01");
        insertArrival("onhost-req", "FNBCC02");
        insertArrival("onhost-req", "FNBRF01");
        long window = expiryWindow();

        expiryScheduler.tick();

        assertEquals("dcre-man", namespaceOfIntent("man-msr-expiry-w" + window),
                "the single global expiry window lives in dcre-man");
        assertEquals(1, countIntentsLike("man-msr-expiry-%"),
                "ONE global expiry window per interval, never one per client");
    }

    @Test
    void suspendSweepLaunchesExactlyOneGlobalWindowNotPerClient() {
        insertArrival("onhost-req", "FNBCC01");
        insertArrival("onhost-req", "FNBRF01");
        long window = suspendWindow();

        suspendScheduler.tick();

        assertEquals("dcre-man", namespaceOfIntent("man-msr-suspend-w" + window),
                "the single global suspend window lives in dcre-man");
        assertEquals(1, countIntentsLike("man-msr-suspend-%"),
                "ONE global suspend window per interval, never one per client");
    }

    @Test
    void expirySweepCarriesJobNameOverrideAndDeterministicSweepInstant() {
        long window = expiryWindow();

        expiryScheduler.tick();

        Job job = job("dcre-man", "man-msr-expiry-w" + window);
        assertNotNull(job, "expiry clock job created in dcre-man");
        assertEquals("msrExpiryJob", NamespaceRoutingTest.envOf(job, JobLauncher.MSR_JOB_ENV),
                "expiry sweep selects msrExpiryJob via DCRE_MSR_JOB_NAME");
        List<String> args = job.getSpec().getTemplate().getSpec().getContainers().get(0).getArgs();
        assertEquals(List.of("sweep.instant=" + expectedSweepInstant(window, config.msrExpiryIntervalSeconds())), args,
                "MSR receives ONLY the deterministic sweep.instant; the env marker never leaks in, got " + args);
    }

    @Test
    void suspendSweepCarriesTheMsrSuspendJobNameOverride() {
        long window = suspendWindow();

        suspendScheduler.tick();

        Job job = job("dcre-man", "man-msr-suspend-w" + window);
        assertNotNull(job, "suspend clock job created in dcre-man");
        assertEquals("msrSuspendJob", NamespaceRoutingTest.envOf(job, JobLauncher.MSR_JOB_ENV),
                "suspend sweep selects msrSuspendJob via DCRE_MSR_JOB_NAME");
        List<String> args = job.getSpec().getTemplate().getSpec().getContainers().get(0).getArgs();
        assertEquals(List.of("sweep.instant=" + expectedSweepInstant(window, config.msrSuspendIntervalSeconds())), args,
                "suspend sweep also receives only the deterministic sweep.instant, got " + args);
    }

    @Test
    void msrSweepClockJobsCarryTheManDbUrl() {
        // B2: MSR owns its schema in dcre_man; the sweep pod must get the man URL.
        long window = expiryWindow();

        expiryScheduler.tick();

        Job job = job("dcre-man", "man-msr-expiry-w" + window);
        assertTrue(NamespaceRoutingTest.dbUrlOf(job).contains("/dcre_man"),
                "MSR sweep clock job env carries the dcre_man URL, got " + NamespaceRoutingTest.dbUrlOf(job));
    }

    @Test
    void sweepWindowIdentityIsStableAcrossTicks() {
        long window = expiryWindow();

        expiryScheduler.tick();
        expiryScheduler.tick();

        assertEquals(1, countIntentsLike("man-msr-expiry-w" + window),
                "every incarnation computes the same run key; the intent unique key dedupes");
    }

    @Test
    void reconciledRecreateReproducesTheJobNameOverrideAndCleanSweepInstant() {
        // Chaos-resume: a SIGKILLed sweep pod is re-created from the intent row
        // alone (createJob). The DCRE_MSR_JOB_NAME env is folded into the durable
        // launch args, so the rebuilt Job must select msrExpiryJob again and carry
        // the same deterministic sweep.instant, never MSR's msrJob default.
        long window = expiryWindow();
        expiryScheduler.tick();
        String name = "man-msr-expiry-w" + window;
        UUID intentId = UUID.fromString(queryString("SELECT id FROM launch_intent WHERE job_name = ?", name));
        mockServer.getClient().batch().v1().jobs().inNamespace("dcre-man").withName(name).delete();

        launcher.createJob(intentId, null, Stage.MSR, name, "dcre-man");

        Job recreated = job("dcre-man", name);
        assertNotNull(recreated, "sweep job re-created from the durable intent");
        assertEquals("msrExpiryJob", NamespaceRoutingTest.envOf(recreated, JobLauncher.MSR_JOB_ENV),
                "recreate must reproduce the job-name override from the intent row");
        List<String> args = recreated.getSpec().getTemplate().getSpec().getContainers().get(0).getArgs();
        assertEquals(List.of("sweep.instant=" + expectedSweepInstant(window, config.msrExpiryIntervalSeconds())), args,
                "recreated program args stay the same deterministic sweep.instant, got " + args);
    }

    @Test
    void dagMsrProjectionLaunchDoesNotCarryTheJobNameOverride() {
        // MAR -> MSR projection DAG launch (SCRUM-79): arrival-scoped serviceJob,
        // NOT a sweep. It must stay on MSR's msrJob default (no override env), or
        // the response-DAG projection would silently run a sweep instead.
        UUID arrivalId = insertArrival("fint-resp-man", "FNBCC01");
        launcher.launch(arrivalId, Stage.MSR);

        Job job = job("dcre-man", JobLauncher.jobName(Flow.MAN, Stage.MSR, arrivalId));
        assertNotNull(job, "DAG MSR projection job created in dcre-man");
        boolean overridden = job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
                .anyMatch(e -> JobLauncher.MSR_JOB_ENV.equals(e.getName()));
        assertFalse(overridden,
                "DAG msrJob launch must not carry the DCRE_MSR_JOB_NAME override");
    }

    private long expiryWindow() {
        return PrgScheduler.window(Instant.now().getEpochSecond(), config.msrExpiryIntervalSeconds());
    }

    private long suspendWindow() {
        return PrgScheduler.window(Instant.now().getEpochSecond(), config.msrSuspendIntervalSeconds());
    }

    private static String expectedSweepInstant(long window, long intervalSeconds) {
        return Instant.ofEpochSecond(window * intervalSeconds).toString();
    }

    private Job job(String namespace, String name) {
        return mockServer.getClient().batch().v1().jobs().inNamespace(namespace).withName(name).get();
    }

    private UUID insertArrival(String route, String client) {
        String tag = UUID.randomUUID().toString().substring(0, 8);
        return arrivalRepo.insertArrival(UUID.randomUUID(), route, client + "_MSR" + tag + ".txt",
                "sha-msr-" + tag, client, "MMSR" + tag, ArrivalStatus.DAG_RUNNING, null,
                "/exchange/claimed/" + client + "_MSR" + tag + ".txt").orElseThrow();
    }

    private String namespaceOfIntent(String jobName) {
        return queryString("SELECT namespace FROM launch_intent WHERE job_name = ?", jobName);
    }

    private String queryString(String sql, String param) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, param);
            try (ResultSet r = p.executeQuery()) {
                assertTrue(r.next(), "no row for " + param);
                return r.getString(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed", e);
        }
    }

    private long countIntentsLike(String pattern) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(
                "SELECT count(*) FROM launch_intent WHERE job_name LIKE ?")) {
            p.setString(1, pattern);
            try (ResultSet r = p.executeQuery()) {
                assertTrue(r.next());
                return r.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed", e);
        }
    }

    private void exec(String sql) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }
}
