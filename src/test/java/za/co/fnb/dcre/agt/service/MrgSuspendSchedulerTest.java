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
 * SCRUM-91: the GLOBAL MRG suspension CLOCK sweep that replaced the two MSR sweeps.
 * It scans every mandate, so AGT launches ONE sweep per interval window on a pure
 * clock (HcsScheduler/CrwScheduler pattern), NEVER a per-client loop: the run key is
 * client-free (man-mrg-suspend-w&lt;window&gt;), exactly one intent per window even with
 * many clients' arrivals present. The sweep carries the DCRE_MRG_JOB_NAME env
 * (mrgSuspendJob), an identifying window parameter and the dcre_man DB URL; the MRG
 * report windows are left WITHOUT the override (default mrgJob).
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
@TestProfile(MrgSuspendSchedulerTest.MrgSweepProfile.class)
class MrgSuspendSchedulerTest {

    /** Long windows (3600s) keep the deterministic keys stable across ticks. */
    public static class MrgSweepProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true",
                    "agt.mrg-image", "dcre-mrg:test",
                    "agt.mrg-interval-seconds", "3600",
                    "agt.mrg-suspend-interval-seconds", "3600");
        }
    }

    @KubernetesTestServer
    KubernetesServer mockServer;

    @Inject
    MrgSuspendScheduler suspendScheduler;

    @Inject
    MrgScheduler reportScheduler;

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
    void suspendSweepLaunchesExactlyOneGlobalWindowNotPerClient() {
        // Arrivals for THREE clients: the sweep is global, so they must NOT fan out
        // into three per-client windows. Exactly one client-free window is minted.
        insertArrival("onhost-req", "FNBCC01");
        insertArrival("onhost-req", "FNBCC02");
        insertArrival("onhost-req", "FNBRF01");
        long window = suspendWindow();

        suspendScheduler.tick();

        assertEquals("dcre-man", namespaceOfIntent("man-mrg-suspend-w" + window),
                "the single global suspend window lives in dcre-man");
        assertEquals(1, countIntentsLike("man-mrg-suspend-%"),
                "ONE global suspend window per interval, never one per client");
    }

    @Test
    void suspendSweepCarriesTheJobNameOverrideAndItsWindowIdentity() {
        long window = suspendWindow();

        suspendScheduler.tick();

        Job job = job("dcre-man", "man-mrg-suspend-w" + window);
        assertNotNull(job, "suspend clock job created in dcre-man");
        assertEquals("mrgSuspendJob", NamespaceRoutingTest.envOf(job, JobLauncher.MRG_JOB_ENV),
                "suspend sweep selects mrgSuspendJob via DCRE_MRG_JOB_NAME");
        List<String> args = job.getSpec().getTemplate().getSpec().getContainers().get(0).getArgs();
        assertEquals(List.of("window=w" + window), args,
                "MRG receives ONLY the identifying window; the env marker never leaks in, got " + args);
    }

    @Test
    void suspendSweepClockJobCarriesTheManDbUrl() {
        // B2: the mandate schema lives in dcre_man; the sweep pod must get the man URL.
        long window = suspendWindow();

        suspendScheduler.tick();

        Job job = job("dcre-man", "man-mrg-suspend-w" + window);
        assertTrue(NamespaceRoutingTest.dbUrlOf(job).contains("/dcre_man"),
                "suspend clock job env carries the dcre_man URL, got " + NamespaceRoutingTest.dbUrlOf(job));
    }

    @Test
    void suspendSweepClockJobCarriesTheCollectionsDbUrl() {
        // The suspension signal (consecutive terminal-failed collections) lives
        // cross-database in dcre_col, which MRG reads over its SECOND, read-only
        // datasource (ColDatasourceConfig, ${DCRE_COL_DB_URL}). AGT never injected
        // it, so every sweep window died in-cluster with
        // "Connection to localhost:26257 refused" (found live 2026-07-26).
        // The env NAME is asserted as a literal on purpose: it is the cross-repo
        // wire contract with mrg's placeholder, exactly like DCRE_DB_URL above.
        long window = suspendWindow();

        suspendScheduler.tick();

        Job job = job("dcre-man", "man-mrg-suspend-w" + window);
        String colUrl = NamespaceRoutingTest.envOf(job, "DCRE_COL_DB_URL");
        assertEquals(config.serviceDbUrl(), colUrl,
                "the sweep reads dcre_col over the same URL every col stage pod gets");
        assertTrue(colUrl.contains("/dcre_col"), "got " + colUrl);
    }

    @Test
    void sweepWindowIdentityIsStableAcrossTicks() {
        long window = suspendWindow();

        suspendScheduler.tick();
        suspendScheduler.tick();

        assertEquals(1, countIntentsLike("man-mrg-suspend-w" + window),
                "every incarnation computes the same run key; the intent unique key dedupes");
    }

    @Test
    void reconciledRecreateReproducesTheJobNameOverrideAndCleanWindowArg() {
        // Chaos-resume: a SIGKILLed sweep pod is re-created from the intent row
        // alone (createJob). The DCRE_MRG_JOB_NAME env is folded into the durable
        // launch args, so the rebuilt Job must select mrgSuspendJob again and carry
        // the same window, never MRG's mrgJob report default.
        long window = suspendWindow();
        suspendScheduler.tick();
        String name = "man-mrg-suspend-w" + window;
        UUID intentId = UUID.fromString(queryString("SELECT id FROM launch_intent WHERE job_name = ?", name));
        mockServer.getClient().batch().v1().jobs().inNamespace("dcre-man").withName(name).delete();

        launcher.createJob(intentId, null, Stage.MRG, name, "dcre-man");

        Job recreated = job("dcre-man", name);
        assertNotNull(recreated, "sweep job re-created from the durable intent");
        assertEquals("mrgSuspendJob", NamespaceRoutingTest.envOf(recreated, JobLauncher.MRG_JOB_ENV),
                "recreate must reproduce the job-name override from the intent row");
        assertEquals(config.serviceDbUrl(), NamespaceRoutingTest.envOf(recreated, "DCRE_COL_DB_URL"),
                "the dcre_col URL rides the same durable env args, so a re-created sweep "
                        + "still reaches the collections DB instead of falling back to localhost");
        List<String> args = recreated.getSpec().getTemplate().getSpec().getContainers().get(0).getArgs();
        assertEquals(List.of("window=w" + window), args,
                "recreated program args stay the same identifying window, got " + args);
    }

    @Test
    void reportWindowDoesNotCarryTheJobNameOverride() {
        // The MRG report window is the same Stage.MRG on the same image; it must
        // stay on the mrgJob default, or a report window would silently run the
        // suspension sweep instead of emitting the report.
        insertArrival("onhost-req-man", "FNBCC01");
        long window = PrgScheduler.window(Instant.now().getEpochSecond(), config.mrgIntervalSeconds());

        reportScheduler.tick();

        Job job = job("dcre-man", "man-mrg-fnbcc01-w" + window);
        assertNotNull(job, "MRG report window job created in dcre-man");
        boolean overridden = job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
                .anyMatch(e -> JobLauncher.MRG_JOB_ENV.equals(e.getName()));
        assertFalse(overridden, "report window must not carry the DCRE_MRG_JOB_NAME override");
        boolean colWired = job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
                .anyMatch(e -> "DCRE_COL_DB_URL".equals(e.getName()));
        assertFalse(colWired, "only the suspension sweep reads dcre_col; the report window "
                + "stays on dcre_man alone, so the col URL is launch-scoped, never stage-blanket");
    }

    private long suspendWindow() {
        return PrgScheduler.window(Instant.now().getEpochSecond(), config.mrgSuspendIntervalSeconds());
    }

    private Job job(String namespace, String name) {
        return mockServer.getClient().batch().v1().jobs().inNamespace(namespace).withName(name).get();
    }

    private UUID insertArrival(String route, String client) {
        String tag = UUID.randomUUID().toString().substring(0, 8);
        return arrivalRepo.insertArrival(UUID.randomUUID(), route, client + "_MRG" + tag + ".txt",
                "sha-mrg-" + tag, client, "MMRG" + tag, ArrivalStatus.DAG_RUNNING, null,
                "/exchange/claimed/" + client + "_MRG" + tag + ".txt").orElseThrow();
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
