package za.co.fnb.dcre.agt.service;

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
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * M10/SCRUM-79 MRG clock windows: per-client run keys over the arrival-derived
 * client set, RESTRICTED to mandate-capable clients (interim agt.man-clients,
 * normalized trim+uppercase like pay-clients), always in the MAN namespace
 * (client-independent flow), window identity stable across ticks (clock-intent
 * dedupe), and an on-demand chaos/run-mrg-&lt;client&gt; manual trigger.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
@TestProfile(MrgSchedulerTest.MrgProfile.class)
class MrgSchedulerTest {

    /** man-clients is deliberately messy (m4 normalization); FNBCC02 is
     *  deliberately absent = not mandate-capable. Long window (3600s) keeps
     *  the window key stable across in-test ticks. */
    public static class MrgProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true",
                    "agt.mrg-image", "dcre-mrg:test",
                    "agt.man-clients", " fnbcc01 , FNBRF01 ",
                    "agt.mrg-interval-seconds", "3600");
        }
    }

    @KubernetesTestServer
    KubernetesServer mockServer;

    @Inject
    MrgScheduler scheduler;

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
    void manCapableClientsGetWindowsInTheManNamespaceOnly() {
        insertArrival("onhost-req", "FNBCC01");
        insertArrival("onhost-req", "FNBCC02");
        insertArrival("onhost-req", "FNBRF01");
        long window = PrgScheduler.window(Instant.now().getEpochSecond(), config.mrgIntervalSeconds());

        scheduler.tick();

        assertEquals("dcre-man", namespaceOfIntent("man-mrg-fnbcc01-w" + window),
                "man-capable collections client: MRG window in dcre-man");
        assertEquals("dcre-man", namespaceOfIntent("man-mrg-fnbrf01-w" + window),
                "pay client rides MAN for MRG: the flow is client-independent");
        assertEquals(0, countIntentsLike("man-mrg-fnbcc02-%"),
                "FNBCC02 is not in agt.man-clients: no MRG window");
    }

    @Test
    void mrgClockJobCarriesTheManDbUrl() {
        // B2 (SCRUM-79 review): MRG persists its report registry in dcre_man;
        // the clock job env must carry the man URL, never dcre_col.
        insertArrival("onhost-req", "FNBCC01");
        long window = PrgScheduler.window(Instant.now().getEpochSecond(), config.mrgIntervalSeconds());

        scheduler.tick();

        io.fabric8.kubernetes.api.model.batch.v1.Job job = mockServer.getClient().batch().v1().jobs()
                .inNamespace("dcre-man").withName("man-mrg-fnbcc01-w" + window).get();
        org.junit.jupiter.api.Assertions.assertNotNull(job, "MRG clock job created in dcre-man");
        String dbUrl = NamespaceRoutingTest.dbUrlOf(job);
        assertTrue(dbUrl.contains("/dcre_man"),
                "MRG clock job env carries the dcre_man URL, got " + dbUrl);
    }

    @Test
    void windowIdentityIsStableAcrossTicks() {
        insertArrival("onhost-req", "FNBCC01");
        long window = PrgScheduler.window(Instant.now().getEpochSecond(), config.mrgIntervalSeconds());

        scheduler.tick();
        scheduler.tick();

        assertEquals(1, countIntentsLike("man-mrg-fnbcc01-w" + window),
                "every incarnation computes the same run key; the intent unique key dedupes");
    }

    @Test
    void manualTriggerLaunchesADistinctManualWindow() throws IOException {
        insertArrival("onhost-req", "FNBCC01");
        // PrgScheduler-clone semantics: the trigger carries the RAW client token
        // (run-prg-FNBRF01 precedent; case-sensitive on the in-cluster volume).
        Path trigger = Path.of(config.exchangeRoot(), "chaos", "run-mrg-FNBCC01");
        Files.createDirectories(trigger.getParent());
        Files.writeString(trigger, "");

        scheduler.tick();

        assertFalse(Files.exists(trigger), "trigger file consumed");
        assertEquals(1, countIntentsLike("man-mrg-fnbcc01-manual-%"),
                "manual window minted with its own run key");
        assertEquals("dcre-man", namespaceOfIntentLike("man-mrg-fnbcc01-manual-%"));
    }

    private void insertArrival(String route, String client) {
        String tag = UUID.randomUUID().toString().substring(0, 8);
        arrivalRepo.insertArrival(UUID.randomUUID(), route, client + "_MRG" + tag + ".txt",
                "sha-mrg-" + tag, client, "MMRG" + tag, ArrivalStatus.DAG_RUNNING, null,
                "/exchange/claimed/" + client + "_MRG" + tag + ".txt").orElseThrow();
    }

    private String namespaceOfIntent(String jobName) {
        return queryString("SELECT namespace FROM launch_intent WHERE job_name = ?", jobName);
    }

    private String namespaceOfIntentLike(String pattern) {
        return queryString(
                "SELECT namespace FROM launch_intent WHERE job_name LIKE ? ORDER BY created_at DESC LIMIT 1",
                pattern);
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
