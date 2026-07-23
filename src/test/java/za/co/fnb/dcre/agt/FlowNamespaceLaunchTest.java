package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.service.JobLauncher;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-70: the write-ahead intent carries the resolved flow's job-name prefix
 * AND target namespace (durable, so a reconciled recreate lands in the same
 * namespace). launch-enabled stays false (%test): the K8s create is gated off
 * and the ledger row is the observable seam.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@TestProfile(FlowNamespaceLaunchTest.LaunchProfile.class)
class FlowNamespaceLaunchTest {

    /** Images feed the Job spec build that precedes the launch gate. */
    public static class LaunchProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.crr-image", "dcre-crr:test", "agt.crw-image", "dcre-crw:test");
        }
    }

    @Inject
    JobLauncher launcher;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    AgtConfig config;

    @Inject
    DataSource ds;

    @Test
    void dcArrivalLaunchesWithColPrefixIntoTheColNamespace() {
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01", "FLN1");
        launcher.launch(arrivalId, Stage.CRR);
        assertIntent(arrivalId, "col-crr-", "dcre-col");
    }

    @Test
    void endoArrivalLaunchesWithPayPrefixIntoThePayNamespace() {
        UUID arrivalId = insertArrival("onhost-req-endo", "FNBCC01", "FLN2");
        launcher.launch(arrivalId, Stage.CRR);
        assertIntent(arrivalId, "pay-crr-", "dcre-pay");
    }

    @Test
    void fintRespReaderFollowsTheClientsFlow() {
        // Interim R-42 map: FNBRF01 is the pay client; FNBCC01 stays collections.
        UUID payArrival = insertArrival("fint-resp", "FNBRF01", "FLN3");
        launcher.launch(payArrival, Stage.PXR);
        assertIntent(payArrival, "pay-pxr-", "dcre-pay");

        UUID colArrival = insertArrival("fint-resp", "FNBCC01", "FLN4");
        launcher.launch(colArrival, Stage.IXR);
        assertIntent(colArrival, "col-ixr-", "dcre-col");
    }

    @Test
    void clockLaunchPersistsTheFlowNamespaceOnTheIntent() {
        String runKey = "2026-07-21-w" + suffix();
        launcher.launchClock(Flow.COL, Stage.CRW, runKey, List.of("run.date=2026-07-21", "window=" + runKey));
        String[] row = intentByJobName("col-crw-" + runKey.toLowerCase());
        assertEquals("dcre-col", row[1], "clock intent persists the flow namespace");
    }

    @Test
    void manArrivalIntentTargetsTheManNamespaceAndFailsFastWithoutImage() {
        // M10/SCRUM-79: the write-ahead intent lands first (man- prefix,
        // dcre-man namespace); the unset image then fails the launch fast
        // (SCRUM-33 semantics: launch-disabled by default until 2.3 images).
        UUID arrivalId = insertArrival("onhost-req-man", "FNBCC01", "FLN5");
        IllegalStateException e = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> launcher.launch(arrivalId, Stage.MRR));
        assertTrue(e.getMessage().contains("AGT_MRR_IMAGE"), "got: " + e.getMessage());
        assertIntent(arrivalId, "man-mrr-", "dcre-man");
    }

    @Test
    void manRespArrivalIntentTargetsTheManNamespace() {
        UUID arrivalId = insertArrival("fint-resp-man", "FNBRF01", "FLN6");
        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                () -> launcher.launch(arrivalId, Stage.MAR), "no MAR image: fail fast");
        assertIntent(arrivalId, "man-mar-", "dcre-man");
    }

    @Test
    void flowConfigDefaultsMatchTheSpec() {
        assertEquals("dcre-col", config.namespaceCol());
        assertEquals("dcre-pay", config.namespacePay());
        assertEquals("dcre-man", config.namespaceMan());
        assertEquals(java.util.Set.of("FNBRF01"), config.payClients(),
                "interim R-42 default until the R-14 client table lands (Set semantics, m4)");
        assertEquals("dcre", config.namespace(), "AGT's own/control namespace is unchanged");
    }

    private void assertIntent(UUID arrivalId, String namePrefix, String namespace) {
        String[] row = intentByArrival(arrivalId);
        assertTrue(row[0].startsWith(namePrefix),
                "job name prefixed by resolved flow: expected " + namePrefix + "* got " + row[0]);
        assertEquals(namespace, row[1], "intent persists the flow namespace for " + row[0]);
        assertTrue(row[0].length() <= 63, "K8s name limit: " + row[0]);
    }

    private String[] intentByArrival(UUID arrivalId) {
        return queryRow("SELECT job_name, namespace FROM launch_intent WHERE arrival_id = ?", arrivalId.toString());
    }

    private String[] intentByJobName(String jobName) {
        return queryRow("SELECT job_name, namespace FROM launch_intent WHERE job_name = ?", jobName);
    }

    private String[] queryRow(String sql, String param) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, param.length() == 36 ? UUID.fromString(param) : param);
            try (ResultSet r = p.executeQuery()) {
                assertTrue(r.next(), "no launch_intent row for " + param);
                return new String[] {r.getString(1), r.getString(2)};
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    private UUID insertArrival(String route, String client, String tag) {
        return arrivalRepo.insertArrival(UUID.randomUUID(), route,
                client + "_PBSR_" + tag + ".txt", "sha-" + tag + "-" + suffix(), client,
                "M" + tag + suffix(), ArrivalStatus.DAG_RUNNING, null,
                "/exchange/claimed/" + client + "_" + tag + ".txt").orElseThrow();
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
