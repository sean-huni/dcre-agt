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
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-70 cross-namespace behavior against the fabric8 CRUD mock API server:
 * the Reconciler unions managed Jobs across the control + flow namespaces,
 * OutcomeWatcher and OrphanRelauncher act in the INTENT's namespace (never a
 * blanket control-namespace fallback), and PRG clock windows route by the
 * client's flow (interim R-42 pay-clients map).
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
@TestProfile(NamespaceRoutingTest.RoutingProfile.class)
class NamespaceRoutingTest {

    /** Real launcher against the CRUD mock server; CRR/MRR images feed the
     *  Job spec builds. pay-clients is deliberately messy (m4): the
     *  reader must trim + uppercase, so FNBRF01 still routes PAY end-to-end. */
    public static class RoutingProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "true", "agt.crr-image", "dcre-crr:test",
                    "agt.mrr-image", "dcre-mrr:test",
                    "agt.ctv-image", "dcre-ctv:test",
                    "agt.pay-clients", " fnbrf01 ");
        }
    }

    @KubernetesTestServer
    KubernetesServer mockServer;

    @Inject
    Reconciler reconciler;

    @Inject
    OutcomeWatcher watcher;

    @Inject
    OrphanRelauncher relauncher;

    @Inject
    PrgScheduler prgScheduler;

    @Inject
    MrgScheduler mrgScheduler;

    @Inject
    JobLauncher launcher;

    @Inject
    LeaseService lease;

    @Inject
    AgtConfig config;

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
    void reconcilerUnionsManagedJobsAcrossControlAndFlowNamespaces() {
        createJob("dcre", "dcre-legacy-w1", false);
        createJob("dcre-col", "col-crr-" + suffix(), false);
        createJob("dcre-pay", "pay-pxr-" + suffix(), false);
        createJob("dcre-man", "man-mrr-" + suffix(), false);

        Map<String, Job> live = reconciler.liveManagedJobs();

        Set<String> namespaces = new java.util.HashSet<>();
        live.values().forEach(j -> namespaces.add(j.getMetadata().getNamespace()));
        assertTrue(namespaces.containsAll(Set.of("dcre", "dcre-col", "dcre-pay", "dcre-man")),
                "managed Jobs from all four namespaces in one union, got " + namespaces);
    }

    @Test
    void outcomeWatcherObservesInTheIntentNamespace() throws IOException {
        String name = "pay-crr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req-endo", "FNBRF01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, "dcre-pay").orElseThrow();
        // The CRUD mock server assigns its own uid: mark launched with the REAL one.
        Job created = createJob("dcre-pay", name, true);
        intentRepo.markIntentLaunched(intentId, created.getMetadata().getUid());
        writeOutcomeSeam(name, "BUSINESS_ACCEPTED");

        watcher.observe(intentOf(arrivalId, intentId));

        assertEquals(Outcome.BUSINESS_ACCEPTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "observation reads the Job from the intent's namespace");
    }

    @Test
    void outcomeWatcherNeverFallsBackToTheControlNamespace() throws IOException {
        String name = "pay-crr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req-endo", "FNBRF01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, "dcre-pay").orElseThrow();
        // The Job exists ONLY in the control namespace, uid-matched to the
        // intent so ONLY the namespace can disqualify it. The seam file is
        // present, so a control-namespace lookup WOULD record an outcome.
        Job controlJob = createJob("dcre", name, true);
        intentRepo.markIntentLaunched(intentId, controlJob.getMetadata().getUid());
        writeOutcomeSeam(name, "BUSINESS_ACCEPTED");

        watcher.observe(intentOf(arrivalId, intentId));

        assertTrue(outcomeRepo.outcomesForArrival(arrivalId).isEmpty(),
                "no outcome may be minted from a control-namespace lookup");
    }

    @Test
    void orphanRelauncherClearsAndRecreatesInTheIntentNamespace() {
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, "dcre-col").orElseThrow();
        Job dead = createJob("dcre-col", name, false);
        String deadUid = dead.getMetadata().getUid();
        intentRepo.markIntentLaunched(intentId, deadUid);
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 137, "Failed/PodKill"));

        relauncher.relaunchOrExhaust(intentOf(arrivalId, intentId), dead);

        Job recreated = k8s.batch().v1().jobs().inNamespace("dcre-col").withName(name).get();
        assertNotNull(recreated, "same-identity relaunch lands back in the flow namespace");
        assertNotEquals(deadUid, recreated.getMetadata().getUid(),
                "the dead Job was deleted in dcre-col first (409-free recreate)");
        assertNull(k8s.batch().v1().jobs().inNamespace("dcre").withName(name).get(),
                "the control namespace never hosts flow-namespace stage Jobs");
    }

    @Test
    void prgClockWindowsRouteByClientFlow() {
        insertArrival("onhost-req", "FNBRF01");
        insertArrival("onhost-req", "FNBCC01");
        exec("UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: lease held");

        prgScheduler.tick();

        assertEquals("dcre-pay", namespaceOfIntentLike("pay-prg-fnbrf01-w%"),
                "R-42 pay client: PRG window in the pay namespace with the pay- prefix");
        assertEquals("dcre-col", namespaceOfIntentLike("col-prg-fnbcc01-w%"),
                "collections client: PRG window stays col-");
    }

    @Test
    void manStageJobCarriesTheManDbUrlAndCollectionsKeepsCol() {
        // B2 (SCRUM-79 review): the nine M-services own their schema in
        // dcre_man; handing them the dcre_col URL would build it there.
        UUID manArrival = insertArrival("onhost-req-man", "FNBCC01");
        launcher.launch(manArrival, Stage.MRR);
        Job manJob = k8s.batch().v1().jobs().inNamespace("dcre-man")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.MAN, Stage.MRR, manArrival)).get();
        assertNotNull(manJob, "MRR job created in dcre-man");
        assertTrue(dbUrlOf(manJob).contains("/dcre_man"),
                "man stage job env carries the dcre_man URL, got " + dbUrlOf(manJob));

        UUID colArrival = insertArrival("onhost-req", "FNBCC01");
        launcher.launch(colArrival, Stage.CRR);
        Job colJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRR, colArrival)).get();
        assertNotNull(colJob, "CRR job created in dcre-col");
        assertTrue(dbUrlOf(colJob).contains("/dcre_col"),
                "collections stage job env keeps the dcre_col URL, got " + dbUrlOf(colJob));
    }

    @Test
    void ctvStageJobCarriesTheMandatesProjectionDbUrlWithoutLeavingDcreCol() {
        // SCRUM-91: CTV's projection-mode mandate gate reads man_ctv_view over a
        // SECOND, read-only datasource (MandatesDatasourceConfig,
        // ${DCRE_CTV_MANDATES_DB_URL}). AGT never injected it, so an in-cluster CTV
        // fell back to ctv's localhost dev default and the gate could not work.
        // Stage-keyed, not launch-scoped: CTV is a DAG stage with no per-launch env
        // seam, and EVERY CTV pod runs the gate, so the url belongs to the stage.
        // The env NAME is asserted as a literal on purpose: it is the cross-repo
        // wire contract with ctv's placeholder, exactly like DCRE_DB_URL.
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");

        launcher.launch(arrivalId, Stage.CTV);

        Job ctvJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CTV, arrivalId)).get();
        assertNotNull(ctvJob, "CTV job created in dcre-col");
        String manUrl = envOf(ctvJob, "DCRE_CTV_MANDATES_DB_URL");
        assertEquals(config.manServiceDbUrl(), manUrl,
                "the gate reads dcre_man over the same URL every man stage pod gets");
        assertTrue(manUrl.contains("/dcre_man"), "got " + manUrl);
        assertTrue(dbUrlOf(ctvJob).contains("/dcre_col"),
                "CTV stays a collections stage: its PRIMARY datasource is dcre_col and the "
                        + "mandates url is a second, read-only seam, got " + dbUrlOf(ctvJob));
    }

    @Test
    void nonCtvStagesDoNotCarryTheMandatesProjectionDbUrl() {
        // Only CTV holds the read-only man_ctv_view seam, so the url is keyed to
        // that stage and never blanket-added to every launched pod.
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");

        launcher.launch(arrivalId, Stage.CRR);

        Job crrJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRR, arrivalId)).get();
        assertNotNull(crrJob, "CRR job created in dcre-col");
        boolean wired = crrJob.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
                .anyMatch(e -> "DCRE_CTV_MANDATES_DB_URL".equals(e.getName()));
        assertFalse(wired, "CRR has no mandates datasource; the url is stage-keyed to CTV");
    }

    @Test
    void ctvStageJobCarriesTheConfiguredMandateSource() {
        // SCRUM-91 Task 11 Step 8: CTV picks its mandate store from
        // ${DCRE_CTV_MANDATE_SOURCE} (ctv MandateGate, legacy|projection). Nothing
        // injected it, so an in-cluster CTV was frozen on ctv's yml default
        // `legacy`, which reads `FROM mandate` in dcre_col - a table only
        // env-reset.sh --seed creates and which is absent. The acceptance step
        // (ACCP mandate PASSes, SUSPENDED one FAIL_MANDATE_NOT_ACTIVE) was
        // therefore not drivable in-cluster at all.
        // Stage-keyed for the same reason as the mandates url: CTV is a DAG stage
        // with no per-launch env seam, and EVERY CTV pod runs the gate.
        // The env NAME is asserted as a literal on purpose: it is the cross-repo
        // wire contract with ctv's placeholder, exactly like DCRE_DB_URL.
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");

        launcher.launch(arrivalId, Stage.CTV);

        Job ctvJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CTV, arrivalId)).get();
        assertNotNull(ctvJob, "CTV job created in dcre-col");
        assertEquals(config.ctvMandateSource(), envOf(ctvJob, "DCRE_CTV_MANDATE_SOURCE"),
                "the CTV pod runs the mandate store AGT is configured for, not ctv's frozen yml default");
        // The shipped default must be ctv's own effective behaviour, so wiring the
        // seam changes nothing for anyone who never sets the knob.
        assertEquals("legacy", config.ctvMandateSource(),
                "default mirrors ctv application.yml (dcre.ctv.mandate-source:legacy)");
    }

    @Test
    void nonCtvStagesDoNotCarryTheMandateSource() {
        // Only CTV has a mandate gate, so the source token is keyed to that stage
        // and never blanket-added to every launched pod.
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");

        launcher.launch(arrivalId, Stage.CRR);

        Job crrJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRR, arrivalId)).get();
        assertNotNull(crrJob, "CRR job created in dcre-col");
        boolean wired = crrJob.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
                .anyMatch(e -> "DCRE_CTV_MANDATE_SOURCE".equals(e.getName()));
        assertFalse(wired, "CRR has no mandate gate; the source is stage-keyed to CTV");
    }

    @Test
    void launchedJobsCarryTheAgtOpsDbUrlEnvOnBothServiceAndClockPaths() {
        // M12/SCRUM-88 (R-47): every launched stage pod gets the agt_ops URL so
        // the platform-batch heartbeat writer (T2) can reach agt_ops. FQDN like
        // the man url (stage pods run in the flow namespaces where the short
        // `crdb` name does not resolve) and identical for all flows; user = root.
        String agtOps = "jdbc:postgresql://crdb.dcre.svc.cluster.local:26257/agt_ops?sslmode=disable";

        // service Job path (JobLauncher.serviceJob)
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");
        launcher.launch(arrivalId, Stage.CRR);
        Job svcJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRR, arrivalId)).get();
        assertNotNull(svcJob, "CRR service job created in dcre-col");
        assertEquals(agtOps, envOf(svcJob, "DCRE_AGTOPS_DB_URL"),
                "service job carries the agt_ops FQDN url, got " + envOf(svcJob, "DCRE_AGTOPS_DB_URL"));
        assertEquals("root", envOf(svcJob, "DCRE_AGTOPS_DB_USER"));

        // clock Job path (JobLauncher.clockJob)
        String runKey = "agtops-" + suffix();
        launcher.launchClock(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.PRG, runKey,
                java.util.List.of("client=FNBCC01", "window=" + runKey));
        Job clockJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.clockJobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.PRG, runKey)).get();
        assertNotNull(clockJob, "PRG clock job created in dcre-col");
        assertEquals(agtOps, envOf(clockJob, "DCRE_AGTOPS_DB_URL"),
                "clock job carries the agt_ops FQDN url, got " + envOf(clockJob, "DCRE_AGTOPS_DB_URL"));
        assertEquals("root", envOf(clockJob, "DCRE_AGTOPS_DB_USER"));
    }

    @Test
    void mrgSchedulerIsLaunchDisabledWithoutAnImage() {
        // M10/SCRUM-79: no agt.mrg-image in this profile -> the scheduler skips
        // entirely. Delta assertion: DB state is shared across test classes.
        insertArrival("onhost-req", "FNBCC01");
        exec("UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: lease held");
        long before = countIntentsLike("man-mrg-%");

        mrgScheduler.tick();

        assertEquals(before, countIntentsLike("man-mrg-%"),
                "absent/empty MRG image = launch-disabled: no new windows minted");
    }

    @Test
    void nullNamespaceIntentObservesInTheControlNamespace() throws IOException {
        // Legacy (pre-SCRUM-70) launch_intent rows keep namespace NULL: the
        // fallback target is the CONTROL namespace, never a flow namespace.
        String name = "dcre-crr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, null).orElseThrow();
        Job legacy = createJob("dcre", name, true);
        intentRepo.markIntentLaunched(intentId, legacy.getMetadata().getUid());
        writeOutcomeSeam(name, "BUSINESS_ACCEPTED");

        watcher.observe(intentOf(arrivalId, intentId));

        assertEquals(Outcome.BUSINESS_ACCEPTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "NULL-namespace intent is observed via the control-namespace fallback");
    }

    @Test
    void nullNamespaceIntentRelaunchesIntoTheControlNamespace() {
        String name = "dcre-crr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, null).orElseThrow();
        Job dead = createJob("dcre", name, false);
        String deadUid = dead.getMetadata().getUid();
        intentRepo.markIntentLaunched(intentId, deadUid);
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 137, "Failed/PodKill"));

        relauncher.relaunchOrExhaust(intentOf(arrivalId, intentId), dead);

        Job recreated = k8s.batch().v1().jobs().inNamespace("dcre").withName(name).get();
        assertNotNull(recreated, "legacy relaunch targets the control namespace");
        assertNotEquals(deadUid, recreated.getMetadata().getUid(),
                "the dead legacy Job was deleted in the control namespace first");
    }

    @Test
    void nullNamespaceIntendedIntentRecreatesInTheControlNamespace() {
        String name = "dcre-crr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, null).orElseThrow();

        reconciler.reconcile(intentOf(arrivalId, intentId), null);

        assertNotNull(k8s.batch().v1().jobs().inNamespace("dcre").withName(name).get(),
                "INTENDED legacy intent recreates in the control namespace");
    }

    private Job createJob(String ns, String name, boolean complete) {
        JobBuilder b = new JobBuilder()
                .withNewMetadata().withName(name).withNamespace(ns)
                    .addToLabels(JobLauncher.LABEL_MANAGED_BY, "agt")
                .endMetadata();
        if (complete) {
            b = b.withNewStatus().addNewCondition().withType("Complete").withStatus("True")
                    .endCondition().endStatus();
        }
        return k8s.batch().v1().jobs().inNamespace(ns).resource(b.build()).create();
    }

    private void writeOutcomeSeam(String jobName, String outcome) throws IOException {
        Path dir = Path.of(config.exchangeRoot(), "outcomes");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(jobName), outcome);
    }

    private LaunchIntent intentOf(UUID arrivalId, UUID intentId) {
        return intentRepo.intentsForArrival(arrivalId).stream()
                .filter(i -> i.id().equals(intentId)).findFirst().orElseThrow();
    }

    private UUID insertArrival(String route, String client) {
        String tag = suffix();
        return arrivalRepo.insertArrival(UUID.randomUUID(), route, client + "_NRT" + tag + ".txt",
                "sha-nrt-" + tag, client, "MNRT" + tag, ArrivalStatus.DAG_RUNNING, null,
                "/exchange/claimed/" + client + "_NRT" + tag + ".txt").orElseThrow();
    }

    /** Value of a named env var on the Job's stage container (0th). */
    static String envOf(Job job, String name) {
        return job.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv().stream()
                .filter(e -> name.equals(e.getName()))
                .map(io.fabric8.kubernetes.api.model.EnvVar::getValue)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "no " + name + " env on " + job.getMetadata().getName()));
    }

    /** DCRE_DB_URL env value of the Job's stage container (B2 seam). */
    static String dbUrlOf(Job job) {
        return envOf(job, "DCRE_DB_URL");
    }

    private long countIntentsLike(String jobNamePattern) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(
                "SELECT count(*) FROM launch_intent WHERE job_name LIKE ?")) {
            p.setString(1, jobNamePattern);
            try (ResultSet r = p.executeQuery()) {
                assertTrue(r.next());
                return r.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed", e);
        }
    }

    private String namespaceOfIntentLike(String jobNamePattern) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(
                "SELECT namespace FROM launch_intent WHERE job_name LIKE ? ORDER BY created_at DESC LIMIT 1")) {
            p.setString(1, jobNamePattern);
            try (ResultSet r = p.executeQuery()) {
                assertTrue(r.next(), "no launch_intent row matching " + jobNamePattern);
                return r.getString(1);
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

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
