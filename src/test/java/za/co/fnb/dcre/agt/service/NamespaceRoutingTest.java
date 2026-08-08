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
import za.co.fnb.dcre.agt.domain.DbFamily;
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
            // A LinkedHashMap, not Map.of: that factory caps at 10 pairs and this
            // roster passed it. A silent cap would drop image knobs, and a stage with
            // no image is launch-disabled, so the affected tests would find NO Job and
            // an assertion phrased as "does not carry the wrong url" would pass on the
            // absence.
            final Map<String, String> overrides = new java.util.LinkedHashMap<>();
            overrides.put("agt.launch-enabled", "true");
            overrides.put("agt.pay-clients", " fnbrf01 ");
            for (final String stage : java.util.List.of(
                    "crr", "ctv", "cde", "crg", "prr", "ptv", "prg",
                    "mrr", "mrv", "mit", "hcs", "acs")) {
                overrides.put("agt." + stage + "-image", "dcre-" + stage + ":test");
            }
            return overrides;
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
    CrgScheduler crgScheduler;

    @Inject
    PrgScheduler prgScheduler;

    @Inject
    StageDatabases stageDatabases;

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
        createJob("dcre-pay", "pay-ppx-" + suffix(), false);
        createJob("dcre-man", "man-mrr-" + suffix(), false);

        Map<String, Job> live = reconciler.liveManagedJobs();

        Set<String> namespaces = new java.util.HashSet<>();
        live.values().forEach(j -> namespaces.add(j.getMetadata().getNamespace()));
        assertTrue(namespaces.containsAll(Set.of("dcre", "dcre-col", "dcre-pay", "dcre-man")),
                "managed Jobs from all four namespaces in one union, got " + namespaces);
    }

    @Test
    void outcomeWatcherObservesInTheIntentNamespace() throws IOException {
        String name = "pay-prr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req-endo", "FNBRF01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.PRR, name, "dcre-pay").orElseThrow();
        // The CRUD mock server assigns its own uid: mark launched with the REAL one.
        Job created = createJob("dcre-pay", name, true);
        intentRepo.markIntentLaunched(intentId, created.getMetadata().getUid());
        writeOutcomeSeam(name, "BUSINESS_ACCEPTED");

        watcher.observe(intentOf(arrivalId, intentId));

        assertEquals(Outcome.BUSINESS_ACCEPTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.PRR),
                "observation reads the Job from the intent's namespace");
    }

    @Test
    void outcomeWatcherNeverFallsBackToTheControlNamespace() throws IOException {
        String name = "pay-prr-" + suffix();
        UUID arrivalId = insertArrival("onhost-req-endo", "FNBRF01");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.PRR, name, "dcre-pay").orElseThrow();
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

        relauncher.relaunchOrExhaust(intentOf(arrivalId, intentId), Outcome.TECH_FAILED, dead);

        Job recreated = k8s.batch().v1().jobs().inNamespace("dcre-col").withName(name).get();
        assertNotNull(recreated, "same-identity relaunch lands back in the flow namespace");
        assertNotEquals(deadUid, recreated.getMetadata().getUid(),
                "the dead Job was deleted in dcre-col first (409-free recreate)");
        assertNull(k8s.batch().v1().jobs().inNamespace("dcre").withName(name).get(),
                "the control namespace never hosts flow-namespace stage Jobs");
    }

    @Test
    void eachFamilysReportGeneratorRunsInItsOwnNamespaceForItsOwnClients() {
        // v1 topology: ONE loop used to launch a single generator for every client
        // on whatever namespace that client's flow resolved to. There are two
        // generators now, each serving only its own family's clients: CRG in
        // dcre-col for collections clients, PRG in dcre-pay for pay clients.
        insertArrival("onhost-req", "FNBRF01");
        insertArrival("onhost-req", "FNBCC01");
        exec("UPDATE agt_lease SET expires_at = now() - INTERVAL '1 second'");
        assertTrue(lease.tryAcquire(config.holderId()), "test precondition: lease held");

        crgScheduler.tick();
        prgScheduler.tick();

        assertEquals("dcre-pay", namespaceOfIntentLike("pay-prg-fnbrf01-w%"),
                "R-42 pay client: PRG window in the pay namespace with the pay- prefix");
        assertEquals("dcre-col", namespaceOfIntentLike("col-crg-fnbcc01-w%"),
                "collections client: the COLLECTIONS generator is CRG, in dcre-col");
        assertEquals(0, countIntentsLike("col-prg-%"),
                "PRG is the payments generator: it must never mint a col- window");
        assertEquals(0, countIntentsLike("pay-crg-%"),
                "CRG is the collections generator: it must never mint a pay- window");
    }

    /**
     * THE PAYMENTS ROUTING ASSERTION. Red-proofed by reverting
     * {@code StageDatabases.urlFor} to its two-way form (man vs everything else)
     * and watching this fail; the fixture below is why the pre-existing man/col
     * assertions could not.
     *
     * <p>{@code manStageJobCarriesTheManDbUrlAndCollectionsKeepsCol} stays GREEN
     * through the entire payments defect, because its fixture contains two families
     * where the code has three. A dimension with only some of its values cannot
     * exercise what that dimension drives.
     */
    @Test
    void paymentsStageJobsCarryTheDcrePayUrlAndNotTheCollectionsOne() {
        UUID payArrival = insertArrival("onhost-req-endo", "FNBRF01");
        launcher.launch(payArrival, Stage.PRR);
        Job payJob = k8s.batch().v1().jobs().inNamespace("dcre-pay")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.PAY, Stage.PRR, payArrival)).get();
        assertNotNull(payJob, "PRR job created in dcre-pay");
        assertTrue(dbUrlOf(payJob).contains("/dcre_pay"),
                "a payments stage pod must address dcre_pay. With the two-way dbUrlFor it"
                        + " receives /dcre_col instead and nothing errors, because the write is"
                        + " perfectly valid against the wrong database. Got " + dbUrlOf(payJob));
        assertFalse(dbUrlOf(payJob).contains("/dcre_col"),
                "and it must not carry the collections url at all, got " + dbUrlOf(payJob));
    }

    /** The payments clock generator too: PRG windows write dcre_pay. */
    @Test
    void thePaymentsReportGeneratorClockJobCarriesTheDcrePayUrl() {
        String runKey = "payclock-" + suffix();
        launcher.launchClock(za.co.fnb.dcre.agt.domain.Flow.PAY, Stage.PRG, runKey,
                java.util.List.of("client=FNBRF01", "window=" + runKey));
        Job clockJob = k8s.batch().v1().jobs().inNamespace("dcre-pay")
                .withName(JobLauncher.clockJobName(za.co.fnb.dcre.agt.domain.Flow.PAY, Stage.PRG, runKey)).get();
        assertNotNull(clockJob, "PRG clock job created in dcre-pay");
        assertTrue(dbUrlOf(clockJob).contains("/dcre_pay"),
                "the payments report generator writes dcre_pay, got " + dbUrlOf(clockJob));
    }

    /**
     * ALL FIVE service databases, asserted over {@link DbFamily} rather than over the
     * stages that happen to exist.
     *
     * <p>It used to iterate stages and assert "three distinct databases". That shape
     * is blind to a family with no stage yet (ACS has none today) and, worse, it
     * expressed the expected count as a number the reader could bump without deciding
     * anything. The expectation is now a literal map of family to database: a family
     * ADDED to the enum fails the size assertion until it is named here, and a family
     * MISROUTED fails its own row. A fixture whose rows all share a value cannot
     * exercise what that value drives, which is exactly how the previous three-family
     * version stayed green while HCS was routed to the collections database.
     */
    @Test
    void everyServiceDatabaseIsRoutedToItsOwnFamilyAndAllFiveAreDistinct() {
        final java.util.Map<DbFamily, String> expected = new java.util.EnumMap<>(DbFamily.class);
        expected.put(DbFamily.COL, "/dcre_col");
        expected.put(DbFamily.PAY, "/dcre_pay");
        expected.put(DbFamily.MAN, "/dcre_man");
        expected.put(DbFamily.HCS, "/dcre_hcs");
        expected.put(DbFamily.ACS, "/dcre_acs");
        assertEquals(DbFamily.values().length, expected.size(),
                "a family added to DbFamily must be named here deliberately, with the database"
                        + " it owns; absent from this map it would be routed by nothing and"
                        + " asserted by nothing");

        final java.util.Set<String> urls = new java.util.HashSet<>();
        expected.forEach((family, database) -> {
            final String url = stageDatabases.urlFor(family);
            assertNotNull(url, "no url for family " + family);
            assertTrue(url.contains(database),
                    family + " must address " + database + ", got " + url);
            urls.add(url);
        });
        assertEquals(DbFamily.values().length, urls.size(),
                "one database per family, none shared, got " + urls);

        // And every STAGE agrees with its family's url, so a stage cannot be routed
        // somewhere its family is not.
        for (final Stage stage : Stage.values()) {
            assertEquals(stageDatabases.urlFor(stageDatabases.dbFamily(stage)),
                    stageDatabases.urlFor(stage),
                    "stage " + stage + " disagrees with its family's database");
        }
    }

    /**
     * THE HCS ROUTING ASSERTION. Red-proofed by putting HCS back with collections in
     * {@code StageDatabases.dbFamily} and watching this fail.
     *
     * <p>{@code StageDatabases.family} enumerated HCS with the collections stages, so
     * AGT handed every HCS pod the {@code dcre_col} url. That was defensible while the
     * calendar lived there; the owner ruled it out on 2026-08-08 as a "Violation of the
     * 12FactorApp" (https://12factor.net/) and {@code shared/hcs} now carries a
     * {@code FamilyGuard} on {@code current_database()} that refuses to migrate against
     * anything but {@code dcre_hcs}. So the old routing is not a silent contamination
     * any more, it is a total outage of the stage: every HCS pod dies at startup.
     *
     * <p>Asserted on the launched Job's env, not just on {@code urlFor}, because the
     * env is what the pod actually reads.
     */
    @Test
    void theHolidaySyncPodWritesItsOwnDatabaseAndNeverTheCollectionsOne() {
        final String runKey = "hcsclock-" + suffix();
        // HCS is HOSTED in dcre-col and OWNS dcre_hcs: the namespace and the database
        // disagree on purpose, and this is the stage that proves the two questions were
        // separated rather than merely renamed.
        launcher.launchClock(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.HCS, runKey,
                java.util.List.of("window=" + runKey));
        Job hcsJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.clockJobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.HCS, runKey)).get();
        assertNotNull(hcsJob, "HCS clock job created in dcre-col, where it is hosted");
        assertTrue(dbUrlOf(hcsJob).contains("/dcre_hcs"),
                "the holiday calendar's single writer must address dcre_hcs. With the old"
                        + " routing it receives /dcre_col and shared/hcs's FamilyGuard kills the"
                        + " pod before any DDL. Got " + dbUrlOf(hcsJob));
        assertFalse(dbUrlOf(hcsJob).contains("/dcre_col"),
                "and it must not carry the collections url at all, got " + dbUrlOf(hcsJob));
    }

    /**
     * The ACS census pod writes the account registry's own database.
     *
     * <p>ACS is a NEW stage, not a rename: {@code shared/acs} owns {@code account} and
     * {@code account_type}, which left {@code dcre_col} and {@code dcre_man}. Its job is
     * the HCS shape exactly (one identifying {@code window} parameter), so it is
     * launched the same way and routed the same way, and it is hosted in the collections
     * namespace for the same reason HCS is: no namespace of its own exists.
     */
    @Test
    void theAccountCensusPodWritesTheAccountRegistryDatabase() {
        final String runKey = "acsclock-" + suffix();
        launcher.launchClock(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.ACS, runKey,
                java.util.List.of("window=" + runKey));
        Job acsJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.clockJobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.ACS, runKey)).get();
        assertNotNull(acsJob, "ACS clock job created in dcre-col, where it is hosted");
        assertTrue(dbUrlOf(acsJob).contains("/dcre_acs"),
                "the account registry's single writer must address dcre_acs, got " + dbUrlOf(acsJob));
        assertFalse(dbUrlOf(acsJob).contains("/dcre_col"), "got " + dbUrlOf(acsJob));
        assertFalse(dbUrlOf(acsJob).contains("/dcre_man"),
                "account/account_type left dcre_man too, got " + dbUrlOf(acsJob));
    }

    /**
     * The five cross-context read seams, asserted on the Job specs.
     *
     * <p>Every one of these fails CLOSED at the consumer: cde, ctv and mrv detect
     * KUBERNETES_SERVICE_HOST and refuse to start rather than use their localhost dev
     * default, naming the exact variable. AGT is the only thing that sets them, so a
     * missing arm in {@code stageEnv} is a crash-looping pod at cutover, which is
     * precisely the failure this test exists to prevent.
     *
     * <p>PTV and MIT are asserted even though no committed consumer reads their
     * variables yet (verified by grep over the spring repo: exit=1 for both, against
     * exit=0 for the other three). Wiring them now makes the gap visible rather than
     * discovered at rollout, and the assertion is what stops the arms being "cleaned up"
     * as dead before the consumers land.
     */
    @Test
    void everyCrossContextReadSeamIsInjectedOnItsOwnStage() {
        record Seam(Stage stage, String route, String client, String namespace,
                    String env, String database) { }
        final java.util.List<Seam> seams = java.util.List.of(
                new Seam(Stage.CDE, "onhost-req", "FNBCC01", "dcre-col",
                        JobLauncher.CDE_HOLIDAYS_DB_URL_ENV, "/dcre_hcs"),
                new Seam(Stage.CTV, "onhost-req", "FNBCC01", "dcre-col",
                        JobLauncher.CTV_ACCOUNTS_DB_URL_ENV, "/dcre_acs"),
                new Seam(Stage.MRV, "onhost-req-man", "FNBCC01", "dcre-man",
                        JobLauncher.MRV_ACCOUNTS_DB_URL_ENV, "/dcre_acs"),
                new Seam(Stage.PTV, "onhost-req-endo", "FNBRF01", "dcre-pay",
                        JobLauncher.PTV_ACCOUNTS_DB_URL_ENV, "/dcre_acs"),
                new Seam(Stage.MIT, "onhost-req-man", "FNBCC01", "dcre-man",
                        JobLauncher.MIT_ACCOUNTS_DB_URL_ENV, "/dcre_acs"));

        for (final Seam seam : seams) {
            final UUID arrivalId = insertArrival(seam.route(), seam.client());
            launcher.launch(arrivalId, seam.stage());
            final Job job = k8s.batch().v1().jobs().inNamespace(seam.namespace())
                    .withName(JobLauncher.jobName(
                            za.co.fnb.dcre.agt.domain.Flow.valueOf(
                                    seam.namespace().substring("dcre-".length()).toUpperCase(java.util.Locale.ROOT)),
                            seam.stage(), arrivalId)).get();
            assertNotNull(job, seam.stage() + " job created in " + seam.namespace());
            final String url = envOf(job, seam.env());
            assertTrue(url.contains(seam.database()),
                    seam.stage() + " must receive " + seam.env() + " addressing "
                            + seam.database() + ", or the pod refuses to start in-cluster."
                            + " Got " + url);
        }
    }

    /** A stage with no cross-context read carries none of the seams: they are
     *  stage-keyed, never blanket-added to every launched pod. */
    @Test
    void aStageWithNoCrossContextReadCarriesNoneOfTheSeams() {
        final UUID arrivalId = insertArrival("onhost-req", "FNBCC01");
        launcher.launch(arrivalId, Stage.CRR);
        final Job crrJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRR, arrivalId)).get();
        assertNotNull(crrJob, "CRR job created in dcre-col");
        final java.util.Set<String> names = crrJob.getSpec().getTemplate().getSpec()
                .getContainers().get(0).getEnv().stream()
                .map(io.fabric8.kubernetes.api.model.EnvVar::getName)
                .collect(java.util.stream.Collectors.toSet());
        for (final String seam : java.util.List.of(
                JobLauncher.CDE_HOLIDAYS_DB_URL_ENV, JobLauncher.CTV_ACCOUNTS_DB_URL_ENV,
                JobLauncher.MRV_ACCOUNTS_DB_URL_ENV, JobLauncher.PTV_ACCOUNTS_DB_URL_ENV,
                JobLauncher.MIT_ACCOUNTS_DB_URL_ENV)) {
            assertFalse(names.contains(seam), "CRR opens no second datasource; " + seam
                    + " must not be on its pod. Got " + names);
        }
    }

    /**
     * The wire-name contract, on the Job specs AGT actually builds.
     *
     * <p>The defect this exists for: eight of the nine payments services read
     * {@code ${DCRE_PAY_DB_URL:...}}, nothing anywhere set it, and AGT injected
     * {@code DCRE_DB_URL}. In a pod those eight fell back to their committed localhost
     * default, which is the pod itself. Both sides were green, because five tests
     * pinned the payments side of the NAME and nothing tested AGT's, even though AGT is
     * the side documenting that its recipients listen on this one.
     *
     * <p>What this can and cannot see: it asserts that AGT publishes ONE name for every
     * family, on BOTH builder paths, and that no family-specific primary-url name has
     * crept in. It cannot see a rename in the consumer repo, which is not on this
     * classpath. Generating both sides from one schema is the durable fix and is
     * recorded as a follow-up; the literal is the honest interim.
     */
    @Test
    void everyFamilysPodReadsItsDatabaseFromTheSameEnvName() {
        // DCRE_COL_DB_URL is deliberately absent from this list: it is a REAL,
        // launch-scoped SECOND datasource for the MRG suspension sweep. These four are
        // the family-specific PRIMARY names that must never exist, one of which is the
        // name eight payments services were reading from nobody.
        final java.util.List<String> banned = java.util.List.of(
                "DCRE_PAY_DB_URL", "DCRE_MAN_DB_URL", "DCRE_HCS_DB_URL", "DCRE_ACS_DB_URL");

        final UUID colArrival = insertArrival("onhost-req", "FNBCC01");
        launcher.launch(colArrival, Stage.CRR);
        final Job colJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRR, colArrival)).get();

        final UUID payArrival = insertArrival("onhost-req-endo", "FNBRF01");
        launcher.launch(payArrival, Stage.PRR);
        final Job payJob = k8s.batch().v1().jobs().inNamespace("dcre-pay")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.PAY, Stage.PRR, payArrival)).get();

        final UUID manArrival = insertArrival("onhost-req-man", "FNBCC01");
        launcher.launch(manArrival, Stage.MRR);
        final Job manJob = k8s.batch().v1().jobs().inNamespace("dcre-man")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.MAN, Stage.MRR, manArrival)).get();

        final String clockKey = "wirename-" + suffix();
        launcher.launchClock(za.co.fnb.dcre.agt.domain.Flow.PAY, Stage.PRG, clockKey,
                java.util.List.of("client=FNBRF01", "window=" + clockKey));
        final Job clockJob = k8s.batch().v1().jobs().inNamespace("dcre-pay")
                .withName(JobLauncher.clockJobName(za.co.fnb.dcre.agt.domain.Flow.PAY, Stage.PRG, clockKey)).get();

        final java.util.Map<Job, String> byJob = new java.util.LinkedHashMap<>();
        byJob.put(colJob, "/dcre_col");
        byJob.put(payJob, "/dcre_pay");
        byJob.put(manJob, "/dcre_man");
        byJob.put(clockJob, "/dcre_pay");

        byJob.forEach((job, database) -> {
            assertNotNull(job, "job not created for " + database);
            final java.util.Set<String> names = job.getSpec().getTemplate().getSpec()
                    .getContainers().get(0).getEnv().stream()
                    .map(io.fabric8.kubernetes.api.model.EnvVar::getName)
                    .collect(java.util.stream.Collectors.toSet());
            assertTrue(names.contains(JobLauncher.DB_URL_ENV),
                    job.getMetadata().getName() + " must carry " + JobLauncher.DB_URL_ENV
                            + ", got " + names);
            for (final String name : banned) {
                assertFalse(names.contains(name), job.getMetadata().getName()
                        + " must not carry the family-specific name " + name
                        + ": one variable, routed per family, is the ruling. Got " + names);
            }
            assertTrue(envOf(job, JobLauncher.DB_URL_ENV).contains(database),
                    job.getMetadata().getName() + " must address " + database
                            + ", got " + envOf(job, JobLauncher.DB_URL_ENV));
        });
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
        // SCRUM-107: CTV picks its mandate store from ${DCRE_CTV_MANDATE_SOURCE}
        // (ctv MandateGate). The vocabulary is now `projection` only: the
        // dcre_col.mandate table the retired `legacy` value read has been dropped,
        // and ctv FAILS CLOSED on that value, so handing it to a stage pod stops the
        // pod from starting rather than degrading to a different gate.
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
        // seam changes nothing for anyone who never sets the knob. This is the THIRD
        // home of the same fact (yml, @WithDefault, and the resolved value asserted
        // here); AgtCtvMandateSourceDefaultTest pins the first two to each other.
        assertEquals("projection", config.ctvMandateSource(),
                "default mirrors ctv application.yml (dcre.ctv.mandate-source:projection)");
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
        launcher.launchClock(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRG, runKey,
                java.util.List.of("client=FNBCC01", "window=" + runKey));
        Job clockJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.clockJobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRG, runKey)).get();
        assertNotNull(clockJob, "PRG clock job created in dcre-col");
        assertEquals(agtOps, envOf(clockJob, "DCRE_AGTOPS_DB_URL"),
                "clock job carries the agt_ops FQDN url, got " + envOf(clockJob, "DCRE_AGTOPS_DB_URL"));
        assertEquals("root", envOf(clockJob, "DCRE_AGTOPS_DB_USER"));
    }

    @Test
    void bothBuilderSitesPinBackoffLimitZeroSoOneJobStaysOnePod() {
        // Config-failure classification (spec "Failure classification") reads the
        // pod's exit code to tell a pre-runner startup failure (78) from a real
        // job failure. OutcomeWatcher.podExitCode reads the FIRST pod carrying the
        // job-name label, which is only correct while one Job produces exactly ONE
        // pod: backoffLimit 0 + restartPolicy Never. Raise the limit to "retry
        // flaky stages" and that read becomes a lottery between attempt pods, so
        // an exit-78 config failure silently re-masks as a generic TECH_FAILED and
        // burns the 3-attempt orphan budget again. Pinned at BOTH builder sites
        // (serviceJob and clockJob), because either one drifting reopens the hole.

        // service Job path (JobLauncher.serviceJob)
        UUID arrivalId = insertArrival("onhost-req", "FNBCC01");
        launcher.launch(arrivalId, Stage.CRR);
        Job svcJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.jobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRR, arrivalId)).get();
        assertNotNull(svcJob, "CRR service job created in dcre-col");
        assertEquals(0, svcJob.getSpec().getBackoffLimit(),
                "serviceJob: one Job, one pod (exit code attributable)");
        assertEquals("Never", svcJob.getSpec().getTemplate().getSpec().getRestartPolicy(),
                "serviceJob: a restarted container would replace the exit code in place");

        // clock Job path (JobLauncher.clockJob)
        String runKey = "backoff-" + suffix();
        launcher.launchClock(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRG, runKey,
                java.util.List.of("client=FNBCC01", "window=" + runKey));
        Job clockJob = k8s.batch().v1().jobs().inNamespace("dcre-col")
                .withName(JobLauncher.clockJobName(za.co.fnb.dcre.agt.domain.Flow.COL, Stage.CRG, runKey)).get();
        assertNotNull(clockJob, "PRG clock job created in dcre-col");
        assertEquals(0, clockJob.getSpec().getBackoffLimit(),
                "clockJob: one Job, one pod (exit code attributable)");
        assertEquals("Never", clockJob.getSpec().getTemplate().getSpec().getRestartPolicy(),
                "clockJob: a restarted container would replace the exit code in place");
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

        relauncher.relaunchOrExhaust(intentOf(arrivalId, intentId), Outcome.TECH_FAILED, dead);

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
