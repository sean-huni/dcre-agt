package za.co.fnb.dcre.agt.service;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Infrastructure-vs-job failure classification (config-plane spec, "Failure
 * classification"). platform-batch reserves exit 78 (EX_CONFIG) for any failure
 * BEFORE its runner phase, so a Failed Job whose pod exited 78 is an
 * infrastructure startup failure, not a job outcome: it is recorded as
 * TECH_CONFIG_FAILED and draws on the infra budget instead of burning the
 * defect-free arrival's 3-attempt orphan budget. Every other exit code, and an
 * unreadable one, keep today's TECH_FAILED behaviour byte for byte.
 * Lives in the service package for package-private access to observe().
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@WithKubernetesTestServer(crud = true)
class ConfigFailureClassificationTest {

    private static final String NAMESPACE = "dcre-col";

    @KubernetesTestServer
    KubernetesServer mockServer;

    @Inject
    OutcomeWatcher watcher;

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
    void exitSeventyEightOnAFailedJobIsAnInfrastructureFailure() {
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival();
        UUID intentId = failedJobWithPodExit(arrivalId, name, OutcomeWatcher.CONFIG_FAILURE_EXIT_CODE);

        watcher.observe(intentOf(arrivalId, intentId));

        assertEquals(Outcome.TECH_CONFIG_FAILED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "a pre-runner startup failure is infrastructure, not a job outcome");
        assertEquals(78, exitCodeOf(intentId), "the observed exit code is recorded verbatim (R-33)");
        String condition = conditionOf(intentId);
        assertTrue(condition.startsWith("Failed/BackoffLimitExceeded"),
                "the real k8s type/reason is preserved, got " + condition);
        assertTrue(condition.endsWith(OutcomeWatcher.INFRA_CONDITION_SUFFIX),
                "the ledger row says WHY it was classed as infrastructure, got " + condition);
    }

    @Test
    void anyOtherExitCodeStaysTechFailed() {
        // 137 is a SIGKILL death (the chaos gate's own signal): a real job
        // failure, on the unchanged 3-attempt orphan budget.
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival();
        UUID intentId = failedJobWithPodExit(arrivalId, name, 137);

        watcher.observe(intentOf(arrivalId, intentId));

        assertEquals(Outcome.TECH_FAILED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "only exit 78 is the config class; every other death is a job failure");
        assertEquals("Failed/BackoffLimitExceeded", conditionOf(intentId),
                "no InfraStartup marker on an ordinary failure");
    }

    @Test
    void unreadableExitCodeStaysTechFailed() {
        // The pod is already gone, so podExitCode returns null. Absence of
        // evidence is not evidence of a config failure: no NPE, and the fallback
        // is today's behaviour exactly.
        String name = "col-crr-" + suffix();
        UUID arrivalId = insertArrival();
        UUID intentId = launchedIntentOnFailedJob(arrivalId, name); // no pod created

        watcher.observe(intentOf(arrivalId, intentId));

        assertEquals(Outcome.TECH_FAILED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "a null exit code keeps the unchanged TECH_FAILED path");
        assertEquals("Failed/BackoffLimitExceeded", conditionOf(intentId));
    }

    /** A LAUNCHED intent whose Job carries a Failed condition, plus the Job's one
     *  pod terminated at the given exit code (backoffLimit 0 = one Job, one pod). */
    private UUID failedJobWithPodExit(final UUID arrivalId, final String name, final int exitCode) {
        UUID intentId = launchedIntentOnFailedJob(arrivalId, name);
        Pod pod = new PodBuilder()
                .withNewMetadata().withName(name + "-pod").withNamespace(NAMESPACE)
                    .addToLabels("job-name", name)
                .endMetadata()
                .withNewStatus()
                    .addNewContainerStatus()
                        .withName("stage")
                        .withNewState().withNewTerminated().withExitCode(exitCode).endTerminated().endState()
                    .endContainerStatus()
                .endStatus()
                .build();
        k8s.pods().inNamespace(NAMESPACE).resource(pod).create();
        return intentId;
    }

    private UUID launchedIntentOnFailedJob(final UUID arrivalId, final String name) {
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR, name, NAMESPACE).orElseThrow();
        Job job = k8s.batch().v1().jobs().inNamespace(NAMESPACE).resource(new JobBuilder()
                .withNewMetadata().withName(name).withNamespace(NAMESPACE)
                    .addToLabels(JobLauncher.LABEL_MANAGED_BY, "agt")
                .endMetadata()
                .withNewStatus().addNewCondition()
                    .withType("Failed").withStatus("True").withReason("BackoffLimitExceeded")
                .endCondition().endStatus()
                .build()).create();
        // The CRUD mock assigns its own uid: mark launched with the REAL one, or
        // the uid guard treats the Job as a foreign object and observes nothing.
        intentRepo.markIntentLaunched(intentId, job.getMetadata().getUid());
        return intentId;
    }

    private LaunchIntent intentOf(final UUID arrivalId, final UUID intentId) {
        return intentRepo.intentsForArrival(arrivalId).stream()
                .filter(i -> i.id().equals(intentId)).findFirst().orElseThrow();
    }

    private UUID insertArrival() {
        String tag = suffix();
        return arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req", "FNBCC01_CFC" + tag + ".txt",
                "sha-cfc-" + tag, "FNBCC01", "MCFC" + tag, ArrivalStatus.DAG_RUNNING, null,
                "/exchange/claimed/FNBCC01_CFC" + tag + ".txt").orElseThrow();
    }

    private int exitCodeOf(final UUID intentId) {
        return query("SELECT exit_code FROM stage_outcome WHERE intent_id=?", intentId, ResultSet::getInt);
    }

    private String conditionOf(final UUID intentId) {
        return query("SELECT k8s_condition FROM stage_outcome WHERE intent_id=?", intentId, ResultSet::getString);
    }

    /** One-column read of the intent's single stage_outcome row. */
    private <T> T query(final String sql, final UUID intentId, final Column<T> column) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, intentId);
            try (ResultSet r = p.executeQuery()) {
                assertTrue(r.next(), "no stage_outcome row for " + intentId);
                return column.read(r, 1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    @FunctionalInterface
    private interface Column<T> {
        T read(ResultSet r, int index) throws SQLException;
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
