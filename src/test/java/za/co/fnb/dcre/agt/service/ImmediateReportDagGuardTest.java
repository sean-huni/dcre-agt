package za.co.fnb.dcre.agt.service;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
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
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-90 DAG-corruption guard: arrival-scoping the IMMEDIATE report must not
 * corrupt the parent arrival's DAG accounting. PRG is never a DAG stage, so the
 * report is excluded from DAG accounting (read-path: outcomesForArrival /
 * intentsForArrival) and its exhaustion never regresses the parent DAG
 * (write-path: OrphanRelauncher skips markDagFailed for reports). The report is
 * downstream of DAG completion; these tests prove the parent stays intact even
 * in the defensive race where the report runs while the DAG is still open.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@TestProfile(ImmediateReportDagGuardTest.DagGuardProfile.class)
class ImmediateReportDagGuardTest {

    /** Launch disabled: the exhaustion path records TECH_EXHAUSTED and (for a
     *  stage) fails the DAG without any K8s call, so no mock server is needed.
     *  Backoff 0 so relaunchOrExhaust is never throttled. */
    public static class DagGuardProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.launch-enabled", "false", "agt.orphan-backoff-seconds", "0");
        }
    }

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

    @Test
    void immediateReportIsNotCountedAsADagStage() {
        final UUID parent = insertArrival("DGA1", ArrivalStatus.DAG_COMPLETE);
        // A completed DC book: CRR -> CTV -> {CDE, CIR} all business-accepted.
        completedStage(parent, Stage.CRR);
        completedStage(parent, Stage.CTV);
        completedStage(parent, Stage.CDE);
        completedStage(parent, Stage.CIR);
        // The arrival-scoped IMMEDIATE report lands AFTER completion, with its own outcome.
        final UUID report = reportIntent(parent, "dga1");
        assertTrue(outcomeRepo.insertOutcome(report, 0, Outcome.BUSINESS_ACCEPTED, 0, "Complete"));

        final Map<Stage, Outcome> dagOutcomes = outcomeRepo.outcomesForArrival(parent);

        assertFalse(dagOutcomes.containsKey(Stage.PRG),
                "the IMMEDIATE report is NOT counted as a DAG stage in outcomesForArrival");
        assertEquals(Map.of(Stage.CRR, Outcome.BUSINESS_ACCEPTED, Stage.CTV, Outcome.BUSINESS_ACCEPTED,
                        Stage.CDE, Outcome.BUSINESS_ACCEPTED, Stage.CIR, Outcome.BUSINESS_ACCEPTED), dagOutcomes,
                "only the real DAG stages remain: the report never enters DAG accounting");
        assertFalse(intentRepo.intentsForArrival(parent).stream().anyMatch(i -> i.stage() == Stage.PRG),
                "the report intent is excluded from the DAG intended-set (intentsForArrival)");
        // The report's presence cannot change the computed terminal verdict.
        assertEquals(ArrivalStatus.DAG_COMPLETE,
                DagEngine.terminalState("onhost-req", dagOutcomes).orElseThrow(),
                "terminalState over the report-excluded outcomes still resolves DAG_COMPLETE");
    }

    @Test
    void reportExhaustionNeverFailsTheParentDag() {
        // Defensive race: even if the report is still retrying while the parent
        // DAG is open (DAG_RUNNING), exhausting the report must NOT flip the
        // arrival to DAG_FAILED - the report's failure is its own, not the DAG's.
        final UUID parent = insertArrival("DGA2", ArrivalStatus.DAG_RUNNING);
        final UUID report = reportIntent(parent, "dga2");
        exhaustCurrentAttempt(report);

        relauncher.relaunchOrExhaust(
                new LaunchIntent(report, parent, Stage.PRG, "col-prg-dga2", LaunchIntent.LAUNCHED,
                        "dga2", 3, "dcre-col"), null);

        assertEquals(ArrivalStatus.DAG_RUNNING, arrivalRepo.arrivalById(parent).orElseThrow().status(),
                "a report's budget exhaustion never regresses the parent arrival's DAG");
        assertEquals(Outcome.TECH_EXHAUSTED, reportOutcomeAtAttempt(report, 4),
                "the report is still terminated on its OWN intent (TECH_EXHAUSTED, no zombie)");
    }

    @Test
    void completedParentStaysCompleteWhenItsReportExhausts() {
        final UUID parent = insertArrival("DGA3", ArrivalStatus.DAG_COMPLETE);
        final UUID report = reportIntent(parent, "dga3");
        exhaustCurrentAttempt(report);

        relauncher.relaunchOrExhaust(
                new LaunchIntent(report, parent, Stage.PRG, "col-prg-dga3", LaunchIntent.LAUNCHED,
                        "dga3", 3, "dcre-col"), null);

        assertEquals(ArrivalStatus.DAG_COMPLETE, arrivalRepo.arrivalById(parent).orElseThrow().status(),
                "the real scenario: a completed parent stays DAG_COMPLETE when its report exhausts");
    }

    @Test
    void stageOrphanExhaustionStillFailsTheDag() {
        // Regression contrast: the guard is report-specific. A real DAG-stage
        // orphan still fails the arrival on exhaustion (unchanged behavior).
        final UUID parent = insertArrival("DGA4", ArrivalStatus.DAG_RUNNING);
        final UUID crr = intentRepo.insertIntent(parent, Stage.CRR, "col-crr-dga4", "dcre-col").orElseThrow();
        intentRepo.markIntentLaunched(crr, "uid-dga4");
        exhaustCurrentAttempt(crr);

        relauncher.relaunchOrExhaust(
                new LaunchIntent(crr, parent, Stage.CRR, "col-crr-dga4", LaunchIntent.LAUNCHED,
                        null, 3, "dcre-col"), null);

        assertEquals(ArrivalStatus.DAG_FAILED, arrivalRepo.arrivalById(parent).orElseThrow().status(),
                "a real stage orphan still fails the DAG on exhaustion (guard is report-only)");
    }

    private UUID insertArrival(final String tag, final ArrivalStatus status) {
        return arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req",
                "FNBCC01_" + tag + "_" + suffix() + ".txt", "sha-" + tag + "-" + suffix(),
                "FNBCC01", "M-" + tag, status, null, "/exchange/claimed/" + tag + ".txt").orElseThrow();
    }

    private void completedStage(final UUID arrivalId, final Stage stage) {
        final UUID intentId = intentRepo.insertIntent(arrivalId, stage,
                "col-" + stage.name().toLowerCase() + "-" + suffix(), "dcre-col").orElseThrow();
        intentRepo.markIntentLaunched(intentId, "uid-" + stage.name());
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.BUSINESS_ACCEPTED, 0, "Complete"));
    }

    private UUID reportIntent(final UUID arrivalId, final String tag) {
        final UUID intentId = intentRepo.insertReportIntent(arrivalId, Stage.PRG, "FNBCC01-imm-" + tag,
                "col-prg-" + tag, "client=FNBCC01\nwindow=imm-" + tag, "dcre-col").orElseThrow();
        intentRepo.markIntentLaunched(intentId, "uid-report-" + tag);
        return intentId;
    }

    private void exhaustCurrentAttempt(final UUID intentId) {
        exec("UPDATE launch_intent SET attempt=3 WHERE id=?", intentId);
        assertTrue(outcomeRepo.insertOutcome(intentId, 3, Outcome.TECH_FAILED, 137, "Failed/PodKill"));
    }

    private Outcome reportOutcomeAtAttempt(final UUID intentId, final int attempt) {
        try (var c = ds.getConnection();
             var p = c.prepareStatement("SELECT outcome FROM stage_outcome WHERE intent_id=? AND attempt=?")) {
            p.setObject(1, intentId);
            p.setInt(2, attempt);
            try (var r = p.executeQuery()) {
                return r.next() ? Outcome.valueOf(r.getString(1)) : null;
            }
        } catch (Exception e) {
            throw new IllegalStateException("reportOutcomeAtAttempt failed", e);
        }
    }

    private void exec(final String sql, final UUID id) {
        try (var c = ds.getConnection(); var p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            p.executeUpdate();
        } catch (Exception e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
