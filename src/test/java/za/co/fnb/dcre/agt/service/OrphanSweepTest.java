package za.co.fnb.dcre.agt.service;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OrphanSweeper relaunch path (R-05 amendment, spec
 * 2026-07-14-stuck-job-recovery-design.md): a vanished or TECH-failed LAUNCHED
 * arrival intent gets a bounded same-identity relaunch instead of a terminal
 * TECH_FAILED; budget exhaustion mints TECH_EXHAUSTED on its own attempt slot
 * and fails the arrival. Lives in the service package for package-private
 * access to reconcile/sweepTechOrphans/relaunchOrExhaust.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@TestProfile(OrphanSweepTest.OrphanSweepProfile.class)
class OrphanSweepTest {

    /** launch-enabled stays false (%test); the CRR image only feeds the Job
     *  spec build that precedes the launch gate in JobLauncher.createJob. */
    public static class OrphanSweepProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("agt.crr-image", "dcre-crr:test");
        }
    }

    @Inject
    Reconciler reconciler;

    @Inject
    OrphanRelauncher relauncher;

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

    @Test
    void vanishedLaunchedIntentRelaunchesSameIdentity() {
        final UUID arrivalId = insertArrival("OSW1");
        final UUID intentId = launchedIntent(arrivalId, "osw1");
        backdateCreatedAt(intentId);

        reconciler.reconcile(intentOf(intentId, arrivalId), null);

        assertEquals(1, attemptOf(intentId), "grace-elapsed vanish bumps the attempt (R-05 amendment)");
        assertTrue(intentRepo.lastAttemptAt(intentId).isPresent(), "relaunch stamps last_attempt_at");
        assertEquals(0, outcomeCount(intentId), "no terminal TECH_FAILED row is minted any more");
        // M12/SCRUM-86: the atomic claim flips LAUNCHED -> ABANDONED (write-ahead
        // claim marker); createJob re-marks it LAUNCHED in prod, but launch is
        // disabled in %test so it rests at the claim state.
        assertEquals(LaunchIntent.ABANDONED, intentOf(intentId, arrivalId).status(),
                "atomic relaunch claim leaves the intent ABANDONED until createJob re-marks LAUNCHED");
    }

    @Test
    void techOutcomeCurrentAttemptRelaunches() {
        final UUID arrivalId = insertArrival("OSW2");
        final UUID intentId = launchedIntent(arrivalId, "osw2");
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 137, "Failed/PodKill"));

        relauncher.sweepTechOrphans(Map.of());

        assertEquals(1, attemptOf(intentId), "TECH outcome on the current attempt triggers a relaunch");
        assertTrue(intentRepo.lastAttemptAt(intentId).isPresent());
    }

    @Test
    void backoffRespected() {
        final UUID arrivalId = insertArrival("OSW3");
        final UUID intentId = launchedIntent(arrivalId, "osw3");
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 137, "Failed/PodKill"));
        stampLastAttemptNow(intentId);

        relauncher.sweepTechOrphans(Map.of());

        assertEquals(0, attemptOf(intentId), "within the backoff window no attempt is consumed");
    }

    @Test
    void budgetExhaustedGoesTerminal() {
        final UUID arrivalId = insertArrival("OSW4");
        final UUID intentId = launchedIntent(arrivalId, "osw4");
        setAttempt(intentId, 3);
        assertTrue(outcomeRepo.insertOutcome(intentId, 3, Outcome.TECH_FAILED, 137, "Failed/PodKill"));

        relauncher.sweepTechOrphans(Map.of());

        assertEquals(4, attemptOf(intentId), "exhaustion consumes one attempt slot for its own ledger row");
        assertEquals(Outcome.TECH_EXHAUSTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "TECH_EXHAUSTED recorded at the final attempt");
        assertEquals(ArrivalStatus.DAG_FAILED,
                arrivalRepo.arrivalById(arrivalId).orElseThrow().status(), "arrival fails terminally");

        relauncher.sweepTechOrphans(Map.of());
        assertEquals(4, attemptOf(intentId), "an exhausted intent leaves the worklist: second sweep no-op");
    }

    @Test
    void configFailureIsRetriedOnTheInfraCeilingNotTheOrphanOne() {
        // Config-plane failure class (spec "Failure classification"): the pod
        // exited 78 BEFORE its runner phase, so the arrival carries no defect and
        // the 3-attempt orphan budget is the wrong purse. At exactly the orphan
        // ceiling a TECH_FAILED exhausts (budgetExhaustedGoesTerminal above); the
        // same attempt with TECH_CONFIG_FAILED must still relaunch. That it is
        // swept at all also proves the class is in the worklist IN-list: left out,
        // the intent would stay LAUNCHED forever and wedge the arrival.
        final int orphanCeiling = config.orphanMaxAttempts();
        assertTrue(config.infraMaxAttempts() > orphanCeiling, "test precondition: infra budget is the larger one");
        final UUID arrivalId = insertArrival("OSW8");
        final UUID intentId = launchedIntent(arrivalId, "osw8");
        setAttempt(intentId, orphanCeiling);
        assertTrue(outcomeRepo.insertOutcome(intentId, orphanCeiling, Outcome.TECH_CONFIG_FAILED,
                78, "Failed/BackoffLimitExceeded/InfraStartup"));

        relauncher.sweepTechOrphans(Map.of());

        assertEquals(orphanCeiling + 1, attemptOf(intentId),
                "TECH_CONFIG_FAILED at the orphan ceiling still relaunches: it spends the infra budget");
        assertEquals(1, outcomeCount(intentId), "no TECH_EXHAUSTED row was minted");
        assertEquals(ArrivalStatus.DAG_RUNNING, arrivalRepo.arrivalById(arrivalId).orElseThrow().status(),
                "a cfg restart must not fail a defect-free arrival");
    }

    @Test
    void configFailureBudgetStaysBoundedAndEndsTerminal() {
        // The infra ceiling is larger, never unbounded: an outcome class that is
        // retried forever would hang the arrival in DAG_RUNNING (DagEngine has an
        // empty tech arm), which is the exact wedge this design avoids.
        final int infraCeiling = config.infraMaxAttempts();
        final UUID arrivalId = insertArrival("OSW9");
        final UUID intentId = launchedIntent(arrivalId, "osw9");
        setAttempt(intentId, infraCeiling);
        assertTrue(outcomeRepo.insertOutcome(intentId, infraCeiling, Outcome.TECH_CONFIG_FAILED,
                78, "Failed/BackoffLimitExceeded/InfraStartup"));

        relauncher.sweepTechOrphans(Map.of());

        assertEquals(infraCeiling + 1, attemptOf(intentId),
                "exhaustion consumes one attempt slot for its own ledger row (unchanged off-by-one)");
        assertEquals(Outcome.TECH_EXHAUSTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "the infra budget still terminates in TECH_EXHAUSTED");
        assertEquals(ArrivalStatus.DAG_FAILED,
                arrivalRepo.arrivalById(arrivalId).orElseThrow().status(), "arrival fails terminally");
    }

    @Test
    void businessOutcomeNeverTouched() {
        final UUID arrivalId = insertArrival("OSW5");
        final UUID intentId = launchedIntent(arrivalId, "osw5");
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.BUSINESS_ACCEPTED, 0, "Complete"));

        relauncher.sweepTechOrphans(Map.of());

        assertEquals(0, attemptOf(intentId), "BUSINESS outcomes are never relaunched");
        assertTrue(intentRepo.lastAttemptAt(intentId).isEmpty());
    }

    @Test
    void dagAdvancesOnBusinessOutcomeOfLaterAttempt() {
        final UUID arrivalId = insertArrival("OSW7");
        final UUID intentId = launchedIntent(arrivalId, "osw7");
        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 137, "Failed/PodKill"));

        // Bump the attempt through the live seam the production path uses.
        assertEquals(1, intentRepo.claimForRelaunch(intentId).orElseThrow());
        assertTrue(outcomeRepo.insertOutcome(intentId, 1, Outcome.BUSINESS_ACCEPTED, 0, "Complete"));

        assertEquals(Outcome.BUSINESS_ACCEPTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "DagEngine's read sees the CURRENT attempt's outcome, not the dead attempt's TECH row");
        assertEquals(2, outcomeCount(intentId),
                "both attempts stay in the ledger as their own immutable rows (audit trail)");
    }

    @Test
    void clockIntentIgnored() {
        final String key = "osw6-" + suffix();
        final UUID clockId = intentRepo.insertClockIntent(Stage.CRG, key, "col-crg-" + key, "{}", "dcre-col")
                .orElseThrow();
        intentRepo.markIntentLaunched(clockId, "uid-" + key);
        assertTrue(outcomeRepo.insertOutcome(clockId, 0, Outcome.TECH_FAILED, 1, "Failed/Test"));

        relauncher.sweepTechOrphans(Map.of());
        relauncher.relaunchOrExhaust(
                new LaunchIntent(clockId, null, Stage.CRG, "col-crg-" + key,
                        LaunchIntent.LAUNCHED, key, 0, "dcre-col"), Outcome.TECH_FAILED, null);

        assertEquals(0, attemptOf(clockId), "clock intents self-heal at the next window boundary");
    }

    private UUID insertArrival(final String tag) {
        return arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req",
                "FNB" + tag + "_M" + UUID.randomUUID() + ".txt", "h-" + tag + "-" + suffix(),
                "FNB" + tag, "M1", ArrivalStatus.DAG_RUNNING, null, null).orElseThrow();
    }

    private UUID launchedIntent(final UUID arrivalId, final String tag) {
        final UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR,
                "col-crr-" + tag + "-" + suffix(), "dcre-col").orElseThrow();
        intentRepo.markIntentLaunched(intentId, "uid-" + tag);
        return intentId;
    }

    private LaunchIntent intentOf(final UUID intentId, final UUID arrivalId) {
        return intentRepo.intentsForArrival(arrivalId).stream()
                .filter(i -> i.id().equals(intentId)).findFirst().orElseThrow();
    }

    private int attemptOf(final UUID intentId) {
        return queryInt("SELECT attempt FROM launch_intent WHERE id=?", intentId);
    }

    private long outcomeCount(final UUID intentId) {
        return queryInt("SELECT count(*) FROM stage_outcome WHERE intent_id=?", intentId);
    }

    private void setAttempt(final UUID intentId, final int attempt) {
        exec("UPDATE launch_intent SET attempt=" + attempt + " WHERE id=?", intentId);
    }

    private void stampLastAttemptNow(final UUID intentId) {
        exec("UPDATE launch_intent SET last_attempt_at = now() WHERE id=?", intentId);
    }

    private void backdateCreatedAt(final UUID intentId) {
        exec("UPDATE launch_intent SET created_at = now() - INTERVAL '10 minutes' WHERE id=?", intentId);
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
