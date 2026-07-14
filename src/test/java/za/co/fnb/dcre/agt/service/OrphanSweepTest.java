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
        assertEquals(LaunchIntent.LAUNCHED, intentOf(intentId, arrivalId).status(),
                "intent stays LAUNCHED across the relaunch");
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

        assertEquals(1, intentRepo.beginRelaunchAttempt(intentId));
        assertTrue(outcomeRepo.insertOutcome(intentId, 1, Outcome.BUSINESS_ACCEPTED, 0, "Complete"));

        assertEquals(Outcome.BUSINESS_ACCEPTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR),
                "DagEngine's read sees the CURRENT attempt's outcome, not the dead attempt's TECH row");
        assertEquals(2, outcomeCount(intentId),
                "both attempts stay in the ledger as their own immutable rows (audit trail)");
    }

    @Test
    void clockIntentIgnored() {
        final String key = "osw6-" + suffix();
        final UUID clockId = intentRepo.insertClockIntent(Stage.PRG, key, "dcre-prg-" + key, "{}")
                .orElseThrow();
        intentRepo.markIntentLaunched(clockId, "uid-" + key);
        assertTrue(outcomeRepo.insertOutcome(clockId, 0, Outcome.TECH_FAILED, 1, "Failed/Test"));

        relauncher.sweepTechOrphans(Map.of());
        relauncher.relaunchOrExhaust(
                new LaunchIntent(clockId, null, Stage.PRG, "dcre-prg-" + key,
                        LaunchIntent.LAUNCHED, key, 0), null);

        assertEquals(0, attemptOf(clockId), "clock intents self-heal at the next window boundary");
    }

    private UUID insertArrival(final String tag) {
        return arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req",
                "FNB" + tag + "_M" + UUID.randomUUID() + ".txt", "h-" + tag + "-" + suffix(),
                "FNB" + tag, "M1", ArrivalStatus.DAG_RUNNING, null, null).orElseThrow();
    }

    private UUID launchedIntent(final UUID arrivalId, final String tag) {
        final UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR,
                "dcre-crr-" + tag + "-" + suffix()).orElseThrow();
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
