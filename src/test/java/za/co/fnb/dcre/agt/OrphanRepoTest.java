package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.LaunchIntent;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * OrphanSweeper repo plumbing (R-05 amendment, spec 2026-07-14-stuck-job-recovery-design.md):
 * attempt bookkeeping on launch_intent, per-attempt insert-once stage_outcome rows,
 * and current-attempt-only readers.
 *
 * Method order is pinned: the suite shares one CRDB, and the second test
 * deliberately leaves a persistent TECH orphan that would break the first
 * test's empty-sweep assertion if it ran first.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class OrphanRepoTest {

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Test
    @Order(1)
    void attemptBookkeepingAndPerAttemptOutcomes() {
        UUID arrivalId = insertArrival("ORPH1");
        UUID intentId = intentRepo.insertIntent(arrivalId, Stage.CRR,
                "col-crr-orph1-" + suffix(), "dcre-col").orElseThrow();
        intentRepo.markIntentLaunched(intentId, "uid-orph1");

        assertTrue(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 5, "Failed/Test"));
        // Bump the attempt through the live seam the production path uses.
        assertEquals(1, intentRepo.claimForRelaunch(intentId).orElseThrow());
        assertTrue(intentRepo.lastAttemptAt(intentId).isPresent());
        // per-attempt insert-once: attempt 0 replay no-op, attempt 1 fresh row ok
        assertFalse(outcomeRepo.insertOutcome(intentId, 0, Outcome.TECH_FAILED, 5, "Failed/Test"));
        assertTrue(outcomeRepo.insertOutcome(intentId, 1, Outcome.BUSINESS_ACCEPTED, 0, "Complete"));
        // current-attempt readers
        assertTrue(intentRepo.launchedArrivalIntentsWithTechCurrentAttempt().isEmpty());
        assertEquals(Outcome.BUSINESS_ACCEPTED, outcomeRepo.outcomesForArrival(arrivalId).get(Stage.CRR));
    }

    @Test
    @Order(2)
    void techCurrentAttemptSweepFindsArrivalIntentButNeverClockIntent() {
        UUID arrivalId = insertArrival("ORPH2");
        UUID intentId2 = intentRepo.insertIntent(arrivalId, Stage.CRR,
                "col-crr-orph2-" + suffix(), "dcre-col").orElseThrow();
        intentRepo.markIntentLaunched(intentId2, "uid-orph2");
        assertTrue(outcomeRepo.insertOutcome(intentId2, 0, Outcome.TECH_FAILED, 1, "Failed/Test"));

        UUID clockId = intentRepo.insertClockIntent(Stage.PRG, "orph-" + suffix(),
                "col-prg-orph-" + suffix(), "{}", "dcre-col").orElseThrow();
        intentRepo.markIntentLaunched(clockId, "uid-orph-clock");
        assertTrue(outcomeRepo.insertOutcome(clockId, 0, Outcome.TECH_FAILED, 1, "Failed/Test"));

        List<LaunchIntent> orphans = intentRepo.launchedArrivalIntentsWithTechCurrentAttempt();
        assertEquals(1, orphans.size(), "exactly the arrival intent with TECH at its current attempt");
        assertEquals(intentId2, orphans.get(0).id());
        assertEquals(0, orphans.get(0).attempt(), "map() must read the real attempt column");
        assertTrue(orphans.stream().noneMatch(i -> i.id().equals(clockId)),
                "clock intents (arrival_id IS NULL) are never swept");
    }

    private UUID insertArrival(final String tag) {
        return arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req",
                "FNB" + tag + "_M" + UUID.randomUUID() + ".txt", "h-" + tag + "-" + suffix(),
                "FNB" + tag, "M1", ArrivalStatus.CLAIMED, null, null).orElseThrow();
    }

    private static String suffix() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
