package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.IntentRepo;
import za.co.fnb.dcre.agt.repo.OutcomeRepo;
import za.co.fnb.dcre.agt.service.LeaseService;

import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class LedgerAndLeaseTest {

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    IntentRepo intentRepo;

    @Inject
    OutcomeRepo outcomeRepo;

    @Inject
    LeaseService lease;

    @Test
    void arrivalIdentityIsUniqueAndRedeliveryIsNoOp() {
        String name = "FNBRF01_TEST" + UUID.randomUUID().toString().substring(0, 6) + ".txt";
        Optional<UUID> first = arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req", name, "hash-a", "FNBRF01", "MSG1",
                ArrivalStatus.CLAIMED, null, null);
        assertTrue(first.isPresent());
        Optional<UUID> replay = arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req", name, "hash-a", "FNBRF01", "MSG1",
                ArrivalStatus.CLAIMED, null, null);
        assertTrue(replay.isEmpty(), "same (route,name,hash) must be a no-op");
    }

    @Test
    void intentIsWriteAheadAndNonOverlapping() {
        UUID arrival = arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req", "FNBCC01_M" + UUID.randomUUID(), "h",
                "FNBCC01", "M1", ArrivalStatus.CLAIMED, null, null).orElseThrow();
        Optional<UUID> intent = intentRepo.insertIntent(arrival, Stage.CRR, "dcre-crr-" + arrival.toString().substring(0, 8));
        assertTrue(intent.isPresent());
        assertTrue(intentRepo.insertIntent(arrival, Stage.CRR, "other-name").isEmpty(),
                "second intent for same (arrival, stage) must be refused");

        assertTrue(outcomeRepo.insertOutcome(intent.get(), Outcome.BUSINESS_ACCEPTED, 0, "Complete"));
        assertFalse(outcomeRepo.insertOutcome(intent.get(), Outcome.TECH_FAILED, 1, "Failed"),
                "duplicate outcome observation must be a no-op");
        assertEquals(Outcome.BUSINESS_ACCEPTED, outcomeRepo.outcomesForArrival(arrival).get(Stage.CRR),
                "first observation wins");
    }

    @Test
    void distinctClientTokensComeFromRequestRoutesOnly() {
        String reqClient = "FNBPR" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();
        String endoClient = "FNBEN" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();
        String respClient = "FNBRS" + UUID.randomUUID().toString().substring(0, 5).toUpperCase();
        arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req", reqClient + "_M1.txt", "h-" + reqClient,
                reqClient, "M1", ArrivalStatus.CLAIMED, null, null).orElseThrow();
        arrivalRepo.insertArrival(UUID.randomUUID(), "onhost-req-endo", endoClient + "_M1.txt", "h-" + endoClient,
                endoClient, "M1", ArrivalStatus.CLAIMED, null, null).orElseThrow();
        arrivalRepo.insertArrival(UUID.randomUUID(), "fint-resp", respClient + "_PBSR.txt", "h-" + respClient,
                respClient, "PBSR", ArrivalStatus.CLAIMED, null, null).orElseThrow();

        var tokens = arrivalRepo.distinctClientTokens();
        assertTrue(tokens.contains(reqClient), "onhost-req client is a PRG window client");
        assertTrue(tokens.contains(endoClient), "onhost-req-endo client gets PSR windows too (M5)");
        assertFalse(tokens.contains(respClient), "fint-resp clients never seed PRG windows");
    }

    @Test
    void leaseIsSingletonWithTakeoverOnlyAfterExpiry() {
        assertTrue(lease.tryAcquire("holder-a"));
        assertFalse(lease.tryAcquire("holder-b"), "live lease must not be stolen");
        assertTrue(lease.tryAcquire("holder-a"), "holder renews freely");
        assertTrue(lease.holds("holder-a"));
        assertFalse(lease.holds("holder-b"));
    }
}
