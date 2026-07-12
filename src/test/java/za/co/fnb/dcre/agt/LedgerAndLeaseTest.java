package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.Outcome;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.LedgerRepo;
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
    LedgerRepo repo;

    @Inject
    LeaseService lease;

    @Test
    void arrivalIdentityIsUniqueAndRedeliveryIsNoOp() {
        String name = "FNBRF01_TEST" + UUID.randomUUID().toString().substring(0, 6) + ".txt";
        Optional<UUID> first = repo.insertArrival("onhost-req", name, "hash-a", "FNBRF01", "MSG1",
                ArrivalStatus.CLAIMED, null);
        assertTrue(first.isPresent());
        Optional<UUID> replay = repo.insertArrival("onhost-req", name, "hash-a", "FNBRF01", "MSG1",
                ArrivalStatus.CLAIMED, null);
        assertTrue(replay.isEmpty(), "same (route,name,hash) must be a no-op");
    }

    @Test
    void intentIsWriteAheadAndNonOverlapping() {
        UUID arrival = repo.insertArrival("onhost-req", "FNBCC01_M" + UUID.randomUUID(), "h",
                "FNBCC01", "M1", ArrivalStatus.CLAIMED, null).orElseThrow();
        Optional<UUID> intent = repo.insertIntent(arrival, Stage.CRR, "dcre-crr-" + arrival.toString().substring(0, 8));
        assertTrue(intent.isPresent());
        assertTrue(repo.insertIntent(arrival, Stage.CRR, "other-name").isEmpty(),
                "second intent for same (arrival, stage) must be refused");

        assertTrue(repo.insertOutcome(intent.get(), Outcome.BUSINESS_ACCEPTED, 0, "Complete"));
        assertFalse(repo.insertOutcome(intent.get(), Outcome.TECH_FAILED, 1, "Failed"),
                "duplicate outcome observation must be a no-op");
        assertEquals(Outcome.BUSINESS_ACCEPTED, repo.outcomesForArrival(arrival).get(Stage.CRR),
                "first observation wins");
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
