package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.repo.DuplicateRepo;
import za.co.fnb.dcre.agt.service.ArrivalService;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-58 file-trace, spec 1.1 / plan Task 3: every re-delivery of already
 * registered bytes is captured write-ahead in duplicate_delivery keyed by the
 * per-delivery claim_id, BEFORE the file is sunk. Covers: (a) one row per
 * re-delivery event carrying its OWN as-delivered name + original_arrival_id,
 * N re-deliveries = N rows; (b) the claim_id idempotency that makes a
 * kill-between-insert-and-move orphan-sweep resume a no-op
 * (count(*) == count(DISTINCT claim_id)); (c) a same-key-different-hash arrival
 * still quarantines and records NO duplicate row (dedup arbiter untouched).
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class DuplicateDeliveryTest {

    private static final String CLIENT = "FNBRF01";
    private static final String BASE = "fnbrf01";

    @Inject
    ArrivalService arrivals;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    DuplicateRepo duplicateRepo;

    @Inject
    DataSource ds;

    private Path drop(final String base, final String channel, final String name,
                      final String content) throws Exception {
        final Path dir = Path.of("build/test-exchange", base, channel, "in");
        Files.createDirectories(dir);
        final Path f = dir.resolve(name);
        Files.writeString(f, content);
        return f;
    }

    @Test
    void reDeliveryRecordsOneRowPerEventWithOwnNameAndOriginal() throws Exception {
        final String route = ArrivalService.ROUTE_ONHOST_REQ;
        final String stem = CLIENT + "_DCRERF" + UUID.randomUUID().toString().substring(0, 8);
        final String body = "content-A-" + UUID.randomUUID();

        final var first = arrivals.register(drop(BASE, route, stem + ".txt", body), route, CLIENT);
        final UUID originalId = assertInstanceOf(ArrivalService.Result.NewArrival.class, first,
                "fresh file registers").id();

        // Same bytes, NEW physical names: two distinct re-delivery events.
        final String re1 = stem + "_RE1.txt";
        final String re2 = stem + "_RE2.txt";
        assertInstanceOf(ArrivalService.Result.DuplicateSameHash.class,
                arrivals.register(drop(BASE, route, re1, body), route, CLIENT), "re-delivery 1 no-ops");
        assertInstanceOf(ArrivalService.Result.DuplicateSameHash.class,
                arrivals.register(drop(BASE, route, re2, body), route, CLIENT), "re-delivery 2 no-ops");

        assertEquals(2, dupCountForOriginal(originalId),
                "N re-deliveries = N duplicate_delivery rows, each on the original flow");
        assertTrue(dupExistsForName(re1), "re-delivery 1 name captured");
        assertTrue(dupExistsForName(re2), "re-delivery 2 name captured");
    }

    @Test
    void resumeReParsesClaimUuidSoWriteAheadInsertIsIdempotent() {
        // An original arrival satisfies the FK; the twin the duplicate points at.
        final String name = CLIENT + "_TWIN" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        final String sha = "sha-" + UUID.randomUUID();
        final UUID originalId = arrivalRepo.insertArrival(UUID.randomUUID(),
                ArrivalService.ROUTE_ONHOST_REQ, name, sha, CLIENT, "TWIN", ArrivalStatus.CLAIMED,
                null, null).orElseThrow();

        // claim_id = the UUID that prefixes the inflight file. A SIGKILL between
        // the write-ahead insert and moveToDuplicates leaves that file in flight;
        // the orphan-sweep resume re-parses the SAME claim_id from the prefix.
        final UUID claimId = UUID.randomUUID();
        final String reName = CLIENT + "_RESUME.txt";
        final String sunk = "/x/archive/duplicates/" + claimId + "_" + reName;

        assertTrue(duplicateRepo.insertDuplicate(claimId, originalId, ArrivalService.ROUTE_ONHOST_REQ,
                CLIENT, reName, sha, sunk), "write-ahead insert records the re-delivery");
        assertFalse(duplicateRepo.insertDuplicate(claimId, originalId, ArrivalService.ROUTE_ONHOST_REQ,
                CLIENT, reName, sha, sunk), "resume re-parse of the same claim_id is an ON CONFLICT no-op");

        assertEquals(rowCountForClaim(claimId), distinctClaimForClaim(claimId),
                "zero-duplicate audit: count(*) == count(DISTINCT claim_id)");
        assertEquals(1, rowCountForClaim(claimId), "exactly one row survives the resume");
    }

    @Test
    void sameKeyDifferentHashStillQuarantinesAndRecordsNoDuplicateRow() throws Exception {
        final String route = ArrivalService.ROUTE_ONHOST_REQ;
        final String stem = CLIENT + "_DCRERF" + UUID.randomUUID().toString().substring(0, 8);

        assertInstanceOf(ArrivalService.Result.NewArrival.class,
                arrivals.register(drop(BASE, route, stem + ".txt", "orig-body"), route, CLIENT));
        final var tampered = arrivals.register(drop(BASE, route, stem + ".txt", "tampered-body"), route, CLIENT);

        final var q = assertInstanceOf(ArrivalService.Result.Quarantined.class, tampered,
                "same logical key, different content still quarantines (dedup arbiter untouched)");
        assertEquals("SAME_KEY_DIFFERENT_HASH", q.reason());
        assertEquals(0, dupCountForName(stem + ".txt"),
                "a quarantine is never a duplicate re-delivery: no duplicate_delivery row");
    }

    private long dupCountForOriginal(final UUID originalId) {
        return queryLong("SELECT count(*) FROM duplicate_delivery WHERE original_arrival_id=?", originalId);
    }

    private long rowCountForClaim(final UUID claimId) {
        return queryLong("SELECT count(*) FROM duplicate_delivery WHERE claim_id=?", claimId);
    }

    private long distinctClaimForClaim(final UUID claimId) {
        return queryLong("SELECT count(DISTINCT claim_id) FROM duplicate_delivery WHERE claim_id=?", claimId);
    }

    private boolean dupExistsForName(final String name) {
        return queryLongStr("SELECT count(*) FROM duplicate_delivery WHERE physical_filename=?", name) > 0;
    }

    private long dupCountForName(final String name) {
        return queryLongStr("SELECT count(*) FROM duplicate_delivery WHERE physical_filename=?", name);
    }

    private long queryLong(final String sql, final UUID id) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setObject(1, id);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }

    private long queryLongStr(final String sql, final String arg) {
        try (Connection c = ds.getConnection(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, arg);
            try (ResultSet r = p.executeQuery()) {
                r.next();
                return r.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed: " + sql, e);
        }
    }
}
