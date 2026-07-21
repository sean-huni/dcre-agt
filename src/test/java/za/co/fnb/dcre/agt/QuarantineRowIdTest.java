package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.service.ArrivalService;
import za.co.fnb.dcre.agt.service.ExchangeSinks;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-58 file-trace, spec 2.3 / plan Task 4: the quarantine row-id fix. Every
 * quarantine (UNPARSEABLE_FILENAME, CLIENT_PATH_MISMATCH, SAME_KEY_DIFFERENT_HASH)
 * must mint the file_arrival row under the SAME claim UUID that prefixes the
 * error-dir file, and store the error sink path in claimed_path, so an error-dir
 * {@code <uuid>_<name>} resolves to its row with no prefix-stripping folklore.
 * Red first: the current code mints a random row id != the error-file prefix.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class QuarantineRowIdTest {

    private static final String CLIENT = "FNBRF01";
    private static final String BASE = "fnbrf01";

    @Inject
    ArrivalService arrivals;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    ExchangeSinks sinks;

    @Inject
    AgtConfig config;

    private Path drop(final String channel, final String name, final String content) throws Exception {
        final Path dir = Path.of(config.exchangeRoot(), BASE, channel, "in");
        Files.createDirectories(dir);
        final Path f = dir.resolve(name);
        Files.writeString(f, content);
        return f;
    }

    @Test
    void unparseableFilenameRowIdIsClaimUuidAndClaimedPathIsSink() throws Exception {
        final String route = ArrivalService.ROUTE_ONHOST_REQ;
        final String name = "notes-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";

        final var r = arrivals.register(drop(route, name, "no-tokens"), route, CLIENT);

        assertEquals("UNPARSEABLE_FILENAME",
                assertInstanceOf(ArrivalService.Result.Quarantined.class, r).reason());
        assertRowResolvesFromErrorFile(route, name, "UNPARSEABLE_FILENAME");
    }

    @Test
    void clientPathMismatchRowIdIsClaimUuidAndClaimedPathIsSink() throws Exception {
        final String route = ArrivalService.ROUTE_ONHOST_REQ;
        // An FNBCC01-named file misfiled into the FNBRF01 drop zone.
        final String name = "FNBCC01_DCRECC" + UUID.randomUUID().toString().substring(0, 8) + ".txt";

        final var r = arrivals.register(drop(route, name, "misfiled"), route, CLIENT);

        assertEquals("CLIENT_PATH_MISMATCH",
                assertInstanceOf(ArrivalService.Result.Quarantined.class, r).reason());
        assertRowResolvesFromErrorFile(route, name, "CLIENT_PATH_MISMATCH");
    }

    @Test
    void sameKeyDifferentHashRowIdIsClaimUuidAndClaimedPathIsSink() throws Exception {
        final String route = ArrivalService.ROUTE_ONHOST_REQ;
        final String name = CLIENT + "_DCRERF" + UUID.randomUUID().toString().substring(0, 8) + ".txt";

        assertInstanceOf(ArrivalService.Result.NewArrival.class,
                arrivals.register(drop(route, name, "orig"), route, CLIENT));
        final var r = arrivals.register(drop(route, name, "tampered"), route, CLIENT);

        assertEquals("SAME_KEY_DIFFERENT_HASH",
                assertInstanceOf(ArrivalService.Result.Quarantined.class, r).reason());
        assertRowResolvesFromErrorFile(route, name, "SAME_KEY_DIFFERENT_HASH");
    }

    /**
     * The error-dir file is {@code <claimUuid>_<name>}; its uuid prefix must be a
     * file_arrival row id (status QUARANTINED, the given reason) whose claimed_path
     * equals the error sink path.
     */
    private void assertRowResolvesFromErrorFile(final String route, final String name,
                                                final String reason) throws Exception {
        final Path errorDir = sinks.error(CLIENT, route);
        final String errorFile;
        try (Stream<Path> s = Files.list(errorDir)) {
            final List<String> matches = s.map(p -> p.getFileName().toString())
                    .filter(fn -> fn.endsWith("_" + name)).toList();
            assertEquals(1, matches.size(), "exactly one error-dir file for " + name + ": " + matches);
            errorFile = matches.get(0);
        }
        final UUID prefix = UUID.fromString(errorFile.substring(0, errorFile.indexOf('_')));

        final var row = arrivalRepo.arrivalById(prefix);
        assertTrue(row.isPresent(),
                "error-dir file prefix " + prefix + " must be the file_arrival row id");
        assertEquals(ArrivalStatus.QUARANTINED, row.get().status());
        assertEquals(reason, row.get().quarantineReason());
        assertEquals(errorDir.resolve(errorFile).toString(), row.get().claimedPath(),
                "claimed_path holds the error sink path");
    }
}
