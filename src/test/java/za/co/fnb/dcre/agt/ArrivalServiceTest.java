package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.service.ArrivalService;
import za.co.fnb.dcre.agt.service.ExchangeSinks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class ArrivalServiceTest {

    private static final String CLIENT = "FNBRF01";
    private static final String BASE = "fnbrf01";

    @Inject
    ArrivalService arrivals;

    @Inject
    ExchangeSinks sinks;

    @Inject
    AgtConfig config;

    /** Drop a file into the per-client inbound drop zone (same filesystem as the sinks). */
    private Path drop(final String base, final String channel, final String name,
                      final String content) throws Exception {
        final Path dir = Path.of(config.exchangeRoot(), base, channel, "in");
        Files.createDirectories(dir);
        final Path f = dir.resolve(name);
        Files.writeString(f, content);
        return f;
    }

    private boolean landedIn(final Path dir, final String needle) throws IOException {
        try (var s = Files.list(dir)) {
            return s.anyMatch(p -> p.getFileName().toString().contains(needle));
        }
    }

    @Test
    void newThenDuplicateThenQuarantine() throws Exception {
        final String route = ArrivalService.ROUTE_ONHOST_REQ;
        final String stem = CLIENT + "_DCRERF" + UUID.randomUUID().toString().substring(0, 8);

        final var first = arrivals.register(drop(BASE, route, stem + ".txt", "content-A"), route, CLIENT);
        assertInstanceOf(ArrivalService.Result.NewArrival.class, first, "fresh file registers");

        final var dup = arrivals.register(drop(BASE, route, stem + ".txt", "content-A"), route, CLIENT);
        assertInstanceOf(ArrivalService.Result.DuplicateSameHash.class, dup, "same key+hash no-ops");

        final var tampered = arrivals.register(drop(BASE, route, stem + ".txt", "content-B"), route, CLIENT);
        assertInstanceOf(ArrivalService.Result.Quarantined.class, tampered,
                "same logical key, different content quarantines");

        final UUID id = ((ArrivalService.Result.NewArrival) first).id();
        assertTrue(landedIn(sinks.inflight(CLIENT, route), id.toString()),
                "claimed file moved to the per-(client,channel) archive/inflight");
    }

    @Test
    void replyTypeSuffixesAreDistinctLogicalFiles() throws Exception {
        final String route = ArrivalService.ROUTE_FINT_RESP;
        final String stem = CLIENT + "_DCRERF" + UUID.randomUUID().toString().substring(0, 8);

        final var isr = arrivals.register(drop(BASE, route, stem + "_ISR.xml", "isr-body"), route, CLIENT);
        final var sbsr = arrivals.register(drop(BASE, route, stem + "_SBSR.xml", "sbsr-body"), route, CLIENT);
        final var pbsr = arrivals.register(drop(BASE, route, stem + "_PBSR.xml", "pbsr-body"), route, CLIENT);
        assertInstanceOf(ArrivalService.Result.NewArrival.class, isr, "ISR registers");
        assertInstanceOf(ArrivalService.Result.NewArrival.class, sbsr,
                "SBSR shares client+msgId but is a distinct logical file, never a key clash");
        assertInstanceOf(ArrivalService.Result.NewArrival.class, pbsr, "PBSR likewise");
    }

    @Test
    void filenameClientDifferentFromPathClientQuarantinesMismatch() throws Exception {
        final String route = ArrivalService.ROUTE_ONHOST_REQ;
        // An FNBCC01-named file dropped into the FNBRF01 drop zone: misfiled, fail closed.
        final String name = "FNBCC01_DCRECC" + UUID.randomUUID().toString().substring(0, 8) + ".txt";

        final var result = arrivals.register(drop(BASE, route, name, "misfiled-body"), route, CLIENT);

        final var quarantined = assertInstanceOf(ArrivalService.Result.Quarantined.class, result,
                "filename client != path client fails closed");
        assertEquals("CLIENT_PATH_MISMATCH", quarantined.reason(), "R-30 amendment mismatch reason");
        assertTrue(landedIn(sinks.error(CLIENT, route), name),
                "quarantined into the path client's error sink");
    }
}
