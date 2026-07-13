package za.co.fnb.dcre.agt;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.ArrivalStatus;
import za.co.fnb.dcre.agt.domain.FileArrival;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;
import za.co.fnb.dcre.agt.service.ArrivalService;
import za.co.fnb.dcre.agt.service.DirectoryWatcher;
import za.co.fnb.dcre.agt.service.ExchangeSinks;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Drives the discovery sweep directly (the scheduler is disabled in %test and
 * the tick is lease-gated) to verify the R-30 amendment (SCRUM-42): a file under
 * {@code <clientbase>/<route>/in} registers with the channel token as its route
 * and the path-derived client. Exercises ALL THREE inbound channels
 * (onhost-req, onhost-req-endo, fint-resp) so the per-(client, channel) sweep is
 * covered, not just onhost-req.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class DirectoryWatcherTest {

    @Inject
    DirectoryWatcher watcher;

    @Inject
    ExchangeSinks sinks;

    @Inject
    AgtConfig config;

    @Inject
    ArrivalRepo repo;

    private Path inDir(final String base, final String channel) throws IOException {
        final Path dir = Path.of(config.exchangeRoot(), base, channel, "in");
        Files.createDirectories(dir);
        return dir;
    }

    private boolean claimedInto(final Path dir, final String name) throws IOException {
        try (var s = Files.list(dir)) {
            return s.anyMatch(p -> p.getFileName().toString().endsWith(name));
        }
    }

    private FileArrival discover(final String base, final String channel,
                                 final String name, final String body) throws Exception {
        final Path f = inDir(base, channel).resolve(name);
        Files.writeString(f, body);
        watcher.scanOnce(); // first sweep records size (not yet stable)
        watcher.scanOnce(); // second sweep: stable -> register
        assertFalse(Files.exists(f), "stable file claimed out of " + base + "/" + channel + "/in");
        return repo.arrivalsByStatus(ArrivalStatus.CLAIMED).stream()
                .filter(a -> name.equals(a.physicalFilename()))
                .findFirst()
                .orElseThrow();
    }

    @Test
    void discoversOnhostReqUnderClientInDirWithPathClient() throws Exception {
        final String msgId = "DCRERF" + UUID.randomUUID().toString().substring(0, 8);
        final String name = "FNBRF01_" + msgId + ".txt";
        final FileArrival arrival =
                discover("fnbrf01", ArrivalService.ROUTE_ONHOST_REQ, name, "hello-" + msgId);

        assertEquals(ArrivalService.ROUTE_ONHOST_REQ, arrival.routeId(), "route id is the channel token");
        assertEquals("FNBRF01", arrival.clientToken(), "client_token is the path-derived client");
        assertTrue(claimedInto(sinks.inflight("FNBRF01", ArrivalService.ROUTE_ONHOST_REQ), name),
                "claimed into the FNBRF01 onhost-req archive/inflight sink");
    }

    @Test
    void discoversOnhostReqEndoChannel() throws Exception {
        final String msgId = "DCRECC" + UUID.randomUUID().toString().substring(0, 8);
        final String name = "FNBCC01_" + msgId + ".txt";
        final FileArrival arrival =
                discover("fnbcc01", ArrivalService.ROUTE_ONHOST_REQ_ENDO, name, "endo-" + msgId);

        assertEquals(ArrivalService.ROUTE_ONHOST_REQ_ENDO, arrival.routeId(),
                "onhost-req-endo channel is scanned");
        assertEquals("FNBCC01", arrival.clientToken());
        assertTrue(claimedInto(sinks.inflight("FNBCC01", ArrivalService.ROUTE_ONHOST_REQ_ENDO), name),
                "claimed into the FNBCC01 onhost-req-endo sink");
    }

    @Test
    void discoversFintRespChannel() throws Exception {
        final String msgId = "DCRECC" + UUID.randomUUID().toString().substring(0, 8);
        final String name = "FNBCC02_" + msgId + "_ISR.xml";
        final FileArrival arrival =
                discover("fnbcc02", ArrivalService.ROUTE_FINT_RESP, name, "isr-" + msgId);

        assertEquals(ArrivalService.ROUTE_FINT_RESP, arrival.routeId(), "fint-resp channel is scanned");
        assertEquals("FNBCC02", arrival.clientToken());
        assertTrue(claimedInto(sinks.inflight("FNBCC02", ArrivalService.ROUTE_FINT_RESP), name),
                "claimed into the FNBCC02 fint-resp sink");
    }

    /** True when the ledger holds ANY arrival (any status) for this physical filename. */
    private boolean ledgerHas(final String physicalFilename) {
        return repo.arrivalsByStatus(ArrivalStatus.values()).stream()
                .anyMatch(a -> physicalFilename.equals(a.physicalFilename()));
    }

    /**
     * The infra layout commits a {@code .gitkeep} inside every watched inbound drop
     * zone; a {@code .tmp} is the producer's not-yet-complete marker. Both must be
     * ignored by {@link DirectoryWatcher#consider}: skipped BEFORE size tracking, so
     * they never claim and never quarantine. Without the dotfile skip every committed
     * {@code .gitkeep} would be quarantined UNPARSEABLE_FILENAME on the first stable
     * tick. This locks that skip.
     */
    @Test
    void skipsDotfilesAndTmpMarkersSoTheyNeverEnterTheLedger() throws Exception {
        final Path dir = inDir("fnbrf01", ArrivalService.ROUTE_ONHOST_REQ);
        final Path gitkeep = dir.resolve(".gitkeep");           // committed drop-zone keepfile
        final Path tmpMarker = dir.resolve(".staging.tmp");     // dot + .tmp producer marker
        Files.writeString(gitkeep, "");
        Files.writeString(tmpMarker, "half-written");

        watcher.scanOnce(); // first sweep records size (not yet stable)
        watcher.scanOnce(); // second sweep: a non-dotfile would register here

        assertTrue(Files.exists(gitkeep), ".gitkeep left untouched in the drop zone (skipped, not claimed)");
        assertTrue(Files.exists(tmpMarker), ".tmp marker left untouched in the drop zone");
        assertFalse(ledgerHas(".gitkeep"),
                ".gitkeep never enters the ledger: neither claimed nor quarantined");
        assertFalse(ledgerHas(".staging.tmp"),
                ".tmp marker never enters the ledger: neither claimed nor quarantined");
    }

    /**
     * A regular file whose name carries no FNB token is unparseable and fails closed
     * (F13): quarantined UNPARSEABLE_FILENAME with the path-derived client as the
     * authoritative {@code client_token} (previously null before the R-30 amendment).
     */
    @Test
    void quarantinesUnparseableFilenameWithPathClientToken() throws Exception {
        final String name = "notes-" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        final Path f = inDir("fnbrf01", ArrivalService.ROUTE_ONHOST_REQ).resolve(name);
        Files.writeString(f, "no-token-body");

        watcher.scanOnce(); // first sweep records size (not yet stable)
        watcher.scanOnce(); // second sweep: stable -> register -> quarantine
        assertFalse(Files.exists(f), "unparseable file claimed then quarantined out of the drop zone");

        final FileArrival quarantined = repo.arrivalsByStatus(ArrivalStatus.QUARANTINED).stream()
                .filter(a -> name.equals(a.physicalFilename()))
                .findFirst()
                .orElseThrow();
        assertEquals("UNPARSEABLE_FILENAME", quarantined.quarantineReason(),
                "no FNB token -> UNPARSEABLE_FILENAME");
        assertEquals("FNBRF01", quarantined.clientToken(),
                "client_token is the path-derived client, not null");
    }
}
