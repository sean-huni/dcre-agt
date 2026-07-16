package za.co.fnb.dcre.agt.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import za.co.fnb.dcre.agt.CrdbTestResource;
import za.co.fnb.dcre.agt.config.AgtConfig;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SCRUM-58 file-trace, spec 2.3/F3 / plan Task 5: LatentDirAuditor. AGT owns no
 * producer for the OUTBOUND onhost-resp / fint-req channels, so their error/ and
 * archive/ sub-dirs are dormant from AGT's view. A slow observation tick WARNs one
 * uniform line per unexpected file and increments
 * dcre_agt_latent_dir_files_total{client, dir} so the chaos-gate trace-resolution
 * audit and a Grafana alert catch any uncontracted producer within one tick.
 */
@QuarkusTest
@QuarkusTestResource(CrdbTestResource.class)
class LatentDirAuditorTest {

    private static final String CLIENT = "FNBRF01";
    private static final String BASE = "fnbrf01";
    private static final String COUNTER = "dcre_agt_latent_dir_files_total";

    @Inject
    LatentDirAuditor auditor;

    @Inject
    MeterRegistry registry;

    @Inject
    AgtConfig config;

    private SimpleMeterRegistry meterReader;
    private CapturingHandler warns;

    /** The dormant out-dirs AGT never produces into (must stay empty). */
    private static final List<String> DORMANT = List.of(
            "onhost-resp/error", "onhost-resp/archive", "fint-req/error", "fint-req/archive");

    @BeforeEach
    void attach() throws Exception {
        meterReader = new SimpleMeterRegistry();
        ((CompositeMeterRegistry) registry).add(meterReader);
        warns = new CapturingHandler();
        Logger.getLogger(LatentDirAuditor.class.getName()).addHandler(warns);
        // Start every case from a pristine dormant state so counts and WARNs are
        // driven only by what the case drops (build/test-exchange persists on disk).
        for (final String dir : DORMANT) {
            cleanDir(Path.of(config.exchangeRoot(), BASE, dir));
        }
    }

    @AfterEach
    void detach() {
        Logger.getLogger(LatentDirAuditor.class.getName()).removeHandler(warns);
        ((CompositeMeterRegistry) registry).remove(meterReader);
        meterReader.close();
    }

    @Test
    void fileInDormantErrorDirWarnsAndCounts() throws Exception {
        final String dir = "onhost-resp/error";
        final Path errorDir = Path.of(config.exchangeRoot(), BASE, dir);
        final String name = "STRAY_" + UUID.randomUUID().toString().substring(0, 8) + ".txt";
        Files.writeString(errorDir.resolve(name), "leaked");

        auditor.tick();

        assertEquals(1.0, counterValue(dir), "one latent file counted for {client, dir}");
        assertTrue(warns.lines.contains(
                        "latent-dir file client=" + CLIENT + " dir=" + dir + " name=" + name),
                "uniform WARN line for the latent file: " + warns.lines);
    }

    @Test
    void emptyDormantDirProducesNoWarnOrCounter() {
        final String dir = "fint-req/archive";

        auditor.tick();

        assertNull(meterReader.find(COUNTER).tag("client", CLIENT).tag("dir", dir).counter(),
                "no counter for an empty dormant dir");
        assertTrue(warns.lines.stream().noneMatch(w -> w.contains(dir)),
                "no WARN for an empty dormant dir: " + warns.lines);
    }

    private double counterValue(final String dir) {
        final var counter = meterReader.find(COUNTER)
                .tag("client", CLIENT).tag("dir", dir).counter();
        assertNotNull(counter, COUNTER + "{client=" + CLIENT + ", dir=" + dir + "} not registered");
        return counter.count();
    }

    private Path cleanDir(final Path dir) throws Exception {
        Files.createDirectories(dir);
        try (Stream<Path> s = Files.list(dir)) {
            for (final Path p : s.toList()) {
                Files.deleteIfExists(p);
            }
        }
        return dir;
    }

    /** Reproduces the printf-style warnf line the auditor emits (SlaMonitorTest pattern). */
    private static final class CapturingHandler extends Handler {
        private final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        public void publish(final LogRecord record) {
            if (record.getLevel().intValue() >= Level.WARNING.intValue()) {
                final Object[] params = record.getParameters();
                lines.add(params == null || params.length == 0
                        ? record.getMessage()
                        : String.format(record.getMessage(), params));
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    }
}
