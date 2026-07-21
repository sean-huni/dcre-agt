package za.co.fnb.dcre.agt.service;

import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig.InboundChannels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * F3 detection arm (spec 2.3/4.4): AGT owns no producer for the OUTBOUND
 * onhost-resp / fint-req channels, so their {@code error/} and {@code archive/}
 * sub-dirs are dormant from AGT's view. Any file landing there is an uncontracted
 * producer (Section 4.4 latent-dir capture contract) the trace layer cannot
 * resolve. A slow observation tick (no lease gate: pure observation, the
 * MetricsService / SlaMonitor precedent) WARNs one uniform line per file and
 * increments {@code dcre_agt_latent_dir_files_total{client, dir}} so the
 * chaos-gate trace-resolution audit and a Grafana alert catch it within one tick.
 */
@ApplicationScoped
public class LatentDirAuditor {

    private static final Logger LOG = Logger.getLogger(LatentDirAuditor.class);
    private static final String COUNTER = "dcre_agt_latent_dir_files_total";

    /** Outbound channels AGT never watches; their error/archive stay empty. */
    private static final List<String> DORMANT = List.of(
            "onhost-resp/error", "onhost-resp/archive", "fint-req/error", "fint-req/archive");

    @Inject
    AgtConfig config;

    @Inject
    AgtExchangeConfig exchange;

    @Inject
    MeterRegistry registry;

    @RunOnVirtualThread
    @Scheduled(every = "300s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        exchange.clients().forEach((client, channels) -> {
            final String base = clientBase(channels);
            DORMANT.forEach(dir -> audit(client, base, dir));
        });
    }

    /** Client base dir segment, read from the client's configured inbound path. */
    private static String clientBase(final InboundChannels channels) {
        final String in = channels.onhostReq().in();   // e.g. "fnbrf01/onhost-req/in"
        final int slash = in.indexOf('/');
        return slash > 0 ? in.substring(0, slash) : in;
    }

    private void audit(final String client, final String base, final String dir) {
        final Path path = Path.of(config.exchangeRoot(), base, dir);
        if (!Files.isDirectory(path)) {
            return;
        }
        try (Stream<Path> files = Files.list(path)) {
            files.filter(Files::isRegularFile)
                    .filter(f -> !f.getFileName().toString().startsWith("."))
                    .forEach(f -> flag(client, dir, f.getFileName().toString()));
        } catch (IOException e) {
            LOG.warnf("latent-dir scan failed client=%s dir=%s: %s", client, dir, e.getMessage());
        }
    }

    private void flag(final String client, final String dir, final String name) {
        LOG.warnf("latent-dir file client=%s dir=%s name=%s", client, dir, name);
        registry.counter(COUNTER, "client", client, "dir", dir).increment();
    }
}
