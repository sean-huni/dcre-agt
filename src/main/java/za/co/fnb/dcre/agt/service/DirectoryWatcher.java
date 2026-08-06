package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig.ChannelDirs;
import za.co.fnb.dcre.agt.config.AgtExchangeConfig.InboundChannels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Polls the per-client inbound exchange drop zones and registers stable files
 * (SPEC-DAG section 3; R-30 amendment, SCRUM-42).
 *
 * <p>The layout is client-first: every configured client owns three inbound
 * channels, each watched under {@code <root>/<clientbase>/<route>/in}. The
 * {@code <route>} segment still equals the AGT {@code route_id}
 * ({@code onhost-req}, {@code onhost-req-endo}, {@code fint-resp}); the
 * {@code <clientbase>} segment above it names the client, so the client is now
 * path-derived and handed to {@link ArrivalService} as {@code pathClient} for a
 * cross-check against the filename FNB token.
 *
 * <p>A file is READY only when its size is stable across two consecutive ticks
 * and it is not a {@code .tmp}/dotfile (producer-ready protocol). Lease-gated.
 */
@ApplicationScoped
public class DirectoryWatcher {

    private static final Logger LOG = Logger.getLogger(DirectoryWatcher.class);

    /** (route id, accessor into a client's dirs) for each of the five AGT
     *  inbound channels (M10/SCRUM-79 adds the dedicated man pair). */
    private static final List<InboundChannel> INBOUND = List.of(
            new InboundChannel(ArrivalService.ROUTE_ONHOST_REQ, InboundChannels::onhostReq),
            new InboundChannel(ArrivalService.ROUTE_ONHOST_REQ_ENDO, InboundChannels::onhostReqEndo),
            new InboundChannel(ArrivalService.ROUTE_FINT_RESP, InboundChannels::fintResp),
            new InboundChannel(ArrivalService.ROUTE_ONHOST_REQ_MAN, InboundChannels::onhostReqMan),
            new InboundChannel(ArrivalService.ROUTE_FINT_RESP_MAN, InboundChannels::fintRespMan));

    private record InboundChannel(String route, Function<InboundChannels, ChannelDirs> dirs) { }

    /** The routes this watcher can produce. Exposed so RouteRegistryConsistencyTest
     *  can assert every one of them resolves to a real DAG: INBOUND and the
     *  RouteDags registries are two hand-maintained copies of one fact
     *  (SCRUM-107). */
    static java.util.List<String> inboundRoutes() {
        return INBOUND.stream().map(InboundChannel::route).toList();
    }

    @Inject
    AgtConfig config;

    @Inject
    AgtExchangeConfig exchange;

    @Inject
    LeaseService lease;

    @Inject
    ArrivalService arrivals;

    private final Map<Path, Long> lastSizes = new ConcurrentHashMap<>();

    @RunOnVirtualThread
    @Scheduled(every = "2s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease()) {
            return;
        }
        scanOnce();
    }

    /** One discovery sweep over every (client, inbound-channel) drop zone. */
    public void scanOnce() {
        exchange.clients().forEach((client, channels) ->
                INBOUND.forEach(ch -> scan(client, ch.route(), ch.dirs().apply(channels))));
        lastSizes.keySet().removeIf(p -> !Files.exists(p));
    }

    private void scan(final String client, final String route, final ChannelDirs dirs) {
        final Path dir = Path.of(config.exchangeRoot(), dirs.in());
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(Files::isRegularFile).forEach(f -> {
                try {
                    consider(f, route, client);
                } catch (Exception e) {
                    LOG.warnf("consider %s failed: %s", f.getFileName(), e.getMessage());
                }
            });
        } catch (IOException e) {
            LOG.warnf("watch tick failed for %s/%s: %s", client, route, e.getMessage());
        }
    }

    void consider(final Path file, final String route, final String pathClient) {
        final String name = file.getFileName().toString();
        if (name.endsWith(".tmp") || name.startsWith(".")) {
            return;
        }
        final long size;
        try {
            size = Files.size(file);
        } catch (IOException e) {
            return; // vanished between list and stat; next tick decides
        }
        final Long previous = lastSizes.put(file, size);
        if (previous == null || previous != size) {
            return; // not yet stable
        }
        lastSizes.remove(file);
        arrivals.register(file, route, pathClient);
    }
}
