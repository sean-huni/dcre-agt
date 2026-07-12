package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Polls the routed exchange directories (onhost-req; fint-resp in M4;
 * onhost-req-endo in M5). A file is READY only when its size is stable across
 * two consecutive ticks and it is not a .tmp (producer-ready protocol,
 * SPEC-DAG section 3). Lease-gated.
 * The directory name IS the route id (R-30 contract).
 */
@ApplicationScoped
public class DirectoryWatcher {

    private static final Logger LOG = Logger.getLogger(DirectoryWatcher.class);

    static final java.util.List<String> ROUTES = java.util.List.of(
            ArrivalService.ROUTE_ONHOST_REQ,
            ArrivalService.ROUTE_FINT_RESP,
            ArrivalService.ROUTE_ONHOST_REQ_ENDO);

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    ArrivalService arrivals;

    private final Map<Path, Long> lastSizes = new ConcurrentHashMap<>();

    @Scheduled(every = "2s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease()) {
            return;
        }
        ROUTES.forEach(this::scan);
        lastSizes.keySet().removeIf(p -> !Files.exists(p));
    }

    private void scan(String route) {
        Path dir = Path.of(config.exchangeRoot(), route);
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (Stream<Path> files = Files.list(dir)) {
            files.filter(Files::isRegularFile).forEach(f -> {
                try {
                    consider(f, route);
                } catch (Exception e) {
                    LOG.warnf("consider %s failed: %s", f.getFileName(), e.getMessage());
                }
            });
        } catch (IOException e) {
            LOG.warnf("watch tick failed for %s: %s", route, e.getMessage());
        }
    }

    void consider(Path file, String route) {
        String name = file.getFileName().toString();
        if (name.endsWith(".tmp") || name.startsWith(".")) {
            return;
        }
        long size;
        try {
            size = Files.size(file);
        } catch (IOException e) {
            return; // vanished between list and stat; next tick decides
        }
        Long previous = lastSizes.put(file, size);
        if (previous == null || previous != size) {
            return; // not yet stable
        }
        lastSizes.remove(file);
        arrivals.register(file, route);
    }
}
