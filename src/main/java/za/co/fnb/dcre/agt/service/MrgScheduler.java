package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * M10/SCRUM-79: launches the MRG mandates-report executor per (client, window),
 * PrgScheduler pattern. Windows derive from the epoch so every AGT incarnation
 * computes the same run key (level-triggered; the clock-intent unique key
 * dedupes). Restricted to mandate-capable clients (interim agt.man-clients set,
 * normalized trim+uppercase like pay-clients, until the R-14 client table
 * lands); all MRG windows ride Flow.MAN (client-independent, unlike PRG's
 * per-client flow). An on-demand trigger file chaos/run-mrg-&lt;client&gt;
 * launches an immediate manual window.
 */
@ApplicationScoped
public class MrgScheduler {

    private static final Logger LOG = Logger.getLogger(MrgScheduler.class);

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    JobLauncher launcher;

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled() || config.mrgImage().isEmpty()) {
            return; // absent/empty image = launch-disabled until the 2.3 line
        }
        long window = PrgScheduler.window(Instant.now().getEpochSecond(), config.mrgIntervalSeconds());
        Set<String> manClients = manClients();
        for (String client : arrivalRepo.distinctClientTokens()) {
            if (!manClients.contains(normalize(client))) {
                continue; // not mandate-capable: no MRG window
            }
            launcher.launchClock(Flow.MAN, Stage.MRG, client + "-w" + window, List.of(
                    "client=" + client,
                    "window=w" + window));
            considerManualTrigger(client, window);
        }
    }

    private void considerManualTrigger(String client, long window) {
        Path trigger = Path.of(config.exchangeRoot(), "chaos", "run-mrg-" + client);
        try {
            if (!Files.deleteIfExists(trigger)) {
                return;
            }
        } catch (IOException e) {
            LOG.warnf("manual MRG trigger for %s failed: %s", client, e.getMessage());
            return;
        }
        // Idempotent within the window: repeated triggers reuse one run key.
        // The window param carries a -manual suffix so the Batch job instance is
        // distinct from the scheduled run of the same window (identifying params
        // are the instance identity; resend alone is non-identifying).
        launcher.launchClock(Flow.MAN, Stage.MRG, client + "-manual-" + window, List.of(
                "client=" + client,
                "window=w" + window + "-manual",
                "resend=true,java.lang.String,false"));
    }

    /** Config values normalized on read (m4): membership never depends on
     *  whitespace or case in AGT_MAN_CLIENTS. */
    private Set<String> manClients() {
        return config.manClients().stream()
                .map(MrgScheduler::normalize)
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String normalize(String token) {
        return token.strip().toUpperCase(Locale.ROOT);
    }
}
