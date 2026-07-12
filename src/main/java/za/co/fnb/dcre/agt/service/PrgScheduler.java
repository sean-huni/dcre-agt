package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;
import za.co.fnb.dcre.agt.repo.ArrivalRepo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * R-28: launches the PRG executor per (client, window). The window counter
 * derives from the epoch so every AGT incarnation computes the same run key
 * (level-triggered; the clock-intent unique key dedupes). An on-demand
 * trigger file chaos/run-prg-&lt;client&gt; launches an immediate manual window.
 */
@ApplicationScoped
public class PrgScheduler {

    private static final Logger LOG = Logger.getLogger(PrgScheduler.class);

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    ArrivalRepo arrivalRepo;

    @Inject
    JobLauncher launcher;

    @Scheduled(every = "10s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled() || config.prgImage().isEmpty()) {
            return;
        }
        long window = window(Instant.now().getEpochSecond(), config.prgIntervalSeconds());
        for (String client : arrivalRepo.distinctClientTokens()) {
            launcher.launchClock(Stage.PRG, client + "-w" + window, List.of(
                    "client=" + client,
                    "window=w" + window));
            considerManualTrigger(client, window);
        }
    }

    /** Same window arithmetic as CrwScheduler: identical keys across incarnations. */
    public static long window(long epochSeconds, long intervalSeconds) {
        return epochSeconds / intervalSeconds;
    }

    private void considerManualTrigger(String client, long window) {
        Path trigger = Path.of(config.exchangeRoot(), "chaos", "run-prg-" + client);
        try {
            if (!Files.deleteIfExists(trigger)) {
                return;
            }
        } catch (IOException e) {
            LOG.warnf("manual PRG trigger for %s failed: %s", client, e.getMessage());
            return;
        }
        // Idempotent within the window: repeated triggers reuse one run key.
        // The window param carries a -manual suffix so the Batch job instance is
        // distinct from the scheduled run of the same window (identifying params
        // are the instance identity; resend alone is non-identifying).
        launcher.launchClock(Stage.PRG, client + "-manual-" + window, List.of(
                "client=" + client,
                "window=w" + window + "-manual",
                "resend=true,java.lang.String,false"));
    }
}
