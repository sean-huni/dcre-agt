package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

/**
 * R-37: launches the CRW Process-Date Executor per (date, window). The window
 * counter derives from the epoch so every AGT incarnation computes the same
 * run key (level-triggered; the clock-intent unique key dedupes).
 */
@ApplicationScoped
public class CrwScheduler {

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @Scheduled(every = "10s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled() || config.crwImage().isEmpty()) {
            return;
        }
        long window = Instant.now().getEpochSecond() / config.crwIntervalSeconds();
        LocalDate runDate = LocalDate.now(ZoneOffset.UTC);
        String runKey = runDate + "-w" + window;
        launcher.launchClock(Stage.CRW, runKey, List.of(
                "run.date=" + runDate,
                "window=" + runKey));
    }
}
