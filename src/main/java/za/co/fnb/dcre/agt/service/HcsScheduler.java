package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Stage;

import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

/**
 * R-38: launches the HCS holiday-calendar sync per 6h window so the
 * public_holiday table re-syncs from Nager.Date four times a day. The window
 * counter derives from the epoch so every AGT incarnation computes the same
 * run key (level-triggered; the clock-intent unique key dedupes).
 */
@ApplicationScoped
public class HcsScheduler {

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled() || config.hcsImage().isEmpty()) {
            return;
        }
        long window = window(Instant.now().getEpochSecond(), config.hcsIntervalHours());
        // Run key carries NO stage token: clockJobName owns the dcre-hcs- prefix.
        launcher.launchClock(Stage.HCS, "w" + window, List.of(
                "sync.date=" + LocalDate.now(),
                "window=w" + window,
                "countries=ZA,java.lang.String,false"));
    }

    /** Same window arithmetic as CrwScheduler/PrgScheduler, hour-grained:
     *  identical keys across incarnations. */
    public static long window(long epochSeconds, long intervalHours) {
        return epochSeconds / (intervalHours * 3600);
    }
}
