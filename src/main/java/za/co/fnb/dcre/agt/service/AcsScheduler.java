package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

import java.time.Instant;
import java.util.List;

/**
 * SCRUM-107: launches the ACS account-registry census per window, so
 * {@code dcre_acs.account} stays current for the four services that read its
 * published views (CTV, MRV, and later PTV and MIT).
 *
 * <p>Deliberately the HcsScheduler shape, because ACS is deliberately the HCS shape:
 * {@code AcsJobConfig} mirrors HCS's job structure exactly, same outcome seam, same
 * heartbeat, same stale-execution sweep, and one identifying {@code window} job
 * parameter. Level-triggered: the window counter derives from the epoch, so every AGT
 * incarnation computes the same run key and the clock-intent unique key dedupes the
 * repeats.
 *
 * <p><b>The cadence is a placeholder and needs a ruling.</b> R-38 gives HCS six hours
 * because Nager.Date publishes a calendar that changes yearly. Nothing equivalent has
 * been decided for the account registry, whose churn rate is a business fact nobody has
 * stated, so this defaults to the same six hours purely so the stage runs at all.
 * Getting it wrong is not silent: too slow and CTV/MRV validate against a stale
 * registry, which surfaces as FAIL_ACCOUNT_NOT_FOUND on accounts that do exist.
 */
@ApplicationScoped
public class AcsScheduler {

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @Inject
    StageImages stageImages;

    /** Absent/empty image = launch-disabled (SCRUM-33: no stub fallback), and no
     *  lease means another incarnation owns the side effects. */
    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled()
                || stageImages.configured(Stage.ACS).isEmpty()) {
            return;
        }
        final long window = ReportWindows.window(Instant.now().getEpochSecond(),
                config.acsIntervalHours() * 3600L);
        // Run key carries NO stage token: clockJobName owns the col-acs- prefix.
        // The census takes one identifying parameter, `window` (acs README).
        launcher.launchClock(Flow.COL, Stage.ACS, "w" + window, List.of("window=w" + window));
    }
}
