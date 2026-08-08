package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;
import za.co.fnb.dcre.agt.domain.Flow;
import za.co.fnb.dcre.agt.domain.Stage;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

/**
 * R-37: launches the CRW Process-Date Executor per (date, window). The window
 * counter derives from the epoch so every AGT incarnation computes the same
 * run key (level-triggered; the clock-intent unique key dedupes).
 *
 * <p><b>COLLECTIONS ONLY, and that is the timing rule.</b> Owner, 2026-08-08: "CRW
 * TxList are processed on the collection-day, but for payments Tx's processed
 * immediately." CRW is the clock that makes a collections instruction wait for its
 * process date. Payments has no analogue and must not acquire one: PRW is a DAG
 * stage on the payments REQ sheet, launched the moment PAI accepts, so there is no
 * payments window job here and there must never be one.
 *
 * <p>Before the v1 split this scheduler carried the comment "ONE window job serves
 * ALL lanes ... payments lanes still run inside it" and a TODO to carve payments
 * out later. The carve-out is this: payments left the window job entirely rather
 * than getting a window job of its own.
 */
@ApplicationScoped
public class CrwScheduler {

    @Inject
    AgtConfig config;

    @Inject
    LeaseService lease;

    @Inject
    JobLauncher launcher;

    @Inject
    StageImages stageImages;

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = io.quarkus.scheduler.Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled()
                || stageImages.configured(Stage.CRW).isEmpty()) {
            return;
        }
        long window = Instant.now().getEpochSecond() / config.crwIntervalSeconds();
        LocalDate runDate = LocalDate.now(ZoneOffset.UTC);
        String runKey = runDate + "-w" + window;
        launcher.launchClock(Flow.COL, Stage.CRW, runKey, List.of(
                "run.date=" + runDate,
                "window=" + runKey));
    }
}
