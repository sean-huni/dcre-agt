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
import java.util.Map;

/**
 * SCRUM-91: the GLOBAL mandate-suspension CLOCK sweep, and the only clock writer
 * left on the mandate response leg. It replaces the two MSR sweeps (A-71): the
 * auth window is a predicate of mandate_effective_status now, so expiry needs no
 * job at all, while the suspension signal lives cross-database in dcre_col, which
 * a CockroachDB view cannot span, so that one stays a job and moved into MRG.
 *
 * <p>Once per window it launches ONE MRG pod selecting mrgSuspendJob via
 * DCRE_MRG_JOB_NAME. GLOBAL, not per-client (the sweep scans every mandate), so
 * the run key is client-free and the K8s Job name is deterministic; the window
 * derives from the epoch, so every AGT incarnation computes the same key and the
 * clock-intent unique key dedupes (HcsScheduler / CrwScheduler pattern).
 */
@ApplicationScoped
public class MrgSuspendScheduler {

    /** MRG's suspension-sweep Batch job, selected via the DCRE_MRG_JOB_NAME env. */
    public static final String MRG_SUSPEND_JOB = "mrgSuspendJob";

    private final AgtConfig config;
    private final LeaseService lease;
    private final JobLauncher launcher;

    @Inject
    MrgSuspendScheduler(final AgtConfig config, final LeaseService lease, final JobLauncher launcher) {
        this.config = config;
        this.lease = lease;
        this.launcher = launcher;
    }

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        if (!lease.holdsLease() || !config.launchEnabled() || config.mrgImage().isEmpty()) {
            return; // absent/empty mrg-image = launch-disabled (SCRUM-33: no stub fallback)
        }
        final long window = PrgScheduler.window(Instant.now().getEpochSecond(),
                config.mrgSuspendIntervalSeconds());
        // The window is the Batch job-instance identity: mrgSuspendJob takes no
        // parameter of its own (its override write keys on the full business
        // identity, so it needs no sweep token), and without an identifying
        // parameter the second window would restart the first COMPLETED instance
        // instead of starting a new one.
        launcher.launchClock(Flow.MAN, Stage.MRG, "suspend-w" + window,
                List.of("window=w" + window),
                Map.of(JobLauncher.MRG_JOB_ENV, MRG_SUSPEND_JOB));
    }
}
