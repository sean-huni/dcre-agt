package za.co.fnb.dcre.agt.service;

import io.quarkus.scheduler.Scheduled;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import za.co.fnb.dcre.agt.config.AgtConfig;

/**
 * M10/SCRUM-78 (A-71): the GLOBAL MSR failed-collection suspension CLOCK sweep.
 * Once per window it launches ONE MSR service pod selecting msrSuspendJob (global
 * {@code WHERE state='ACCP'} with >= dcre.msr.suspend-after consecutive failed
 * collections roll to SUSPENDED/MS03). The global window mechanics live in
 * MsrSweep (shared with the expiry sweep); this bean owns only the clock tick and
 * the suspend job selection.
 */
@ApplicationScoped
public class MsrSuspendScheduler {

    /** MSR's suspension-sweep Batch job, selected via the DCRE_MSR_JOB_NAME env. */
    public static final String MSR_SUSPEND_JOB = "msrSuspendJob";

    private final MsrSweep sweep;
    private final AgtConfig config;

    @Inject
    MsrSuspendScheduler(final MsrSweep sweep, final AgtConfig config) {
        this.sweep = sweep;
        this.config = config;
    }

    @RunOnVirtualThread
    @Scheduled(every = "10s", concurrentExecution = Scheduled.ConcurrentExecution.SKIP)
    void tick() {
        sweep.run("suspend", MSR_SUSPEND_JOB, config.msrSuspendIntervalSeconds());
    }
}
